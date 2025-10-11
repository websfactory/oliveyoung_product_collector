import os
import time
import random
from datetime import datetime
from curl_cffi import requests
from requests.cookies import create_cookie

from config.settings import (
    OLIVEYOUNG_BASE_URL, 
    OLIVEYOUNG_CATEGORY_URL, 
    OLIVEYOUNG_INGREDIENT_URL,
    BATCH_SIZE,
    USER_AGENT,
    REQUEST_TIMEOUT
)
from utils.logger import setup_logger
from utils.html_parser import OliveYoungParser
from utils.webshare_proxy import get_webshare_proxy_manager
from api.ingredient_api import IngredientAPI
from api.product_api import ProductAPI
from models.database import CosmeticsCategory
from config.session import CosmeticsSession
from sqlalchemy.future import select

logger = setup_logger(__name__, "oliveyoung_collector_curl.log")

class OliveYoungCollectorCurl:
    """
    올리브영 웹사이트 전용 제품 정보 수집기 - curl-cffi 버전
    TLS Fingerprinting 우회를 위해 Safari 브라우저로 위장
    """
    
    def __init__(self, ingredient_api=None, product_api=None, use_proxy=False):
        """
        OliveYoungCollectorCurl 생성자
        
        Args:
            ingredient_api (IngredientAPI, optional): 성분 분석 API 클라이언트. 기본값은 None(새로 생성).
            product_api (ProductAPI, optional): 제품 정보 저장 API 클라이언트. 기본값은 None(새로 생성).
            use_proxy (bool, optional): Webshare 프록시 사용 여부. 기본값은 False.
        """
        self.ingredient_api = ingredient_api or IngredientAPI()
        self.product_api = product_api or ProductAPI()
        self.products = []
        self.use_proxy = use_proxy
        self.proxy_manager = None
        self.current_proxy = None
        
        # 프록시 사용 시 초기화
        if self.use_proxy:
            try:
                self.proxy_manager = get_webshare_proxy_manager()
                logger.info("Webshare 프록시 매니저 초기화 완료")
            except Exception as e:
                logger.error(f"프록시 매니저 초기화 실패: {e}")
                logger.warning("프록시 없이 계속 진행합니다")
                self.use_proxy = False
        
        # Safari로 위장 (테스트 결과 성공한 브라우저)
        self.impersonate = "safari15_5"
        
        # 헤더 설정 - 더 자연스러운 Safari 헤더
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        
        # AJAX 요청용 헤더
        self.headers_ajax = self.headers.copy()
        self.headers_ajax.update({
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        })
        
        # curl-cffi 세션 생성
        try:
            self.session = requests.Session(impersonate=self.impersonate)
            self.session.headers.update(self.headers)
            logger.info(f"curl-cffi 세션 생성 완료 (Safari로 위장)")
            
            # 쿠키 초기화
            self._init_cookies()
            
        except Exception as e:
            logger.error(f"curl-cffi 세션 생성 실패: {str(e)}")
            raise
        
        logger.info("OliveYoungCollectorCurl 초기화 완료")
    
    def _init_cookies(self):
        """
        올리브영 웹사이트 접속에 필요한 초기 쿠키 설정
        """
        try:
            # AWS_WAF_TOKEN 환경변수에서 가져오기
            token = os.getenv("AWS_WAF_TOKEN")
            if token:
                # 도메인·경로 지정된 쿠키 생성
                self.session.cookies.set_cookie(
                    create_cookie(
                        name="aws-waf-token",
                        value=token,
                        domain=".oliveyoung.co.kr",
                        path="/"
                    )
                )
                logger.info("aws-waf-token 쿠키 설정 완료")
            else:
                logger.warning("aws-waf-token이 설정되지 않았습니다. 계속 진행합니다.")
            
            # 메인 페이지 방문하여 기본 쿠키 설정
            logger.info("초기 쿠키 설정을 위해 메인 페이지 방문")
            main_url = f"{OLIVEYOUNG_BASE_URL}/store/main/main.do"
            
            # Referer를 Google로 설정하여 자연스러운 접속 시뮬레이션
            initial_headers = self.headers.copy()
            initial_headers['Referer'] = 'https://www.google.com/'
            
            response = self._get_with_delay(main_url, headers=initial_headers, timeout=REQUEST_TIMEOUT)
            
            if response.ok:
                logger.info(f"메인 페이지 접속 성공. 쿠키 {len(self.session.cookies)} 개 설정됨")
                for cookie in self.session.cookies:
                    logger.debug(f"쿠키 설정: {cookie.name}={cookie.value}")
            else:
                logger.warning(f"메인 페이지 접속 실패: {response.status_code}")
                
        except Exception as e:
            logger.error(f"쿠키 초기화 중 오류 발생: {str(e)}")
            # 쿠키 초기화 실패해도 계속 진행
            logger.warning("쿠키 초기화 실패, 쿠키 없이 진행")
    
    def _get_with_delay(self, url, headers=None, **kwargs):
        """
        요청 전 랜덤 지연을 추가하고 응답 코드를 확인하는 session.get 래퍼 함수
        
        Args:
            url (str): 요청할 URL
            headers (dict): 요청 헤더 (None이면 기본 헤더 사용)
            **kwargs: requests.Session.get에 전달할 추가 인자
            
        Returns:
            requests.Response: 응답 객체
            
        Raises:
            RuntimeError: Cloudflare 챌린지 실패 또는 서버 오류가 발생한 경우
        """
        # 요청 전 랜덤 딜레이 추가 (2~3초로 증가)
        delay = random.uniform(1.0, 3.0)
        logger.info(f"요청 전 {delay:.2f}초 대기: {url}")
        time.sleep(delay)
        
        # 헤더 설정
        if headers is None:
            headers = self.headers
        
        # 프록시 설정
        if self.use_proxy and self.proxy_manager:
            # 새 프록시 가져오기 (현재 프록시가 없거나 실패한 경우)
            if not self.current_proxy:
                self.current_proxy = self.proxy_manager.get_proxy_dict()
                if self.current_proxy:
                    logger.info("새 프록시 사용")
                else:
                    logger.warning("사용 가능한 프록시가 없습니다. 직접 연결 사용")
            
            kwargs['proxies'] = self.current_proxy
        
        # 요청 수행
        response = self.session.get(url, headers=headers, **kwargs)
        
        # Cloudflare 또는 서버 오류 확인
        if response.status_code in [403, 503, 405]:
            error_msg = f"Cloudflare 챌린지 실패 또는 서버 오류 (HTTP {response.status_code}): {url}"
            logger.error(error_msg)
            
            # 프록시 사용 중이면 실패한 프록시로 표시하고 새 프록시 시도
            if self.use_proxy and self.current_proxy and self.proxy_manager:
                logger.warning("현재 프록시 실패로 표시")
                # 프록시를 실패로 표시하고 초기화
                self.current_proxy = None
            
            raise RuntimeError(error_msg)
        elif response.status_code == 429:
            logger.error(f"Rate limiting 발생: {url}")
            raise RuntimeError("너무 많은 요청으로 인한 일시적 차단. 잠시 후 재시도하세요.")
            
        return response
    
    def _post_with_delay(self, url, data=None, headers=None, **kwargs):
        """
        POST 요청 전 랜덤 지연을 추가하고 응답 코드를 확인하는 session.post 래퍼 함수
        
        Args:
            url (str): 요청할 URL
            data (dict): POST 데이터
            headers (dict): 요청 헤더 (None이면 기본 헤더 사용)
            **kwargs: requests.Session.post에 전달할 추가 인자
            
        Returns:
            requests.Response: 응답 객체
        """
        # 요청 전 랜덤 딜레이 추가
        delay = random.uniform(1.0, 3.0)
        logger.info(f"POST 요청 전 {delay:.2f}초 대기: {url}")
        time.sleep(delay)
        
        # 헤더 설정
        if headers is None:
            headers = self.headers
        
        # 프록시 설정 (GET과 동일)
        if self.use_proxy and self.proxy_manager:
            if not self.current_proxy:
                self.current_proxy = self.proxy_manager.get_proxy_dict()
                if self.current_proxy:
                    logger.info("새 프록시 사용 (POST)")
                else:
                    logger.warning("사용 가능한 프록시가 없습니다. 직접 연결 사용 (POST)")
            
            kwargs['proxies'] = self.current_proxy
        
        # POST 요청 수행
        response = self.session.post(url, data=data, headers=headers, **kwargs)
        
        # 오류 확인
        if response.status_code in [403, 503, 405]:
            error_msg = f"서버 오류 (HTTP {response.status_code}): {url}"
            logger.error(error_msg)
            
            # 프록시 사용 중이면 실패한 프록시로 표시
            if self.use_proxy and self.current_proxy and self.proxy_manager:
                logger.warning("현재 프록시 실패로 표시 (POST)")
                self.current_proxy = None
            
            raise RuntimeError(error_msg)
        elif response.status_code == 429:
            logger.error(f"Rate limiting 발생: {url}")
            raise RuntimeError("너무 많은 요청으로 인한 일시적 차단.")
            
        return response
    
    def _retry_request(self, func, max_retry=3, base_delay=2):
        """
        요청 함수를 재시도하는 백오프 래퍼 함수
        
        Args:
            func (callable): 실행할 요청 함수 (lambda 등으로 전달)
            max_retry (int): 최대 재시도 횟수 (기본값: 3)
            base_delay (int): 기본 지연 시간 (초, 기본값: 2)
            
        Returns:
            요청 함수의 반환값
            
        Raises:
            Exception: 모든 재시도가 실패한 경우 마지막 예외를 다시 발생시킵니다.
        """
        for attempt in range(1, max_retry+1):
            try:
                return func()
            except (RuntimeError, Exception) as e:
                if attempt == max_retry:
                    logger.error(f"재시도 {attempt}회 실패: {e}")
                    raise
                
                # Rate limiting의 경우 더 긴 대기 시간 적용
                if "Rate limiting" in str(e) or "429" in str(e):
                    backoff = base_delay * (5 ** attempt) + random.uniform(5, 10)  # 더 긴 대기
                    logger.warning(f"[Rate Limit Retry {attempt}/{max_retry}] {backoff:.1f}s 후 재시도: {e}")
                else:
                    backoff = base_delay * (2 ** (attempt-1)) + random.uniform(2, 5)
                    logger.warning(f"[Retry {attempt}/{max_retry}] {backoff:.1f}s 후 재시도: {e}")
                
                time.sleep(backoff)
    
    def collect_goods_numbers(self, category_id, sort_type=None):
        """
        카테고리 페이지에서 상품 번호(goodsNo)만 수집
        
        Args:
            category_id (str): 카테고리 ID
            sort_type (str, optional): 정렬 방식 
                                      None: 인기도 순(기본값)
                                      '03': 판매량 순
            
        Returns:
            list: 수집된 상품 번호(goodsNo) 목록
        """
        logger.info(f"카테고리 {category_id}에서 상품 번호 수집 시작 (정렬: {sort_type or '인기도 순'})")
        
        try:
            # 카테고리 URL 구성 (한 페이지당 48개 제품 표시)
            sort_param = f"&prdSort={sort_type}" if sort_type else ""
            category_url = f"{OLIVEYOUNG_CATEGORY_URL}{category_id}&rowsPerPage=48{sort_param}"
            logger.debug(f"카테고리 URL: {category_url}")
            
            # Referer 헤더 추가
            headers = self.headers.copy()
            headers['Referer'] = f"{OLIVEYOUNG_BASE_URL}/store/main/main.do"
            
            # 첫 페이지 요청 (재시도 로직 추가)
            response = self._retry_request(
                lambda: self._get_with_delay(category_url, headers=headers, timeout=REQUEST_TIMEOUT)
            )
            if not response.ok:
                logger.error(f"카테고리 페이지 접근 실패: {response.status_code}")
                return []
            
            # 전체 페이지 수 확인
            total_pages = OliveYoungParser.get_total_pages(response.text)
            logger.info(f"전체 페이지 수: {total_pages}")
            
            # 수집된 상품 번호 목록
            goods_numbers = []
            
            # 모든 페이지 처리
            for page in range(1, total_pages + 1):
                logger.info(f"페이지 {page}/{total_pages} 처리 중...")
                
                # 페이지 URL 구성 (첫 페이지는 이미 요청함)
                if page > 1:
                    page_url = f"{category_url}&pageIdx={page}"
                    # Referer를 이전 페이지로 설정
                    headers['Referer'] = category_url if page == 2 else f"{category_url}&pageIdx={page-1}"
                    
                    response = self._retry_request(
                        lambda: self._get_with_delay(page_url, headers=headers, timeout=REQUEST_TIMEOUT)
                    )
                    if not response.ok:
                        logger.error(f"페이지 {page} 접근 실패: {response.status_code}")
                        continue
                
                # OliveYoungParser를 사용하여 상품 목록 파싱
                goods_no_list = OliveYoungParser.parse_product_list(response.text)
                
                # 파싱된 결과에서 goods_no 추출
                page_goods_numbers = []
                for goods_no in goods_no_list:
                    if goods_no:
                        page_goods_numbers.append(goods_no)
                
                logger.info(f"페이지 {page}에서 {len(page_goods_numbers)}개 상품 번호 추출")
                goods_numbers.extend(page_goods_numbers)
                
                # 과도한 요청 방지를 위한 지연 
                time.sleep(random.uniform(1.0, 3.0))
            
            return goods_numbers
            
        except Exception as e:
            logger.error(f"상품 번호 수집 중 오류 발생: {str(e)}")
            return []
    
    def collect_product_detail(self, goods_no):
        """
        상품 상세 페이지에서 제품 정보 수집
        
        Args:
            goods_no (str): 상품 번호
            
        Returns:
            dict/str: 수집된 제품 정보 또는 'deleted' (제품이 삭제된 경우)
        """
        logger.info(f"상품 {goods_no} 상세 정보 수집 시작")
        
        try:
            # 상세 페이지 URL 구성
            detail_url = f"{OLIVEYOUNG_BASE_URL}/goods/getGoodsDetail.do?goodsNo={goods_no}"
            logger.info(f"상세 페이지 URL: {detail_url}")
            
            # Referer 헤더 추가
            headers = self.headers.copy()
            headers['Referer'] = f"{OLIVEYOUNG_BASE_URL}/store/main/main.do"
            
            # 상세 페이지 요청 (재시도 로직 추가)
            response = self._retry_request(
                lambda: self._get_with_delay(detail_url, headers=headers, timeout=REQUEST_TIMEOUT)
            )
            if not response.ok:
                logger.error(f"상품 {goods_no} 상세 페이지 접근 실패: {response.status_code}")
                return None
                
            html = response.text
            
            # 삭제된 제품 확인
            if 'error-page noProduct' in html or '상품을 찾을 수 없습니다' in html:
                logger.warning(f"제품 {goods_no}은(는) 삭제되었거나 더 이상 존재하지 않습니다.")
                return 'deleted'
            
            # 응답이 너무 짧으면 404 페이지 또는 리다이렉션일 수 있음
            if len(html) < 1000:
                logger.error(f"상품 {goods_no} 상세 페이지 응답이 너무 짧습니다: {len(html)} 바이트")
                logger.debug(f"응답 미리보기: {html[:200]}...")
                return None
            
            # 기본 정보 초기화
            product_info = {
                'site': 'oliveyoung',
                'collected_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'goods_no': goods_no,
            }
            
            # 메타 태그에서 정보 추출
            meta_info = OliveYoungParser.parse_meta_info(html)
            product_info.update(meta_info)

            # 상세 페이지에서 평점 텍스트, 평점 퍼센트, 리뷰수
            detail_info = OliveYoungParser.parse_product_info(html)
            product_info.update(detail_info)

            product_info['product_url'] = detail_url
            
            # item_no가 없으면 기본값 '001' 설정
            if not product_info.get('item_no'):
                product_info['item_no'] = '001'
                
            # 필수 필드 검증
            if not self._validate_required_fields(product_info, goods_no):
                logger.error(f"상품 {goods_no} 필수 필드 누락으로 건너뜀")
                return None
            
            logger.info(f"상품 {goods_no} 상세 정보 수집 완료")
            return product_info
            
        except Exception as e:
            logger.error(f"상품 {goods_no} 상세 정보 수집 중 오류 발생: {str(e)}")
            return None
    
    def _validate_required_fields(self, product_info, goods_no):
        """
        제품 정보의 필수 필드 검증 및 보완
        
        Args:
            product_info (dict): 검증할 제품 정보
            goods_no (str): 상품 번호 (로깅용)
            
        Returns:
            bool: 유효성 검증 결과 (True: 유효, False: 무효)
        """
        # 필수 필드 목록
        required_fields = ['brand', 'name', 'price']
        missing_fields = []
        
        # 필수 필드 검증 및 보완
        for field in required_fields:
            if not product_info.get(field):
                missing_fields.append(field)
                
                # 기본값 설정
                if field == 'brand' and not product_info.get('brand'):
                    product_info['brand'] = '올리브영'
                    missing_fields.remove(field)
                elif field == 'name' and not product_info.get('name'):
                    product_info['name'] = f'올리브영 상품 {goods_no}'
                    missing_fields.remove(field)
                elif field == 'price' and not product_info.get('price'):
                    product_info['price'] = {'original': None, 'current': None}
        
        # 누락된 필드가 있는 경우
        if missing_fields:
            logger.error(f"제품({goods_no}) 필수 필드 누락: {missing_fields}")
            return False
        
        # price 필드가 있지만 내부 값이 모두 None인 경우 검증
        if 'price' in product_info and isinstance(product_info['price'], dict):
            if not (product_info['price'].get('original') or product_info['price'].get('current')):
                logger.error(f"제품({goods_no}) 가격 정보가 없습니다")
                return False
        
        return True
    
    def enrich_product_with_ingredients(self, product, item_no='001'):
        """
        제품의 성분 정보 및 제조업자 정보를 가져와서 추가

        Args:
            product (dict): 성분 정보를 추가할 제품 객체
            item_no (str, optional): 아이템 번호. 기본값은 '001'.
        """
        try:
            goods_no = product.get('goods_no')

            if not goods_no:
                logger.warning("상품 번호가 없어 성분 정보를 수집할 수 없습니다")
                return

            logger.debug(f"제품 성분 정보 분석 시작: {goods_no}")

            # 성분 및 제조업자 정보 수집 (동기식 메서드 호출)
            result = self.fetch_ingredients(goods_no, item_no)

            if not result:
                logger.warning(f"제품 {goods_no}의 성분 정보를 찾을 수 없습니다")
                return

            ingredients = result.get('ingredients')
            manufacturer_info = result.get('manufacturer_info')

            # 제조업자 정보를 제품에 추가
            if manufacturer_info:
                product['manufacturer_info'] = manufacturer_info

            # 성분이 있는 경우에만 분석 API 호출
            if ingredients:
                # 성분 정보 분석 API 호출 (동기식 메서드 사용)
                analysis_result = self.ingredient_api.fetch_ingredients_info(ingredients, goods_no)

                # 분석 결과 처리
                if analysis_result.get('status') == 'success' and analysis_result.get('data'):
                    logger.info(f"제품 {goods_no} 성분 분석 성공")
                    product['analysis'] = analysis_result.get('data')
                else:
                    logger.warning(f"제품 {goods_no} 성분 분석 실패: {analysis_result.get('message')}")
                    product['analysis'] = {'error': analysis_result.get('message')}

        except Exception as e:
            logger.error(f"성분 정보 처리 중 오류: {str(e)}")
            product['analysis'] = {'error': str(e)}
    
    def fetch_ingredients(self, goods_no, item_no='001'):
        """
        제품의 성분 정보 및 제조업자 정보 수집 - POST 메서드 사용

        Args:
            goods_no (str): 상품 번호
            item_no (str, optional): 아이템 번호. 기본값은 '001'.

        Returns:
            dict: {'ingredients': str, 'manufacturer_info': str} 또는 None
        """
        try:
            logger.debug(f"성분 및 제조업자 정보 수집: {goods_no}, {item_no}")

            # 폼 데이터 구성
            form_data = {
                "goodsNo": goods_no,
                "itemNo": item_no,
                "pkgGoodsYn": "N"
            }

            # AJAX 헤더 + Content-Type 설정
            headers = self.headers_ajax.copy()
            headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
            headers['Referer'] = f"{OLIVEYOUNG_BASE_URL}/goods/getGoodsDetail.do?goodsNo={goods_no}"

            # POST 요청 (재시도 로직 추가)
            response = self._retry_request(
                lambda: self._post_with_delay(
                    OLIVEYOUNG_INGREDIENT_URL,
                    data=form_data,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT
                )
            )

            if response.status_code != 200:
                logger.error(f"성분 정보 API 호출 실패: {response.status_code}")
                return None

            # 성분 정보 추출
            ingredients = OliveYoungParser.parse_ingredients(response.text)

            # 제조업자 정보 추출 (같은 response에서)
            manufacturer_info = OliveYoungParser.parse_manufacturer_info(response.text)

            if ingredients:
                logger.debug(f"성분 정보 추출 성공 ({len(ingredients)} 글자)")
            else:
                logger.warning(f"성분 정보를 찾을 수 없습니다: {goods_no}")

            if manufacturer_info:
                logger.info(f"제조업자 정보 수집 완료: {manufacturer_info[:50]}{'...' if len(manufacturer_info) > 50 else ''}")
            else:
                logger.debug(f"제품 {goods_no}에 제조업자 정보가 없습니다")

            return {
                'ingredients': ingredients,
                'manufacturer_info': manufacturer_info
            }

        except Exception as e:
            logger.error(f"성분 정보 수집 중 오류: {str(e)}")
            return None
    
    def process_ingredients_batch(self, products):
        """
        제품 배치에 대한 성분 정보 처리 (순차 처리)
        
        Args:
            products (list): 성분 정보를 수집할 제품 목록
        """
        logger.info(f"{len(products)}개 제품의 성분 정보 처리 시작")
        
        # 순차 처리
        for product in products:
            if product.get('goods_no'):
                item_no = product.get('item_no', '001')
                self.enrich_product_with_ingredients(product, item_no)
                
                # 요청 간 지연 추가
                time.sleep(random.uniform(1, 3))
        
        logger.info(f"{len(products)}개 제품의 성분 정보 처리 완료")
    
    def collect_from_category(self, category_id, category_name=None):
        """
        카테고리 페이지에서 제품 정보 수집 (순차 처리)
        
        Args:
            category_id (str): 카테고리 ID
            category_name (str, optional): 카테고리 이름. 기본값은 None.
            
        Returns:
            dict: 수집 결과
        """
        logger.info(f"카테고리 수집 시작: {category_name or category_id}")
        
        try:
            # 1. 인기도 순으로 상품 번호(goodsNo) 수집
            popularity_goods_numbers = self.collect_goods_numbers(category_id)
            
            if not popularity_goods_numbers:
                logger.warning(f"카테고리 {category_id}에서 상품을 찾을 수 없습니다")
                return {
                    'success': False,
                    'total_products': 0,
                    'collected_products': 0,
                    'saved_products': 0,
                    'error': "상품을 찾을 수 없습니다"
                }
            
            # 2. 판매량 순으로 상품 번호(goodsNo) 수집
            logger.info(f"판매량 순 데이터 수집 시작")
            sales_goods_numbers = self.collect_goods_numbers(category_id, sort_type="03")
            
            # 3. 판매량 순위 맵핑 생성
            sales_rank_map = {goods_no: rank for rank, goods_no in enumerate(sales_goods_numbers, 1)}
            logger.info(f"판매량 순위 맵핑 생성 완료 ({len(sales_rank_map)}개 상품)")
            
            # 4. 수집된 상품 수
            total_products = len(popularity_goods_numbers)
            logger.info(f"총 {total_products}개 상품 처리 예정")
            
            # 5. 수집 및 저장 카운터
            collected_products = 0
            saved_products = 0
            
            # 6. 각 상품 번호에 대해 순차적으로 처리
            products_batch = []
            
            for i, goods_no in enumerate(popularity_goods_numbers, 1):
                logger.info(f"상품 {i}/{total_products} 처리 중: {goods_no}")
                
                # 상세 정보 수집
                product = self.collect_product_detail(goods_no)
                
                if product:
                    if product == 'deleted':
                        logger.info(f"상품 {goods_no} 는 삭제되어 건너뜁니다.")
                        continue
                    
                    # disp_cat_no 값을 category_id로 덮어쓰기
                    product['disp_cat_no'] = category_id
                    
                    # 인기도 순위 추가
                    product['popularity_rank'] = i
                    
                    # 판매량 순위 추가
                    product['sales_rank'] = sales_rank_map.get(goods_no, None)
                    
                    # 성분 정보 수집 및 분석 (즉시 처리)
                    item_no = product.get('item_no', '001')
                    self.enrich_product_with_ingredients(product, item_no)
                    
                    # 배치에 추가
                    products_batch.append(product)
                    collected_products += 1
                    
                    # 배치 크기에 도달하면 저장
                    if len(products_batch) >= BATCH_SIZE:
                        # 제품 정보 저장
                        result = self.product_api.save_products(products_batch)
                        if result.get('status') == 'success':
                            saved_products += len(products_batch)
                            logger.info(f"{len(products_batch)}개 제품 저장 성공")
                        else:
                            logger.error(f"제품 저장 실패: {result.get('message')}")
                        
                        # 배치 초기화
                        products_batch = []
                
                # 과도한 요청 방지를 위한 지연 
                time.sleep(random.uniform(2, 5))
            
            # 5. 남은 제품 처리
            if products_batch:
                # 제품 정보 저장
                result = self.product_api.save_products(products_batch)
                if result.get('status') == 'success':
                    saved_products += len(products_batch)
                    logger.info(f"{len(products_batch)}개 제품 저장 성공")
                else:
                    logger.error(f"제품 저장 실패: {result.get('message')}")
            
            logger.info(f"카테고리 {category_name or category_id} 수집 완료. "
                       f"총 {total_products}개 중 {collected_products}개 수집, {saved_products}개 저장")
            
            return {
                'success': True,
                'total_products': total_products,
                'collected_products': collected_products,
                'saved_products': saved_products,
                'error': None
            }
            
        except Exception as e:
            logger.error(f"카테고리 수집 중 오류 발생: {str(e)}")
            return {
                'success': False,
                'total_products': 0,
                'collected_products': 0,
                'saved_products': 0,
                'error': str(e)
            }
    
    def close(self):
        """세션 종료"""
        self.session.close()
        
        # 프록시 사용 통계 출력
        if self.use_proxy and self.proxy_manager:
            proxy_info = self.proxy_manager.get_proxy_info()
            logger.info(f"프록시 사용 통계: {proxy_info}")
        
        logger.info("수집기 세션 종료")
import os
import time
import random
import requests
from datetime import datetime
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
from api.ingredient_api import IngredientAPI
from api.product_api import ProductAPI
from models.database import CosmeticsCategory
from config.session import CosmeticsSession
from sqlalchemy.future import select

logger = setup_logger(__name__, "oliveyoung_collector.log")

class OliveYoungCollector:
    """
    올리브영 웹사이트 전용 제품 정보 수집기
    
    기존 크롬 확장프로그램의 OliveYoungCollector 클래스를 파이썬으로 구현
    """
    
    def __init__(self, ingredient_api=None, product_api=None):
        """
        OliveYoungCollector 생성자
        
        Args:
            ingredient_api (IngredientAPI, optional): 성분 분석 API 클라이언트. 기본값은 None(새로 생성).
            product_api (ProductAPI, optional): 제품 정보 저장 API 클라이언트. 기본값은 None(새로 생성).
        """
        self.ingredient_api = ingredient_api or IngredientAPI()
        self.product_api = product_api or ProductAPI()
        self.products = []
        self.headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": "https://www.oliveyoung.co.kr/store/main/main.do",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # 쿠키 초기화
        self._init_cookies()
        
        logger.info("OliveYoungCollector 초기화 완료")
    
    def _init_cookies(self):
        """
        올리브영 웹사이트 접속에 필요한 초기 쿠키 설정
        """
        try:
            # AWS_WAF_TOKEN 환경변수에서 가져오기
            token = os.getenv("AWS_WAF_TOKEN")
            if not token:
                raise RuntimeError("aws-waf-token 설정되지 않았습니다.")
                
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
            
            # 메인 페이지 방문하여 기본 쿠키 설정
            logger.info("초기 쿠키 설정을 위해 메인 페이지 방문")
            # 올리브영 메인 URL (기존 중복 '/store' 제거)
            main_url = f"{OLIVEYOUNG_BASE_URL}/store/main/main.do"
            
            response = self._get_with_delay(main_url, timeout=REQUEST_TIMEOUT)
            
            if response.ok:
                logger.info(f"메인 페이지 접속 성공. 쿠키 {len(self.session.cookies)} 개 설정됨")
                for cookie in self.session.cookies:
                    logger.debug(f"쿠키 설정: {cookie.name}={cookie.value}")
            else:
                logger.warning(f"메인 페이지 접속 실패: {response.status_code}")
        except Exception as e:
            logger.error(f"쿠키 초기화 중 오류 발생: {str(e)}")
            raise
    
    def _get_with_delay(self, url, **kwargs):
        """
        요청 전 랜덤 지연을 추가하고 응답 코드를 확인하는 session.get 래퍼 함수
        
        Args:
            url (str): 요청할 URL
            **kwargs: requests.Session.get에 전달할 추가 인자
            
        Returns:
            requests.Response: 응답 객체
            
        Raises:
            RuntimeError: AWS WAF 캡차가 발생한 경우 (응답 코드 405)
        """
        # 요청 전 랜덤 딜레이 추가 (2~4초)
        delay = random.uniform(2.0, 3.0)
        logger.info(f"요청 전 {delay:.2f}초 대기: {url}")
        time.sleep(delay)
        
        # 요청 수행
        response = self.session.get(url, **kwargs)
        
        # AWS WAF 캡차 확인 (응답 코드 405)
        if response.status_code == 405:
            logger.error(f"AWS WAF 캡차가 발생했습니다. URL: {url}")
            raise RuntimeError("AWS WAF 캡차가 발생했습니다. 토큰을 업데이트하세요.")
            
        return response
    
    def _retry_request(self, func, max_retry=3, base_delay=1):
        """
        요청 함수를 재시도하는 백오프 래퍼 함수
        
        Args:
            func (callable): 실행할 요청 함수 (lambda 등으로 전달)
            max_retry (int): 최대 재시도 횟수 (기본값: 3)
            base_delay (int): 기본 지연 시간 (초, 기본값: 1)
            
        Returns:
            요청 함수의 반환값
            
        Raises:
            Exception: 모든 재시도가 실패한 경우 마지막 예외를 다시 발생시킵니다.
        """
        for attempt in range(1, max_retry+1):
            try:
                return func()
            except (RuntimeError, requests.RequestException) as e:
                if attempt == max_retry:
                    logger.error(f"재시도 {attempt}회 실패: {e}")
                    raise
                backoff = base_delay * (2 ** (attempt-1)) + random.uniform(1, 3)
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
            
            # 첫 페이지 요청 (재시도 로직 추가)
            response = self._retry_request(
                lambda: self._get_with_delay(category_url, timeout=REQUEST_TIMEOUT)
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
                    response = self._retry_request(
                        lambda: self._get_with_delay(page_url, timeout=REQUEST_TIMEOUT)
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
                time.sleep(random.uniform(3.0, 6.0))
            
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
            # 상세 페이지 URL 구성 (올바른 URL 형식으로 수정)
            detail_url = f"{OLIVEYOUNG_BASE_URL}/goods/getGoodsDetail.do?goodsNo={goods_no}"
            logger.info(f"상세 페이지 URL: {detail_url}")
            
            # 상세 페이지 요청 (재시도 로직 추가)
            response = self._retry_request(
                lambda: self._get_with_delay(detail_url, timeout=REQUEST_TIMEOUT)
            )
            if not response.ok:
                logger.error(f"상품 {goods_no} 상세 페이지 접근 실패: {response.status_code}")
                return None
                
            html = response.text
            
            # 삭제된 제품 확인 - 더 빠른 반환을 위해 응답 확인 후 바로 처리
            if 'error-page noProduct' in html or '상품을 찾을 수 없습니다' in html:
                logger.warning(f"제품 {goods_no}은(는) 삭제되었거나 더 이상 존재하지 않습니다.")
                return 'deleted'
            
            # 응답이 너무 짧으면 404 페이지 또는 리다이렉션일 수 있음
            if len(html) < 1000:
                logger.error(f"상품 {goods_no} 상세 페이지 응답이 너무 짧습니다: {len(html)} 바이트")
                # 참고용으로 응답의 일부 출력
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
            # 목록 페이지에서는 설정되었지만 상세 페이지에서 누락된 경우 대비
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
                    # price는 이미 딕셔너리로 초기화되어 있어야 함
                    product_info['price'] = {'original': None, 'current': None}
                    # 여전히 필드가 누락되었기에 missing_fields에서 제거하지 않음
        
        # 누락된 필드가 있는 경우
        if missing_fields:
            logger.error(f"제품({goods_no}) 필수 필드 누락: {missing_fields}")
            return False
        
        # price 필드가 있지만 내부 값이 모두 None인 경우 검증
        if 'price' in product_info and isinstance(product_info['price'], dict):
            if not (product_info['price'].get('original') or product_info['price'].get('current')):
                logger.error(f"제품({goods_no}) 가격 정보가 없습니다")
                return False
        
        # 필수 필드가 모두 있으면 true 반환
        return True
    
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
                time.sleep(random.uniform(3, 5))
        
        logger.info(f"{len(products)}개 제품의 성분 정보 처리 완료")
    
    def enrich_product_with_ingredients(self, product, item_no='001'):
        """
        제품의 성분 정보를 가져와서 분석 결과를 추가
        
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
            
            # 성분 정보 수집 (동기식 메서드 호출)
            ingredients = self.fetch_ingredients(goods_no, item_no)
            
            if not ingredients:
                logger.warning(f"제품 {goods_no}의 성분 정보를 찾을 수 없습니다")
                return
            
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
        제품의 성분 정보 수집
        
        Args:
            goods_no (str): 상품 번호
            item_no (str, optional): 아이템 번호. 기본값은 '001'.
            
        Returns:
            str: 성분 정보 문자열 또는 None
        """
        try:
            logger.debug(f"성분 정보 수집: {goods_no}, {item_no}")
            
            # 폼 데이터 구성
            form_data = {
                "goodsNo": goods_no,
                "itemNo": item_no,
                "pkgGoodsYn": "N"
            }
            
            # API 요청 (재시도 로직 추가)
            response = self._retry_request(
                lambda: self.session.post(
                    OLIVEYOUNG_INGREDIENT_URL,
                    data=form_data,
                    headers=self.headers,
                    timeout=REQUEST_TIMEOUT
                )
            )
            
            if response.status_code != 200:
                logger.error(f"성분 정보 API 호출 실패: {response.status_code}")
                return None
            
            # 성분 정보 추출
            ingredients = OliveYoungParser.parse_ingredients(response.text)
            
            if ingredients:
                logger.debug(f"성분 정보 추출 성공 ({len(ingredients)} 글자)")
                return ingredients
            else:
                logger.warning(f"성분 정보를 찾을 수 없습니다: {goods_no}")
                return None
                    
        except Exception as e:
            logger.error(f"성분 정보 수집 중 오류: {str(e)}")
            return None
    
    def collect_from_category(self, category_id, category_name=None):
        """
        카테고리 페이지에서 제품 정보 수집 (순차 처리)
        
        Args:
            category_id (str): 카테고리 ID
            category_name (str, optional): 카테고리 이름. 기본값은 None.
            
        Returns:
            dict: 수집 결과 {
                'success': bool,
                'total_products': int,
                'collected_products': int,
                'saved_products': int,
                'error': str
            }
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
                    
                    # disp_cat_no 값을 category_id로 덮어쓰기 (웹사이트에서 가져온 값 대신 DB카테고리 ID 사용)
                    product['disp_cat_no'] = category_id
                    
                    # 인기도 순위 추가 (수집된 순서가 인기도 순위)
                    product['popularity_rank'] = i
                    
                    # 판매량 순위 추가 (판매량 순위 맵에서 검색)
                    product['sales_rank'] = sales_rank_map.get(goods_no, None)
                    
                    # 성분 정보 수집 및 분석 (즉시 처리)
                    item_no = product.get('item_no', '001')
                    self.enrich_product_with_ingredients(product, item_no)
                    
                    # 배치에 추가
                    products_batch.append(product)
                    collected_products += 1
                    
                    # 배치 크기에 도달하면 저장
                    if len(products_batch) >= BATCH_SIZE:
                        # 제품 정보 저장 (동기식 메서드 사용)
                        result = self.product_api.save_products(products_batch)
                        if result.get('status') == 'success':
                            saved_products += len(products_batch)
                            logger.info(f"{len(products_batch)}개 제품 저장 성공")
                        else:
                            logger.error(f"제품 저장 실패: {result.get('message')}")
                        
                        # 배치 초기화
                        products_batch = []
                
                # 과도한 요청 방지를 위한 지연
                time.sleep(random.uniform(3, 5))
            
            # 5. 남은 제품 처리
            if products_batch:
                # 제품 정보 저장 (동기식 메서드 사용)
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
    
    def collect_all_categories(self, category_ids=None):
        """
        여러 카테고리의 제품 정보 수집 (순차 처리)
        
        Args:
            category_ids (list, optional): 수집할 카테고리 ID 목록. 기본값은 None(DB에서 조회).
            
        Returns:
            dict: 수집 결과
        """
        try:
            # 카테고리 목록이 없으면 DB에서 조회
            if not category_ids:
                logger.info("DB에서 카테고리 목록 조회")
                
                # DB 세션 생성
                session = CosmeticsSession()
                
                try:
                    # 카테고리 조회
                    result = session.execute(
                        select(CosmeticsCategory.category_id, CosmeticsCategory.category_name)
                    ).fetchall()
                    
                    # 카테고리 ID 및 이름 추출
                    categories = [{'id': row[0], 'name': row[1]} for row in result]
                    logger.info(f"DB에서 {len(categories)}개 카테고리 조회 완료")
                    
                finally:
                    session.close()
            else:
                # 직접 지정된 카테고리 목록 사용
                categories = [{'id': cat_id, 'name': None} for cat_id in category_ids]
                logger.info(f"{len(categories)}개 카테고리 처리 예정")
            
            # 전체 결과 초기화
            total_result = {
                'total_categories': len(categories),
                'processed_categories': 0,
                'successful_categories': 0,
                'total_products': 0,
                'collected_products': 0,
                'saved_products': 0,
                'failed_categories': []
            }
            
            # 각 카테고리 순차 처리
            for category in categories:
                logger.info(f"카테고리 처리: {category['name'] or category['id']}")
                
                # 카테고리 제품 수집
                category_result = self.collect_from_category(
                    category['id'], 
                    category['name']
                )
                
                # 결과 업데이트
                total_result['processed_categories'] += 1
                
                if category_result.get('success'):
                    total_result['successful_categories'] += 1
                    total_result['total_products'] += category_result.get('total_products', 0)
                    total_result['collected_products'] += category_result.get('collected_products', 0)
                    total_result['saved_products'] += category_result.get('saved_products', 0)
                else:
                    total_result['failed_categories'].append({
                        'id': category['id'],
                        'name': category['name'],
                        'error': category_result.get('error')
                    })
                
                # 과부하 방지를 위한 지연 (카테고리 간 더 긴 지연 적용)
                logger.info(f"다음 카테고리 처리를 위해 대기 중...")
                time.sleep(random.uniform(3.0, 6.0))
            
            logger.info(f"전체 카테고리 처리 완료. 성공: {total_result['successful_categories']}/{total_result['total_categories']}")
            
            return total_result
            
        except Exception as e:
            logger.error(f"카테고리 일괄 처리 중 오류 발생: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def collect_rankings(self, category_id, target_goods, sort_type=None):
        """
        카테고리 페이지에서 특정 상품들(target_goods)의 순위만 수집
        
        Args:
            category_id (str): 카테고리 ID
            target_goods (set): 순위를 찾을 상품 번호(goodsNo) 집합
            sort_type (str, optional): 정렬 방식 
                                      None: 인기도 순(기본값)
                                      '03': 판매량 순
            
        Returns:
            dict: {goods_no: rank} 형태의 순위 정보 딕셔너리
                 카테고리가 비어있을 경우 {'category_empty': True} 반환
        """
        logger.info(f"카테고리 {category_id}에서 {len(target_goods)}개 상품의 순위 수집 시작 (정렬: {sort_type or '인기도 순'})")
        
        try:
            # 결과 초기화
            rankings = {}
            found_goods = set()  # 이미 찾은 상품들
            per_page = 48  # 한 페이지당 상품 수
            
            # 카테고리 URL 구성
            sort_param = f"&prdSort={sort_type}" if sort_type else ""
            category_url = f"{OLIVEYOUNG_CATEGORY_URL}{category_id}&rowsPerPage={per_page}{sort_param}"
            logger.debug(f"카테고리 URL: {category_url}")
            
            # 첫 페이지 요청 (재시도 로직 적용)
            response = self._retry_request(
                lambda: self._get_with_delay(category_url, timeout=REQUEST_TIMEOUT)
            )
            if not response.ok:
                logger.error(f"카테고리 페이지 접근 실패: {response.status_code}")
                return {}
            
            # 카테고리 상품 개수 확인
            product_count = OliveYoungParser.check_category_product_count(response.text)
            if product_count == 0:
                logger.warning(f"카테고리 {category_id}에 등록된 상품이 없습니다.")
                # 카테고리 비어있음을 알리는 특수 반환값 사용
                return {'category_empty': True}
            
            # 전체 페이지 수 확인
            total_pages = OliveYoungParser.get_total_pages(response.text)
            logger.info(f"전체 페이지 수: {total_pages}")
            
            # 모든 페이지 처리
            for page in range(1, total_pages + 1):
                logger.info(f"페이지 {page}/{total_pages} 처리 중... (현재까지 찾은 상품: {len(found_goods)}/{len(target_goods)})")
                
                # 페이지 URL 구성 (첫 페이지는 이미 요청함)
                if page > 1:
                    page_url = f"{category_url}&pageIdx={page}"
                    response = self._retry_request(
                        lambda: self._get_with_delay(page_url, timeout=REQUEST_TIMEOUT)
                    )
                    if not response.ok:
                        logger.error(f"페이지 {page} 접근 실패: {response.status_code}")
                        continue
                
                # OliveYoungParser를 사용하여 상품 목록 파싱
                goods_no_list = OliveYoungParser.parse_product_list(response.text)
                
                # 파싱된 결과에서 target_goods에 포함된 상품 확인 및 순위 계산
                for idx, goods_no in enumerate(goods_no_list, 1):
                    if goods_no and goods_no in target_goods and goods_no not in found_goods:
                        # 순위 계산: (현재 페이지 - 1) * 페이지당 상품 수 + 페이지 내 인덱스
                        rank = (page - 1) * per_page + idx
                        rankings[goods_no] = rank
                        found_goods.add(goods_no)
                        logger.info(f"상품 {goods_no}의 순위 발견: {rank} (페이지 {page}, 위치 {idx})")
                
                # 모든 대상 상품을 찾았으면 탐색 중단
                if len(found_goods) >= len(target_goods):
                    logger.info(f"모든 대상 상품({len(target_goods)}개)의 순위를 찾았습니다. 탐색 종료.")
                    break
                
                # 과도한 요청 방지를 위한 지연
                time.sleep(random.uniform(3, 5))
            
            # 찾지 못한 상품 기록
            not_found = target_goods - found_goods
            if not_found:
                logger.warning(f"다음 상품들의 순위를 찾지 못했습니다: {not_found}")
            
            logger.info(f"카테고리 {category_id}에서 총 {len(rankings)}/{len(target_goods)}개 상품의 순위를 찾았습니다.")
            return rankings
            
        except Exception as e:
            logger.error(f"순위 수집 중 오류 발생: {str(e)}")
            return {}
        
    def collect_and_save_single_product(self, goods_no, disp_cat_no, year, week_of_year, rankings=None, brandId=None):
        """
        단일 제품 정보를 수집하고 시계열 데이터 테이블에 저장
        
        Args:
            goods_no (str): 상품 번호
            disp_cat_no (str): 카테고리 번호
            year (int): 저장할 연도
            week_of_year (int): 저장할 주차
            rankings (dict, optional): 제품의 순위 정보 {'popularity_rank': x, 'sales_rank': y}
            brandId (int, optional): 이전에 저장된 브랜드 ID 값
            
        Returns:
            str/bool: 'deleted' - 상품이 삭제됨, True - 성공, False - 실패
        """
        try:
            logger.info(f"단일 제품 수집 시작: {goods_no} (카테고리: {disp_cat_no})")
            
            # 1. 제품 상세 정보 수집
            product_result = self.collect_product_detail(goods_no)
            
            # 삭제된 제품 처리 (collect_product_detail에서 'deleted' 문자열 반환 시)
            if product_result == 'deleted':
                return 'deleted'
            
            # 수집 실패 처리
            if not product_result:
                logger.error(f"제품 {goods_no} 상세 정보 수집 실패")
                return False
            
            # 제품 정보 할당
            product = product_result
            
            # 2. disp_cat_no 설정 (제공된 카테고리 ID 사용)
            product['disp_cat_no'] = disp_cat_no
            
            # 2-1. brandId 설정 (이전 주차의 값이 제공된 경우)
            if brandId is not None:
                product['brandId'] = brandId
                logger.info(f"제품 {goods_no}에 기존 브랜드 ID 적용: {brandId}")
            
            # 3. 인기도 순위와 판매 순위 설정 (rankings 파라미터 활용)
            if rankings:
                product['popularity_rank'] = rankings.get('popularity_rank')
                product['sales_rank'] = rankings.get('sales_rank')
                logger.info(f"제품 {goods_no}에 순위 정보 적용: 인기도={product['popularity_rank']}, 판매량={product['sales_rank']}")
            else:
                # 순위 정보가 없는 경우 None으로 설정
                product['popularity_rank'] = None
                product['sales_rank'] = None
            
            # 4. 성분 정보 수집 및 분석
            item_no = product.get('item_no', '001')
            self.enrich_product_with_ingredients(product, item_no)
            
            # 5. 시계열 데이터 테이블에 저장을 위한 추가 정보 설정
            # 중요: 명시적으로 제공된 year와 week_of_year 사용
            collection_date = datetime.now()  # 수집 시간(현재)
            month = collection_date.month      # 월
            
            products_batch = [product]
            
            # 전처리 및 저장 (저장 전용 API 호출, 시계열 데이터만 저장)
            # year와 week_of_year 명시적 전달 (동기식 메서드 사용)
            saved = self.product_api.save_products(
                products_batch, 
                save_to_history=True,
                target_year=year,
                target_week=week_of_year
            )
            
            if saved.get('status') == 'success':
                logger.info(f"제품 {goods_no} 정보 저장 성공")
                return True
            else:
                logger.error(f"제품 {goods_no} 정보 저장 실패: {saved.get('message')}")
                return False
                
        except Exception as e:
            logger.error(f"제품 {goods_no} 수집 및 저장 중 오류 발생: {str(e)}")
            return False
    
    def close(self):
        """세션 종료"""
        self.session.close()
        logger.info("수집기 세션 종료")

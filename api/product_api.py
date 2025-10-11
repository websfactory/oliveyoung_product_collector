import aiohttp
import requests
import json
from datetime import datetime
from sqlalchemy.exc import OperationalError, TimeoutError  # 재시도할 특정 DB 예외 임포트
from config.settings import API_BASE_URL, API_RETRY_MAX, API_RETRY_DELAY, API_RETRY_MAX_DELAY
from utils.logger import setup_logger
from utils.product_preprocessor import preprocess_product_data
from utils.db_retry import retry_db_operation  # 새로운 DB 재시도 로직 임포트
from config.session import CosmeticsSession
from models.database import CosmeticsProductHistory, CosmBrand
from sqlalchemy import select, tuple_  # tuple_ 명시적 임포트 추가

logger = setup_logger(__name__, "product_api.log")

# 재시도할 예외인지 확인하는 함수
def is_transient_db_error(e):
    """DB 연결/타임아웃 등 일시적인 오류인지 확인"""
    return isinstance(e, (OperationalError, TimeoutError))

class ProductAPI:
    """
    제품 정보 저장 API 클라이언트
    
    기존 크롬 확장프로그램의 ProductAPI 클래스를 파이썬으로 구현
    """
    
    def __init__(self, base_url=None):
        """
        ProductAPI 생성자
        
        Args:
            base_url (str, optional): API 기본 URL. 기본값은 설정 파일의 API_BASE_URL.
        """
        self.base_url = base_url or API_BASE_URL
        logger.info(f"ProductAPI 초기화: 기본 URL = {self.base_url}")
    
    @retry_db_operation(max_retries=3, base_delay=1.0)
    def _get_or_create_brand(self, session, brand_name):
        """
        브랜드 이름을 통해 브랜드 ID를 조회하거나 새로 생성합니다.
        일시적인 DB 오류 발생 시 재시도 로직이 적용됩니다.
        
        Args:
            session: 데이터베이스 세션
            brand_name (str): 브랜드 이름
            
        Returns:
            int: 브랜드 ID 또는 실패 시 None
        """
        if not brand_name:
            logger.warning("브랜드 이름이 없어 브랜드 생성을 건너뜁니다")
            return None
            
        try:
            # 1. 기존 브랜드 조회 시도
            # OperationalError, TimeoutError 등이 여기서 발생하면 @retry가 재시도함
            brand = session.execute(
                select(CosmBrand).filter(CosmBrand.name == brand_name)
            ).scalar_one_or_none()
            
            # 2. 존재하면 ID 반환
            if brand:
                # 성공 시 로그 레벨 조정 (너무 많은 로그 방지)
                logger.debug(f"기존 브랜드 발견: {brand_name} (ID: {brand.id})")
                return brand.id
                
            # 3. 존재하지 않으면 새로 생성 시도
            logger.info(f"새로운 브랜드 생성 시도: {brand_name}")
            new_brand = CosmBrand(
                name=brand_name,
                is_active=1,  # 기본적으로 활성 상태
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            session.add(new_brand)
            # INSERT/FLUSH 중에도 OperationalError 등이 발생하면 @retry가 재시도함
            session.flush()  # 즉시 ID 생성
            
            logger.info(f"브랜드 생성 완료: {brand_name} (ID: {new_brand.id})")
            return new_brand.id
            
        except Exception as e:
            # @retry가 처리하지 않는 다른 예외 발생 시,
            # 또는 재시도 모두 실패 시 여기로 와서 None 반환
            logger.error(f"브랜드 조회/생성 중 최종 오류 ({type(e).__name__}): {str(e)}")
            return None
    
    @retry_db_operation(max_retries=3, base_delay=1.0)
    def _save_to_history_table(self, products, collection_date=None, target_year=None, target_week=None):
        """
        제품 데이터를 CosmeticsProductHistory 테이블에 저장합니다.
        저장 전 중복을 확인하여 이미 존재하는 레코드(동일 goods_no, disp_cat_no, year, week_of_year)는 건너뜁니다.
        
        Args:
            products (list): 저장할 제품 데이터 목록
            collection_date (datetime, optional): 수집 시간. 기본값은 현재 시간.
            target_year (int, optional): 저장할 연도. 기본값은 현재 연도.
            target_week (int, optional): 저장할 주차. 기본값은 현재 주차.
            
        Returns:
            bool: 일부라도 성공적으로 저장되었으면 True, 아니면 False
        """
        session = None  # 예외 처리의 finally 블록에서 참조하기 위해 밖에서 선언
        
        try:
            if not products or not isinstance(products, list):
                logger.warning("저장할 제품이 없거나 유효하지 않은 형식입니다")
                return False
            
            if collection_date is None:
                collection_date = datetime.now()
            
            # target_year와 target_week가 지정되었다면 사용, 그렇지 않으면 현재 날짜 기반으로 계산
            year = target_year if target_year is not None else collection_date.year
            month = collection_date.month
            week_of_year = target_week if target_week is not None else collection_date.isocalendar()[1]  # ISO 주차
            
            logger.debug(f"시계열 데이터 저장 대상 주차: {year}년 {week_of_year}주차 (타겟 지정: {target_year is not None and target_week is not None})")
            
            # 1. 들어오는 배치에서 잠재적 키 추출
            # 배치 내 잠재적 중복을 처리하기 위해 set 사용
            incoming_keys = set()
            valid_products_for_batch = []  # 필요한 키가 있는 제품 저장
            
            for product in products:
                goods_no = product.get('goods_no')
                disp_cat_no = product.get('disp_cat_no')
                
                # goods_no와 disp_cat_no가 모두 있는지 확인
                if goods_no and disp_cat_no:
                    incoming_keys.add((goods_no, disp_cat_no, year, week_of_year))
                    valid_products_for_batch.append(product)
                else:
                    # 필요한 키가 없는 경우 로그 
                    missing_keys = []
                    if not goods_no:
                        missing_keys.append('goods_no')
                    if not disp_cat_no:
                        missing_keys.append('disp_cat_no')
                        
                    logger.warning(f"{', '.join(missing_keys)}가 없는 제품은 건너뜁니다: {product.get('name', 'N/A')}")
            
            if not incoming_keys:
                logger.info("유효한 키를 가진 제품이 없어 저장을 건너뜁니다.")
                return False
            
            # 2. 데이터베이스 세션 생성
            session = CosmeticsSession()
            
            # 3. 이 배치의 타임프레임에서 기존 키 쿼리
            existing_keys = set()
            
            # (goods_no, disp_cat_no) 쌍 집합 생성 (중복 제거)
            query_pairs = set((k[0], k[1]) for k in incoming_keys)
            
            # 이미 존재하는 키 조회
            query = session.query(
                CosmeticsProductHistory.goods_no,
                CosmeticsProductHistory.disp_cat_no,
                CosmeticsProductHistory.year,
                CosmeticsProductHistory.week_of_year
            ).filter(
                CosmeticsProductHistory.year == year,
                CosmeticsProductHistory.week_of_year == week_of_year
            )
            
            # tuple_.in_()을 사용한 효율적인 복합 키 조회
            if query_pairs:
                query = query.filter(
                    tuple_(
                        CosmeticsProductHistory.goods_no, 
                        CosmeticsProductHistory.disp_cat_no
                    ).in_(query_pairs)
                )
            
            # 결과를 가져와 세트에 추가
            for row in query.all():
                existing_keys.add((row.goods_no, row.disp_cat_no, row.year, row.week_of_year))
            
            logger.debug(f"이번 주 ({year}-{week_of_year})에 이미 존재하는 키: {len(existing_keys)}개")
            
            # 4. 제품 필터링: existing_keys에 없는 키만 유지
            records_to_insert = []
            skipped_count = 0
            
            # 브랜드 캐시 (중복 조회 방지)
            brand_id_cache = {}
            # 통계용 카운터
            with_brand_id_count = 0  # 이미 brandId가 있는 항목 수
            from_name_count = 0  # 브랜드 이름으로 조회한 수
            created_count = 0  # 새로 생성한 브랜드 수
            
            for product in valid_products_for_batch:
                goods_no = product.get('goods_no')
                disp_cat_no = product.get('disp_cat_no')
                current_key = (goods_no, disp_cat_no, year, week_of_year)
                
                if current_key not in existing_keys:
                    # 브랜드 처리: 이미 brandId가 있는지 확인하고 없으면 brand 이름으로 조회/생성
                    brand_id = product.get('brandId')  # 이미 brandId가 있으면 그것을 사용
                    
                    if brand_id is None:  # brandId가 없는 경우에만 brand 이름으로 조회
                        brand_name = product.get('brand')
                        if brand_name:
                            if brand_name in brand_id_cache:
                                brand_id = brand_id_cache[brand_name]
                                logger.debug(f"제품 {goods_no}의 브랜드 '{brand_name}'는 캐시에서 가져옴 (ID: {brand_id})")
                                from_name_count += 1
                            else:
                                # 브랜드 조회 또는 생성
                                brand_id = self._get_or_create_brand(session, brand_name)
                                brand_id_cache[brand_name] = brand_id
                                if brand_id:
                                    created_count += 1
                        else:
                            logger.warning(f"제품 {goods_no}에 브랜드 이름이 없어 브랜드 ID를 설정할 수 없습니다.")
                            brand_id = None
                    elif brand_id is not None:
                        with_brand_id_count += 1
                        logger.info(f"제품 {goods_no}에 기존 브랜드 ID 사용: {brand_id}")
                    
                    # 이번 주에 새로운 경우에만 SQLAlchemy 객체 생성
                    history_record = CosmeticsProductHistory(
                        site=product.get('site', 'oliveyoung'),
                        collected_at=collection_date,
                        goods_no=goods_no,
                        disp_cat_no=disp_cat_no,  # 필수 컬럼으로 변경
                        item_no=product.get('item_no'),
                        product_url=product.get('product_url'),
                        brandId=brand_id,  # brand 필드 대신 brandId 사용
                        manufacturer_info=product.get('manufacturer_info'),
                        name=product.get('name'),
                        image_url=product.get('image_url'),
                        price_original=product.get('price', {}).get('original'),
                        price_current=product.get('price', {}).get('current'),
                        rating_percent=product.get('rating', {}).get('percent'),
                        rating_text=product.get('rating', {}).get('text'),
                        review_count=product.get('review_count'),
                        popularity_rank=product.get('popularity_rank', None),
                        sales_rank=product.get('sales_rank', None),
                        year=year,
                        month=month,
                        week_of_year=week_of_year
                    )
                    records_to_insert.append(history_record)
                else:
                    skipped_count += 1
            
            # 5. 새 레코드만으로 일괄 삽입 수행
            if records_to_insert:
                logger.info(f"시계열 데이터 테이블에 {len(records_to_insert)}개 신규 레코드 저장 시작 "
                           f"(중복 {skipped_count}개 건너뜀, 기존ID {with_brand_id_count}개, "
                           f"이름조회 {from_name_count}개, 신규생성 {created_count}개)")
                
                try:
                    session.bulk_save_objects(records_to_insert)
                    session.commit()
                    logger.info(f"시계열 데이터 테이블 저장 완료 ({len(records_to_insert)}개 저장)")
                    return True
                except Exception as e:
                    session.rollback()
                    logger.error(f"시계열 데이터 저장 중 오류 발생 (bulk_save_objects): {type(e).__name__}: {str(e)}")
                    return False
            else:
                logger.info(f"저장할 신규 레코드가 없습니다 (중복 {skipped_count}개 건너뜀).")
                return True  # 모든 레코드가 이미 존재하는 경우도 성공적인 작업으로 간주
                
        except Exception as e:
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"롤백 중 오류 발생: {type(rollback_error).__name__}: {str(rollback_error)}")
            
            logger.error(f"시계열 데이터 처리 중 예외 발생: {type(e).__name__}: {str(e)}")
            return False
            
        finally:
            if session:
                try:
                    session.close()
                except Exception as close_error:
                    logger.error(f"세션 종료 중 오류 발생: {type(close_error).__name__}: {str(close_error)}")
        
    def save_products(self, products, save_to_history=True, target_year=None, target_week=None):
        """
        수집된 제품 데이터를 서버에 일괄 저장 (동기 방식)
        
        Args:
            products (list): 저장할 제품 배열
            
        Returns:
            dict: 저장 결과
            
        Raises:
            Exception: API 요청 실패 시
        """
        try:
            if not products or not isinstance(products, list):
                logger.warning("저장할 제품이 없거나 유효하지 않은 형식입니다")
                return {
                    "status": "error",
                    "message": "저장할 제품이 없거나 유효하지 않은 형식입니다"
                }
            
            endpoint = f"{self.base_url}/products/batch"
            logger.info(f"제품 저장 API 호출 시작: {endpoint}")
            logger.info(f"저장할 제품 수: {len(products)}")
            
            # 데이터 전처리 수행 (숫자형 변환)
            processed_products = preprocess_product_data(products)
            logger.info(f"제품 데이터 전처리 완료")
            
            # API 요청 수행
            response = requests.post(
                endpoint,
                headers={"Content-Type": "application/json"},
                json={"products": processed_products}
            )
            
            # 시계열 데이터 테이블 저장 (옵션에 따라)
            if save_to_history:
                logger.info(f"시계열 데이터 테이블 저장 시작")
                self._save_to_history_table(processed_products, target_year=target_year, target_week=target_week)
            
            # 응답 확인
            if not response.ok:
                error_message = f"제품 저장 API 오류: {response.status_code}"
                try:
                    error_text = response.text
                    logger.error(f"API 오류 응답: {error_text}")
                    error_message = error_text
                except Exception:
                    pass
                
                logger.error(error_message)
                return {
                    "status": "error",
                    "message": error_message
                }
            
            logger.info(f"제품 저장 완료: {len(products)}개")
            
            # 응답 처리
            try:
                response_data = response.json()
                return response_data
            except json.JSONDecodeError:
                # JSON 응답이 아닌 경우
                return {
                    "status": "success",
                    "message": "제품 저장 완료",
                    "saved_count": len(products)
                }
            
        except Exception as e:
            logger.error(f"제품 저장 API 오류: {str(e)}")
            return {
                "status": "error",
                "message": f"제품 저장 API 오류: {str(e)}"
            }
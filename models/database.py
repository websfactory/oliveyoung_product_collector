from sqlalchemy import (
    Column, Integer, String, Text, DateTime, ForeignKey, Enum, func, Numeric
)
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

# 1. 제품 정보 테이블
class CosmeticsProduct(Base):
    __tablename__ = 'cosmetics_products'

    id = Column(Integer, primary_key=True, autoincrement=True)
    site = Column(String(50), nullable=False)
    collected_at = Column(DateTime, nullable=False)
    goods_no = Column(String(50), nullable=False, unique=True)
    item_no = Column(String(20), nullable=True)
    disp_cat_no = Column(String(50), nullable=True)
    product_url = Column(Text, nullable=False)
    brand = Column(String(100), nullable=False)
    name = Column(Text, nullable=False)
    image_url = Column(Text, nullable=True)
    price_original = Column(String(20), nullable=True)
    price_current = Column(String(20), nullable=True)
    rating_percent = Column(String(10), nullable=True)
    rating_text = Column(String(10), nullable=True)
    review_count = Column(String(20), nullable=True)
    del_yn = Column(String(1), nullable=True, default='N')
    created_at = Column(DateTime, server_default=func.now())

    # 관계 설정
    ingredients = relationship('CosmeticsProductIngredient', back_populates='product')
    purposes = relationship('CosmeticsProductPurpose', back_populates='product')


# 2. 원료 정보 테이블
class CosmeticsIngredient(Base):
    __tablename__ = 'cosmetics_ingredients'

    ingredient_code = Column(Integer, primary_key=True)
    ewg_grade = Column(String(50), nullable=True)
    standard_name_kr = Column(String(600), nullable=False)
    standard_name_en = Column(String(1800), nullable=True)
    old_name_kr = Column(String(300), nullable=True)
    old_name_en = Column(String(700), nullable=True)
    definition = Column(String(2900), nullable=True)
    purpose = Column(String(150), nullable=True)
    korean_synonyms = Column(String(1000), nullable=True)
    remark = Column(String(1000), nullable=True)
    created_dt = Column(DateTime, default=datetime.utcnow)
    updated_dt = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 관계 설정
    products = relationship('CosmeticsProductIngredient', back_populates='ingredient')


# 3. 제품-원료 연결 테이블 (다대다 관계)
class CosmeticsProductIngredient(Base):
    __tablename__ = 'cosmetics_product_ingredients'

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, ForeignKey('cosmetics_products.id', ondelete='CASCADE'), nullable=False)
    ingredient_id = Column(Integer, ForeignKey('cosmetics_ingredients.ingredient_code', ondelete='CASCADE'), nullable=False)
    searched_ingredient = Column(String(100), nullable=False)

    product = relationship('CosmeticsProduct', back_populates='ingredients')
    ingredient = relationship('CosmeticsIngredient', back_populates='products')


# 4. 제품-용도 연결 테이블 (다대다 관계)
class CosmeticsProductPurpose(Base):
    __tablename__ = 'cosmetics_product_purpose'

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, ForeignKey('cosmetics_products.id', ondelete='CASCADE'), nullable=False)
    purpose_id = Column(Integer, ForeignKey('cosmetics_purposes_master.purpose_id', ondelete='CASCADE'), nullable=False)

    product = relationship('CosmeticsProduct', back_populates='purposes')
    purpose_master = relationship('CosmeticsPurposesMaster', back_populates='product_purposes')


# 5. 용도(마스터) 테이블
class CosmeticsPurposesMaster(Base):
    __tablename__ = 'cosmetics_purposes_master'

    purpose_id = Column(Integer, primary_key=True, autoincrement=True, comment='용도 ID')
    purpose_name = Column(String(100), nullable=False, unique=True, comment='용도명')
    category = Column(String(50), nullable=True, default=None, comment='용도 분류')
    purpose_related_features = Column(String(1000), nullable=False, comment='화장품 목적 관련 기능들 (예: 피부 유연성 향상, 피부 장벽 강화)')
    purpose_detailed_description = Column(String(1000), nullable=False, comment='화장품 목적 상세 설명')
    purpose_main = Column(String(1000), nullable=False, comment='화장품 주요 목적')
    rmrk = Column(String(500), nullable=True, default=None, comment='비고')
    del_yn = Column(String(1), nullable=False, default='N', comment='삭제여부')
    replace_purpose_id = Column(Integer, nullable=True, default=None, comment='대체 용도 ID')
    use_yn = Column(String(1), nullable=False, default='Y', comment='사용여부')
    created_dt = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_dt = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # 관계 설정
    product_purposes = relationship('CosmeticsProductPurpose', back_populates='purpose_master')


# 6. 오류 로그 테이블
class CosmeticsErrorLog(Base):
    __tablename__ = 'cosmetics_error_logs'

    error_id = Column(Integer, primary_key=True, autoincrement=True)
    process_type = Column(Enum('PURPOSE_PROCESS', 'INGREDIENT_PROCESS'), nullable=False, comment='작업 구분')
    error_type = Column(Enum('NOT_FOUND', 'SPECIAL_CHAR'), nullable=False, comment='오류 유형')
    searched_value = Column(String(200), nullable=False, comment='검색된 값')
    original_value = Column(String(200), nullable=True, comment='원본 값')
    error_count = Column(Integer, default=1, nullable=False)
    last_occurred_dt = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    first_occurred_dt = Column(DateTime, default=datetime.utcnow, nullable=False)

#7. 카테고리 테이블
class CosmeticsCategory(Base):
    """화장품 카테고리 정보를 저장하는 테이블
    
    Attributes:
        category_id (str): 카테고리 아이디 
        category_name (str): 카테고리명
        parent_category_id (str): 부모 카테고리 아이디
        product_cnt (int): 카테고리에 속한 제품 수
        scheduled_day (int): 작업 요일 (0: 미실시, 1~7: 월~일)
        last_run_dt (datetime): 마지막 작업 실행 일시
        is_processed (int): 오늘 작업 완료 여부 (0:미완료, 1:완료)
        del_yn (str): 삭제 여부 (N:미삭제, Y:삭제)
        created_dt (datetime): 생성일시
        updated_dt (datetime): 수정일시
    """
    __tablename__ = 'cosmetics_categories'

    category_id = Column(String(50), primary_key=True, nullable=False, comment='카테고리 아이디')
    category_name = Column(String(100), nullable=False, comment='카테고리명')
    parent_category_id = Column(String(50), nullable=True, comment='부모 카테고리 아이디')
    product_cnt = Column(Integer, nullable=False, default=0, comment='카테고리에 속한 제품 수')
    scheduled_day = Column(Integer, nullable=False, default=0, comment='작업 요일 (0: 미실시, 1~7: 월~일)')
    last_run_dt = Column(DateTime, nullable=True, comment='마지막 작업 실행 일시')
    is_processed = Column(Integer, nullable=False, default=0, comment='오늘 작업 완료 여부 (0:미완료, 1:완료)')
    del_yn = Column(String(2), nullable=False, default='N', comment='삭제 여부 (N:미삭제, Y:삭제)')
    created_dt = Column(DateTime, default=datetime.utcnow, nullable=False, comment='생성일시')
    updated_dt = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, comment='수정일시')

# 8. 새로운 시계열 분석용 테이블 정의
class CosmeticsProductHistory(Base):
    """제품 정보의 시계열 데이터를 저장하는 테이블
    
    매일/주별로 수집된 제품 정보를 카테고리별로 저장하여 시간에 따른 변화를 추적
    """
    __tablename__ = 'cosmetics_products_history'

    # 기본 키 변경: disp_cat_no 추가
    goods_no = Column(String(50), nullable=False, comment='제품 식별자', primary_key=True)
    disp_cat_no = Column(String(200), nullable=False, comment='카테고리 번호', primary_key=True)
    year = Column(Integer, nullable=False, comment='수집 연도', primary_key=True)
    week_of_year = Column(Integer, nullable=False, comment='수집 주차 (ISO 주차 기준, 1-53)', primary_key=True)
    
    # 기타 필드
    site = Column(String(50), nullable=False, comment='사이트명')
    collected_at = Column(DateTime, nullable=False, comment='수집 시간', index=True)
    item_no = Column(String(20), nullable=True, comment='아이템 번호')
    product_url = Column(Text, nullable=False, comment='제품 URL')
    brandId = Column(Integer, nullable=True, comment='브랜드 ID', index=True)
    name = Column(Text, nullable=False, comment='제품명')
    image_url = Column(Text, nullable=True, comment='이미지 URL')
    price_original = Column(Integer, nullable=True, comment='원래 가격')
    price_current = Column(Integer, nullable=True, comment='현재 가격')
    rating_percent = Column(Numeric(5, 1), nullable=True, comment='평점 퍼센트')
    rating_text = Column(Numeric(5, 1), nullable=True, comment='평점 텍스트')
    review_count = Column(Integer, nullable=True, comment='리뷰 수')
    popularity_rank = Column(Integer, nullable=True, comment='인기 랭킹')
    sales_rank = Column(Integer, nullable=True, comment='판매 랭킹')
    month = Column(Integer, nullable=False, comment='수집 월')
    created_at = Column(DateTime, server_default=func.now(), comment='생성일시')


# 9. 제품 수집 재시도 관리 테이블
class CosmeticsProductsHistoryRetries(Base):
    """제품 수집 재시도 관리를 위한 테이블
    
    이전 주차에 수집되었으나 현재 주차에 누락된 제품들의 재시도 상태를 관리
    """
    __tablename__ = 'cosmetics_products_history_retries'

    retry_id = Column(Integer, primary_key=True, autoincrement=True, comment='재시도 ID')
    goods_no = Column(String(50), nullable=False, comment='제품 식별자')
    disp_cat_no = Column(String(200), nullable=False, comment='카테고리 번호')
    target_year = Column(Integer, nullable=False, comment='수집 대상 연도')
    target_week_of_year = Column(Integer, nullable=False, comment='수집 대상 주차')
    status = Column(Enum('pending', 'processing', 'failed', 'success', 'max_retries_reached', 'product_deleted', name='retry_status_enum'), 
                   default='pending', comment='처리 상태')
    attempt_count = Column(Integer, default=0, comment='시도 횟수')
    max_attempts = Column(Integer, default=3, comment='최대 시도 횟수')
    last_attempt_at = Column(DateTime, nullable=True, comment='마지막 시도 시간')
    error_message = Column(Text, nullable=True, comment='오류 메시지')
    created_at = Column(DateTime, default=datetime.utcnow, comment='생성 시간')
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='갱신 시간')

# 코드에 브랜드 테이블 추가
class CosmBrand(Base):
    """화장품 브랜드 정보를 저장하는 테이블
    
    Attributes:
        id (int): 브랜드 ID (자동 증가)
        name (str): 브랜드 이름 (활성 브랜드 중 고유해야 함)
        is_active (bool): 브랜드 활성 상태
        logo_url (str): 브랜드 로고 URL
        created_at (datetime): 생성 일시
        updated_at (datetime): 수정 일시
    """
    __tablename__ = 'cosmetics_brands'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    is_active = Column(Integer, nullable=False, default=1)  # boolean -> int 변환 (1: true, 0: false)
    logo_url = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
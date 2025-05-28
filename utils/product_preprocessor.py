import logging
from utils.logger import setup_logger
logger = setup_logger(__name__, "product_preprocessor.log")


def safe_convert_to_int(value, key=None, product_id=None):
    """
    값을 안전하게 정수로 변환합니다. 이미 정수인 경우 그대로 반환합니다.
    
    Args:
        value: 변환할 값
        key: 로깅을 위한 필드 이름
        product_id: 로깅을 위한 제품 ID
        
    Returns:
        int: 변환된 정수 값 또는 변환 실패 시 원래 값
    """
    if isinstance(value, int):
        return value
    try:
        return int(str(value).replace(',', '').replace('건', '').strip())
    except (ValueError, TypeError):
        if key and product_id:
            logging.warning(f"제품 '{product_id}': {key} 변환 실패 - '{value}'")
        return value


def safe_convert_to_float(value, key=None, product_id=None):
    """
    값을 안전하게 실수로 변환합니다. 이미 실수인 경우 그대로 반환합니다.
    
    Args:
        value: 변환할 값
        key: 로깅을 위한 필드 이름
        product_id: 로깅을 위한 제품 ID
        
    Returns:
        float: 변환된 실수 값 또는 변환 실패 시 원래 값
    """
    if isinstance(value, (int, float)):
        return value
    try:
        return float(value)
    except (ValueError, TypeError):
        if key and product_id:
            logging.warning(f"제품 '{product_id}': {key} 변환 실패 - '{value}'")
        return value


def preprocess_product_data(products):
    """
    제품 데이터를 DB 저장을 위해 전처리합니다.
    원래 데이터 구조는 유지하면서 문자열 값을 숫자형으로 변환합니다.
    이미 숫자형인 경우 변환을 건너뜁니다.
    
    Args:
        products (list): 처리할 제품 데이터 목록
        
    Returns:
        list: 전처리된 제품 데이터 목록
    """
    
    for product in products:
        try:
            # 1. price 객체 내부 값 처리 (문자열 → 정수)
            goods_no = product.get('goods_no')
            if product.get('price'):
                # price.original 처리
                if product['price'].get('original') is not None:
                    product['price']['original'] = safe_convert_to_int(
                        product['price']['original'], 'price.original', goods_no
                    )
                
                # price.current 처리
                if product['price'].get('current') is not None:
                    product['price']['current'] = safe_convert_to_int(
                        product['price']['current'], 'price.current', goods_no
                    )
            
            # 2. rating 객체 내부 값 처리 (문자열 → 소수점)
            if product.get('rating'):
                # rating.text 처리
                if product['rating'].get('text') is not None:
                    product['rating']['text'] = safe_convert_to_float(
                        product['rating']['text'], 'rating.text', goods_no
                    )
                
                # rating.percent 처리
                if product['rating'].get('percent') is not None:
                    product['rating']['percent'] = safe_convert_to_float(
                        product['rating']['percent'], 'rating.percent', goods_no
                    )
            
            # 3. review_count 처리 ("559건" → 559)
            if product.get('review_count') is not None:
                product['review_count'] = safe_convert_to_int(
                    product['review_count'], 'review_count', goods_no
                )
                
        except Exception as e:
            logging.warning(f"제품 '{product.get('goods_no')}' 처리 중 오류 발생: {str(e)}")
            
    return products
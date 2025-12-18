import logging
import re
from utils.logger import setup_logger
logger = setup_logger(__name__, "product_preprocessor.log")


def decode_unicode_escapes(text):
    r"""
    유니코드 이스케이프 시퀀스(\uXXXX)를 실제 문자로 변환합니다.
    \n, \t 등 다른 이스케이프 시퀀스는 변환하지 않습니다.

    Args:
        text: 변환할 문자열

    Returns:
        str: 유니코드 이스케이프가 디코딩된 문자열

    Examples:
        >>> decode_unicode_escapes("데일리\\u0026패밀리")
        '데일리&패밀리'
    """
    if not isinstance(text, str):
        return text
    return re.sub(r'\\u[0-9a-fA-F]{4}', lambda m: chr(int(m.group(0)[2:], 16)), text)


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
            goods_no = product.get('goods_no')

            # 1. 문자열 필드 유니코드 이스케이프 디코딩 (\u0026 → & 등)
            if 'name' in product and product['name']:
                product['name'] = decode_unicode_escapes(product['name'])
            if 'brand' in product and product['brand']:
                product['brand'] = decode_unicode_escapes(product['brand'])

            # 2. price 객체 내부 값 처리 (문자열 → 정수)
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
            
            # 3. rating 객체 내부 값 처리 (문자열 → 소수점)
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
            
            # 4. review_count 처리 ("559건" → 559)
            if product.get('review_count') is not None:
                product['review_count'] = safe_convert_to_int(
                    product['review_count'], 'review_count', goods_no
                )
                
        except Exception as e:
            logging.warning(f"제품 '{product.get('goods_no')}' 처리 중 오류 발생: {str(e)}")
            
    return products
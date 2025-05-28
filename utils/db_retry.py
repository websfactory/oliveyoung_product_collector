import time
import random
import functools
from sqlalchemy.exc import OperationalError, TimeoutError
import logging

logger = logging.getLogger(__name__)

def retry_db_operation(max_retries=3, base_delay=1.0):
    """
    데이터베이스 작업에 재시도 로직을 적용하는 데코레이터
    
    Args:
        max_retries (int): 최대 재시도 횟수 (기본값: 3)
        base_delay (float): 기본 지연 시간 (초, 기본값: 1.0)
        
    Returns:
        함수 데코레이터
    
    사용법:
        @retry_db_operation(max_retries=3, base_delay=1.0)
        def some_db_function(session, ...):
            # 데이터베이스 작업 수행
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (OperationalError, TimeoutError) as e:
                    last_error = e
                    # 마지막 시도였으면 예외 다시 발생
                    if attempt >= max_retries:
                        logger.error(f"데이터베이스 작업 {max_retries}회 재시도 실패: {e}")
                        raise
                    
                    # 지수 백오프 + 랜덤 지터 적용
                    backoff = base_delay * (2 ** (attempt - 1)) + random.uniform(0.1, 1.0)
                    logger.warning(f"[DB Retry {attempt}/{max_retries}] {backoff:.1f}초 후 재시도: {str(e)[:100]}...")
                    time.sleep(backoff)
            
            # 여기까지 오면 모든 재시도 실패 (사실상 위에서 raise로 종료됨)
            raise last_error
            
        return wrapper
    return decorator 
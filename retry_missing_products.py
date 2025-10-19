"""
누락된 제품 재수집 독립 실행 스크립트

이 스크립트는 collect_all_categories.py와 별개로 독립적으로 실행할 수 있습니다.
예약된 카테고리 수집 없이 누락된 제품만 수집하고자 할 때 사용합니다.
"""
import sys
import logging
from datetime import datetime
from config.session import CosmeticsSession
# Cloudflare Bot Management 우회를 위해 curl-cffi 버전 사용
from collectors.oliveyoung_collector_curl import OliveYoungCollectorCurl
from api.ingredient_api import IngredientAPI
from api.product_api import ProductAPI
from retry.manager import RetryManager
from utils.logger import setup_logger

# 로거 설정
logger = setup_logger(__name__, "retry_missing_products.log")

def retry_missing_products():
    """
    누락된 제품 재시도 프로세스 실행
    """
    logger.info("누락된 제품 재수집 프로그램 시작")
    start_time = datetime.now()
    
    # API 클라이언트 초기화
    ingredient_api = IngredientAPI()
    product_api = ProductAPI()

    # 수집기 초기화 (curl-cffi 버전, Cloudflare 우회)
    collector = OliveYoungCollectorCurl(ingredient_api, product_api, use_proxy=False)
    
    # DB 세션 생성
    session = CosmeticsSession()
    
    try:
        # 재시도 관리자 초기화
        retry_manager = RetryManager(collector, session)
        
        # 누락된 제품 재시도 처리
        result = retry_manager.process_missing_products()
        
        # 결과 출력
        if result.get('success'):
            success_count = result.get('success_count', 0)
            fail_count = result.get('fail_count', 0)
            deleted_count = result.get('deleted_count', 0)  # 삭제된 상품 수
            total_count = success_count + fail_count + deleted_count
            
            if total_count > 0:
                logger.info(f"누락 제품 처리 완료: {success_count}/{total_count} ({success_count/total_count*100:.1f}%) 성공, "
                           f"{fail_count} 실패, {deleted_count} 삭제됨")
            else:
                logger.info("처리할 누락된 제품이 없습니다.")
        else:
            logger.error(f"누락 제품 처리 실패: {result.get('message')}")
            
    except Exception as e:
        logger.error(f"누락 제품 재시도 처리 중 예외 발생: {str(e)}", exc_info=True)
        
    finally:
        # 세션 및 수집기 종료
        session.close()
        collector.close()
        
        # 실행 시간 계산
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"총 실행 시간: {duration}")
        logger.info("누락된 제품 재수집 프로그램 종료")

if __name__ == "__main__":
    try:
        # 메인 함수 실행
        retry_missing_products()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램 중단")
    except Exception as e:
        logger.critical(f"프로그램 실행 중 심각한 오류 발생: {str(e)}", exc_info=True)
        sys.exit(1)

"""
누락된 제품 재수집 독립 실행 스크립트

이 스크립트는 collect_all_categories.py와 별개로 독립적으로 실행할 수 있습니다.
예약된 카테고리 수집 없이 누락된 제품만 수집하고자 할 때 사용합니다.

사용법:
  - 기본 실행 (현재 주차 vs 이전 주차):
      python retry_missing_products.py

  - 특정 주차 지정 (예: 2024년 52주차 vs 51주차 비교):
      python retry_missing_products.py --target-year 2024 --target-week 52

  - 오프셋 사용 (현재 주차에서 N주 전 기준):
      python retry_missing_products.py --offset 1
      (현재 2025년 1주차라면 2024년 52주차 기준으로 실행)
"""
import sys
import argparse
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

def retry_missing_products(target_year=None, target_week=None):
    """
    누락된 제품 재시도 프로세스 실행

    Args:
        target_year (int, optional): 목표 연도 (지정하지 않으면 현재 연도 사용)
        target_week (int, optional): 목표 주차 (지정하지 않으면 현재 주차 사용)
    """
    if target_year and target_week:
        logger.info(f"누락된 제품 재수집 프로그램 시작 (목표 주차: {target_year}년 {target_week}주차)")
    else:
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

        # 누락된 제품 재시도 처리 (주차 오버라이드 파라미터 전달)
        result = retry_manager.process_missing_products(target_year, target_week)
        
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

def parse_arguments():
    """
    커맨드라인 인자 파싱
    """
    from retry.utils import get_current_iso_week, get_previous_iso_week

    parser = argparse.ArgumentParser(
        description="누락된 제품 재수집 스크립트",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  # 기본 실행 (현재 주차 기준)
  python retry_missing_products.py

  # 2024년 52주차 기준으로 실행 (51주차와 비교)
  python retry_missing_products.py --target-year 2024 --target-week 52

  # 현재 주차에서 1주 전 기준으로 실행
  python retry_missing_products.py --offset 1
        """
    )

    parser.add_argument(
        "--target-year",
        type=int,
        help="목표 연도 (예: 2024)"
    )
    parser.add_argument(
        "--target-week",
        type=int,
        help="목표 주차 (예: 52)"
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="현재 주차에서 N주 전 기준으로 실행 (기본값: 0)"
    )

    args = parser.parse_args()

    # 주차 계산
    target_year = args.target_year
    target_week = args.target_week

    # offset이 지정된 경우 주차 계산
    if args.offset > 0 and not (target_year and target_week):
        current_year, current_week = get_current_iso_week()
        # offset만큼 이전 주차로 이동
        for _ in range(args.offset):
            current_year, current_week = get_previous_iso_week(current_year, current_week)
        target_year = current_year
        target_week = current_week
        logger.info(f"오프셋 {args.offset} 적용: {target_year}년 {target_week}주차 기준으로 실행")

    # target-year와 target-week는 함께 지정되어야 함
    if (target_year is None) != (target_week is None):
        parser.error("--target-year와 --target-week는 함께 지정해야 합니다.")

    return target_year, target_week


if __name__ == "__main__":
    try:
        # 커맨드라인 인자 파싱
        target_year, target_week = parse_arguments()

        # 메인 함수 실행
        retry_missing_products(target_year, target_week)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램 중단")
    except Exception as e:
        logger.critical(f"프로그램 실행 중 심각한 오류 발생: {str(e)}", exc_info=True)
        sys.exit(1)

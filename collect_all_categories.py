import sys
import os
import time
import json
from datetime import datetime
from sqlalchemy import update, select
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# 재시도 관리자 임포트
from retry.manager import RetryManager

from config.session import CosmeticsSession
from models.database import CosmeticsCategory
from collectors.oliveyoung_collector import OliveYoungCollector
from api.ingredient_api import IngredientAPI
from api.product_api import ProductAPI
from utils.logger import setup_logger
from config.settings import DEBUG

# 로거 설정
logger = setup_logger(__name__, "collect_all_categories.log")

# DB 작업 재시도 관련 설정
DB_MAX_RETRIES = 2  # 최대 재시도 횟수 
DB_RETRY_DELAY = 3  # 재시도 간 대기 시간(초)

# 오류 추적 및 결과 보고서 관련 설정
ERROR_LOG_DIR = "error_logs"
REPORT_DIR = "reports"

# 디렉토리가 없으면 생성
os.makedirs(ERROR_LOG_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

def update_category_status(session, category_id, is_processed=1, product_cnt=None):
    """
    카테고리 상태 업데이트
    
    Args:
        session: DB 세션
        category_id (str): 카테고리 ID
        is_processed (int): 처리 상태 (0: 미완료, 1: 완료)
        product_cnt (int, optional): 제품 수
    """
    retries = 0
    while retries <= DB_MAX_RETRIES:  # 원래 시도 + 최대 2번 재시도
        try:
            update_values = {
                'is_processed': is_processed,
                'last_run_dt': datetime.now()
            }
            
            if product_cnt is not None:
                update_values['product_cnt'] = product_cnt
            
            session.execute(
                update(CosmeticsCategory)
                .where(CosmeticsCategory.category_id == category_id)
                .values(**update_values)
            )
            session.commit()
            logger.info(f"카테고리 {category_id} 상태 업데이트 완료: is_processed={is_processed}")
            break  # 성공 시 루프 종료
        
        except (OperationalError, SQLAlchemyError) as e:
            session.rollback()
            retries += 1
            
            if retries > DB_MAX_RETRIES:
                logger.error(f"카테고리 상태 업데이트 실패 (최대 재시도 횟수 초과): {str(e)}")
                break
            
            logger.warning(f"카테고리 상태 업데이트 실패, {retries}/{DB_MAX_RETRIES} 재시도 중: {str(e)}")
            time.sleep(DB_RETRY_DELAY)
            
            # 세션 재연결 시도
            try:
                session.close()
                session = CosmeticsSession()
            except Exception as session_ex:
                logger.error(f"세션 재연결 실패: {str(session_ex)}")
        
        except Exception as e:
            logger.error(f"카테고리 상태 업데이트 중 예기치 않은 오류: {str(e)}")
            session.rollback()
            break

def process_category(collector, category, session, failed_categories):
    """
    카테고리 처리 함수
    
    Args:
        collector (OliveYoungCollector): 올리브영 수집기
        category (dict 또는 CosmeticsCategory): 카테고리 객체 또는 {'id': category_id, 'name': category_name} 형태의 딕셔너리
        session: DB 세션
        failed_categories (list): 실패한 카테고리 목록
        
    Returns:
        dict: 처리 결과
    """
    # 카테고리 정보 획득 (객체 또는 딕셔너리 지원)
    if isinstance(category, dict):
        category_id = category['id']
        category_name = category['name']
    else:
        # CosmeticsCategory 객체로부터 정보 추출
        category_id = category.category_id
        category_name = category.category_name
    
    logger.info(f"카테고리 처리: {category_name} (ID: {category_id})")
    
    try:
        # 제품 수집 실행 (순차 처리)
        result = collector.collect_from_category(category_id, category_name)
            
        # 결과 업데이트
        if result['success']:
            update_category_status(session, category_id, 1, result['collected_products'])
            logger.info(f"카테고리 {category_name} 처리 완료: {result['collected_products']}개 제품")
        else:
            logger.error(f"카테고리 {category_name} 처리 실패: {result['error']}")
            # 실패 카테고리 기록
            failed_categories.append({
                'category_id': category_id,
                'category_name': category_name,
                'error': result['error'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return result
    
    except Exception as e:
        logger.error(f"카테고리 {category_name} 처리 중 예외 발생: {str(e)}")
        # 실패 카테고리 기록
        failed_categories.append({
            'category_id': category_id,
            'category_name': category_name,
            'error': str(e),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        return {
            'success': False,
            'error': str(e),
            'category_id': category_id,
            'category_name': category_name
        }

def save_error_log(failed_categories):
    """
    실패한 카테고리 정보를 JSON 파일로 저장
    
    Args:
        failed_categories (list): 실패한 카테고리 목록
    """
    if not failed_categories:
        return
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{ERROR_LOG_DIR}/failed_categories_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(failed_categories, f, ensure_ascii=False, indent=2)
        logger.info(f"실패한 카테고리 정보 저장 완료: {filename}")
    except Exception as e:
        logger.error(f"실패한 카테고리 정보 저장 실패: {str(e)}")

def save_report(report_data):
    """
    작업 결과 요약 보고서 저장
    
    Args:
        report_data (dict): 보고서 데이터
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{REPORT_DIR}/collection_report_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        logger.info(f"작업 결과 요약 보고서 저장 완료: {filename}")
    except Exception as e:
        logger.error(f"작업 결과 요약 보고서 저장 실패: {str(e)}")

def collect_all_scheduled_categories():
    """
    scheduled_day가 0이 아니고 is_processed가 0인 모든 카테고리 수집 실행
    """
    logger.info("올리브영 모든 예약된 카테고리 제품 수집 프로그램 시작")
    start_time = datetime.now()
    
    # 실패한 카테고리 목록 초기화
    failed_categories = []
    
    # API 클라이언트 초기화
    ingredient_api = IngredientAPI()
    product_api = ProductAPI()
    
    # 수집기 초기화
    collector = OliveYoungCollector(ingredient_api, product_api)
    
    # 순차적으로 작업 실행
    
    # DB 세션 생성
    session = CosmeticsSession()
    
    try:
        # 카테고리 조회 재시도 로직
        categories = []
        retries = 0
        
        while retries <= DB_MAX_RETRIES:
            try:
                # scheduled_day가 0이 아니고 is_processed가 0인 모든 카테고리 조회
                query = (
                    select(CosmeticsCategory)
                    .where(CosmeticsCategory.scheduled_day != 0)
                    .where(CosmeticsCategory.is_processed == 0)
                    .where(CosmeticsCategory.del_yn == 'N')  # 삭제되지 않은 카테고리만 처리
                )
                
                # # 디버그 모드일 경우 테스트용 추가 조건 적용
                if DEBUG:
                    logger.debug("디버그 모드 활성화: 데이터 제한 적용")
                    query = query.limit(2)  # 디버그 시 6개 카테고리 1시간
                
                result = session.execute(query).fetchall()
                categories = [row[0] for row in result]
                break  # 성공 시 루프 종료
                
            except (OperationalError, SQLAlchemyError) as e:
                session.rollback()
                retries += 1
                
                if retries > DB_MAX_RETRIES:
                    logger.error(f"카테고리 조회 실패 (최대 재시도 횟수 초과): {str(e)}")
                    return
                
                logger.warning(f"카테고리 조회 실패, {retries}/{DB_MAX_RETRIES} 재시도 중: {str(e)}")
                time.sleep(DB_RETRY_DELAY)
                
                # 세션 재연결 시도
                try:
                    session.close()
                    session = CosmeticsSession()
                except Exception as session_ex:
                    logger.error(f"세션 재연결 실패: {str(session_ex)}")
        
        if not categories:
            logger.info("처리할 카테고리가 없습니다.")
            # 카테고리가 없어도 재시도 로직이 실행되도록 초기화
            results = []
            success_count = 0
            total_products = 0
            weekday_counts = {}
            failed_categories = []
        else:
            logger.info(f"처리할 카테고리: {len(categories)}개")
        
        # 카테고리별 요일 그룹 로깅 (통계 목적)
        if categories:
            weekday_counts = {}
            for category in categories:
                day = category.scheduled_day
                if day not in weekday_counts:
                    weekday_counts[day] = 0
                weekday_counts[day] += 1
            
            # 요일별 카테고리 수 로깅
            for day, count in sorted(weekday_counts.items()):
                day_name = ['', '월', '화', '수', '목', '금', '토', '일'][day]
                logger.info(f"- {day_name}요일({day}) 카테고리: {count}개")
            
            # 카테고리 목록 로깅
            for category in categories:
                logger.info(f"- {category.category_name} (ID: {category.category_id}, 요일: {category.scheduled_day})")
        
        # 각 카테고리 순차 처리
        # 서버 부하 관리를 위해 하나씩 순차적으로 처리
        results = []
        
        # 카테고리가 있는 경우에만 처리
        if categories:
            for i, category in enumerate(categories, 1):
                # 진행 상황 로깅 (모니터링 목적)
                logger.info(f"진행 상황: {i}/{len(categories)} ({i/len(categories)*100:.1f}%)")
                
                try:
                    # 카테고리 ID와 이름 복사 (세션 객체에 의존하지 않기 위해)
                    category_id = category.category_id
                    category_name = category.category_name
                    
                    # 세션이 유효한지 확인
                    if not session.is_active:
                        logger.warning("세션이 활성 상태가 아닙니다. 새 세션으로 재연결합니다.")
                        session.close()
                        session = CosmeticsSession()
                        
                    # 안전하게 카테고리 객체 대신 ID와 이름을 전달하도록 수정
                    result = process_category(collector, {"id": category_id, "name": category_name}, session, failed_categories)
                    results.append(result)
                except Exception as e:
                    logger.error(f"카테고리 {i} 처리 중 예외 발생: {str(e)}")
                    if 'is not bound to a Session' in str(e) or 'DetachedInstanceError' in str(e):
                        # 세션 재연결 시도
                        logger.warning("세션 분리 오류 감지. 세션을 재연결합니다.")
                        try:
                            session.close()
                        except:
                            pass
                        session = CosmeticsSession()
                        
                    # 실패한 카테고리로 추가
                    try:
                        category_info = {
                            'category_id': getattr(category, 'category_id', f"unknown-{i}"),
                            'category_name': getattr(category, 'category_name', f"Unknown Category {i}"),
                            'error': str(e),
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        failed_categories.append(category_info)
                    except:
                        # 최후의 수단으로 식별 가능한 정보로 실패 기록
                        failed_categories.append({
                            'category_id': f"error-{i}",
                            'category_name': f"Error retrieving category {i}",
                            'error': f"Session error: {str(e)}",
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                
                # 주기적으로 실패 목록 저장 (10개 카테고리마다)
                if i % 10 == 0 and failed_categories:
                    save_error_log(failed_categories)
            
            # 결과 요약
            success_count = sum(1 for r in results if r.get('success', False))
            total_products = sum(r.get('collected_products', 0) for r in results if r.get('success', False))
            
            # 작업 결과 요약
            logger.info(f"카테고리 작업 완료: {success_count}/{len(categories)} 카테고리 성공, 총 {total_products}개 제품 수집")
            logger.info(f"실패한 카테고리: {len(failed_categories)}개")
        else:
            # 카테고리가 없는 경우
            logger.info("처리할 카테고리가 없습니다. 누락된 제품 재시도 로직으로 넘어갑니다.")
        
        # ================================
        # 누락된 제품 재시도 로직 시작
        # ================================
        # logger.info("모든 카테고리 수집 완료. 누락된 제품 확인 중...")
        
        # try:
        #     # 재시도 관리자를 통한 누락 제품 처리
        #     retry_manager = RetryManager(collector, session)
        #     retry_result = retry_manager.process_missing_products()
            
        #     # 재시도 결과 로깅
        #     if retry_result.get('success'):
        #         logger.info(f"누락 제품 처리 결과: {retry_result.get('success_count')}개 성공, "
        #                    f"{retry_result.get('fail_count')}개 실패")
        #     else:
        #         logger.error(f"누락 제품 처리 실패: {retry_result.get('message')}")
                
        #     # 재시도 정보를 보고서에 추가
        #     retry_info = {
        #         'missing_products_processed': retry_result.get('success_count', 0) + retry_result.get('fail_count', 0) + retry_result.get('deleted_count', 0),
        #         'missing_products_success': retry_result.get('success_count', 0),
        #         'missing_products_failed': retry_result.get('fail_count', 0),
        #         'missing_products_deleted': retry_result.get('deleted_count', 0),  # 삭제된 상품 수 추가
        #         'message': retry_result.get('message', '')
        #     }
            
        # except Exception as e:
        #     logger.error(f"누락 제품 재시도 처리 중 예외 발생: {str(e)}", exc_info=True)
        #     retry_info = {
        #         'error': str(e),
        #         'missing_products_processed': 0,
        #         'missing_products_success': 0,
        #         'missing_products_failed': 0
        #     }
        # ================================
        # 누락된 제품 재시도 로직 종료
        # ================================
            
        # 보고서 데이터 준비
        report_data = {
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_categories': len(categories) if categories else 0,
            'successful_categories': success_count,
            'failed_categories': len(failed_categories),
            'total_products_collected': total_products,
            # 카테고리가 없을 경우 비어있는 사전 사용
            'weekday_distribution': {f"{k} ({['', '월', '화', '수', '목', '금', '토', '일'][k]}요일)": v for k, v in weekday_counts.items()} if 'weekday_counts' in locals() and weekday_counts else {},
            # 'retry_info': retry_info  # 재시도 정보 추가
        }
        
        # 오류 로그 및 보고서 저장
        if failed_categories:
            save_error_log(failed_categories)
        save_report(report_data)
        
    except Exception as e:
        logger.error(f"작업 중 치명적 오류 발생: {str(e)}", exc_info=True)
        # 진행 중이던 작업 정보 저장
        if failed_categories:
            save_error_log(failed_categories)
    finally:
        session.close()
        collector.close()
        
        # 실행 시간 계산
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"총 실행 시간: {duration}")
        logger.info("올리브영 모든 예약된 카테고리 제품 수집 프로그램 종료")

def reset_category_status():
    """
    모든 카테고리의 처리 상태 초기화 (선택적 사용)
    """
    logger.info("카테고리 상태 초기화 시작")
    
    # DB 세션 생성
    session = CosmeticsSession()
    
    retries = 0
    while retries <= DB_MAX_RETRIES:  # 원래 시도 + 최대 2번 재시도
        try:
            # 모든 카테고리 상태 초기화
            session.execute(
                update(CosmeticsCategory)
                .values(is_processed=0)
            )
            session.commit()
            logger.info("모든 카테고리 상태 초기화 완료")
            break  # 성공 시 루프 종료
        
        except (OperationalError, SQLAlchemyError) as e:
            session.rollback()
            retries += 1
            
            if retries > DB_MAX_RETRIES:
                logger.error(f"카테고리 상태 초기화 실패 (최대 재시도 횟수 초과): {str(e)}")
                break
            
            logger.warning(f"카테고리 상태 초기화 실패, {retries}/{DB_MAX_RETRIES} 재시도 중: {str(e)}")
            time.sleep(DB_RETRY_DELAY)  # 동기 함수이므로 time.sleep 사용
            
            # 세션 재연결 시도
            try:
                session.close()
                session = CosmeticsSession()
            except Exception as session_ex:
                logger.error(f"세션 재연결 실패: {str(session_ex)}")
        
        except Exception as e:
            logger.error(f"카테고리 상태 초기화 중 예기치 않은 오류: {str(e)}")
            session.rollback()
            break
        finally:
            session.close()

if __name__ == "__main__":
    try:
        # 커맨드라인 인자 확인
        if len(sys.argv) > 1 and sys.argv[1] == "--reset":
            # 카테고리 상태 초기화 모드
            reset_category_status()
        else:
            # 모든 예약된 카테고리 수집 모드
            collect_all_scheduled_categories()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램 중단")
    except Exception as e:
        logger.critical(f"프로그램 실행 중 심각한 오류 발생: {str(e)}", exc_info=True)
        sys.exit(1)

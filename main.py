import sys
import os
import time
import json
from datetime import datetime
from sqlalchemy import update, select
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from config.session import CosmeticsSession
from models.database import CosmeticsCategory
# Collector 선택 (기본: curl-cffi 버전)
# VS Code 디버그 모드에서는 직접 수정하여 사용
USE_CURL_CFFI = True  # True: curl-cffi 버전, False: 기존 cloudscraper 버전

if USE_CURL_CFFI:
    from collectors.oliveyoung_collector_curl import OliveYoungCollectorCurl as OliveYoungCollector
    print("[INFO] curl-cffi 수집기 사용")
else:
    from collectors.oliveyoung_collector import OliveYoungCollector
    print("[INFO] 기본 수집기 사용")
from api.ingredient_api import IngredientAPI
from api.product_api import ProductAPI
from utils.logger import setup_logger
from config.settings import DEBUG

# 로거 설정
logger = setup_logger(__name__, "main.log")

# DB 작업 재시도 관련 설정
DB_MAX_RETRIES = 2  # 최대 재시도 횟수 
DB_RETRY_DELAY = 3  # 재시도 간 대기 시간(초)

# 오류 추적 및 결과 보고서 관련 설정
ERROR_LOG_DIR = "error_logs"

# 디렉토리가 없으면 생성
os.makedirs(ERROR_LOG_DIR, exist_ok=True)

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

def process_category(collector, category, session, failed_categories=None):
    """
    카테고리 처리 함수
    
    Args:
        collector (OliveYoungCollector): 올리브영 수집기
        category (dict 또는 CosmeticsCategory): 카테고리 객체 또는 {'id': category_id, 'name': category_name} 형태의 딕셔너리
        session: DB 세션
        failed_categories (list, optional): 실패한 카테고리 목록
        
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
            # 실패 카테고리 기록 (선택적)
            if failed_categories is not None:
                failed_categories.append({
                    'category_id': category_id,
                    'category_name': category_name,
                    'error': result['error'],
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        
        return result
    
    except Exception as e:
        logger.error(f"카테고리 {category_name} 처리 중 예외 발생: {str(e)}")
        # 실패 카테고리 기록 (선택적)
        if failed_categories is not None:
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

def collect_today_categories():
    """
    오늘 작업할 카테고리 수집 실행
    """
    logger.info("올리브영 제품 수집 프로그램 시작")
    start_time = datetime.now()
    
    # 오늘 요일 계산 (1~7: 월~일)
    today_weekday = datetime.now().isoweekday()
    logger.info(f"오늘 요일: {today_weekday} ({['월', '화', '수', '목', '금', '토', '일'][today_weekday-1]}요일)")
    
    # API 클라이언트 초기화
    ingredient_api = IngredientAPI()
    product_api = ProductAPI()
    
    # 수집기 초기화
    try:
        collector = OliveYoungCollector(ingredient_api, product_api)
        print(f"[INFO] 수집기 초기화 완료: {collector.__class__.__name__}")
        logger.info(f"수집기 초기화 완료: {collector.__class__.__name__}")
    except Exception as e:
        print(f"[ERROR] 수집기 초기화 실패: {str(e)}")
        logger.error(f"수집기 초기화 실패: {str(e)}")
        raise
    
    # 실패한 카테고리 목록 초기화
    failed_categories = []
    
    # DB 세션 생성
    session = CosmeticsSession()
    
    try:
        # 카테고리 조회 재시도 로직
        categories = []
        retries = 0
        
        while retries <= DB_MAX_RETRIES:
            try:
                # 오늘 작업할 카테고리 조회
                query = (
                    select(CosmeticsCategory)
                    .where(CosmeticsCategory.scheduled_day == today_weekday)
                    .where(CosmeticsCategory.is_processed == 0)
                    .where(CosmeticsCategory.del_yn == 'N')  # 삭제되지 않은 카테고리만 처리
                )
                
                # 디버그 모드일 경우 테스트용 추가 조건 적용
                if DEBUG:
                    logger.debug("디버그 모드 활성화: 데이터 제한 적용")
                    query = query.limit(20)  # 디버그 시 3개 카테고리만 처리
                
                result = session.execute(query).fetchall()
                
                # 세션에 의존하지 않는 딕셔너리 리스트로 변환 (중요한 개선!)
                categories = [{'id': row[0].category_id, 'name': row[0].category_name} for row in result]
                
                logger.info(f"카테고리 {len(categories)}개를 딕셔너리로 변환 완료")
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
            logger.info("오늘 처리할 카테고리가 없습니다.")
            return
        
        logger.info(f"오늘 처리할 카테고리: {len(categories)}개")
        
        # 카테고리 목록 로깅
        for category in categories:
            logger.info(f"- {category['name']} (ID: {category['id']})")
        
        # 각 카테고리 순차 처리
        # 서버 부하 관리를 위해 하나씩 순차적으로 처리
        results = []
        
        for i, category in enumerate(categories, 1):
            # 진행 상황 로깅 (모니터링 목적)
            logger.info(f"진행 상황: {i}/{len(categories)} ({i/len(categories)*100:.1f}%)")
            
            try:
                # 세션이 유효한지 확인
                if not session.is_active:
                    logger.warning("세션이 활성 상태가 아닙니다. 새 세션으로 재연결합니다.")
                    try:
                        session.close()
                    except:
                        pass
                    session = CosmeticsSession()
                
                # 이미 딕셔너리에 카테고리 정보가 있으므로 세션 객체 의존성 없음
                result = process_category(collector, category, session, failed_categories)
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
                
                # 실패한 카테고리 정보 추가 (이미 딕셔너리 형태이므로 안전)
                failed_categories.append({
                    'category_id': category['id'],
                    'category_name': category['name'],
                    'error': str(e),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
            
            # 주기적으로 실패 목록 저장 (5개 카테고리마다)
            if i % 5 == 0 and failed_categories:
                save_error_log(failed_categories)
        
        # 결과 요약
        success_count = sum(1 for r in results if r.get('success', False))
        total_products = sum(r.get('collected_products', 0) for r in results if r.get('success', False))
        
        logger.info(f"작업 완료: {success_count}/{len(categories)} 카테고리 성공, {len(failed_categories)}개 실패, 총 {total_products}개 제품 수집")
        
        # 남은 실패 목록 저장
        if failed_categories:
            save_error_log(failed_categories)
            
    except Exception as e:
        logger.error(f"작업 중 오류 발생: {str(e)}")
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
        logger.info("올리브영 제품 수집 프로그램 종료")

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
            # 일반 수집 모드
            collect_today_categories()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램 중단")
    except Exception as e:
        logger.critical(f"프로그램 실행 중 심각한 오류 발생: {str(e)}")
        sys.exit(1)

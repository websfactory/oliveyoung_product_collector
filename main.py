import sys
import os
import time
import json
from datetime import datetime
from sqlalchemy import update, select
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from config.session import CosmeticsSession
from models.database import CosmeticsCategory
# Collector 선택 (curl-cffi 버전 고정)
from collectors.oliveyoung_collector_curl import OliveYoungCollectorCurl as OliveYoungCollector
print("[INFO] curl-cffi 수집기 사용")
from api.ingredient_api import IngredientAPI
from api.product_api import ProductAPI
from utils.logger import setup_logger
from utils.webshare_proxy import get_webshare_proxy_manager
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

def select_proxy_mode():
    """
    프록시 사용 여부 선택 메뉴

    Returns:
        bool 또는 str: 프록시 사용 여부(bool) 또는 'single'(단일 상품 수집 모드)
    """
    print("\n=== 올리브영 수집기 설정 ===")
    print("1. 로컬 연결 사용 (기본)")
    print("2. Webshare 프록시 사용")
    print("3. 프록시 연결 테스트")
    print("4. 단일 상품 수집")
    print("5. 특정 요일 카테고리 수집")
    print("0. 종료")

    while True:
        choice = input("\n옵션을 선택하세요 (0-5): ").strip()

        if choice == '0':
            print("프로그램을 종료합니다.")
            sys.exit(0)
        elif choice == '1':
            print("\n[INFO] 로컬 연결을 사용합니다.")
            return False
        elif choice == '2':
            print("\n[INFO] Webshare 프록시를 사용합니다.")
            return True
        elif choice == '3':
            test_proxy_connection()
            # 테스트 후 다시 메뉴 표시
            continue
        elif choice == '4':
            return 'single'
        elif choice == '5':
            return 'day'
        else:
            print("\n[ERROR] 잘못된 선택입니다. 다시 선택해주세요.")

def test_proxy_connection():
    """
    프록시 연결 테스트
    """
    print("\n=== 프록시 연결 테스트 ===")
    
    try:
        proxy_manager = get_webshare_proxy_manager()
        print("프록시 매니저 초기화 성공")
        
        # 프록시 정보 표시
        proxy_info = proxy_manager.get_proxy_info()
        print(f"\n프록시 상태:")
        print(f"- 전체 프록시: {proxy_info['total_proxies']}개")
        print(f"- 유효한 프록시: {proxy_info['valid_proxies']}개")
        print(f"- 실패한 프록시: {proxy_info['failed_proxies']}개")
        
        # 프록시 테스트
        print("\n프록시 연결 테스트 중...")
        if proxy_manager.test_proxy():
            print("[✓] 프록시 연결 테스트 성공!")
        else:
            print("[✗] 프록시 연결 테스트 실패")
            
    except Exception as e:
        print(f"\n[ERROR] 프록시 테스트 중 오류 발생: {e}")
    
    input("\n계속하려면 Enter키를 누르세요...")

def collect_today_categories(use_proxy=False, target_day=None):
    """
    오늘 작업할 카테고리 수집 실행

    Args:
        use_proxy (bool): 프록시 사용 여부
        target_day (int, optional): 특정 요일 지정 (1=월 ~ 7=일). None이면 오늘 요일 사용
    """
    logger.info("올리브영 제품 수집 프로그램 시작")
    logger.info(f"프록시 사용: {'Yes' if use_proxy else 'No'}")
    start_time = datetime.now()

    # 요일 결정: target_day가 지정되면 해당 요일, 아니면 오늘 요일 (1~7: 월~일)
    today_weekday = target_day if target_day else datetime.now().isoweekday()
    if target_day:
        logger.info(f"지정 요일: {today_weekday} ({['월', '화', '수', '목', '금', '토', '일'][today_weekday-1]}요일)")
    else:
        logger.info(f"오늘 요일: {today_weekday} ({['월', '화', '수', '목', '금', '토', '일'][today_weekday-1]}요일)")
    
    # API 클라이언트 초기화
    ingredient_api = IngredientAPI()
    product_api = ProductAPI()
    
    # 수집기 초기화 (프록시 옵션 포함)
    try:
        collector = OliveYoungCollector(ingredient_api, product_api, use_proxy=use_proxy)
        print(f"[INFO] 수집기 초기화 완료: {collector.__class__.__name__}")
        logger.info(f"수집기 초기화 완료: {collector.__class__.__name__}")
        
        # 프록시 사용 시 프록시 정보 표시
        if use_proxy and hasattr(collector, 'proxy_manager') and collector.proxy_manager:
            proxy_info = collector.proxy_manager.get_proxy_info()
            logger.info(f"프록시 정보: {proxy_info}")
            
    except Exception as e:
        print(f"[ERROR] 수집기 초기화 실패: {str(e)}")
        logger.error(f"수집기 초기화 실패: {str(e)}")
        raise
    
    # 실패한 카테고리 목록 초기화
    failed_categories = []

    # 수집 요약 (텔레그램 보고용) — finally에서 참조하므로 try 이전에 초기화
    success_count = 0
    total_products = 0
    total_categories = 0
    crashed = False

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
        total_categories = len(categories)
        success_count = sum(1 for r in results if r.get('success', False))
        total_products = sum(r.get('collected_products', 0) for r in results if r.get('success', False))

        logger.info(f"작업 완료: {success_count}/{total_categories} 카테고리 성공, {len(failed_categories)}개 실패, 총 {total_products}개 제품 수집")
        
        # 남은 실패 목록 저장
        if failed_categories:
            save_error_log(failed_categories)
            
    except Exception as e:
        crashed = True
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

        # 상태 판정 (임계치 가드: 0건/급감 = alert → 침묵 실패 차단)
        note_parts = []
        if crashed:
            run_status = 'failed'
            note_parts.append('수집 중 예외 발생')
        elif total_categories == 0:
            run_status = 'alert'
            note_parts.append('처리할 카테고리 0개 (주간 리셋 누락 또는 재실행?)')
        elif total_products == 0:
            run_status = 'alert'
            note_parts.append('수집 0건')
        else:
            run_status = 'partial' if failed_categories else 'success'
            try:
                from utils.run_log import last_week_collected
                lw = last_week_collected('oliveyoung')
                if lw and lw >= 100 and total_products < lw * 0.4:
                    run_status = 'alert'
                    note_parts.append(f'전주 동요일 대비 급감: {total_products} vs {lw}')
            except Exception as e:
                logger.error(f"전주 비교 실패: {str(e)}")
        if failed_categories:
            note_parts.append('실패 카테고리: ' + ', '.join(
                str(c.get('category_name', c.get('category_id', '?'))) for c in failed_categories[:10]))
        run_note = ' / '.join(note_parts) if note_parts else None

        # collection_runs 기록 (관측 중앙화)
        try:
            from utils.run_log import record_run
            record_run(
                site='oliveyoung', job_type='collect',
                started_at=start_time, finished_at=end_time, status=run_status,
                scheduled_day=today_weekday, category_count=total_categories,
                collected_count=total_products, failed_count=len(failed_categories),
                note=run_note,
            )
        except Exception as e:
            logger.error(f"collection_runs 기록 실패: {str(e)}")

        # 수집 완주 텔레그램 보고 (상태 반영, 전송 실패가 수집에 영향 주지 않도록 격리)
        try:
            from utils.telegram import send_collection_report
            send_collection_report(
                weekday=today_weekday, status=run_status,
                success_count=success_count, total_categories=total_categories,
                total_products=total_products, failed_categories=failed_categories,
                duration=duration,
                extra_note=(run_note if run_status in ('alert', 'failed') else None),
            )
        except Exception as e:
            logger.error(f"텔레그램 수집 보고 전송 실패: {str(e)}")

def collect_single_product():
    """
    단일 상품 수집 모드
    사용자로부터 goods_no와 disp_cat_no를 입력받아 해당 상품만 수집하고 DB에 저장
    """
    print("\n=== 단일 상품 수집 ===")

    # 사용자 입력 받기
    goods_no = input("상품번호(goods_no)를 입력하세요: ").strip()
    if not goods_no:
        print("[ERROR] 상품번호가 입력되지 않았습니다.")
        return

    disp_cat_no = input("카테고리번호(disp_cat_no)를 입력하세요: ").strip()
    if not disp_cat_no:
        print("[ERROR] 카테고리번호가 입력되지 않았습니다.")
        return

    # 프록시 사용 여부 확인
    use_proxy_input = input("프록시를 사용하시겠습니까? (y/N): ").strip().lower()
    use_proxy = use_proxy_input == 'y'

    print(f"\n[INFO] 수집 시작: goods_no={goods_no}, disp_cat_no={disp_cat_no}")
    print(f"[INFO] 프록시 사용: {'Yes' if use_proxy else 'No'}")

    logger.info(f"단일 상품 수집 시작: goods_no={goods_no}, disp_cat_no={disp_cat_no}")
    start_time = datetime.now()

    # API 클라이언트 초기화
    ingredient_api = IngredientAPI()
    product_api = ProductAPI()

    # 수집기 초기화
    try:
        collector = OliveYoungCollector(ingredient_api, product_api, use_proxy=use_proxy)
        print(f"[INFO] 수집기 초기화 완료")
    except Exception as e:
        print(f"[ERROR] 수집기 초기화 실패: {str(e)}")
        logger.error(f"수집기 초기화 실패: {str(e)}")
        return

    try:
        # 1. 상품 상세 정보 수집
        print("[INFO] 상품 상세 정보 수집 중...")
        product = collector.collect_product_detail(goods_no)

        if not product:
            print(f"[ERROR] 상품 {goods_no} 상세 정보 수집 실패")
            logger.error(f"상품 {goods_no} 상세 정보 수집 실패")
            return

        if product == 'deleted':
            print(f"[WARN] 상품 {goods_no}은(는) 삭제되었거나 존재하지 않습니다.")
            logger.warning(f"상품 {goods_no}은(는) 삭제됨")
            return

        print(f"[INFO] 상품 상세 정보 수집 완료: {product.get('name', 'N/A')}")

        # 2. 카테고리 번호 설정
        product['disp_cat_no'] = disp_cat_no

        # 3. 순위 정보 (단일 수집이므로 None)
        product['popularity_rank'] = None
        product['sales_rank'] = None

        # 4. 성분 정보 수집
        print("[INFO] 성분 정보 수집 중...")
        item_no = product.get('item_no', '001')
        collector.enrich_product_with_ingredients(product, item_no)
        print("[INFO] 성분 정보 수집 완료")

        # 5. DB 저장 (API + History 테이블)
        print("[INFO] DB 저장 중...")
        products_batch = [product]

        result = product_api.save_products(products_batch, save_to_history=True)

        if result.get('status') == 'success':
            print("[INFO] DB 저장 완료!")
            logger.info(f"단일 상품 {goods_no} 저장 성공")
        else:
            print(f"[ERROR] DB 저장 실패: {result.get('message')}")
            logger.error(f"단일 상품 {goods_no} 저장 실패: {result.get('message')}")

        # 결과 요약 출력
        print("\n=== 수집 결과 ===")
        print(f"상품번호: {goods_no}")
        print(f"상품명: {product.get('name', 'N/A')}")
        print(f"브랜드: {product.get('brand', 'N/A')}")
        print(f"가격: {product.get('price', {}).get('current', 'N/A')}원")
        print(f"평점: {product.get('rating', {}).get('text', 'N/A')}")
        print(f"리뷰수: {product.get('review_count', 'N/A')}")

    except Exception as e:
        print(f"[ERROR] 수집 중 오류 발생: {str(e)}")
        logger.error(f"단일 상품 수집 중 오류: {str(e)}")
    finally:
        collector.close()

        # 실행 시간 계산
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\n총 실행 시간: {duration}")
        logger.info(f"단일 상품 수집 종료. 실행 시간: {duration}")


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
        if len(sys.argv) > 1:
            if sys.argv[1] == "--reset":
                # 카테고리 상태 초기화 모드
                reset_category_status()
            elif sys.argv[1] == "--proxy":
                # 커맨드라인에서 프록시 사용 지정
                print("[INFO] 커맨드라인 옵션: 프록시 사용")
                collect_today_categories(use_proxy=True)
            elif sys.argv[1] == "--local":
                # 커맨드라인에서 로컬 사용 지정
                print("[INFO] 커맨드라인 옵션: 로컬 연결")
                collect_today_categories(use_proxy=False)
            elif sys.argv[1] == "--day":
                # 특정 요일 지정 실행 (예: python main.py --day 4)
                if len(sys.argv) < 3:
                    print("[ERROR] 요일 번호를 입력하세요. (1=월 ~ 7=일)")
                    sys.exit(1)
                target_day = int(sys.argv[2])
                if target_day < 1 or target_day > 7:
                    print("[ERROR] 요일 번호는 1~7 사이여야 합니다. (1=월 ~ 7=일)")
                    sys.exit(1)
                day_names = ['월', '화', '수', '목', '금', '토', '일']
                print(f"[INFO] 커맨드라인 옵션: {day_names[target_day-1]}요일({target_day}) 카테고리 수집")
                # --proxy 추가 옵션 확인
                use_proxy = "--proxy" in sys.argv[3:]
                collect_today_categories(use_proxy=use_proxy, target_day=target_day)
            else:
                print(f"[ERROR] 알 수 없는 옵션: {sys.argv[1]}")
                print("\n사용법:")
                print("  python main.py              # 인터랙티브 모드")
                print("  python main.py --local      # 로컬 연결 사용")
                print("  python main.py --proxy      # 프록시 사용")
                print("  python main.py --day 4      # 특정 요일 카테고리 수집 (1=월 ~ 7=일)")
                print("  python main.py --reset      # 카테고리 상태 초기화")
                sys.exit(1)
        else:
            # 인터랙티브 모드 - 사용자가 선택
            mode = select_proxy_mode()
            if mode == 'single':
                # 단일 상품 수집 모드
                collect_single_product()
            elif mode == 'day':
                # 특정 요일 수집 모드
                day_names = ['월', '화', '수', '목', '금', '토', '일']
                print("\n=== 특정 요일 카테고리 수집 ===")
                for i, name in enumerate(day_names, 1):
                    print(f"  {i}. {name}요일")
                target_day = input("\n요일 번호를 입력하세요 (1=월 ~ 7=일): ").strip()
                try:
                    target_day = int(target_day)
                    if target_day < 1 or target_day > 7:
                        print("[ERROR] 요일 번호는 1~7 사이여야 합니다.")
                    else:
                        print(f"\n[INFO] {day_names[target_day-1]}요일({target_day}) 카테고리 수집을 시작합니다.")
                        collect_today_categories(use_proxy=False, target_day=target_day)
                except ValueError:
                    print("[ERROR] 숫자를 입력해주세요.")
            else:
                # 카테고리 전체 수집 모드
                collect_today_categories(use_proxy=mode)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램 중단")
    except Exception as e:
        logger.critical(f"프로그램 실행 중 심각한 오류 발생: {str(e)}")
        sys.exit(1)

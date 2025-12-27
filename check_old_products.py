"""
오래된 품번 판매 여부 확인 스크립트
============================================

[목적]
- DB에서 N일 이상 업데이트 안된 품번 조회
- 올리브영 사이트에서 실제 판매 여부 확인
- 삭제된 품번에 대한 UPDATE 쿼리 자동 생성

[사용법]
python check_old_products.py

[사용자 입력]
1. 기간 입력 (기본: 180일) - 엔터 시 기본값 적용
2. Webshare 프록시 사용 여부 (기본: Y) - 엔터 시 프록시 사용
3. 진행 확인 (기본: Y) - 엔터 시 진행

[출력 파일]
deleted_check_reports/
├── check_YYYYMMDD_HHMMSS.csv           # 전체 결과 (goods_no, status, name, last_updated, days_ago)
├── update_deleted_YYYYMMDD_HHMMSS.sql  # 삭제 품번 UPDATE 쿼리 (del_yn = 'Y')
└── update_selling_YYYYMMDD_HHMMSS.sql  # 판매중 품번 UPDATE 쿼리 (del_yn = 'N', updated_at 갱신)

[UPDATE 쿼리 형식]
-- 삭제된 품번
UPDATE cosmetics_products SET del_yn = 'Y' WHERE goods_no IN (...);
-- 판매중 품번 (updated_at 갱신용 - 다음 체크에서 제외됨)
UPDATE cosmetics_products SET del_yn = 'N' WHERE goods_no IN (...);
※ updated_at은 DB가 ON UPDATE CURRENT_TIMESTAMP로 자동 갱신

[조회 조건]
- cosmetics_products.updated_at이 N일 이상 오래됨
- del_yn = 'N' 또는 NULL인 품번만 (이미 삭제 처리된 품번 제외)

[상태값]
- selling: 판매중
- deleted: 삭제됨 (페이지 없음 또는 가격 정보 없음)
"""
import os
import csv
from datetime import datetime
from sqlalchemy import text

from config.session import CosmeticsSession
from collectors.oliveyoung_collector_curl import OliveYoungCollectorCurl
from utils.logger import setup_logger

logger = setup_logger(__name__, "check_old_products.log")

# 결과 저장 폴더
REPORT_DIR = "deleted_check_reports"


def get_user_input():
    """
    사용자 입력을 받아 설정값 반환

    Returns:
        dict: {days: int, use_proxy: bool}
    """
    print("\n" + "=" * 50)
    print("오래된 품번 판매 여부 확인")
    print("=" * 50)

    # 1. 기간 입력 (기본값: 180일)
    days_input = input("\n1. 며칠 동안 업데이트 안된 품번을 확인할까요? (기본: 180): ").strip()
    days = int(days_input) if days_input else 180

    # 2. 프록시 사용 여부 (기본값: Y)
    proxy_input = input("2. Webshare 프록시를 사용하시겠습니까? (Y/n): ").strip().lower()
    use_proxy = proxy_input != 'n'

    return {"days": days, "use_proxy": use_proxy}


def get_old_products_from_db(days: int):
    """
    DB에서 N일 이상 업데이트 안된 품번 목록 조회

    Args:
        days: 기준 일수

    Returns:
        list: [{goods_no, name, last_updated, days_ago}, ...]
    """
    session = CosmeticsSession()

    try:
        # cosmetics_products 테이블 기준으로 조회 (메인 테이블)
        # updated_at이 N일 이상 지난 품번만 대상
        # del_yn = 'N'인 품번만 (이미 삭제 처리된 품번 제외)
        query = text("""
            SELECT
                p.goods_no,
                p.name,
                p.updated_at as last_updated,
                DATEDIFF(NOW(), p.updated_at) as days_ago
            FROM cosmetics_products p
            WHERE DATEDIFF(NOW(), p.updated_at) > :days
              AND (p.del_yn IS NULL OR p.del_yn = 'N')
            ORDER BY days_ago DESC
        """)

        result = session.execute(query, {"days": days})

        products = []
        for row in result:
            products.append({
                "goods_no": row[0],
                "name": row[1],
                "last_updated": row[2].strftime("%Y-%m-%d") if row[2] else "",
                "days_ago": row[3]
            })

        return products

    except Exception as e:
        logger.error(f"DB 조회 중 오류: {e}")
        return []
    finally:
        session.close()


def check_products(products: list, use_proxy: bool = True):
    """
    품번 리스트의 판매 여부 확인

    Args:
        products: DB에서 조회한 품번 정보 리스트
        use_proxy: Webshare 프록시 사용 여부

    Returns:
        list: 확인 결과 리스트
    """
    results = []

    # 수집기 초기화
    try:
        collector = OliveYoungCollectorCurl(use_proxy=use_proxy)
        print(f"[INFO] 수집기 초기화 완료 (프록시: {'Yes' if use_proxy else 'No'})\n")
    except Exception as e:
        print(f"[ERROR] 수집기 초기화 실패: {e}")
        return results

    try:
        total = len(products)

        for i, product in enumerate(products, 1):
            goods_no = product["goods_no"]
            name = product["name"]

            print(f"[{i}/{total}] {goods_no} 확인 중...")

            # 상품 상세 정보 수집 시도 (리뷰 API 호출 건너뜀 - 판매여부만 확인)
            result = collector.collect_product_detail(goods_no, skip_review=True)

            # 결과 판정
            if result == 'deleted':
                status = "deleted"
                print(f"  → ❌ 삭제됨")
            elif result is None:
                # 수집 실패 = 삭제된 것으로 간주
                status = "deleted"
                print(f"  → ❌ 삭제됨 (수집 실패)")
            else:
                status = "selling"
                current_name = result.get('name', name)[:40]
                price = result.get('price', {}).get('current', 'N/A')
                print(f"  → ✅ 판매중: {current_name}... ({price}원)")

            # 결과 저장
            results.append({
                "goods_no": goods_no,
                "status": status,
                "name": name[:100] if name else "",  # 이름 길이 제한
                "last_updated": product["last_updated"],
                "days_ago": product["days_ago"]
            })

            print()

    except KeyboardInterrupt:
        print("\n[INFO] 사용자에 의해 중단됨")
    except Exception as e:
        print(f"[ERROR] 확인 중 오류: {e}")
        logger.error(f"확인 중 오류: {e}")
    finally:
        collector.close()

    return results


def save_to_csv(results: list, timestamp: str = None):
    """
    결과를 CSV 파일로 저장

    Args:
        results: 확인 결과 리스트
        timestamp: 파일명에 사용할 타임스탬프 (None이면 현재 시간)

    Returns:
        str: 저장된 파일 경로
    """
    # 폴더 생성
    os.makedirs(REPORT_DIR, exist_ok=True)

    # 파일명 생성
    if not timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"check_{timestamp}.csv"
    filepath = os.path.join(REPORT_DIR, filename)

    # CSV 저장
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=["goods_no", "status", "name", "last_updated", "days_ago"])
        writer.writeheader()
        writer.writerows(results)

    return filepath


def save_update_queries(results: list, timestamp: str = None):
    """
    삭제/판매중 품번에 대한 UPDATE 쿼리를 SQL 파일로 저장

    Args:
        results: 확인 결과 리스트
        timestamp: 파일명에 사용할 타임스탬프 (None이면 현재 시간)

    Returns:
        dict: {'deleted': filepath, 'selling': filepath} 또는 빈 dict
    """
    # 품번 분류
    deleted_goods = [r["goods_no"] for r in results if r["status"] == "deleted"]
    selling_goods = [r["goods_no"] for r in results if r["status"] == "selling"]

    if not deleted_goods and not selling_goods:
        return {}

    # 폴더 생성
    os.makedirs(REPORT_DIR, exist_ok=True)

    # 타임스탬프 설정
    if not timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    result_files = {}

    # 삭제된 품번 SQL 생성
    if deleted_goods:
        filename = f"update_deleted_{timestamp}.sql"
        filepath = os.path.join(REPORT_DIR, filename)

        goods_list = ", ".join([f"'{gno}'" for gno in deleted_goods])
        query = f"UPDATE cosmetics_products SET del_yn = 'Y' WHERE goods_no IN ({goods_list});"

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- 삭제된 품번 UPDATE 쿼리\n")
            f.write(f"-- 생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- 대상 품번 수: {len(deleted_goods)}개\n")
            f.write(f"-- updated_at은 DB가 자동 갱신함\n\n")
            f.write(query)

        result_files['deleted'] = filepath

    # 판매중 품번 SQL 생성 (updated_at 갱신용)
    if selling_goods:
        filename = f"update_selling_{timestamp}.sql"
        filepath = os.path.join(REPORT_DIR, filename)

        goods_list = ", ".join([f"'{gno}'" for gno in selling_goods])
        query = f"UPDATE cosmetics_products SET del_yn = 'N' WHERE goods_no IN ({goods_list});"

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- 판매중 품번 UPDATE 쿼리 (updated_at 갱신용)\n")
            f.write(f"-- 생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- 대상 품번 수: {len(selling_goods)}개\n")
            f.write(f"-- updated_at은 DB가 자동 갱신함\n\n")
            f.write(query)

        result_files['selling'] = filepath

    return result_files


def print_summary(results: list, csv_filepath: str, sql_files: dict = None):
    """
    결과 요약 출력
    """
    selling_count = sum(1 for r in results if r["status"] == "selling")
    deleted_count = sum(1 for r in results if r["status"] == "deleted")

    print("\n" + "=" * 50)
    print("결과 요약")
    print("=" * 50)
    print(f"✅ 판매중: {selling_count}개")
    print(f"❌ 삭제됨: {deleted_count}개")
    print(f"\n📁 CSV 결과: {csv_filepath}")

    # SQL 파일이 생성된 경우 안내
    if sql_files:
        if sql_files.get('deleted'):
            print(f"📁 삭제 SQL: {sql_files['deleted']}")
        if sql_files.get('selling'):
            print(f"📁 판매중 SQL: {sql_files['selling']}")
        print(f"\n💡 SQL 파일들을 DB에서 직접 실행하세요.")


def main():
    """메인 함수"""
    print(f"\n시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. 사용자 입력
    config = get_user_input()
    days = config["days"]
    use_proxy = config["use_proxy"]

    # 2. DB에서 품번 조회
    print(f"\n[INFO] DB에서 {days}일 이상 업데이트 안된 품번 조회 중...")
    products = get_old_products_from_db(days)

    if not products:
        print("[INFO] 해당 조건에 맞는 품번이 없습니다.")
        return

    # 3. 진행 확인
    confirm = input(f"\n3. 총 {len(products)}개 품번이 대상입니다. 진행하시겠습니까? (Y/n): ").strip().lower()
    if confirm == 'n':
        print("[INFO] 취소되었습니다.")
        return

    print(f"\n[INFO] {len(products)}개 품번 확인 시작...\n")

    # 4. 품번 확인
    results = check_products(products, use_proxy=use_proxy)

    if not results:
        print("[INFO] 확인된 결과가 없습니다.")
        return

    # 5. CSV 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filepath = save_to_csv(results, timestamp)

    # 6. SQL 파일 생성 (삭제/판매중 품번)
    sql_files = save_update_queries(results, timestamp)

    # 7. 결과 요약
    print_summary(results, csv_filepath, sql_files)

    print(f"\n종료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()

"""
재시도 관련 유틸리티 함수 모듈
"""
import logging
from datetime import datetime, date
from sqlalchemy import text, select, func
from sqlalchemy.exc import SQLAlchemyError

from models.database import CosmeticsProductHistory, CosmBrand

logger = logging.getLogger(__name__)

def get_current_iso_week():
    """
    현재 날짜의 ISO 연도와 주차를 반환
    
    Returns:
        tuple: (year, week_of_year)
    """
    now = datetime.now()
    year, week_of_year, _ = now.isocalendar()
    return year, week_of_year

def get_previous_iso_week(current_year, current_week):
    """
    이전 주차의 ISO 연도와 주차를 반환
    
    Args:
        current_year (int): 현재 연도
        current_week (int): 현재 주차
        
    Returns:
        tuple: (previous_year, previous_week)
    """
    if current_week > 1:
        return current_year, current_week - 1
    else:
        # 연도를 넘어가는 경우 (1주차에서 이전 주차는 이전 연도의 마지막 주차)
        previous_year = current_year - 1
        
        # 이전 연도의 마지막 주차 계산 (12월 28일은 항상 마지막 주차에 포함됨)
        last_day_of_previous_year = date(previous_year, 12, 28)
        _, last_week_of_previous_year, _ = last_day_of_previous_year.isocalendar()
        
        return previous_year, last_week_of_previous_year

def find_missing_products(session, prev_year, prev_week, curr_year, curr_week):
    """
    이전 주차에는 있었으나 현재 주차에는 누락된 제품 목록을 조회
    
    Args:
        session: 데이터베이스 세션
        prev_year (int): 이전 연도
        prev_week (int): 이전 주차
        curr_year (int): 현재 연도
        curr_week (int): 현재 주차
        
    Returns:
        list: 누락된 제품 정보 목록 (dict 형태)
    """
    try:
        # 브랜드 정보를 포함한 SQL 쿼리 작성 (LEFT JOIN으로 브랜드 정보 가져오기, 단 지난주 판매순위 100위 이내만)
        query = text("""
            SELECT DISTINCT 
                prev.goods_no,
                prev.name,
                b.name AS brand_name,
                prev.brandId,
                prev.disp_cat_no,
                prev.product_url,
                prev.popularity_rank AS last_week_popularity_rank,
                prev.sales_rank      AS last_week_sales_rank
            FROM cosmetics_products_history AS prev
            LEFT JOIN cosmetics_brands AS b
                ON prev.brandId = b.id
            LEFT JOIN cosmetics_products_history AS curr
                ON prev.goods_no      = curr.goods_no
                AND prev.disp_cat_no  = curr.disp_cat_no
                AND curr.year         = :curr_year
                AND curr.week_of_year = :curr_week
            JOIN cosmetics_products AS p  /* 추가된 JOIN */
                ON prev.goods_no = p.goods_no
            WHERE prev.year          = :prev_year
            AND prev.week_of_year    = :prev_week
            AND prev.sales_rank     <= 100
            AND curr.goods_no IS NULL
            AND p.del_yn = 'N'  /* 삭제되지 않은 상품만 필터링 */
        """)
        
        # 쿼리 실행
        result = session.execute(
            query, 
            {
                'prev_year': prev_year, 
                'prev_week': prev_week, 
                'curr_year': curr_year, 
                'curr_week': curr_week
            }
        )
        
        # 결과를 딕셔너리 목록으로 변환
        missing_products = []
        for row in result:
            missing_products.append({
                'goods_no': row.goods_no,
                'name': row.name,
                'brand': row.brand_name,  # 브랜드 이름 사용
                'brandId': row.brandId,   # 브랜드 ID도 추가
                'disp_cat_no': row.disp_cat_no
            })
        
        logger.info(f"누락된 제품 {len(missing_products)}개 발견: {prev_year}년 {prev_week}주차 → {curr_year}년 {curr_week}주차")
        return missing_products
        
    except SQLAlchemyError as e:
        logger.error(f"누락된 제품 조회 중 오류 발생: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"예기치 않은 오류 발생: {str(e)}")
        return []

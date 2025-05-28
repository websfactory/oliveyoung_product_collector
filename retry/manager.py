"""
재시도 관리자 모듈
"""
import logging
import time  # time 모듈 추가
from datetime import datetime
from sqlalchemy import select, update, and_
from sqlalchemy.exc import SQLAlchemyError

from models.database import CosmeticsProductsHistoryRetries
from retry.utils import get_current_iso_week, get_previous_iso_week, find_missing_products

logger = logging.getLogger(__name__)

class RetryManager:
    """
    누락된 제품의 재수집 시도를 관리하는 클래스
    """
    
    def __init__(self, collector, session):
        """
        RetryManager 초기화
        
        Args:
            collector: OliveYoungCollector 인스턴스
            session: 데이터베이스 세션
        """
        self.collector = collector
        self.session = session
        self.retry_delay_seconds = 3  # 재시도 간 대기 시간 (초)
        self.missing_product_details_map = {}  # 누락된 제품 정보를 저장할 맵 (메모리 효율화)
        logger.info("RetryManager 초기화 완료")
    
    def process_missing_products(self):
        """
        누락된 제품들을 찾아 재수집 프로세스를 실행
        
        Returns:
            dict: 처리 결과
        """
        logger.info("누락된 제품 처리 시작")
        
        try:
            # 1. 현재 주차 및 이전 주차 계산
            current_year, current_week = get_current_iso_week()
            previous_year, previous_week = get_previous_iso_week(current_year, current_week)
            
            logger.info(f"처리 대상 주차: {current_year}년 {current_week}주차 (이전 주차: {previous_year}년 {previous_week}주차)")
            
            # 2. 누락된 제품 목록 조회
            missing_products = find_missing_products(
                self.session, previous_year, previous_week, current_year, current_week
            )
            
            # 결과를 메모리 맵에 저장 (goods_no, disp_cat_no)를 키로 사용하여 효율적 조회 지원
            self.missing_product_details_map = {
                (p['goods_no'], p['disp_cat_no']): p for p in missing_products
            }
            
            if not missing_products:
                logger.info("지난 주 대비 누락된 제품이 없습니다.")
                return {"success": True, "success_count": 0, "fail_count": 0, "message": "누락된 제품 없음"}
            
            logger.info(f"총 {len(missing_products)}개의 누락된 제품에 대한 재시도를 시작합니다.")
            
            # 3. 재시도 테이블에 레코드 생성
            self._create_retry_records(missing_products, current_year, current_week)
            
            # 4. 재시도 큐 처리
            result = self._process_retry_queue(current_year, current_week)
            
            # 처리 완료 후 메모리 정리
            self.missing_product_details_map = {}
            
            return result
            
        except Exception as e:
            logger.error(f"누락된 제품 처리 중 오류 발생: {str(e)}")
            return {"success": False, "success_count": 0, "fail_count": 0, "message": f"오류: {str(e)}"}
    
    def _create_retry_records(self, missing_products, target_year, target_week):
        """
        누락된 제품들을 재시도 테이블에 등록
        
        Args:
            missing_products (list): 누락된 제품 목록
            target_year (int): 목표 연도
            target_week (int): 목표 주차
        """
        try:
            # 이미 등록된 레코드 확인 (중복 방지)
            existing_records = {}
            
            # goods_no, disp_cat_no, target_year, target_week로 이미 존재하는 레코드 조회
            query = select(CosmeticsProductsHistoryRetries.goods_no, CosmeticsProductsHistoryRetries.disp_cat_no) \
                .where(and_(
                    CosmeticsProductsHistoryRetries.target_year == target_year,
                    CosmeticsProductsHistoryRetries.target_week_of_year == target_week
                ))
            
            for row in self.session.execute(query).fetchall():
                key = f"{row[0]}_{row[1]}"
                existing_records[key] = True
            
            # 새 레코드 등록 (중복 제외)
            new_records = []
            skipped_count = 0
            
            for product in missing_products:
                key = f"{product['goods_no']}_{product['disp_cat_no']}"
                
                if key not in existing_records:
                    retry_record = CosmeticsProductsHistoryRetries(
                        goods_no=product['goods_no'],
                        disp_cat_no=product['disp_cat_no'],
                        target_year=target_year,
                        target_week_of_year=target_week,
                        status='pending',
                        attempt_count=0,
                        max_attempts=3,  # 기본값 3회
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    new_records.append(retry_record)
                else:
                    skipped_count += 1
            
            # 새 레코드가 있으면 일괄 삽입
            if new_records:
                self.session.bulk_save_objects(new_records)
                self.session.commit()
                logger.info(f"{len(new_records)}개 제품의 재시도 레코드 생성 완료 (중복 {skipped_count}개 건너뜀)")
            else:
                logger.info(f"생성할 새 레코드 없음 (이미 존재하는 레코드 {skipped_count}개)")
                
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"재시도 레코드 생성 중 DB 오류: {str(e)}")
        except Exception as e:
            self.session.rollback()
            logger.error(f"재시도 레코드 생성 중 예기치 않은 오류: {str(e)}")
    
    def _group_products_by_category(self, retry_items):
        """
        재시도 항목들을 카테고리별로 그룹화
        
        Args:
            retry_items (list): 재시도 항목 목록
            
        Returns:
            dict: {카테고리_ID: set(상품_ID들)}
        """
        goods_by_cat = {}
        
        for item in retry_items:
            cat_id = item.disp_cat_no
            goods_no = item.goods_no
            
            if cat_id not in goods_by_cat:
                goods_by_cat[cat_id] = set()
            
            goods_by_cat[cat_id].add(goods_no)
        
        return goods_by_cat
    
    def _mark_category_products_as_deleted(self, category_id, items):
        """
        카테고리 내 모든 상품을 'product_deleted'로 표시
        
        Args:
            category_id (str): 카테고리 ID
            items (list): 해당 카테고리의 retry_item 목록
            
        Returns:
            int: 업데이트된 상품 수
        """
        try:
            for retry_item in items:
                retry_item.status = 'product_deleted'
                retry_item.error_message = "카테고리가 비어있어 상품이 삭제된 것으로 판단됨"
                retry_item.updated_at = datetime.now()
                retry_item.last_attempt_at = datetime.now()
                
            self.session.commit()
            logger.info(f"카테고리 {category_id}의 {len(items)}개 상품을 '상품 삭제됨'으로 표시")
            return len(items)
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"카테고리 {category_id} 상품 상태 업데이트 중 DB 오류: {str(e)}")
            return 0
        except Exception as e:
            self.session.rollback()
            logger.error(f"카테고리 {category_id} 상품 상태 업데이트 중 예기치 않은 오류: {str(e)}")
            return 0
            
    def _process_retry_queue(self, target_year, target_week):
        """
        재시도 큐 처리 (status가 pending이고 attempt_count < max_attempts인 항목들)
        
        Args:
            target_year (int): 목표 연도
            target_week (int): 목표 주차
            
        Returns:
            dict: 처리 결과
        """
        try:
            # 1. pending/failed & attempt_count < max_attempts인 항목 조회
            retry_items = self.session.execute(
                select(CosmeticsProductsHistoryRetries)
                .where(
                    and_(
                        CosmeticsProductsHistoryRetries.target_year == target_year,
                        CosmeticsProductsHistoryRetries.target_week_of_year == target_week,
                        CosmeticsProductsHistoryRetries.status.in_(["pending", "failed"]),
                        CosmeticsProductsHistoryRetries.attempt_count < CosmeticsProductsHistoryRetries.max_attempts,
                    )
                )
                .order_by(CosmeticsProductsHistoryRetries.created_at)
            ).scalars().all()
            
            if not retry_items:
                logger.info("처리할 재시도 항목이 없습니다.")
                return {"success": True, "success_count": 0, "fail_count": 0, "message": "처리할 항목 없음"}
                
            logger.info(f"{len(retry_items)}개 항목에 대한 재시도 처리를 시작합니다.")
            
            # 2. 카테고리별로 항목 그룹화 (cat_id → List[retry_item])
            from collections import defaultdict

            items_by_cat = defaultdict(list)
            for item in retry_items:
                items_by_cat[item.disp_cat_no].append(item)
                
            logger.info(f"{len(items_by_cat)}개 카테고리에 걸쳐 전체 {len(retry_items)}개 상품을 처리합니다.")

            total_success = total_fail = total_deleted = total_brand_used = 0

            # 3. 카테고리별 순회
            for cat_id, items in items_by_cat.items():
                goods_set = {it.goods_no for it in items}
                logger.info(f"카테고리 {cat_id}: {len(items)}개 항목 처리 시작")

                # 카테고리별 통계 카운터 초기화
                cat_success = cat_fail = cat_deleted = cat_brand_used = 0
                
                # 3‑1. 순위 수집
                try:
                    # 인기도 순 순위 수집
                    pop_rankings = self.collector.collect_rankings(cat_id, goods_set)
                    
                    # 빈 카테고리 감지 및 처리 (추가된 부분)
                    if isinstance(pop_rankings, dict) and pop_rankings.get('category_empty', False):
                        logger.warning(f"카테고리 {cat_id}가 비어있습니다. 모든 관련 상품을 '삭제됨'으로 표시합니다.")
                        deleted_count = self._mark_category_products_as_deleted(cat_id, items)
                        cat_deleted += deleted_count
                        total_deleted += deleted_count
                        continue  # 다음 카테고리로 넘어감
                    
                    logger.info(f"인기도 순 순위 수집 결과: {len(pop_rankings)}/{len(goods_set)}개 상품 찾음")
                    
                    # 판매량 순 순위 수집
                    sales_rankings = self.collector.collect_rankings(cat_id, goods_set, sort_type="03")
                    
                    # 판매량 순 결과에서도 카테고리 비어있음 확인 (이중 확인)
                    if isinstance(sales_rankings, dict) and sales_rankings.get('category_empty', False):
                        logger.warning(f"카테고리 {cat_id}가 비어있습니다. 모든 관련 상품을 '삭제됨'으로 표시합니다.")
                        deleted_count = self._mark_category_products_as_deleted(cat_id, items)
                        cat_deleted += deleted_count
                        total_deleted += deleted_count
                        continue  # 다음 카테고리로 넘어감
                    
                    logger.info(f"판매량 순 순위 수집 결과: {len(sales_rankings)}/{len(goods_set)}개 상품 찾음")
                except Exception as e:
                    logger.error(f"카테고리 {cat_id} 순위 수집 실패: {str(e)}", exc_info=True)
                    # 순위 수집에 실패해도 다음 카테고리로
                    continue

                # 카테고리별 통계 카운터 초기화
                cat_success = cat_fail = cat_deleted = cat_brand_used = 0
                
                # 3‑2. 각 retry_item 처리
                for idx, retry_item in enumerate(items, 1):
                    goods_no = retry_item.goods_no
                    
                    logger.info(f"[{idx}/{len(items)}] 제품 재시도: {goods_no} (카테고리: {cat_id}, 시도: {retry_item.attempt_count + 1}/{retry_item.max_attempts})")
                    
                    # 상태 업데이트: processing으로 변경
                    retry_item.status = "processing"
                    retry_item.attempt_count += 1
                    retry_item.last_attempt_at = datetime.now()
                    self.session.commit()

                    # 제품 정보와 순위 데이터 준비
                    key = (goods_no, cat_id)
                    rankings = {
                        "popularity_rank": pop_rankings.get(goods_no),
                        "sales_rank": sales_rankings.get(goods_no),
                    }
                    
                    # 메모리 맵에서 브랜드 ID 조회
                    product_details = self.missing_product_details_map.get((goods_no, cat_id))
                    brand_id = product_details.get('brandId') if product_details else None
                    
                    if brand_id is not None:
                        cat_brand_used += 1
                        total_brand_used += 1
                        logger.info(f"제품 {goods_no}의 이전 브랜드 ID: {brand_id} 활용 (메모리 맵)")
                    
                    if rankings and (rankings.get('popularity_rank') or rankings.get('sales_rank')):
                        logger.info(f"제품 {goods_no} (카테고리: {cat_id})의 순위 정보 발견: "  
                                   f"인기도={rankings.get('popularity_rank')}, 판매량={rankings.get('sales_rank')}")
                    else:
                        logger.warning(f"제품 {goods_no} (카테고리: {cat_id})의 순위 정보를 찾지 못했습니다.")

                    try:
                        # 제품 수집 시도
                        result = self.collector.collect_and_save_single_product(
                            goods_no, 
                            cat_id, 
                            target_year, 
                            target_week,
                            rankings,
                            brand_id
                        )

                        # 수정된 결과 처리 로직
                        if result == 'deleted':
                            # 삭제된 상품 처리
                            retry_item.status = 'product_deleted'
                            retry_item.error_message = "상품이 삭제되었거나 더 이상 존재하지 않습니다."
                            self.session.commit()
                            logger.info(f"  => 상품 삭제 확인: 제품 {goods_no}은(는) 더 이상 존재하지 않습니다.")
                            cat_deleted += 1
                            total_deleted += 1
                        elif result is True:
                            # 성공 시 상태 업데이트
                            retry_item.status = 'success'
                            retry_item.error_message = None
                            self.session.commit()
                            logger.info(f"  => 성공: 제품 {goods_no} 수집 완료")
                            cat_success += 1
                            total_success += 1
                        else:
                            # 실패 시 상태 업데이트
                            if retry_item.attempt_count >= retry_item.max_attempts:
                                retry_item.status = 'max_retries_reached'
                                logger.warning(f"  => 최종 실패: 제품 {goods_no}의 최대 재시도 횟수 도달")
                            else:
                                retry_item.status = 'failed'
                                logger.warning(f"  => 실패: 제품 {goods_no} 수집 실패, 다음 실행 시 재시도 예정")
                            
                            retry_item.error_message = "제품 수집 또는 저장 실패"
                            self.session.commit()
                            cat_fail += 1
                            total_fail += 1
                            
                    except Exception as e:
                        # 예외 발생 시 상태 업데이트
                        if retry_item.attempt_count >= retry_item.max_attempts:
                            retry_item.status = 'max_retries_reached'
                            logger.error(f"  => 최종 실패 (예외): 제품 {goods_no}의 최대 재시도 횟수 도달")
                        else:
                            retry_item.status = 'failed'
                            logger.error(f"  => 실패 (예외): 제품 {goods_no} 처리 중 오류 발생, 다음 실행 시 재시도 예정")
                        
                        retry_item.error_message = f"오류: {str(e)}"
                        self.session.commit()
                        cat_fail += 1
                        total_fail += 1
                    
                    # 다음 항목 처리 전 대기 (서버 부하 방지)
                    time.sleep(self.retry_delay_seconds)  
                # 3‑3. 카테고리 메모리 해제 및 완료 로깅
                del pop_rankings, sales_rankings
                logger.info(f"카테고리 {cat_id} 처리 완료: 성공={cat_success}, 실패={cat_fail}, 삭제={cat_deleted}, 브랜드ID 활용={cat_brand_used}")
                
                # 서버 부하 방지를 위한 카테고리 간 지연
                time.sleep(3.0) 

            logger.info(
                f"전체 재시도 완료: 성공={total_success}, 실패={total_fail}, 삭제={total_deleted}, "
                f"브랜드ID 활용={total_brand_used}"
            )
            
            return {
                "success": True,
                "success_count": total_success,
                "fail_count": total_fail,
                "deleted_count": total_deleted,
                "with_brand_id_count": total_brand_used,
                "message": f"처리 완료: {total_success}개 성공, {total_fail}개 실패, {total_deleted}개 삭제됨"
            }
            
        except Exception as e:
            logger.error(f"재시도 큐 처리 중 오류 발생: {str(e)}", exc_info=True)
            return {
                "success": False,
                "success_count": 0,
                "fail_count": 0,
                "message": f"오류: {str(e)}"
            }

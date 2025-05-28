#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cloudscraper 적용 테스트 스크립트

올리브영 웹사이트 접속과 기본 페이지 로딩이 정상적으로 동작하는지 확인합니다.
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python path에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from collectors.oliveyoung_collector import OliveYoungCollector
from utils.logger import setup_logger

def test_cloudscraper_connection():
    """
    cloudscraper를 사용한 올리브영 연결 테스트
    """
    logger = setup_logger(__name__, "test_cloudscraper.log")
    
    try:
        logger.info("=" * 50)
        logger.info("cloudscraper 연결 테스트 시작")
        logger.info("=" * 50)
        
        # OliveYoungCollector 초기화 (cloudscraper 적용됨)
        collector = OliveYoungCollector()
        
        logger.info("✓ OliveYoungCollector 초기화 성공")
        
        # 기본 카테고리 목록 페이지 테스트
        test_category_id = "100000100030005"  # 실제 카테고리
        
        logger.info(f"카테고리 {test_category_id} 첫 페이지 테스트 중...")
        
        # 첫 페이지만 요청해서 연결 확인
        from config.settings import OLIVEYOUNG_CATEGORY_URL, REQUEST_TIMEOUT
        category_url = f"{OLIVEYOUNG_CATEGORY_URL}{test_category_id}&rowsPerPage=48"
        
        response = collector._get_with_delay(category_url, timeout=REQUEST_TIMEOUT)
        
        if response.ok:
            logger.info(f"✓ 카테고리 페이지 접속 성공: HTTP {response.status_code}")
            logger.info(f"✓ 응답 크기: {len(response.text)} 바이트")
            
            # HTML 내용 일부 확인
            if "oliveyoung" in response.text.lower():
                logger.info("✓ 올리브영 사이트 응답 확인됨")
            else:
                logger.warning("⚠ 예상과 다른 응답 내용")
                
            # 쿠키 정보 출력
            logger.info(f"✓ 설정된 쿠키 수: {len(collector.session.cookies)}")
            for cookie in collector.session.cookies:
                logger.debug(f"쿠키: {cookie.name}={cookie.value[:50]}...")
                
        else:
            logger.error(f"✗ 카테고리 페이지 접속 실패: HTTP {response.status_code}")
            return False
            
        # 상품 상세 페이지 테스트 (실제 상품번호)
        test_goods_no = "A000000000207"
        logger.info(f"상품 {test_goods_no} 상세 페이지 테스트 중...")
        
        product_info = collector.collect_product_detail(test_goods_no)
        
        if product_info and product_info != 'deleted':
            logger.info("✓ 상품 상세 정보 수집 성공")
            logger.info(f"  - 브랜드: {product_info.get('brand', 'N/A')}")
            logger.info(f"  - 상품명: {product_info.get('name', 'N/A')[:50]}...")
            logger.info(f"  - 가격: {product_info.get('price', 'N/A')}")
        elif product_info == 'deleted':
            logger.info("✓ 삭제된 상품 감지 정상 동작")
        else:
            logger.warning("⚠ 상품 상세 정보 수집 실패 (연결은 성공)")
            
        logger.info("=" * 50)
        logger.info("cloudscraper 연결 테스트 완료 - 성공")
        logger.info("=" * 50)
        
        collector.close()
        return True
        
    except Exception as e:
        logger.error("=" * 50)
        logger.error(f"cloudscraper 연결 테스트 실패: {str(e)}")
        logger.error("=" * 50)
        
        # 상세 오류 정보 출력
        import traceback
        logger.error("상세 오류 정보:")
        logger.error(traceback.format_exc())
        
        return False

if __name__ == "__main__":
    success = test_cloudscraper_connection()
    
    if success:
        print("✓ cloudscraper 테스트 성공!")
        print("로그 파일을 확인하여 자세한 정보를 확인하세요.")
        sys.exit(0)
    else:
        print("✗ cloudscraper 테스트 실패!")
        print("로그 파일을 확인하여 오류 원인을 파악하세요.")
        sys.exit(1)
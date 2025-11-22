#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
수정된 수집기 테스트 스크립트
parse_meta_info() 및 fetch_review_info() 테스트
"""

import os
import sys
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collectors.oliveyoung_collector_curl import OliveYoungCollectorCurl
from utils.logger import setup_logger

# 로거 설정
logger = setup_logger(__name__, "test_collector.log")

def test_single_product():
    """
    단일 제품으로 수정된 수집기 테스트
    """
    print("\n" + "="*60)
    print("올리브영 수집기 테스트 - 수정된 버전")
    print("="*60)

    # 테스트할 상품 번호
    test_goods_no = "A000000176342"
    print(f"\n테스트 상품: {test_goods_no}")

    try:
        # 수집기 초기화
        print("\n1. 수집기 초기화...")
        collector = OliveYoungCollectorCurl(use_proxy=False)
        print("   ✓ 수집기 초기화 완료")

        # 상품 상세 정보 수집
        print(f"\n2. 상품 {test_goods_no} 상세 정보 수집...")
        product_info = collector.collect_product_detail(test_goods_no)

        if not product_info or product_info == 'deleted':
            print(f"   ✗ 상품 정보 수집 실패 또는 삭제됨")
            return False

        print("   ✓ 상품 정보 수집 완료")

        # 결과 출력
        print("\n3. 수집된 정보:")
        print(f"   브랜드: {product_info.get('brand', 'N/A')}")
        print(f"   상품명: {product_info.get('name', 'N/A')[:50]}...")
        print(f"   카테고리: {product_info.get('disp_cat_no', 'N/A')}")

        # 가격 정보
        price = product_info.get('price', {})
        print(f"   정상가: {price.get('original', 'N/A')}")
        print(f"   판매가: {price.get('current', 'N/A')}")

        # 리뷰 정보
        print(f"   리뷰수: {product_info.get('review_count', 'N/A')}")
        rating = product_info.get('rating', {})
        if rating:
            print(f"   평점: {rating.get('text', 'N/A')} ({rating.get('percent', 'N/A')}%)")
        else:
            print(f"   평점: N/A")

        # 이미지 URL
        image_url = product_info.get('image_url', 'N/A')
        if image_url != 'N/A':
            print(f"   이미지: {image_url[:50]}...")

        # 필수 필드 검증
        print("\n4. 필수 필드 검증:")
        required_fields = ['brand', 'name', 'price']
        all_valid = True

        for field in required_fields:
            if field == 'price':
                # 가격은 dict이고 내부 값 확인
                price_info = product_info.get(field, {})
                has_price = price_info.get('original') or price_info.get('current')
                status = "✓" if has_price else "✗"
                print(f"   {status} {field}: {has_price if has_price else '없음'}")
                if not has_price:
                    all_valid = False
            else:
                value = product_info.get(field)
                status = "✓" if value else "✗"
                print(f"   {status} {field}: {'있음' if value else '없음'}")
                if not value:
                    all_valid = False

        # 리뷰 정보 검증
        print("\n5. 리뷰 정보 검증:")
        has_review = product_info.get('review_count') or product_info.get('rating')
        print(f"   {'✓' if has_review else '⚠'} 리뷰 정보: {'있음' if has_review else '없음 (선택 필드)'}")

        # 최종 결과
        print("\n" + "="*60)
        if all_valid:
            print("✅ 테스트 성공: 모든 필수 필드가 정상적으로 수집되었습니다!")
        else:
            print("❌ 테스트 실패: 일부 필수 필드가 누락되었습니다.")
        print("="*60)

        return all_valid

    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {str(e)}")
        logger.error(f"테스트 중 오류: {str(e)}", exc_info=True)
        return False
    finally:
        # 수집기 종료
        try:
            collector.close()
            print("\n수집기 세션 종료 완료")
        except:
            pass

if __name__ == "__main__":
    success = test_single_product()
    sys.exit(0 if success else 1)
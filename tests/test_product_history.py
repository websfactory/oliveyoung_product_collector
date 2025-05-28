import pytest
import json
from datetime import datetime
from api.product_api import ProductAPI
from collectors.oliveyoung_collector import OliveYoungCollector
from config.session import CosmeticsSession
from models.database import CosmeticsProductHistory, CosmBrand
from pprint import pprint
from sqlalchemy import select, func, desc, and_

"""
제품 데이터 히스토리 저장 기능 테스트

이 테스트는 제품 데이터를 수집한 후 히스토리 테이블에 저장하는 기능을 검증합니다.
save_to_history 파라미터 동작을 테스트합니다.
"""

def test_save_to_history():
    """
    히스토리 테이블에 제품 데이터 저장 테스트
    
    테스트 상품번호: A000000012910
    테스트 내용: 
    1. 히스토리 테이블에 저장하는 경우
    2. 히스토리 테이블에 저장하지 않는 경우
    """
    # 수집기 인스턴스 생성
    collector = OliveYoungCollector()
    product_api = ProductAPI()
    
    try:
        # 테스트할 상품 번호
        goods_no = "A000000012910"
        
        print(f"\n[테스트] 상품번호 {goods_no}에 대한 정보 수집 시작")
        
        # 상품 상세 정보 수집 - 필수 필드 검증 비활성화
        original_validate = collector._validate_required_fields
        collector._validate_required_fields = lambda p, g: True
        
        # 상품 상세 정보 수집
        product = collector.collect_product_detail(goods_no)
        
        # 원래 검증 메서드 복원
        collector._validate_required_fields = original_validate
        
        # 상품 정보가 성공적으로 수집되었는지 확인
        assert product is not None, "상품 정보 수집 실패"
        
        # 전체 수집된 정보 출력
        print("\n===== 수집된 상품 정보 =====")
        pprint(product)
        
        # 히스토리 테이블에 저장할 현재 날짜/시간 기록
        current_time = datetime.now()
        print(f"\n===== 테스트 실행 시간: {current_time} =====")
        
        # 1. 먼저 히스토리 테이블에 저장하는 테스트
        print("\n[테스트 1] 히스토리 테이블 저장 ON (save_to_history=True)")
        result1 = product_api.save_products([product], save_to_history=True)
        print(f"API 저장 결과: {result1}")
        
        # 히스토리 테이블에 저장되었는지 확인
        session = CosmeticsSession()
        try:
            # 최근에 저장된 레코드 조회
            history_record1 = session.execute(
                select(CosmeticsProductHistory)
                .filter(CosmeticsProductHistory.goods_no == goods_no)
                .order_by(desc(CosmeticsProductHistory.collected_at))
                .limit(1)
            ).scalar_one_or_none()
            
            if history_record1:
                # 브랜드 정보 조회
                if history_record1.brandId:
                    brand = session.execute(
                        select(CosmBrand).filter(CosmBrand.id == history_record1.brandId)
                    ).scalar_one_or_none()
                    brand_name = brand.name if brand else "알 수 없음"
                else:
                    brand_name = "브랜드 ID 없음"
                
                print(f"✅ 히스토리 테이블 저장 확인 - ID: {history_record1.goods_no}, 수집시간: {history_record1.collected_at}")
                print(f"   상품명: {history_record1.name}")
                print(f"   브랜드: {brand_name} (ID: {history_record1.brandId})")
                print(f"   가격: {history_record1.price_current}")
                print(f"   평점: {history_record1.rating_text}")
            else:
                print("❌ 히스토리 테이블에 저장된 레코드를 찾을 수 없습니다.")
        finally:
            session.close()
        
        # 2. 히스토리 테이블에 저장하지 않는 테스트
        print("\n[테스트 2] 히스토리 테이블 저장 OFF (save_to_history=False)")
        
        # 기존 레코드 수 확인
        session = CosmeticsSession()
        try:
            record_count_before = session.execute(
                select(func.count()).select_from(CosmeticsProductHistory)
                .filter(CosmeticsProductHistory.goods_no == goods_no)
            ).scalar_one()
            
            print(f"저장 전 히스토리 레코드 수: {record_count_before}")
        finally:
            session.close()
        
        # API 호출 (히스토리 저장 비활성화)
        result2 = product_api.save_products([product], save_to_history=False)
        print(f"API 저장 결과: {result2}")
        
        # 레코드 수가 증가하지 않았는지 확인
        session = CosmeticsSession()
        try:
            record_count_after = session.execute(
                select(func.count()).select_from(CosmeticsProductHistory)
                .filter(CosmeticsProductHistory.goods_no == goods_no)
            ).scalar_one()
            
            print(f"저장 후 히스토리 레코드 수: {record_count_after}")
            
            if record_count_after == record_count_before:
                print("✅ 히스토리 저장 비활성화가 정상적으로 작동합니다.")
            else:
                print("❌ 히스토리 저장 비활성화에도 불구하고 레코드가 추가되었습니다.")
        finally:
            session.close()
        
    finally:
        # 수집기 종료
        collector.close()

def test_direct_history_save():
    """히스토리 저장 내부 함수 직접 테스트"""
    # 테스트 제품 데이터 준비
    test_product = {
        'site': 'oliveyoung_test',
        'goods_no': 'TEST12345',
        'item_no': '001',
        'disp_cat_no': '10000010003',
        'product_url': 'https://www.oliveyoung.co.kr/test/product',
        'brand': '테스트 브랜드',
        'name': '테스트 제품',
        'image_url': 'https://image.oliveyoung.co.kr/test/image.jpg',
        'price': {
            'original': 30000,
            'current': 25000
        },
        'rating': {
            'text': 4.5,
            'percent': 90.0
        },
        'review_count': 120
    }
    
    print("\n===== 히스토리 테이블 직접 저장 테스트 =====")
    
    # ProductAPI 인스턴스 생성
    product_api = ProductAPI()
    
    # 현재 시간 기록
    current_time = datetime.now()
    print(f"테스트 실행 시간: {current_time}")
    
    # 히스토리 테이블에 직접 저장
    save_result = product_api._save_to_history_table([test_product], current_time)
    
    print(f"저장 결과: {save_result}")
    
    # 저장된 레코드 확인
    session = CosmeticsSession()
    try:
        history_record = session.execute(
            select(CosmeticsProductHistory)
            .filter(CosmeticsProductHistory.goods_no == 'TEST12345')
            .order_by(desc(CosmeticsProductHistory.collected_at))
            .limit(1)
        ).scalar_one_or_none()
        
        if history_record:
            # 브랜드 정보 확인
            brand = None
            if history_record.brandId:
                brand = session.execute(
                    select(CosmBrand).filter(CosmBrand.id == history_record.brandId)
                ).scalar_one_or_none()
            
            print(f"✅ 히스토리 테이블 저장 확인")
            print(f"   상품번호: {history_record.goods_no}")
            print(f"   상품명: {history_record.name}")
            print(f"   브랜드ID: {history_record.brandId}")
            if brand:
                print(f"   브랜드명: {brand.name}")
            print(f"   가격: {history_record.price_current}")
            print(f"   평점: {history_record.rating_text}")
            
            assert history_record.site == 'oliveyoung_test'
            assert history_record.price_current == 25000
            assert history_record.price_original == 30000
            assert history_record.rating_text == 4.5
            assert history_record.year == current_time.year
            assert history_record.month == current_time.month
            assert history_record.week_of_year == current_time.isocalendar()[1]
            
            # 브랜드 확인
            if brand:
                assert brand.name == '테스트 브랜드', f"브랜드명이 일치하지 않습니다. 예상: '테스트 브랜드', 실제: '{brand.name}'"
                assert brand.is_active == 1, "브랜드가 활성 상태가 아닙니다."
        else:
            print("❌ 히스토리 테이블에 저장된 레코드를 찾을 수 없습니다.")
            assert False, "히스토리 테이블에 레코드가 저장되지 않았습니다."
    finally:
        session.close()

def test_brand_creation_and_reuse():
    """브랜드 생성 및 재사용 테스트"""
    # 테스트 데이터 준비 - 동일한 브랜드의 제품 2개
    test_brand = "유니크 테스트 브랜드"
    products = [
        {
            'site': 'oliveyoung_test',
            'goods_no': 'BRAND_TEST1',
            'disp_cat_no': '10000010001',
            'product_url': 'https://test.com/product1',
            'brand': test_brand,
            'name': '브랜드 테스트 제품 1',
            'price': {'original': 10000, 'current': 8000},
            'rating': {'text': 4.0, 'percent': 80.0},
            'review_count': 50
        },
        {
            'site': 'oliveyoung_test',
            'goods_no': 'BRAND_TEST2',
            'disp_cat_no': '10000010001',
            'product_url': 'https://test.com/product2',
            'brand': test_brand,
            'name': '브랜드 테스트 제품 2',
            'price': {'original': 20000, 'current': 15000},
            'rating': {'text': 4.2, 'percent': 84.0},
            'review_count': 30
        }
    ]
    
    print(f"\n===== 브랜드 생성 및 재사용 테스트 =====")
    print(f"테스트 브랜드: {test_brand}")
    
    # 기존 브랜드 정보 삭제 (테스트 환경 정리)
    session = CosmeticsSession()
    try:
        # 기존 테스트 브랜드가 있다면 삭제
        existing_brand = session.execute(
            select(CosmBrand).filter(CosmBrand.name == test_brand)
        ).scalar_one_or_none()
        
        if existing_brand:
            print(f"기존 테스트 브랜드 '{test_brand}' 삭제 (ID: {existing_brand.id})")
            session.delete(existing_brand)
            session.commit()
    except Exception as e:
        print(f"브랜드 정리 중 오류: {str(e)}")
        session.rollback()
    finally:
        session.close()
    
    # ProductAPI 인스턴스 생성
    product_api = ProductAPI()
    
    # 현재 시간 및 날짜 정보
    current_time = datetime.now()
    current_year = current_time.year
    current_week = current_time.isocalendar()[1]
    
    print(f"테스트 실행 시간: {current_time} (연도: {current_year}, 주차: {current_week})")
    
    # 1. 첫 번째 제품 저장 - 브랜드 생성 발생
    print("\n1. 첫 번째 제품 저장 (브랜드 생성 발생)")
    save_result1 = product_api._save_to_history_table([products[0]], current_time, current_year, current_week)
    assert save_result1, "첫 번째 제품 저장 실패"
    
    # 저장된 레코드와 브랜드 확인
    session = CosmeticsSession()
    try:
        # 첫 번째 제품 레코드 조회
        history_record1 = session.execute(
            select(CosmeticsProductHistory)
            .filter(and_(
                CosmeticsProductHistory.goods_no == 'BRAND_TEST1',
                CosmeticsProductHistory.year == current_year,
                CosmeticsProductHistory.week_of_year == current_week
            ))
        ).scalar_one_or_none()
        
        assert history_record1, "첫 번째 제품 레코드를 찾을 수 없습니다."
        
        # 생성된 브랜드 정보 확인
        brand = session.execute(
            select(CosmBrand).filter(CosmBrand.name == test_brand)
        ).scalar_one_or_none()
        
        assert brand, f"브랜드 '{test_brand}'가 생성되지 않았습니다."
        assert history_record1.brandId == brand.id, "제품의 brandId가 생성된 브랜드의 ID와 일치하지 않습니다."
        
        brand_id = brand.id
        print(f"브랜드 생성 확인: ID={brand_id}, 이름='{brand.name}', 활성 상태={brand.is_active}")
        print(f"첫 번째 제품의 brandId: {history_record1.brandId}")
    finally:
        session.close()
    
    # 2. 두 번째 제품 저장 - 동일 브랜드 재사용
    print("\n2. 두 번째 제품 저장 (브랜드 재사용)")
    save_result2 = product_api._save_to_history_table([products[1]], current_time, current_year, current_week)
    assert save_result2, "두 번째 제품 저장 실패"
    
    # 저장된 레코드와 브랜드 확인
    session = CosmeticsSession()
    try:
        # 두 번째 제품 레코드 조회
        history_record2 = session.execute(
            select(CosmeticsProductHistory)
            .filter(and_(
                CosmeticsProductHistory.goods_no == 'BRAND_TEST2',
                CosmeticsProductHistory.year == current_year,
                CosmeticsProductHistory.week_of_year == current_week
            ))
        ).scalar_one_or_none()
        
        assert history_record2, "두 번째 제품 레코드를 찾을 수 없습니다."
        
        # 브랜드 정보 확인 - 새로 생성되지 않고 재사용되어야 함
        brands = session.execute(
            select(CosmBrand).filter(CosmBrand.name == test_brand)
        ).all()
        
        assert len(brands) == 1, f"동일한 이름의 브랜드가 중복 생성되었습니다. 발견된 브랜드 수: {len(brands)}"
        
        # 두 제품이 동일한 브랜드 ID를 사용하는지 확인
        brand = brands[0][0]
        assert history_record2.brandId == brand_id, f"두 번째 제품의 brandId가 첫 번째와 다릅니다. 기대: {brand_id}, 실제: {history_record2.brandId}"
        
        print(f"브랜드 재사용 확인: 두 번째 제품의 brandId: {history_record2.brandId}")
        print(f"브랜드 테이블에는 여전히 1개의 '{test_brand}' 항목이 있음")
    finally:
        session.close()
import pytest
import sys
import os
from bs4 import BeautifulSoup

# 상위 디렉토리 경로 추가 (import 문제 해결)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 테스트할 함수 import (이 부분은 실제 import 경로에 맞게 수정해야 합니다)
# 예: from oliveyoung_product_collector.collector import OliveyoungCollector
# 아래는 예시로 함수만 직접 구현한 형태입니다
def parse_ingredients(html_content):
    """
    올리브영 성분 정보 파싱
    
    Args:
        html_content (str): HTML 내용
            
    Returns:
        str: 추출된 성분 문자열 또는 None
    """
    try:
        import re
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 성분 정보 찾기
        detail_info_lists = soup.select('dl.detail_info_list')
        
        for dl in detail_info_lists:
            dt = dl.select_one('dt')
            if dt and '화장품법에 따라 기재해야 하는 모든 성분' in dt.text:
                dd = dl.select_one('dd')
                if dd:
                    # 기존 방식으로 텍스트 추출
                    ingredients_text = dd.text.strip()
                    
                    # HTML에서 <br> 태그 확인
                    dd_html = str(dd)
                    if '<br' in dd_html:
                        # <br> 태그 처리
                        # 1. <dd> 태그 내용만 추출
                        match = re.search(r'<dd.*?>(.*?)</dd>', dd_html, re.DOTALL)
                        if match:
                            content = match.group(1)
                            
                            # 2. <br> 태그 처리
                            content = re.sub(r'<br\s*/?>\s*<br\s*/?>', ', ', content)  # 연속된 <br>
                            content = re.sub(r'^<br\s*/?>', '', content)               # 시작 부분 <br>
                            content = re.sub(r'<br\s*/?>$', '', content)               # 끝 부분 <br>
                            content = re.sub(r'<br\s*/?>', ', ', content)              # 나머지 <br>
                            
                            # 3. 남은 HTML 태그 제거 및 정리
                            ingredients = re.sub(r'<[^>]*>', '', content)             # HTML 태그 제거
                            ingredients = re.sub(r'\s+', ' ', ingredients)            # 연속된 공백 정리
                            ingredients = re.sub(r',\s*,', ',', ingredients)          # 연속된 쉼표 정리
                            ingredients = ingredients.strip()
                        else:
                            ingredients = ingredients_text
                    else:
                        ingredients = ingredients_text
                    
                    print(f"성분 정보 추출: {ingredients}")
                    return ingredients
        
        print("성분 정보를 찾을 수 없습니다")
        return None
        
    except Exception as e:
        print(f"성분 정보 파싱 중 오류 발생: {str(e)}")
        return None


# 테스트 케이스 정의
def test_normal_ingredients():
    """일반적인 쉼표로 구분된, 성분 정보 테스트"""
    # 일반적인 쉼표로 구분된 성분 정보 HTML
    html = """
    <dl class="detail_info_list">
        <dt>화장품법에 따라 기재해야 하는 모든 성분</dt>
        <dd>해바라기씨오일, 향료, 토코페롤, 식물성오일, 피마자씨오일, 마카다미아씨오일, 포도씨오일</dd>
    </dl>
    """
    
    result = parse_ingredients(html)
    expected = "해바라기씨오일, 향료, 토코페롤, 식물성오일, 피마자씨오일, 마카다미아씨오일, 포도씨오일"
    
    print(f"\n===== 일반 성분 테스트 =====")
    print(f"기대 결과: {expected}")
    print(f"실제 결과: {result}")
    
    assert result == expected, f"일반 성분 테스트 실패!\n기대: {expected}\n실제: {result}"


def test_br_tag_ingredients():
    """<br> 태그로 구분된 성분 정보 테스트"""
    # <br> 태그로 구분된 성분 정보 HTML
    html = """
    <dl class="detail_info_list">
        <dt>화장품법에 따라 기재해야 하는 모든 성분</dt>
        <dd>정제수<br>소듐하이알루로네이트<br>하이드록시에틸셀룰로오스<br>1,2-헥산다이올<br>유칼립투스잎추출물<br>구기자추출물<br>복분자딸기열매추출물<br>커먼자스민추출물<br>베르가못민트잎추출물<br>워터민트추출물<br>병풀추출물<br>알로에베라잎즙<br>알란토인<br>베타인<br>부틸렌글라이콜<br>시트릭애씨드<br>클로페네신</dd>
    </dl>
    """
    
    result = parse_ingredients(html)
    expected = "정제수, 소듐하이알루로네이트, 하이드록시에틸셀룰로오스, 1,2-헥산다이올, 유칼립투스잎추출물, 구기자추출물, 복분자딸기열매추출물, 커먼자스민추출물, 베르가못민트잎추출물, 워터민트추출물, 병풀추출물, 알로에베라잎즙, 알란토인, 베타인, 부틸렌글라이콜, 시트릭애씨드, 클로페네신"
    
    print(f"\n===== BR 태그 테스트 =====")
    print(f"기대 결과: {expected}")
    print(f"실제 결과: {result}")
    
    assert result == expected, f"BR 태그 테스트 실패!\n기대: {expected}\n실제: {result}"


def test_complex_br_pattern():
    """복잡한 <br> 패턴 (연속된 <br>, 시작/끝에 <br>) 테스트"""
    # 복잡한 패턴의 <br> 태그가 포함된 HTML
    html = """
    <dl class="detail_info_list">
        <dt>화장품법에 따라 기재해야 하는 모든 성분</dt>
        <dd><br>1. 복숭아향 : 소듐폴리아크릴레이트, 향료<br><br>2. 설향딸기향 : 소듐폴리아크릴레이트, 향료<br></dd>
    </dl>
    """
    
    result = parse_ingredients(html)
    expected = "1. 복숭아향 : 소듐폴리아크릴레이트, 향료, 2. 설향딸기향 : 소듐폴리아크릴레이트, 향료"
    
    print(f"\n===== 복잡한 BR 패턴 테스트 =====")
    print(f"기대 결과: {expected}")
    print(f"실제 결과: {result}")
    
    assert result == expected, f"복잡한 BR 패턴 테스트 실패!\n기대: {expected}\n실제: {result}"


if __name__ == "__main__":
    # 개별 실행 시 모든 테스트 수행
    print("\n=== 테스트 시작 ===")
    test_normal_ingredients()
    test_br_tag_ingredients()
    test_complex_br_pattern()
    print("\n=== 모든 테스트 성공 ===")

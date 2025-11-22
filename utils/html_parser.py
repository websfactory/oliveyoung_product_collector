import re
from bs4 import BeautifulSoup
from utils.logger import setup_logger

logger = setup_logger(__name__, "html_parser.log")

class OliveYoungParser:
    """올리브영 웹페이지 파싱 유틸리티 클래스"""
    
    @staticmethod
    def parse_product_list(html_content):
        """
        올리브영 상품 목록 페이지 파싱
        
        Args:
            html_content (str): HTML 내용
            
        Returns:
            list: 상품 goods_no 목록
        """
        products = []
        soup = BeautifulSoup(html_content, 'html.parser')
        
        try:
            # 상품 리스트 추출
            product_list = soup.select('ul.cate_prd_list > li')
            logger.debug(f"상품 목록에서 {len(product_list)}개 항목 발견")
            
            for item in product_list:
                goods_no = None
                
                # 1. 상품 링크에서 goods_no 추출 (기존 방식)
                a_tag = item.select_one('a.prd_thumb')
                if a_tag and 'href' in a_tag.attrs:
                    href = a_tag.get('href', '')
                    # goodsNo 파라미터 추출
                    goods_no_match = re.search(r'goodsNo=([A-Za-z0-9]+)', href)
                    if goods_no_match:
                        goods_no = goods_no_match.group(1)
                
                # 2. 메타 태그 방식으로 goods_no 추출 (제공된 JavaScript 코드 방식)
                if not goods_no:
                    # 상품별 메타 태그가 있다면 (항목 내 메타 태그)
                    meta_tag = item.select_one('meta[property="eg:itemUrl"]')
                    if meta_tag:
                        item_url = meta_tag.get('content', '')
                        url_match = re.search(r'goodsNo=([A-Za-z0-9]+)', item_url)
                        if url_match:
                            goods_no = url_match.group(1)
                
                # 3. 데이터 속성에서 goods_no 추출 (대체 방법)
                if not goods_no:
                    data_goods_no = item.get('data-goods-no') or item.get('data-goodsno')
                    if data_goods_no:
                        goods_no = data_goods_no
                
                # 유효한 goods_no가 있는 경우에만 추가
                if goods_no:
                    products.append(goods_no)
            
            logger.info(f"총 {len(products)}개 상품 goods_no 파싱 완료")
            return products
        
        except Exception as e:
            logger.error(f"상품 목록 파싱 중 오류 발생: {str(e)}")
            return []
    
    @staticmethod
    def parse_ingredients(html_content):
        """
        올리브영 성분 정보 파싱
        
        Args:
            html_content (str): HTML 내용
                
        Returns:
            str: 추출된 성분 문자열 또는 None
        """
        try:
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
                        
                        logger.debug(f"성분 정보 추출 성공: {len(ingredients)} 글자")
                        return ingredients
                
            logger.warning("성분 정보를 찾을 수 없습니다")
            return None
            
        except Exception as e:
            logger.error(f"성분 정보 파싱 중 오류 발생: {str(e)}")
            return None
    
    @staticmethod
    def get_total_pages(html_content):
        """
        올리브영 카테고리 페이지에서 총 페이지 수 파싱 (확인이 필요함 - 개발 해야함)
        
        Args:
            html_content (str): HTML 내용
            
        Returns:
            int: 총 페이지 수
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 페이징 영역 찾기
            pagination = soup.select_one('div.pageing')
            if not pagination:
                logger.warning("페이징 영역을 찾을 수 없습니다")
                return 1
            
            # 마지막 페이지 찾기
            page_links = pagination.select('a')
            if not page_links:
                logger.warning("페이지 링크를 찾을 수 없습니다")
                return 1
            
            # 페이지 번호 추출
            page_numbers = []
            for link in page_links:
                try:
                    # 페이지 번호만 저장 (화살표 등은 제외)
                    if link.text.strip().isdigit():
                        page_numbers.append(int(link.text.strip()))
                except (ValueError, TypeError):
                    continue
            
            if not page_numbers:
                return 1
            
            total_pages = max(page_numbers)
            logger.info(f"총 {total_pages}개 페이지 감지됨")
            return total_pages
        
        except Exception as e:
            logger.error(f"총 페이지 수 파싱 중 오류 발생: {str(e)}")
            return 1
    
    @staticmethod
    def parse_meta_info(html_content):
        """
        메타 태그에서 제품 정보 추출 (프론트엔드와 동일한 방식)
        메타 태그가 없을 경우 script JSON에서 추출

        Args:
            html_content (str): HTML 컨텐츠

        Returns:
            dict: 추출된 메타 정보
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        meta_info = {}

        # 프론트엔드와 동일한 getMetaContent 함수 구현
        def get_meta_content(property_name):
            meta_tag = soup.select_one(f'meta[property="eg:{property_name}"]')
            return meta_tag.get('content', '').strip() if meta_tag else ""

        # 가격 형식화 함수 (프론트엔드의 formatPrice와 동일)
        def format_price(price_str):
            if not price_str:
                return ""

            # 숫자만 추출
            number_only = re.sub(r'[^\d]', '', price_str)

            # 숫자가 아닌 경우 빈 문자열 반환
            if not number_only:
                return ""

            # 천단위 콤마 적용
            return "{:,}".format(int(number_only))

        # 1. 기존 메타 태그 방식 시도
        meta_info['brand'] = get_meta_content("brandName")
        meta_info['name'] = get_meta_content("itemName")
        meta_info['disp_cat_no'] = get_meta_content("category3")

        # 이미지 URL 처리 (프론트엔드와 동일)
        image_url = get_meta_content("itemImage")
        if image_url:
            meta_info['image_url'] = image_url if image_url.startswith("http") else f"https://image.oliveyoung.co.kr/uploads/images/goods/{image_url}"


        # 가격 정보 설정 (프론트엔드와 동일)
        original_price = get_meta_content("originalPrice")
        sale_price = get_meta_content("salePrice")

        meta_info['price'] = {
            'original': format_price(original_price),
            'current': format_price(sale_price or original_price)  # 할인가가 없으면 원가 사용
        }

        # 2. 메타 태그가 없으면 script JSON에서 파싱
        if not meta_info.get('brand') or not meta_info.get('name'):
            logger.debug("메타 태그가 없음. script JSON에서 파싱 시도")

            # script 태그들 확인
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'salePrice' in script.string:
                    script_text = script.string

                    # 브랜드명 추출
                    brand_match = re.search(r'onlineBrandName\\":\\"([^"]+)\\"', script_text)
                    if brand_match:
                        meta_info['brand'] = brand_match.group(1)

                    # 상품명 추출
                    name_match = re.search(r'goodsName\\":\\"([^"]+)\\"', script_text)
                    if name_match:
                        meta_info['name'] = name_match.group(1)

                    # 카테고리 추출
                    if not meta_info.get('disp_cat_no'):
                        cat_match = re.search(r'lowerCategory\\":\\"([^"]+)\\"', script_text)
                        if cat_match:
                            meta_info['disp_cat_no'] = cat_match.group(1)

                    # 이미지 URL 추출 (없는 경우에만)
                    if not meta_info.get('image_url'):
                        img_match = re.search(r'thumbnailImage.*?url\\":\\"([^"]+)\\".*?path\\":\\"([^"]+)\\"', script_text, re.DOTALL)
                        if img_match:
                            meta_info['image_url'] = f"{img_match.group(1)}/{img_match.group(2)}"

                    # 가격 추출 (없는 경우에만)
                    if not meta_info.get('price') or not meta_info['price'].get('original'):
                        sale_price_match = re.search(r'salePrice\\":(\d+)', script_text)
                        final_price_match = re.search(r'finalPrice\\":(\d+)', script_text)

                        if sale_price_match and final_price_match:
                            meta_info['price'] = {
                                'original': format_price(sale_price_match.group(1)),
                                'current': format_price(final_price_match.group(1))
                            }

                    # 데이터를 찾았으면 종료
                    if meta_info.get('brand') and meta_info.get('name'):
                        logger.debug(f"script JSON에서 파싱 성공: brand={meta_info['brand']}, name={meta_info['name'][:30]}...")
                        break

        return meta_info
    
    @staticmethod
    def check_category_product_count(html_content):
        """
        카테고리 내 상품 개수 확인

        Args:
            html_content (str): HTML 내용

        Returns:
            int: 카테고리 내 상품 개수, 파싱 실패 시 -1 반환
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # 카테고리 정보 텍스트 찾기
            cate_info = soup.select_one('p.cate_info_tx')
            if not cate_info:
                logger.warning("카테고리 정보 텍스트를 찾을 수 없습니다")
                return -1

            # 상품 개수 추출
            count_span = cate_info.select_one('span')
            if count_span:
                count_text = count_span.text.strip()
                try:
                    return int(count_text)
                except (ValueError, TypeError):
                    logger.warning(f"상품 개수를 숫자로 변환할 수 없습니다: {count_text}")
                    return -1

            # 텍스트 전체에서 숫자 추출 시도
            text = cate_info.text.strip()
            match = re.search(r'(\d+)\s*개의 상품', text)
            if match:
                return int(match.group(1))

            logger.warning(f"상품 개수를 찾을 수 없습니다: {text}")
            return -1

        except Exception as e:
            logger.error(f"카테고리 상품 개수 확인 중 오류 발생: {str(e)}")
            return -1

    @staticmethod
    def parse_manufacturer_info(html_content):
        """
        제품 상세 페이지에서 제조업자/책임판매업자 정보 추출

        Args:
            html_content (str): HTML 컨텐츠

        Returns:
            str: 제조업자/책임판매업자 정보 또는 None
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # detail_info_list 클래스를 가진 dl 태그들 찾기
            detail_info_lists = soup.select('dl.detail_info_list')

            for dl in detail_info_lists:
                dt = dl.select_one('dt')
                if dt and '화장품제조업자,화장품책임판매업자 및 맞춤형화장품판매업자' in dt.text:
                    dd = dl.select_one('dd')
                    if dd:
                        manufacturer_info = dd.text.strip()
                        logger.debug(f"제조업자 정보 추출 성공: {manufacturer_info}")
                        return manufacturer_info

            logger.warning("제조업자 정보를 찾을 수 없습니다")
            return None

        except Exception as e:
            logger.error(f"제조업자 정보 파싱 중 오류 발생: {str(e)}")
            return None

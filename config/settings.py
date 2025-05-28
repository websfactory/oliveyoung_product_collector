import os
from pathlib import Path
from dotenv import load_dotenv
import logging

# .env 파일 로드
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / '.env')

# 로깅 설정
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')
LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# API 설정
API_BASE_URL = os.getenv('API_BASE_URL', 'https://api.beauticslab.com/v1/api')
API_RETRY_MAX = int(os.getenv('API_RETRY_MAX', '3'))
API_RETRY_DELAY = int(os.getenv('API_RETRY_DELAY', '500'))  # 밀리초
API_RETRY_MAX_DELAY = int(os.getenv('API_RETRY_MAX_DELAY', '10000'))  # 밀리초

# 웹 크롤링 설정
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))  # 초
USER_AGENT = os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

# 올리브영 사이트 설정
OLIVEYOUNG_BASE_URL = 'https://www.oliveyoung.co.kr/store'
OLIVEYOUNG_CATEGORY_URL = f'{OLIVEYOUNG_BASE_URL}/display/getMCategoryList.do?dispCatNo='
OLIVEYOUNG_PRODUCT_DETAIL_URL = f'{OLIVEYOUNG_BASE_URL}/goods/getGoodsDetail.do?goodsNo='
OLIVEYOUNG_INGREDIENT_URL = f'{OLIVEYOUNG_BASE_URL}/goods/getGoodsArtcAjax.do'

# 데이터 수집 배치 설정
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))  # 배치당 처리할 제품 수

# 디버깅 모드
DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'

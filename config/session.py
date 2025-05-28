from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

# 데이터베이스 설정
HOSTNAME = os.getenv('HOSTNAME')
PORT = os.getenv('PORT')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
PROD_DATABASE = os.getenv('PROD_DATABASE')

CHARSET = os.getenv('CHARSET1')

PROD_DATABASE_URL = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{PROD_DATABASE}?charset={CHARSET}"

# COSMETICS 데이터베이스 엔진 생성
engine = create_engine(PROD_DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)

# 세션 팩토리 생성
CosmeticsSession = sessionmaker(bind=engine)
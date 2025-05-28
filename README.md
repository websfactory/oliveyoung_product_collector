# Oliveyoung Product Collector

올리브영 웹사이트에서 제품 정보를 수집하는 Python 애플리케이션입니다.

## 주요 기능

- 카테고리별 제품 정보 자동 수집
- 제품 상세 정보 및 성분 분석
- 인기도 및 판매량 순위 추적
- 시계열 데이터 저장 및 관리

## 설치 방법

1. 필요한 패키지 설치:
```bash
pip install -r requirements.txt
```

2. 환경 변수 설정:
`.env` 파일을 생성하고 다음 정보를 입력:
```
HOSTNAME=your_db_host
PORT=your_db_port
USERNAME=your_db_username
PASSWORD=your_db_password
PROD_DATABASE=your_database_name
CHARSET1=utf8mb4
AWS_WAF_TOKEN=your_token_here
```

## 사용 방법

### 전체 카테고리 수집
```bash
python main.py
```

### 특정 카테고리 수집
```bash
python collect_all_categories.py
```

### 누락된 제품 재수집
```bash
python retry_missing_products.py
```

## 주의사항

- AWS WAF 토큰이나 Cloudflare 인증이 필요할 수 있습니다
- 과도한 요청을 방지하기 위해 적절한 딜레이가 설정되어 있습니다
- 민감한 정보(토큰, 패스워드 등)는 절대 코드에 하드코딩하지 마세요

## 라이센스

Private repository - All rights reserved
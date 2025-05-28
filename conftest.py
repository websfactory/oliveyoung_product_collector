"""
pytest 환경 설정 파일
모듈 import 경로 문제 해결을 위한 설정
"""
import os
import sys
from pathlib import Path

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 모듈 import 경로 확인을 위한 디버그 코드
print(f"프로젝트 루트 디렉토리: {project_root}")
print(f"현재 sys.path: {sys.path}") 
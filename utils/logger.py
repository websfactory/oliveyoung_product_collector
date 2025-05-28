import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from config.settings import LOG_LEVEL, LOG_FORMAT, BASE_DIR

def setup_logger(name, log_file=None):
    """
    로거 설정 함수
    
    Args:
        name (str): 로거 이름
        log_file (str, optional): 로그 파일 경로. 기본값은 None.
    
    Returns:
        logging.Logger: 설정된 로거 객체
    """
    # 로거 생성
    logger = logging.getLogger(name)
    
    # 로그 레벨 설정
    log_level = getattr(logging, LOG_LEVEL.upper())
    logger.setLevel(log_level)
    
    # 핸들러가 이미 있으면 추가하지 않음
    if logger.handlers:
        return logger
    
    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # 포맷터 설정
    formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(formatter)
    
    # 로거에 핸들러 추가
    logger.addHandler(console_handler)
    
    # 파일 핸들러 설정 (지정된 경우)
    if log_file:
        # 로그 디렉토리 생성
        log_dir = BASE_DIR / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        log_path = log_dir / log_file
        file_handler = RotatingFileHandler(
            log_path, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

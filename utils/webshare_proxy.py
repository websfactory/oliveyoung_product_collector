"""
Webshare 프록시 관리 유틸리티
"""
import os
import requests
import random
import logging
from typing import Optional, Dict, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class WebshareProxyManager:
    """Webshare 프록시 관리 클래스"""
    
    def __init__(self):
        self.api_key = os.getenv('WEBSHARE_API')
        if not self.api_key:
            raise ValueError("WEBSHARE_API 환경변수가 설정되지 않았습니다")
            
        self.base_url = "https://proxy.webshare.io/api/v2"
        self.headers = {"Authorization": f"Token {self.api_key}"}
        self._proxy_cache = None
        self._cache_timestamp = None
        self._cache_duration = timedelta(minutes=5)  # 5분간 캐시
        self._failed_proxies = set()  # 실패한 프록시 추적
        
    def _is_cache_valid(self) -> bool:
        """캐시가 유효한지 확인"""
        if not self._proxy_cache or not self._cache_timestamp:
            return False
        return datetime.now() - self._cache_timestamp < self._cache_duration
    
    def get_proxy_list(self, mode: str = "direct") -> List[Dict]:
        """Webshare API에서 프록시 목록 가져오기
        
        Args:
            mode: "direct" 또는 "backbone" (기본값: "direct")
            
        Returns:
            프록시 정보 리스트
        """
        # 캐시가 유효하면 캐시된 데이터 반환
        if self._is_cache_valid():
            logger.info("캐시된 프록시 목록 사용")
            return self._proxy_cache
            
        try:
            # API 호출
            response = requests.get(
                f"{self.base_url}/proxy/list/",
                headers=self.headers,
                params={
                    "mode": mode,
                    "page_size": 100,  # 충분한 프록시 확보
                    "valid": True  # 유효한 프록시만
                }
            )
            response.raise_for_status()
            
            data = response.json()
            proxies = data.get("results", [])
            
            if proxies:
                logger.info(f"Webshare에서 {len(proxies)}개 프록시 로드")
                self._proxy_cache = proxies
                self._cache_timestamp = datetime.now()
                return proxies
            else:
                logger.warning("Webshare에서 프록시를 가져올 수 없습니다")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Webshare API 오류: {e}")
            # 캐시가 있으면 캐시 반환 (fallback)
            if self._proxy_cache:
                logger.info("API 오류로 인해 캐시된 프록시 사용")
                return self._proxy_cache
            return []
    
    def get_random_proxy(self, mode: str = "direct") -> Optional[Dict]:
        """랜덤 프록시 하나 선택 (실패한 프록시 제외)
        
        Returns:
            프록시 정보 딕셔너리 또는 None
        """
        proxies = self.get_proxy_list(mode)
        if not proxies:
            return None
            
        # 유효한 프록시만 필터링 (실패한 프록시 제외)
        valid_proxies = [
            p for p in proxies 
            if p.get("valid", False) and p.get("id") not in self._failed_proxies
        ]
        
        if not valid_proxies:
            logger.warning("사용 가능한 프록시가 없습니다")
            # 실패 목록 초기화 후 재시도
            self._failed_proxies.clear()
            valid_proxies = [p for p in proxies if p.get("valid", False)]
            
        if not valid_proxies:
            return None
            
        return random.choice(valid_proxies)
    
    def mark_proxy_failed(self, proxy: Dict):
        """프록시를 실패 목록에 추가"""
        proxy_id = proxy.get("id")
        if proxy_id:
            self._failed_proxies.add(proxy_id)
            logger.warning(f"프록시 {proxy_id} 실패로 표시됨")
    
    def format_proxy_url(self, proxy: Dict, mode: str = "direct") -> Optional[str]:
        """프록시 정보를 URL 형식으로 변환
        
        Args:
            proxy: 프록시 정보 딕셔너리
            mode: "direct" 또는 "backbone"
            
        Returns:
            프록시 URL 문자열 또는 None
        """
        if not proxy:
            return None
            
        username = proxy.get("username")
        password = proxy.get("password")
        port = proxy.get("port")
        
        if mode == "direct":
            # Direct 모드: proxy_address 사용
            host = proxy.get("proxy_address")
        else:
            # Backbone 모드: p.webshare.io 사용
            host = "p.webshare.io"
            
        if not all([username, password, host, port]):
            logger.error(f"프록시 정보 불완전: {proxy}")
            return None
            
        return f"http://{username}:{password}@{host}:{port}"
    
    def get_proxy_dict(self, mode: str = "direct") -> Optional[Dict[str, str]]:
        """requests/curl-cffi에서 사용할 프록시 딕셔너리 반환
        
        Returns:
            {"http": proxy_url, "https": proxy_url} 형식의 딕셔너리
        """
        proxy = self.get_random_proxy(mode)
        if not proxy:
            return None
            
        proxy_url = self.format_proxy_url(proxy, mode)
        if not proxy_url:
            return None
            
        return {
            "http": proxy_url,
            "https": proxy_url
        }
    
    def test_proxy(self, proxy_dict: Dict[str, str] = None) -> bool:
        """프록시 연결 테스트
        
        Args:
            proxy_dict: 테스트할 프록시 딕셔너리 (None이면 랜덤 선택)
            
        Returns:
            성공 여부
        """
        if not proxy_dict:
            proxy_dict = self.get_proxy_dict()
            
        if not proxy_dict:
            logger.error("테스트할 프록시가 없습니다")
            return False
            
        try:
            # httpbin.org를 사용한 프록시 테스트
            response = requests.get(
                "http://httpbin.org/ip",
                proxies=proxy_dict,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"프록시 테스트 성공 - IP: {result.get('origin')}")
                return True
            else:
                logger.error(f"프록시 테스트 실패 - 상태 코드: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"프록시 테스트 중 오류: {e}")
            return False
    
    def get_proxy_info(self) -> Dict:
        """현재 프록시 상태 정보 반환"""
        proxies = self.get_proxy_list()
        
        return {
            "total_proxies": len(proxies),
            "valid_proxies": len([p for p in proxies if p.get("valid", False)]),
            "failed_proxies": len(self._failed_proxies),
            "cache_valid": self._is_cache_valid()
        }


# 싱글톤 인스턴스
_proxy_manager = None

def get_webshare_proxy_manager() -> WebshareProxyManager:
    """WebshareProxyManager 싱글톤 인스턴스 반환"""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = WebshareProxyManager()
    return _proxy_manager
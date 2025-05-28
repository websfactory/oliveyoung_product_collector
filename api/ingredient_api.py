import aiohttp
import requests
import json
from config.settings import API_BASE_URL
from utils.logger import setup_logger

logger = setup_logger(__name__, "ingredient_api.log")

class IngredientAPI:
    """
    성분 분석 API 클라이언트
    
    기존 크롬 확장프로그램의 IngredientAPI 클래스를 파이썬으로 구현
    """
    
    def __init__(self, base_url=None):
        """
        IngredientAPI 생성자
        
        Args:
            base_url (str, optional): API 기본 URL. 기본값은 설정 파일의 API_BASE_URL.
        """
        self.base_url = base_url or API_BASE_URL
        logger.info(f"IngredientAPI 초기화: 기본 URL = {self.base_url}")
    
    def fetch_ingredients_info(self, ingredients, goods_no=None):
        """
        성분 정보를 분석하는 API 요청 (동기 방식)
        
        Args:
            ingredients (str): 분석할 성분 문자열
            goods_no (str, optional): 상품 번호. 기본값은 None.
            
        Returns:
            dict: 분석 결과
            
        Raises:
            Exception: API 요청 실패 시
        """
        try:
            if not ingredients or not ingredients.strip():
                logger.warning("성분 정보가 비어있습니다")
                return {
                    "status": "error",
                    "message": "성분 정보가 비어있습니다"
                }
            
            endpoint = f"{self.base_url}/ai/cosmetic_ingredients"
            logger.debug(f"성분 분석 API 호출: {endpoint}")
            
            # 요청 본문 구성
            request_body = {"ingredients": ingredients}
            if goods_no:
                request_body["goods_no"] = goods_no
            
            # API 요청 수행
            response = requests.post(
                endpoint,
                headers={"Content-Type": "application/json"},
                json=request_body
            )
            
            # 응답 확인
            if not response.ok:
                error_message = f"성분 분석 API 오류: {response.status_code}"
                try:
                    error_data = response.json()
                    error_message = error_data.get("message", error_message)
                except Exception:
                    pass
                
                logger.error(error_message)
                return {
                    "status": "error",
                    "message": error_message
                }
            
            # 응답 처리
            response_data = response.json()
            
            # 오류 응답 처리
            if response_data.get("status") == "error":
                logger.error(f"API 오류 응답: {response_data.get('message')}")
                return response_data
            
            logger.info("성분 분석 성공")
            return response_data
            
        except Exception as e:
            logger.error(f"성분 분석 API 오류: {str(e)}")
            return {
                "status": "error",
                "message": f"성분 분석 API 오류: {str(e)}"
            }

    def check_health(self):
        """
        API 상태 확인
        
        Returns:
            bool: API 가용성 여부
        """
        try:
            health_endpoint = f"{self.base_url}/health"
            logger.debug(f"API 상태 확인: {health_endpoint}")
            
            with aiohttp.ClientSession() as session:
                with session.get(
                    health_endpoint,
                    headers={"Accept": "application/json"}
                ) as response:
                    if not response.ok:
                        logger.warning(f"API 상태 확인 실패: {response.status}")
                        return False
                    
                    data = response.json()
                    health_status = data.get("status") == "success"
                    logger.info(f"API 상태: {'정상' if health_status else '비정상'}")
                    return health_status
                    
        except Exception as e:
            logger.error(f"API 상태 확인 중 오류: {str(e)}")
            return False

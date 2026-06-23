"""collection_runs 런 로그 기록 + 임계치 비교 헬퍼.

수집 완주 시 collection_runs 테이블에 1행을 남겨, 4개 수집기 상태를
한 테이블에서 보고(하트비트)·침묵 실패(0건/급감)를 가드한다.
관측이 수집을 죽이면 안 되므로 모든 DB 작업은 예외를 내부에서 흡수한다.
"""
import socket

from sqlalchemy import text

from config.session import CosmeticsSession
from utils.logger import setup_logger

logger = setup_logger(__name__, "run_log.log")

_HOSTNAME = None


def _host():
    global _HOSTNAME
    if _HOSTNAME is None:
        try:
            _HOSTNAME = socket.gethostname().split('.')[0][:50]
        except Exception:
            _HOSTNAME = 'unknown'
    return _HOSTNAME


def record_run(site, job_type, started_at, finished_at, status,
               scheduled_day=None, category_count=None, collected_count=None,
               failed_count=None, exit_code=None, note=None):
    """collection_runs 테이블에 런 1행 기록. 실패해도 예외를 던지지 않는다."""
    sql = text(
        """INSERT INTO collection_runs
             (site, job_type, host, scheduled_day, started_at, finished_at,
              status, category_count, collected_count, failed_count, exit_code, note)
           VALUES
             (:site, :job_type, :host, :scheduled_day, :started_at, :finished_at,
              :status, :category_count, :collected_count, :failed_count, :exit_code, :note)"""
    )
    session = CosmeticsSession()
    try:
        session.execute(sql, {
            'site': site, 'job_type': job_type, 'host': _host(),
            'scheduled_day': scheduled_day, 'started_at': started_at,
            'finished_at': finished_at, 'status': status,
            'category_count': category_count, 'collected_count': collected_count,
            'failed_count': failed_count, 'exit_code': exit_code,
            'note': (str(note)[:60000] if note else None),
        })
        session.commit()
        logger.info(f"collection_runs 기록: {site}/{job_type} status={status} "
                    f"collected={collected_count}")
        return True
    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        logger.error(f"collection_runs 기록 실패: {e}")
        return False
    finally:
        session.close()


def last_week_collected(site):
    """전주 동일 요일(=7일 전)의 해당 site history 수집 건수. 임계치 비교용. 실패 시 None."""
    session = CosmeticsSession()
    try:
        r = session.execute(text(
            """SELECT COUNT(*) FROM cosmetics_products_history
               WHERE site = :site
                 AND DATE(collected_at) = DATE(DATE_SUB(CURDATE(), INTERVAL 7 DAY))"""
        ), {'site': site}).scalar()
        return int(r) if r is not None else None
    except Exception as e:
        logger.error(f"전주 수집량 조회 실패: {e}")
        return None
    finally:
        session.close()

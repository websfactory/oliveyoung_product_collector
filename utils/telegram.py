"""텔레그램 수집 보고 전송 헬퍼.

수집(main.py) / 누락 재수집(retry_missing_products.py)이 완주하면
대표 DM(@webs_openclaw_bot)으로 결과 요약을 보낸다.
전송 실패가 수집 자체에 영향을 주지 않도록 모든 예외를 내부에서 흡수한다.
"""
import os
from datetime import timedelta

import requests

from utils.logger import setup_logger

logger = setup_logger(__name__, "telegram.log")

_WEEKDAYS = ['월', '화', '수', '목', '금', '토', '일']


def send_telegram(text: str) -> bool:
    """대표 DM으로 메시지 전송. 실패해도 예외를 던지지 않는다."""
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        logger.warning("TELEGRAM_BOT_TOKEN/CHAT_ID 미설정 — 텔레그램 전송 생략")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={'chat_id': chat_id, 'text': text},
            timeout=15,
        )
        if resp.status_code == 200 and resp.json().get('ok'):
            logger.info("텔레그램 전송 성공")
            return True
        logger.error(f"텔레그램 전송 실패: {resp.status_code} {resp.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"텔레그램 전송 예외: {e}")
        return False


def _fmt_duration(duration) -> str:
    total = int(duration.total_seconds()) if isinstance(duration, timedelta) else int(duration)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}시간 {m}분"
    if m:
        return f"{m}분 {s}초"
    return f"{s}초"


_STATUS_HEADER = {
    'success': ('✅', '수집 완료'),
    'partial': ('⚠️', '수집 완료(일부 실패)'),
    'alert':   ('🔴', '수집 점검 필요'),
    'failed':  ('❌', '수집 실패'),
}


def send_collection_report(weekday, status, success_count, total_categories,
                           total_products, failed_categories, duration, extra_note=None):
    """요일 카테고리 수집 완주 보고. status=success|partial|alert|failed."""
    wd = _WEEKDAYS[weekday - 1] if isinstance(weekday, int) and 1 <= weekday <= 7 else str(weekday)
    n_failed = len(failed_categories) if failed_categories else 0
    emoji, title = _STATUS_HEADER.get(status, ('ℹ️', '수집'))
    lines = [
        f"{emoji} 올리브영 {title} ({wd}요일)",
        f"카테고리: {success_count}/{total_categories} 성공" + (f", {n_failed} 실패" if n_failed else ""),
        f"수집 제품: {total_products:,}건",
        f"소요: {_fmt_duration(duration)}",
    ]
    if extra_note:
        lines.append(str(extra_note))
    if n_failed:
        names = ", ".join(
            str(f.get('category_name', f.get('category_id', '?'))) for f in failed_categories[:8]
        )
        more = f" 외 {n_failed - 8}개" if n_failed > 8 else ""
        lines.append(f"실패 카테고리: {names}{more}")
    send_telegram("\n".join(lines))


def send_retry_report(total_retried, success_count, fail_count, duration, extra=None):
    """누락 재수집(일요일) 완주 보고."""
    status = "✅" if fail_count == 0 else "⚠️"
    lines = [
        f"{status} 올리브영 누락 재수집 완료",
        f"대상: {total_retried:,}건 / 성공 {success_count:,} / 실패 {fail_count:,}",
        f"소요: {_fmt_duration(duration)}",
    ]
    if extra:
        lines.append(str(extra))
    send_telegram("\n".join(lines))

"""수집 파이프라인 하트비트 (데드맨 스위치).

collection_runs(소스별 마지막 런 신선도+상태) + DB event_scheduler + 주간 리셋 이벤트를
매일 점검해 텔레그램으로 상태 펄스를 보낸다. 완주 보고가 "한 일"을 알린다면, 하트비트는
"안 한 일"(런이 아예 안 뜸·DB 이벤트 OFF)을 잡는다.

cron 예: 0 11 * * *  (모든 일일 수집이 끝난 뒤)
"""
from datetime import datetime, timedelta

from sqlalchemy import text

from config.session import CosmeticsSession
from utils.telegram import send_telegram
from utils.logger import setup_logger

logger = setup_logger(__name__, "heartbeat.log")

# (site, job_type, 라벨, 최대 허용 신선도[시간])
CHECKS = [
    ('oliveyoung',  'collect',   '올리브 수집',   52),   # 월~토 04:00 (토→월 갭 ~48h 커버)
    ('oliveyoung',  'retry',     '올리브 재수집', 204),  # 주간(일 04:00)
    ('enuri',       'collect',   'enuri 수집',    30),   # 매일 01:00 (DB서버)
    ('daiso',       'collect',   '다이소 수집',   204),  # 주간(금 03:00)
    ('monthly_best', 'aggregate', '월간 베스트',  204),  # 주간(월 06:00)
]


def _fmt_ago(dt):
    if dt is None:
        return '기록 없음'
    secs = (datetime.now() - dt).total_seconds()
    h = secs / 3600
    return f"{int(h)}시간 전" if h < 48 else f"{int(h / 24)}일 전"


def main():
    session = CosmeticsSession()
    issues, lines = [], []
    try:
        rows = session.execute(text(
            """SELECT site, job_type, MAX(finished_at) AS last_fin,
                      SUBSTRING_INDEX(GROUP_CONCAT(status ORDER BY finished_at DESC), ',', 1) AS last_status
               FROM collection_runs WHERE finished_at IS NOT NULL
               GROUP BY site, job_type"""
        )).fetchall()
        last = {(r[0], r[1]): (r[2], r[3]) for r in rows}

        for site, jt, label, max_h in CHECKS:
            fin, st = last.get((site, jt), (None, None))
            ago = _fmt_ago(fin)
            if fin is None:
                # 배선 후 아직 한 번도 안 돈 소스 — 오경보 방지(첫 실행 시 자동 ✅ 전환)
                lines.append(f"❔ {label}: 아직 기록 없음")
            elif (datetime.now() - fin) > timedelta(hours=max_h):
                issues.append(label)
                lines.append(f"🔴 {label}: {ago} (기대 {max_h}h 내)")
            elif st in ('failed', 'alert'):
                issues.append(label)
                lines.append(f"⚠️ {label}: 마지막 런 {st} ({ago})")
            else:
                lines.append(f"✅ {label}: {ago}")

        # DB event_scheduler + 주간 리셋 이벤트 (수집기 머신과 무관한 단일점)
        es = session.execute(text("SHOW VARIABLES LIKE 'event_scheduler'")).fetchone()
        if not (es and str(es[1]).upper() == 'ON'):
            issues.append('event_scheduler')
            lines.append("🔴 DB event_scheduler OFF (주간 리셋 위험!)")
        else:
            ev = session.execute(text(
                """SELECT STATUS, LAST_EXECUTED FROM information_schema.EVENTS
                   WHERE EVENT_SCHEMA='beauticslab_webapp'
                     AND EVENT_NAME='reset_is_processed_weekly'"""
            )).fetchone()
            if not ev or ev[0] != 'ENABLED':
                issues.append('reset_event')
                lines.append("🔴 주간 리셋 이벤트 비활성")
            elif ev[1] and (datetime.now() - ev[1]) > timedelta(days=8):
                issues.append('reset_event')
                lines.append(f"🔴 주간 리셋 {_fmt_ago(ev[1])} (8일 초과)")
            else:
                lines.append(f"✅ event_scheduler ON · 주간리셋 {_fmt_ago(ev[1]) if ev and ev[1] else '?'}")
    except Exception as e:
        logger.error(f"하트비트 점검 실패: {e}")
        send_telegram(f"🔴 수집 하트비트 점검 자체 실패: {e}")
        return
    finally:
        session.close()

    head = "🔴 수집 파이프라인 이상" if issues else "🟢 수집 파이프라인 점검"
    ts = datetime.now().strftime('%m-%d %H:%M')
    send_telegram(f"{head} ({ts})\n" + "\n".join(lines))
    logger.info(f"하트비트 전송 완료: issues={len(issues)}")


if __name__ == "__main__":
    main()

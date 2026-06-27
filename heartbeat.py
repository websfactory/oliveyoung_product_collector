"""수집 파이프라인 하트비트 (데드맨 스위치).

목적은 딱 하나 — "안 한 일/잘못된 일"을 잡는다. 완주 보고가 아니다.
점검 항목:
  1) 소스별 마지막 런 신선도 (런이 아예 안 떴는지 = 머신/cron/터널 죽음)
  2) 수집량 바닥 (런은 success인데 0건/급감 = 소스 빈응답·인증깨짐)
  3) 적재 갭 (런은 success인데 DB 저장이 조용히 실패하는지 = DB_ERROR 급증)
  4) DB event_scheduler ON + 주간 리셋 이벤트 살아있는지

발송 정책 (정직한 데드맨 스위치):
  - 이상 있을 때만 🔴 발송. 평소엔 무발송.
  - 단, 주 1회(월요일) "살아있음" 펄스를 보낸다. 안 그러면 하트비트(혹은 이 cron)
    자체가 죽어도 조용해서 알 수가 없다 = 데드맨 스위치의 데드맨.
  - 결과는 매일 로컬 로그(cron_heartbeat.log)에는 항상 남긴다.

cron 예: 0 18 * * *  (모든 일일 수집이 끝난 뒤. enuri는 11시대 종료라 11:00은
          오늘 런이 끝나기 전이었음 → 18:00이면 당일 결과를 본다.)
"""
from datetime import datetime, timedelta

from sqlalchemy import text

from config.session import CosmeticsSession
from utils.telegram import send_telegram
from utils.logger import setup_logger

logger = setup_logger(__name__, "heartbeat.log")

# (site, job_type, 라벨, 최대 허용 신선도[시간], 정상 최소 수집량)
# ※ collection_runs에 실제로 기록되는 (site, job_type)만 점검한다. 기록이 없는
#    잡(올리브 retry·monthly_best aggregate)은 매일 "기록 없음" 영구 노이즈만
#    내므로 제외했다. 관측 신호가 생기면 그때 추가한다.
# ※ 최소 수집량은 정상치의 한참 아래로 잡아 진짜 붕괴(0건·급감)만 잡는다.
#    (enuri 평소 ~7000~9000, olive ~2000+, daiso 주간 ~6)
CHECKS = [
    ('oliveyoung', 'collect', '올리브 수집', 52,  500),   # 월~토 04:00 (토→월 갭 ~48h 커버)
    ('enuri',      'collect', 'enuri 수집',  30,  1000),  # 매일 01:00 (DB서버)
    ('daiso',      'collect', '다이소 수집', 204, 1),     # 주간(금 03:00)
]

# 적재 갭: 최근 N시간 동안 DB_ERROR 저장 실패가 임계 초과면 경보.
# (수정 후 정상 베이스라인 = 0/일. pre-fix charset/brand 붕괴 때는 13~87/일이었다.
#  ★임계는 그 붕괴 하단(13~14건)도 잡도록 낮게 잡는다.)
# ※ cosmetics_product_save_fails.created_at 은 UTC 저장 → UTC_TIMESTAMP() 기준.
DB_ERROR_WINDOW_HOURS = 26
DB_ERROR_THRESHOLD = 10

# 주간 생존 펄스 요일 (0=월요일)
WEEKLY_PULSE_WEEKDAY = 0


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
        # 1) 소스별 마지막 런 신선도 + 상태 + 수집량
        rows = session.execute(text(
            """SELECT site, job_type, MAX(finished_at) AS last_fin,
                      SUBSTRING_INDEX(GROUP_CONCAT(status ORDER BY finished_at DESC), ',', 1) AS last_status,
                      SUBSTRING_INDEX(GROUP_CONCAT(collected_count ORDER BY finished_at DESC), ',', 1) AS last_count
               FROM collection_runs WHERE finished_at IS NOT NULL
               GROUP BY site, job_type"""
        )).fetchall()
        last = {(r[0], r[1]): (r[2], r[3], r[4]) for r in rows}

        for site, jt, label, max_h, min_count in CHECKS:
            fin, st, cnt = last.get((site, jt), (None, None, None))
            ago = _fmt_ago(fin)
            cnt_i = int(cnt) if cnt is not None and str(cnt).lstrip('-').isdigit() else None
            if fin is None:
                issues.append(label)
                lines.append(f"🔴 {label}: 런 기록 없음 (한 번도 안 돎?)")
            elif (datetime.now() - fin) > timedelta(hours=max_h):
                issues.append(label)
                lines.append(f"🔴 {label}: {ago} (기대 {max_h}h 내)")
            elif st in ('failed', 'alert'):
                issues.append(label)
                lines.append(f"⚠️ {label}: 마지막 런 {st} ({ago})")
            elif cnt_i is not None and cnt_i < min_count:
                # 런은 정상 종료인데 수집량이 바닥 = 조용한 붕괴 (소스 빈응답 등)
                issues.append(label)
                lines.append(f"🔴 {label}: 수집 {cnt_i}건 (정상 {min_count}+ 기대, {ago})")
            else:
                lines.append(f"✅ {label}: {ago} · {cnt_i if cnt_i is not None else '?'}건")

        # 2) 적재 갭 — 런은 success인데 DB 저장이 조용히 실패하는 구간을 잡는다.
        #    (charset/brand 사태처럼 failed_count=0인데 수천 건 유실되던 사각)
        n_dberr = session.execute(text(
            """SELECT COUNT(*) FROM cosmetics_product_save_fails
               WHERE fail_type='DB_ERROR'
                 AND created_at >= UTC_TIMESTAMP() - INTERVAL :h HOUR"""
        ), {"h": DB_ERROR_WINDOW_HOURS}).scalar()
        if n_dberr and n_dberr > DB_ERROR_THRESHOLD:
            issues.append('적재 갭')
            lines.append(
                f"🔴 적재 갭: 최근 {DB_ERROR_WINDOW_HOURS}h DB 저장 실패 {n_dberr}건 "
                f"(임계 {DB_ERROR_THRESHOLD})")
        else:
            lines.append(f"✅ 적재: 최근 {DB_ERROR_WINDOW_HOURS}h DB_ERROR {int(n_dberr or 0)}건")

        # 3) DB event_scheduler + 주간 리셋 이벤트 (수집기 머신과 무관한 단일점)
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
        # 점검 자체 실패도 이상 신호 → 발송
        logger.error(f"하트비트 점검 실패: {e}")
        send_telegram(f"🔴 수집 하트비트 점검 자체 실패: {e}")
        return
    finally:
        session.close()

    ts = datetime.now().strftime('%m-%d %H:%M')
    body = "\n".join(lines)
    # 항상 로컬 로그에는 전체 상태를 남긴다 (조용해도 추적 가능)
    logger.info(f"하트비트 점검: issues={len(issues)}\n{body}")

    if issues:
        send_telegram(f"🔴 수집 파이프라인 이상 ({ts})\n" + body)
        logger.info("이상 발송 완료")
    elif datetime.now().weekday() == WEEKLY_PULSE_WEEKDAY:
        # 주간 생존 펄스: 평소 조용하므로 하트비트 자체가 살아있음을 주1회 알린다
        send_telegram(f"🟢 수집 파이프라인 정상 (주간 점검 {ts})\n" + body)
        logger.info("주간 생존 펄스 발송 완료")
    else:
        logger.info("정상 — 무발송")


if __name__ == "__main__":
    main()

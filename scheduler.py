import time
import threading
import logging
from datetime import datetime
from config import PRIORITY_INTERVALS
import db
import scraper
import bot

logger = logging.getLogger(__name__)

state = {
    'last_scraped_sub': None,
    'last_scraped_at': None,
    'last_scrape_ok': None,
    'consecutive_failures': 0,
    'next_scrape_times': {},
    'first_boot_done': False,
}

# FIX: Prevents overlapping scrape cycles
_scrape_running = threading.Event()


def first_boot_scrape():
    logger.info('=== FIRST BOOT: scraping all subs (fast mode) ===')
    subs = db.get_subreddits()
    extra_keywords = db.get_all_extra_keywords()
    total = 0

    for sub_obj in subs:
        name = sub_obj['name']
        try:
            posts = scraper.fetch_subreddit(name, extra_keywords, fast_mode=True)
            for post in posts:
                db.upsert_post(post)
            total += len(posts)
            state['last_scraped_sub'] = name
            state['last_scraped_at'] = datetime.utcnow().isoformat()
            state['last_scrape_ok'] = True
            priority = sub_obj.get('priority', 'medium')
            state['next_scrape_times'][name] = time.time() + PRIORITY_INTERVALS[priority]
        except Exception as e:
            logger.error(f'First boot error r/{name}: {e}')
        time.sleep(2)

    logger.info(f'=== FIRST BOOT DONE: {total} posts across {len(subs)} subs ===')
    state['first_boot_done'] = True
    _send_pending_notifications()


def scrape_due_subs():
    """Run only subs whose interval has elapsed. Skips if already running."""
    # FIX: Don't pile up concurrent scrape calls
    if _scrape_running.is_set():
        logger.debug('Scrape already running — skipping this tick')
        return

    _scrape_running.set()
    try:
        subs = db.get_subreddits()
        if not subs:
            return
        now = time.time()
        extra_keywords = db.get_all_extra_keywords()
        next_times = state['next_scrape_times']

        for sub_obj in subs:
            name = sub_obj['name']
            priority = sub_obj.get('priority', 'medium')
            interval = PRIORITY_INTERVALS.get(priority, PRIORITY_INTERVALS['medium'])

            if now >= next_times.get(name, 0):
                logger.info(f'Scraping r/{name} [{priority}]')
                try:
                    posts = scraper.fetch_subreddit(name, extra_keywords, fast_mode=False)
                    for post in posts:
                        db.upsert_post(post)
                    next_times[name] = time.time() + interval
                    state['last_scraped_sub'] = name
                    state['last_scraped_at'] = datetime.utcnow().isoformat()
                    state['last_scrape_ok'] = True
                    state['consecutive_failures'] = 0
                    logger.info(f'r/{name}: {len(posts)} posts. Next in {interval//60}m')
                except Exception as e:
                    state['last_scrape_ok'] = False
                    state['consecutive_failures'] += 1
                    retry_delay = min(60 * (2 ** state['consecutive_failures']), 300)
                    next_times[name] = time.time() + retry_delay
                    logger.error(f'Scrape error r/{name}: {e} — retry in {retry_delay}s')

                time.sleep(4)  # avoid Reddit rate limiting between subreddits

        if state['first_boot_done']:
            threading.Thread(target=_send_pending_notifications, daemon=True).start()

    except Exception as e:
        logger.error(f'scrape_due_subs error: {e}')
    finally:
        _scrape_running.clear()


def _send_pending_notifications():
    """
    Send Telegram alerts for new posts.
    FIX: Only alert on posts fetched in last 2 hours (avoids restart spam).
    FIX: Cap at 50 per cycle (was 10 — too low).
    """
    try:
        unnotified = db.get_unnotified_posts(max_age_hours=2)
        if not unnotified:
            return
        logger.info(f'Sending {len(unnotified)} Telegram alerts...')
        for post in unnotified[:50]:
            bot.send_post_alert(post)
            db.mark_notified(post['id'])
            time.sleep(0.3)  # ~3/sec, well under Telegram's 30/sec limit
        # Mark any beyond cap as notified so they don't accumulate
        for post in unnotified[50:]:
            db.mark_notified(post['id'])
    except Exception as e:
        logger.error(f'Notification error: {e}')


def _scrape_loop():
    time.sleep(5)
    try:
        stats = db.get_stats()
        if stats['total'] == 0:
            logger.info('Empty DB — running first boot scrape')
            first_boot_scrape()
        else:
            logger.info(f'DB has {stats["total"]} posts — skipping first boot')
            state['first_boot_done'] = True
    except Exception as e:
        logger.error(f'First boot check failed: {e}')
        state['first_boot_done'] = True

    while True:
        try:
            scrape_due_subs()
        except Exception as e:
            logger.error(f'Scrape loop error: {e}')
        # FIX: Sleep 30s — each scrape cycle takes much longer than 15s anyway
        # The _scrape_running flag ensures no overlap regardless
        time.sleep(30)


def _scheduler_loop():
    """Daily + weekly Telegram summaries."""
    last_daily = None
    last_weekly = None
    while True:
        try:
            now = datetime.now()
            today_str = now.strftime('%Y-%m-%d')
            if now.hour == 8 and last_daily != today_str:
                stats = db.get_stats()
                posts = db.get_posts(limit=500, post_type='all')
                today_start = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                today_posts = [p for p in posts if p['fetched_at'] >= today_start]
                bot.send_daily_summary(stats, today_posts)
                last_daily = today_str
            week_str = now.strftime('%Y-W%W')
            if now.weekday() == 0 and now.hour == 9 and last_weekly != week_str:
                bot.send_weekly_stats(db.get_stats())
                last_weekly = week_str
        except Exception as e:
            logger.error(f'Scheduler error: {e}')
        time.sleep(60)


def start():
    """Start all background threads. Must be called once at app startup."""
    threading.Thread(target=_scrape_loop, daemon=True).start()
    threading.Thread(target=_scheduler_loop, daemon=True).start()
    logger.info('Scheduler started: scrape loop + daily/weekly summaries')

import time
import threading
import logging
from datetime import datetime
from config import PRIORITY_INTERVALS
import db
import scraper
import bot

logger = logging.getLogger(__name__)

# ── Shared state ──
state = {
    'last_scraped_sub': None,
    'last_scraped_at': None,
    'last_scrape_ok': None,
    'consecutive_failures': 0,
    'next_scrape_times': {},
    'first_boot_done': False,
}


def first_boot_scrape():
    """Scrape all subs with pagination on first run to build DB fast."""
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
            logger.info(f'First boot r/{name}: {len(posts)} posts (total: {total})')
            state['last_scraped_sub'] = name
            state['last_scraped_at'] = datetime.utcnow().isoformat()
            state['last_scrape_ok'] = True
            priority = sub_obj.get('priority', 'medium')
            state['next_scrape_times'][name] = time.time() + PRIORITY_INTERVALS[priority]
        except Exception as e:
            logger.error(f'First boot error r/{name}: {e}')
        time.sleep(2)  # Reduced from 10 to 2 seconds

    logger.info(f'=== FIRST BOOT DONE: {total} posts across {len(subs)} subs ===')
    state['first_boot_done'] = True

    # Send Telegram alerts in background
    try:
        unnotified = db.get_unnotified_posts()
        logger.info(f'Sending {len(unnotified)} Telegram alerts...')
        for post in unnotified:
            bot.send_post_alert(post)
            db.mark_notified(post['id'])
            time.sleep(0.1)  # Faster
    except Exception as e:
        logger.error(f'First boot Telegram error: {e}')


def scrape_due_subs():
    """Run only subs whose interval has elapsed."""
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
                    # Use a shorter timeout for scraping
                    posts = scraper.fetch_subreddit(name, extra_keywords, fast_mode=False)
                    
                    # Upsert posts in smaller batches
                    for post in posts:
                        db.upsert_post(post)

                    next_times[name] = now + interval
                    state['last_scraped_sub'] = name
                    state['last_scraped_at'] = datetime.utcnow().isoformat()
                    state['last_scrape_ok'] = True
                    state['consecutive_failures'] = 0
                    logger.info(f'r/{name}: {len(posts)} posts. Next in {interval//60}m')

                except Exception as e:
                    state['last_scrape_ok'] = False
                    state['consecutive_failures'] += 1
                    logger.error(f'Scrape error r/{name}: {e}')
                    # Exponential backoff for retries
                    retry_delay = min(60 * (2 ** state['consecutive_failures']), 300)
                    next_times[name] = now + retry_delay
                    logger.info(f'Retry r/{name} in {retry_delay}s')

                # Small delay between subreddits
                time.sleep(1)

        # Send notifications in a separate thread to not block
        if state['first_boot_done']:
            threading.Thread(target=_send_pending_notifications, daemon=True).start()

    except Exception as e:
        logger.error(f'scrape_due_subs error: {e}')


def _send_pending_notifications():
    """Send notifications for new posts (runs in background)"""
    try:
        unnotified = db.get_unnotified_posts()
        if unnotified:
            logger.info(f'Sending {len(unnotified)} Telegram alerts...')
            for post in unnotified[:10]:  # Only send first 10 to avoid rate limits
                bot.send_post_alert(post)
                db.mark_notified(post['id'])
                time.sleep(0.1)
    except Exception as e:
        logger.error(f'Notification error: {e}')


def _scrape_loop():
    time.sleep(5)  # Reduced from 10 to 5 seconds
    # Check if DB empty — if so, first boot scrape
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
        time.sleep(15)  # Reduced from 30 to 15 seconds for faster response


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
                today_start = int(now.replace(hour=0, minute=0, second=0).timestamp())
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
    """Start all background threads. Call once at app startup."""
    threading.Thread(target=_scrape_loop, daemon=True).start()
    threading.Thread(target=_scheduler_loop, daemon=True).start()
    logger.info('Scheduler started: scrape loop + daily/weekly summaries')
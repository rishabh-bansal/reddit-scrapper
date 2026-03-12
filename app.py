import os
import logging
import threading
import time
import requests as req
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

import db
import scraper
import bot

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

db.init_db()

DASHBOARD_URL = os.environ.get('RENDER_EXTERNAL_URL', 'http://localhost:5000')

# ── API Routes ──

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/posts')
def get_posts():
    post_type = request.args.get('type', 'all')
    hide_contacted = request.args.get('hide_contacted', 'false') == 'true'
    limit = int(request.args.get('limit', 200))
    offset = int(request.args.get('offset', 0))
    posts = db.get_posts(limit=limit, offset=offset, post_type=post_type, hide_contacted=hide_contacted)
    return jsonify(posts)

@app.route('/api/stats')
def get_stats():
    return jsonify(db.get_stats())

@app.route('/api/subreddits', methods=['GET'])
def get_subreddits():
    return jsonify(db.get_subreddits())

@app.route('/api/subreddits', methods=['POST'])
def update_subreddits():
    data = request.json
    subs = data.get('subreddits', [])
    db.save_subreddits(subs)
    return jsonify({'ok': True})

@app.route('/api/contacted/<post_id>', methods=['POST'])
def mark_contacted(post_id):
    db.mark_contacted(post_id)
    return jsonify({'ok': True})

@app.route('/api/contacted/<post_id>', methods=['DELETE'])
def unmark_contacted(post_id):
    db.unmark_contacted(post_id)
    return jsonify({'ok': True})

@app.route('/api/scrape', methods=['POST'])
def manual_scrape():
    """Force all due subs to be scraped now by resetting their next_scrape_times."""
    scraper_state['next_scrape_times'] = {}  # reset all — everything becomes due
    threading.Thread(target=scrape_due_subs, daemon=True).start()
    return jsonify({'ok': True, 'message': 'Forced scrape of all subreddits triggered'})

# ── Event Keywords ──

@app.route('/api/events', methods=['GET'])
def get_events():
    return jsonify(db.get_event_keywords())

@app.route('/api/events', methods=['POST'])
def add_events():
    data = request.json
    names = data.get('names', [])
    if not names:
        return jsonify({'ok': False, 'error': 'No names provided'}), 400
    db.add_event_keywords(names)
    return jsonify({'ok': True, 'added': len(names)})

@app.route('/api/events/<int:kid>', methods=['DELETE'])
def delete_event(kid):
    db.delete_event_keyword(kid)
    return jsonify({'ok': True})

# ── Status Check ──

@app.route('/api/status')
def status():
    results = {}

    # Reddit status — use last scrape result, never hit Reddit just for a status check
    last_sub = scraper_state.get('last_scraped_sub')
    last_at = scraper_state.get('last_scraped_at')
    last_ok = scraper_state.get('last_scrape_ok', None)
    results['reddit'] = {
        'ok': last_ok if last_ok is not None else True,  # assume ok until proven otherwise
        'last_sub': last_sub,
        'last_scraped_at': last_at,
        'note': 'status from last scrape cycle, not a live check'
    }

    # Telegram check
    telegram_token = os.environ.get('TELEGRAM_TOKEN', '')
    telegram_chat = os.environ.get('TELEGRAM_CHAT_ID', '')
    if telegram_token:
        try:
            r = req.get(f'https://api.telegram.org/bot{telegram_token}/getMe', timeout=8)
            data = r.json()
            results['telegram'] = {
                'ok': data.get('ok', False),
                'bot_name': data.get('result', {}).get('username', ''),
                'chat_configured': bool(telegram_chat)
            }
        except Exception as e:
            results['telegram'] = {'ok': False, 'error': str(e)}
    else:
        results['telegram'] = {'ok': False, 'error': 'TELEGRAM_TOKEN not set'}

    # Claude AI check
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
    results['claude'] = {'ok': bool(anthropic_key), 'configured': bool(anthropic_key)}

    # DB check
    try:
        stats = db.get_stats()
        results['database'] = {'ok': True, 'total_posts': stats['total']}
    except Exception as e:
        results['database'] = {'ok': False, 'error': str(e)}

    # Scraper status
    results['scraper'] = {
        'last_scraped_sub': scraper_state['last_scraped_sub'],
        'last_scraped_at': scraper_state['last_scraped_at'],
        'next_scrape_times': {k: round(v - time.time()) for k, v in scraper_state['next_scrape_times'].items()},
        'intervals': PRIORITY_INTERVALS,
        'subs_total': len(db.get_subreddits())
    }

    return jsonify(results)

@app.route('/ping')
def ping():
    return 'pong'

@app.route('/api/debug/scrape')
def debug_scrape():
    """Test-scrape a single sub and return raw results without storing — for diagnosis."""
    sub = request.args.get('sub', 'ConcertTicketsIndia')
    extra = db.get_all_extra_keywords()
    posts = scraper.fetch_subreddit(sub, extra)
    return jsonify({
        'subreddit': sub,
        'found': len(posts),
        'posts': [{'id': p['id'], 'title': p['title'], 'type': p['post_type']} for p in posts[:10]]
    })

# ── Scraper State ──

# Priority-based intervals (safe for Reddit rate limits):
#   High   → every 5 min  (ticket-focused subs, most valuable)
#   Medium → every 15 min (concert community subs)
#   Low    → every 45 min (general city/marketplace subs)
#
# Total max requests per hour across all subs ≈ 20 → well within Reddit limits

PRIORITY_INTERVALS = {
    'high':   5 * 60,   # 300s
    'medium': 15 * 60,  # 900s
    'low':    45 * 60,  # 2700s
}

scraper_state = {
    'last_scraped_sub': None,
    'last_scraped_at': None,
    'last_scrape_ok': None,
    'consecutive_failures': 0,
    'next_scrape_times': {},  # sub_name -> next scrape unix timestamp
}

def scrape_due_subs():
    """Scrape all subreddits that are due based on their priority interval."""
    try:
        subs = db.get_subreddits()  # list of {name, priority}
        if not subs:
            return

        now = time.time()
        extra_keywords = db.get_all_extra_keywords()
        next_times = scraper_state['next_scrape_times']

        for sub_obj in subs:
            name = sub_obj['name']
            priority = sub_obj.get('priority', 'medium')
            interval = PRIORITY_INTERVALS.get(priority, PRIORITY_INTERVALS['medium'])

            # Due if never scraped or past its interval
            if now >= next_times.get(name, 0):
                logger.info(f'Scraping r/{name} [{priority}]')
                try:
                    posts = scraper.fetch_subreddit(name, extra_keywords)
                    for post in posts:
                        db.upsert_post(post)

                    # Notify new posts
                    unnotified = db.get_unnotified_posts()
                    for post in unnotified:
                        bot.send_post_alert(post)
                        db.mark_notified(post['id'])
                        time.sleep(0.3)

                    next_times[name] = now + interval
                    scraper_state['last_scraped_sub'] = name
                    scraper_state['last_scraped_at'] = datetime.utcnow().isoformat()
                    scraper_state['last_scrape_ok'] = True
                    scraper_state['consecutive_failures'] = 0
                    logger.info(f'r/{name}: {len(posts)} posts. Next in {interval//60}m')

                except Exception as e:
                    scraper_state['last_scrape_ok'] = False
                    scraper_state['consecutive_failures'] += 1
                    logger.error(f'Error scraping r/{name}: {e}')
                    next_times[name] = now + 60  # retry in 1 min on error

                # Small gap between each sub to avoid burst
                time.sleep(1)

    except Exception as e:
        logger.error(f'scrape_due_subs error: {e}')


def scrape_loop():
    """
    Checks every 30 seconds which subs are due.
    Runs each due sub sequentially with small gaps — never blocks the check timer.
    """
    time.sleep(10)  # let app fully boot first
    while True:
        try:
            scrape_due_subs()
        except Exception as e:
            logger.error(f'scrape_loop error: {e}')
        time.sleep(30)  # check every 30s, only fires what's actually due


def self_ping_loop():
    """Ping self every 10 minutes to prevent Render free tier sleeping."""
    time.sleep(30)
    while True:
        try:
            req.get(f'{DASHBOARD_URL}/ping', timeout=5)
        except:
            pass
        time.sleep(600)


def scheduler_loop():
    """Daily summary at 8am, weekly stats Monday 9am."""
    last_daily = None
    last_weekly = None
    while True:
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')
        if now.hour == 8 and last_daily != today_str:
            stats = db.get_stats()
            new_today = db.get_posts(limit=500, post_type='all')
            today_start = int(now.replace(hour=0, minute=0, second=0).timestamp())
            new_today_filtered = [p for p in new_today if p['fetched_at'] >= today_start]
            bot.send_daily_summary(stats, new_today_filtered)
            last_daily = today_str
        week_str = now.strftime('%Y-W%W')
        if now.weekday() == 0 and now.hour == 9 and last_weekly != week_str:
            stats = db.get_stats()
            bot.send_weekly_stats(stats)
            last_weekly = week_str
        time.sleep(60)


def start_background_threads():
    threading.Thread(target=scrape_loop, daemon=True).start()
    threading.Thread(target=self_ping_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    logger.info('Background threads started. Priority scraping: high=5min, medium=15min, low=45min.')


if __name__ == '__main__':
    db.init_db()
    start_background_threads()
    if os.environ.get('TELEGRAM_TOKEN'):
        time.sleep(2)
        bot.send_startup_message(DASHBOARD_URL)
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

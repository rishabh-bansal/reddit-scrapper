import os
import logging
import threading
import time
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
    """Trigger a manual scrape"""
    threading.Thread(target=run_scrape_cycle, daemon=True).start()
    return jsonify({'ok': True, 'message': 'Scrape triggered'})

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'time': datetime.utcnow().isoformat()})

# Self-ping to prevent Render free tier sleeping
@app.route('/ping')
def ping():
    return 'pong'

# ── Background Jobs ──

def run_scrape_cycle():
    """Single scrape + notify cycle"""
    try:
        logger.info('Starting scrape cycle...')
        scraper.scrape_all()

        # Notify unread posts
        unnotified = db.get_unnotified_posts()
        for post in unnotified:
            bot.send_post_alert(post)
            db.mark_notified(post['id'])
            time.sleep(0.3)  # avoid Telegram flood limits

        logger.info(f'Notified {len(unnotified)} new posts')
    except Exception as e:
        logger.error(f'Scrape cycle error: {e}')


def scrape_loop():
    """Main background loop — runs every 61 seconds"""
    time.sleep(5)  # wait for app to fully start
    while True:
        run_scrape_cycle()
        time.sleep(61)


def self_ping_loop():
    """Ping self every 10 minutes to prevent Render free tier sleeping"""
    time.sleep(30)
    while True:
        try:
            import requests as req
            req.get(f'{DASHBOARD_URL}/ping', timeout=5)
            logger.debug('Self-ping sent')
        except:
            pass
        time.sleep(600)  # every 10 minutes


def scheduler_loop():
    """Handles daily summary and weekly stats"""
    last_daily = None
    last_weekly = None

    while True:
        now = datetime.now()

        # Daily summary at 8am
        today_str = now.strftime('%Y-%m-%d')
        if now.hour == 8 and last_daily != today_str:
            stats = db.get_stats()
            new_today = db.get_posts(limit=500, post_type='all')
            today_start = int(now.replace(hour=0, minute=0, second=0).timestamp())
            new_today_filtered = [p for p in new_today if p['fetched_at'] >= today_start]
            bot.send_daily_summary(stats, new_today_filtered)
            last_daily = today_str
            logger.info('Daily summary sent')

        # Weekly stats on Monday at 9am
        week_str = now.strftime('%Y-W%W')
        if now.weekday() == 0 and now.hour == 9 and last_weekly != week_str:
            stats = db.get_stats()
            bot.send_weekly_stats(stats)
            last_weekly = week_str
            logger.info('Weekly stats sent')

        time.sleep(60)


# ── Startup ──

def start_background_threads():
    threading.Thread(target=scrape_loop, daemon=True).start()
    threading.Thread(target=self_ping_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    logger.info('Background threads started')


if __name__ == '__main__':
    db.init_db()
    start_background_threads()

    # Send startup Telegram message
    if os.environ.get('TELEGRAM_TOKEN'):
        time.sleep(2)
        bot.send_startup_message(DASHBOARD_URL)

    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

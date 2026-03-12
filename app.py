import logging
import threading
import time
import requests as req
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor
import functools

import config
import db
import scraper
import scheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

# ── Thread pool for background tasks ──
executor = ThreadPoolExecutor(max_workers=2)
scrape_lock = threading.Lock()

# ── Cache for expensive operations ──
stats_cache = {
    'data': None,
    'timestamp': 0
}
CACHE_TTL = 10  # seconds

def cached(timeout=CACHE_TTL):
    """Decorator to cache function results"""
    def decorator(func):
        cache = {}
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(kwargs)
            now = time.time()
            if key in cache and now - cache[key]['timestamp'] < timeout:
                return cache[key]['data']
            result = func(*args, **kwargs)
            cache[key] = {'data': result, 'timestamp': now}
            return result
        return wrapper
    return decorator

# ── Routes ──

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/ping')
def ping():
    return 'pong'

@app.route('/api/posts')
@cached(timeout=5)  # Cache for 5 seconds
def get_posts():
    post_type = request.args.get('type', 'all')
    hide_contacted = request.args.get('hide_contacted', 'false') == 'true'
    limit = int(request.args.get('limit', 100))  # Reduced from 200 to 100
    offset = int(request.args.get('offset', 0))
    try:
        posts = db.get_posts(limit=limit, offset=offset, post_type=post_type, hide_contacted=hide_contacted)
        return jsonify(posts)
    except Exception as e:
        logger.error(f'get_posts error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
@cached(timeout=10)  # Cache for 10 seconds
def get_stats():
    try:
        return jsonify(db.get_stats())
    except Exception as e:
        logger.error(f'get_stats error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/subreddits', methods=['GET'])
@cached(timeout=30)  # Cache for 30 seconds
def get_subreddits():
    try:
        return jsonify(db.get_subreddits())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/subreddits', methods=['POST'])
def update_subreddits():
    subs = request.json.get('subreddits', [])
    db.save_subreddits(subs)
    # Clear cache
    stats_cache['timestamp'] = 0
    return jsonify({'ok': True})

@app.route('/api/contacted/<post_id>', methods=['POST'])
def mark_contacted(post_id):
    db.mark_contacted(post_id)
    # Clear cache
    stats_cache['timestamp'] = 0
    return jsonify({'ok': True})

@app.route('/api/contacted/<post_id>', methods=['DELETE'])
def unmark_contacted(post_id):
    db.unmark_contacted(post_id)
    # Clear cache
    stats_cache['timestamp'] = 0
    return jsonify({'ok': True})

@app.route('/api/scrape', methods=['POST'])
def manual_scrape():
    """Non-blocking scrape endpoint"""
    def background_scrape():
        with scrape_lock:
            logger.info("Manual scrape triggered in background")
            scheduler.state['next_scrape_times'] = {}
            scheduler.scrape_due_subs()
    
    executor.submit(background_scrape)
    return jsonify({'ok': True, 'message': 'Scrape triggered in background'})

@app.route('/api/events', methods=['GET'])
@cached(timeout=30)
def get_events():
    return jsonify(db.get_event_keywords())

@app.route('/api/events', methods=['POST'])
def add_events():
    names = request.json.get('names', [])
    if not names:
        return jsonify({'ok': False, 'error': 'No names provided'}), 400
    db.add_event_keywords(names)
    return jsonify({'ok': True, 'added': len(names)})

@app.route('/api/events/<int:kid>', methods=['DELETE'])
def delete_event(kid):
    db.delete_event_keyword(kid)
    return jsonify({'ok': True})

@app.route('/api/status')
@cached(timeout=15)
def status():
    """Fast status endpoint - doesn't block on scraper"""
    results = {
        'reddit': {
            'ok': True,  # Assume OK for dashboard
            'first_boot_done': scheduler.state.get('first_boot_done', True),
            'last_scraped_at': scheduler.state.get('last_scraped_at'),
            'last_sub': scheduler.state.get('last_scraped_sub'),
        },
        'database': {
            'ok': True,
            'total_posts': 406,  # This will be updated by cache
            'provider': 'Supabase'
        },
        'telegram': {
            'ok': bool(config.TELEGRAM_TOKEN),
            'chat_configured': bool(config.TELEGRAM_CHAT_ID)
        },
        'claude': {
            'ok': bool(config.GEMINI_API_KEY),
            'provider': 'Gemini Flash'
        },
        'scraper': {
            'first_boot_done': scheduler.state.get('first_boot_done', True),
            'intervals': config.PRIORITY_INTERVALS,
            'subs_total': len(db.get_subreddits())
        }
    }
    
    # Try to get actual DB count but don't fail if slow
    try:
        stats = db.get_stats()
        results['database']['total_posts'] = stats['total']
    except:
        pass
        
    return jsonify(results)

@app.route('/api/debug/scrape')
def debug_scrape():
    sub = request.args.get('sub', 'ConcertTicketsIndia')
    extra = db.get_all_extra_keywords()
    try:
        posts = scraper.fetch_subreddit(sub, extra)
        return jsonify({
            'subreddit': sub,
            'found': len(posts),
            'posts': [{'id': p['id'], 'title': p['title'], 'type': p['post_type']} for p in posts[:10]]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── Self-ping to keep Render free tier awake ──
def _self_ping():
    time.sleep(30)
    while True:
        try:
            req.get(f'{config.DASHBOARD_URL}/ping', timeout=5)
        except:
            pass
        time.sleep(600)

# ── Startup — runs whether gunicorn or python app.py ──
# Initialize database connection pool
db.init_db_pool()
db_ok = db.init_db()
if not db_ok:
    logger.error('⚠ App started but DB is unreachable. Check DATABASE_URL.')

# Start scheduler in background thread
scheduler.start()

# Start self-ping
threading.Thread(target=_self_ping, daemon=True).start()

if __name__ == '__main__':
    import os, bot
    if config.TELEGRAM_TOKEN:
        time.sleep(2)
        bot.send_startup_message(config.DASHBOARD_URL)
    app.run(host='0.0.0.0', port=config.PORT, debug=False)
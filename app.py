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
        logger.error(f'get_subreddits error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/subreddits', methods=['POST'])
def update_subreddits():
    try:
        subs = request.json.get('subreddits', [])
        db.save_subreddits(subs)
        # Clear cache
        stats_cache['timestamp'] = 0
        return jsonify({'ok': True})
    except Exception as e:
        logger.error(f'update_subreddits error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/contacted/<post_id>', methods=['POST'])
def mark_contacted(post_id):
    try:
        db.mark_contacted(post_id)
        # Clear cache
        stats_cache['timestamp'] = 0
        return jsonify({'ok': True})
    except Exception as e:
        logger.error(f'mark_contacted error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/contacted/<post_id>', methods=['DELETE'])
def unmark_contacted(post_id):
    try:
        db.unmark_contacted(post_id)
        # Clear cache
        stats_cache['timestamp'] = 0
        return jsonify({'ok': True})
    except Exception as e:
        logger.error(f'unmark_contacted error: {e}')
        return jsonify({'error': str(e)}), 500

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
    try:
        return jsonify(db.get_event_keywords())
    except Exception as e:
        logger.error(f'get_events error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/events', methods=['POST'])
def add_events():
    try:
        names = request.json.get('names', [])
        if not names:
            return jsonify({'ok': False, 'error': 'No names provided'}), 400
        db.add_event_keywords(names)
        return jsonify({'ok': True, 'added': len(names)})
    except Exception as e:
        logger.error(f'add_events error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/events/<int:kid>', methods=['DELETE'])
def delete_event(kid):
    try:
        db.delete_event_keyword(kid)
        return jsonify({'ok': True})
    except Exception as e:
        logger.error(f'delete_event error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/status')
@cached(timeout=15)
def status():
    """Fast status endpoint - doesn't block on scraper"""
    try:
        # Get DB stats for total posts
        total_posts = 0
        try:
            stats = db.get_stats()
            total_posts = stats['total']
        except:
            pass
        
        results = {
            'reddit': {
                'ok': scheduler.state.get('last_scrape_ok', True),
                'first_boot_done': scheduler.state.get('first_boot_done', True),
                'last_scraped_at': scheduler.state.get('last_scraped_at'),
                'last_sub': scheduler.state.get('last_scraped_sub'),
            },
            'database': {
                'ok': True,
                'total_posts': total_posts,
                'provider': 'Supabase'
            },
            'telegram': {
                'ok': bool(config.TELEGRAM_TOKEN),
                'bot_name': '',
                'chat_configured': bool(config.TELEGRAM_CHAT_ID)
            },
            'claude': {
                'ok': bool(config.GEMINI_API_KEY),
                'provider': 'Gemini Flash'
            },
            'scraper': {
                'first_boot_done': scheduler.state.get('first_boot_done', True),
                'intervals': config.PRIORITY_INTERVALS,
                'subs_total': len(db.get_subreddits()),
                'last_scraped_sub': scheduler.state.get('last_scraped_sub'),
                'last_scraped_at': scheduler.state.get('last_scraped_at')
            }
        }
        
        # Try to get Telegram bot name if configured
        if config.TELEGRAM_TOKEN:
            try:
                r = req.get(f'https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/getMe', timeout=3)
                data = r.json()
                if data.get('ok'):
                    results['telegram']['bot_name'] = data.get('result', {}).get('username', '')
                    results['telegram']['ok'] = True
            except:
                pass
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f'status error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug/scrape')
def debug_scrape():
    """Debug endpoint to test scraping a specific subreddit"""
    sub = request.args.get('sub', 'ConcertTicketsIndia')
    try:
        extra = db.get_all_extra_keywords()
        posts = scraper.fetch_subreddit(sub, extra, fast_mode=False)
        return jsonify({
            'subreddit': sub,
            'found': len(posts),
            'posts': [{'id': p['id'], 'title': p['title'], 'type': p['post_type']} for p in posts[:10]]
        })
    except Exception as e:
        logger.error(f'debug_scrape error: {e}')
        return jsonify({'error': str(e)}), 500

# ── Self-ping to keep Render free tier awake ──
def _self_ping():
    """Ping the app every 10 minutes to prevent sleeping"""
    time.sleep(30)  # Wait for app to fully start
    while True:
        try:
            # Ping self
            req.get(f'{config.DASHBOARD_URL}/ping', timeout=5)
            logger.debug('Self-ping successful')
        except Exception as e:
            logger.debug(f'Self-ping failed: {e}')
        time.sleep(600)  # 10 minutes

# ── Startup — runs whether gunicorn or python app.py ──
def startup():
    """Run startup tasks"""
    logger.info('🚀 Starting Reticket backend...')
    
    # Initialize database connection pool
    logger.info('Initializing database connection pool...')
    db_ok = db.init_db()
    if not db_ok:
        logger.error('⚠ App started but DB is unreachable. Check DATABASE_URL.')
        logger.error('  Make sure you are using the session pooler URL:')
        logger.error('  postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:5432/postgres')
    else:
        logger.info('✅ Database connection successful')
    
    # Start scheduler
    logger.info('Starting scheduler...')
    scheduler.start()
    
    # Start self-ping
    logger.info('Starting self-ping thread...')
    threading.Thread(target=_self_ping, daemon=True).start()
    
    logger.info('✅ Startup complete')

# Run startup
startup()

if __name__ == '__main__':
    """Run the app directly (for development)"""
    import os
    import bot
    
    logger.info(f'Starting Flask development server on port {config.PORT}')
    
    # Send startup message if Telegram is configured
    if config.TELEGRAM_TOKEN and config.TELEGRAM_CHAT_ID:
        time.sleep(2)  # Wait for app to fully initialize
        try:
            bot.send_startup_message(config.DASHBOARD_URL)
            logger.info('Startup message sent to Telegram')
        except Exception as e:
            logger.error(f'Failed to send startup message: {e}')
    
    app.run(host='0.0.0.0', port=config.PORT, debug=False)

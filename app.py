import logging
import threading
import time
import requests as req
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor

import config
import db
import scraper
import scheduler
import classifier

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

executor = ThreadPoolExecutor(max_workers=3)
scrape_lock = threading.Lock()

# ── Simple time-based cache (keyed by full URL including query string) ──
_cache: dict = {}
_cache_lock = threading.Lock()


def _cache_get(key: str, ttl: int):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry['ts']) < ttl:
            return entry['val']
    return None


def _cache_set(key: str, val):
    with _cache_lock:
        _cache[key] = {'val': val, 'ts': time.time()}


def _cache_clear_prefix(prefix: str):
    with _cache_lock:
        for k in list(_cache.keys()):
            if k.startswith(prefix):
                del _cache[k]


# ── Routes ──

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/ping')
def ping():
    return 'pong'


@app.route('/api/posts')
def get_posts():
    # FIX: cache key includes all query params so different filters don't collide
    cache_key = f'posts:{request.query_string.decode()}'
    cached = _cache_get(cache_key, ttl=5)
    if cached is not None:
        return jsonify(cached)
    try:
        post_type    = request.args.get('type', 'all')
        hide_contacted = request.args.get('hide_contacted', 'false') == 'true'
        limit        = min(int(request.args.get('limit', 100)), 200)
        offset       = int(request.args.get('offset', 0))
        posts = db.get_posts(limit=limit, offset=offset, post_type=post_type,
                             hide_contacted=hide_contacted)
        _cache_set(cache_key, posts)
        return jsonify(posts)
    except Exception as e:
        logger.error(f'get_posts error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats')
def get_stats():
    cached = _cache_get('stats', ttl=10)
    if cached is not None:
        return jsonify(cached)
    try:
        stats = db.get_stats()
        _cache_set('stats', stats)
        return jsonify(stats)
    except Exception as e:
        logger.error(f'get_stats error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/subreddits', methods=['GET'])
def get_subreddits():
    cached = _cache_get('subreddits', ttl=30)
    if cached is not None:
        return jsonify(cached)
    try:
        subs = db.get_subreddits()
        _cache_set('subreddits', subs)
        return jsonify(subs)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/subreddits', methods=['POST'])
def update_subreddits():
    try:
        subs = request.json.get('subreddits', [])
        db.save_subreddits(subs)
        _cache_clear_prefix('subreddits')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacted/<post_id>', methods=['POST'])
def mark_contacted(post_id):
    try:
        db.mark_contacted(post_id)
        _cache_clear_prefix('posts:')
        _cache_clear_prefix('stats')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacted/<post_id>', methods=['DELETE'])
def unmark_contacted(post_id):
    try:
        db.unmark_contacted(post_id)
        _cache_clear_prefix('posts:')
        _cache_clear_prefix('stats')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scrape', methods=['POST'])
def manual_scrape():
    def _bg():
        with scrape_lock:
            logger.info('Manual scrape triggered')
            scheduler.state['next_scrape_times'] = {}
            scheduler.scrape_due_subs()
    executor.submit(_bg)
    return jsonify({'ok': True, 'message': 'Scrape triggered in background'})


@app.route('/api/reclassify', methods=['POST'])
def reclassify():
    """Re-run Groq AI on all 'unclear' posts in DB to clean up keyword-fallback classifications."""
    def _bg():
        try:
            posts = db.get_posts(limit=500, post_type='unclear')
            if not posts:
                logger.info('Reclassify: no unclear posts found')
                return
            logger.info(f'Reclassify: processing {len(posts)} unclear posts')
            extra = db.get_all_extra_keywords()
            results = classifier.classify_batch(posts, extra)
            updated = 0
            with db._PoolConn() as conn:
                with conn:
                    with conn.cursor() as c:
                        for post in posts:
                            label = results.get(post['id'], 'unclear')
                            if label in ('buy', 'sell'):
                                c.execute(
                                    'UPDATE posts SET post_type=%s, ai_classified=1 WHERE id=%s',
                                    (label, post['id'])
                                )
                                updated += 1
            _cache_clear_prefix('posts:')
            _cache_clear_prefix('stats')
            logger.info(f'Reclassify: updated {updated} posts to buy/sell')
        except Exception as e:
            logger.error(f'Reclassify error: {e}')
    executor.submit(_bg)
    return jsonify({'ok': True, 'message': 'Reclassification started in background'})


@app.route('/api/events', methods=['GET'])
def get_events():
    cached = _cache_get('events', ttl=30)
    if cached is not None:
        return jsonify(cached)
    try:
        events = db.get_event_keywords()
        _cache_set('events', events)
        return jsonify(events)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/events', methods=['POST'])
def add_events():
    try:
        names = request.json.get('names', [])
        if not names:
            return jsonify({'ok': False, 'error': 'No names provided'}), 400
        db.add_event_keywords(names)
        _cache_clear_prefix('events')
        return jsonify({'ok': True, 'added': len(names)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/events/<int:kid>', methods=['DELETE'])
def delete_event(kid):
    try:
        db.delete_event_keyword(kid)
        _cache_clear_prefix('events')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/status')
def status():
    cached = _cache_get('status', ttl=15)
    if cached is not None:
        return jsonify(cached)
    try:
        total_posts = 0
        try:
            stats = db.get_stats()
            total_posts = stats['total']
        except Exception:
            pass

        results = {
            'reddit': {
                'ok': scheduler.state.get('last_scrape_ok', True),
                'first_boot_done': scheduler.state.get('first_boot_done', True),
                'last_scraped_at': scheduler.state.get('last_scraped_at'),
                'last_sub': scheduler.state.get('last_scraped_sub'),
            },
            'database': {'ok': True, 'total_posts': total_posts, 'provider': 'Supabase'},
            'telegram': {
                'ok': bool(config.TELEGRAM_TOKEN),
                'chat_configured': bool(config.TELEGRAM_CHAT_ID),
            },
            'claude': {
                'ok': bool(config.GROQ_API_KEY),
                'model': 'llama-3.1-8b-instant (Groq)',
            },
            'scraper': {
                'first_boot_done': scheduler.state.get('first_boot_done', True),
                'intervals': config.PRIORITY_INTERVALS,
                'subs_total': len(db.get_subreddits()),
                'last_scraped_sub': scheduler.state.get('last_scraped_sub'),
                'last_scraped_at': scheduler.state.get('last_scraped_at'),
            },
        }
        if config.TELEGRAM_TOKEN:
            try:
                r = req.get(f'https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/getMe', timeout=3)
                data = r.json()
                if data.get('ok'):
                    results['telegram']['bot_name'] = data['result'].get('username', '')
                    results['telegram']['ok'] = True
            except Exception:
                pass

        _cache_set('status', results)
        return jsonify(results)
    except Exception as e:
        logger.error(f'status error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/debug/scrape')
def debug_scrape():
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
        return jsonify({'error': str(e)}), 500


# ── Self-ping ──
def _self_ping():
    time.sleep(30)
    while True:
        try:
            req.get(f'{config.DASHBOARD_URL}/ping', timeout=5)
        except Exception:
            pass
        time.sleep(600)


# ── Startup ──
def startup():
    logger.info('🚀 Starting Reticket backend...')
    db_ok = db.init_db()
    if not db_ok:
        logger.error('⚠ App started but DB is unreachable — check DATABASE_URL')
    scheduler.start()
    threading.Thread(target=_self_ping, daemon=True).start()
    logger.info('✅ Startup complete')


startup()

if __name__ == '__main__':
    import bot
    if config.TELEGRAM_TOKEN and config.TELEGRAM_CHAT_ID:
        try:
            bot.send_startup_message(config.DASHBOARD_URL)
        except Exception:
            pass
    app.run(host='0.0.0.0', port=config.PORT, debug=False)

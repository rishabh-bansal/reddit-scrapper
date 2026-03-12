import logging
import threading
import time
import requests as req
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

import config
import db
import scraper
import scheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

# ── Routes ──

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/ping')
def ping():
    return 'pong'

@app.route('/api/posts')
def get_posts():
    post_type = request.args.get('type', 'all')
    hide_contacted = request.args.get('hide_contacted', 'false') == 'true'
    limit = int(request.args.get('limit', 200))
    offset = int(request.args.get('offset', 0))
    try:
        posts = db.get_posts(limit=limit, offset=offset, post_type=post_type, hide_contacted=hide_contacted)
        return jsonify(posts)
    except Exception as e:
        logger.error(f'get_posts error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    try:
        return jsonify(db.get_stats())
    except Exception as e:
        logger.error(f'get_stats error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/subreddits', methods=['GET'])
def get_subreddits():
    try:
        return jsonify(db.get_subreddits())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/subreddits', methods=['POST'])
def update_subreddits():
    subs = request.json.get('subreddits', [])
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
    scheduler.state['next_scrape_times'] = {}
    threading.Thread(target=scheduler.scrape_due_subs, daemon=True).start()
    return jsonify({'ok': True, 'message': 'Scrape triggered'})

@app.route('/api/events', methods=['GET'])
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
def status():
    results = {}

    # Reddit / scraper
    results['reddit'] = {
        'ok': scheduler.state.get('last_scrape_ok', True),
        'last_sub': scheduler.state.get('last_scraped_sub'),
        'last_scraped_at': scheduler.state.get('last_scraped_at'),
        'first_boot_done': scheduler.state['first_boot_done'],
    }

    # Telegram
    if config.TELEGRAM_TOKEN:
        try:
            r = req.get(f'https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/getMe', timeout=8)
            data = r.json()
            results['telegram'] = {
                'ok': data.get('ok', False),
                'bot_name': data.get('result', {}).get('username', ''),
                'chat_configured': bool(config.TELEGRAM_CHAT_ID)
            }
        except Exception as e:
            results['telegram'] = {'ok': False, 'error': str(e)}
    else:
        results['telegram'] = {'ok': False, 'error': 'TELEGRAM_TOKEN not set'}

    # AI
    results['claude'] = {
        'ok': bool(config.GEMINI_API_KEY),
        'provider': 'Gemini Flash'
    }

    # DB
    try:
        stats = db.get_stats()
        results['database'] = {'ok': True, 'total_posts': stats['total'], 'provider': 'Supabase'}
    except Exception as e:
        results['database'] = {'ok': False, 'error': str(e)}

    # Scraper details
    results['scraper'] = {
        'last_scraped_sub': scheduler.state['last_scraped_sub'],
        'last_scraped_at': scheduler.state['last_scraped_at'],
        'first_boot_done': scheduler.state['first_boot_done'],
        'intervals': config.PRIORITY_INTERVALS,
        'subs_total': len(db.get_subreddits())
    }

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
db_ok = db.init_db()
if not db_ok:
    logger.error('⚠ App started but DB is unreachable. Check DATABASE_URL.')
    logger.error('  Use session pooler URL for Render (IPv4):')
    logger.error('  postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:5432/postgres')

scheduler.start()
threading.Thread(target=_self_ping, daemon=True).start()

if __name__ == '__main__':
    import os, bot
    if config.TELEGRAM_TOKEN:
        time.sleep(2)
        bot.send_startup_message(config.DASHBOARD_URL)
    app.run(host='0.0.0.0', port=config.PORT, debug=False)

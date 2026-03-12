import os
import json
import logging
from datetime import datetime
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL', '')

def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def init_db():
    """Tables are created via Supabase migration — this just verifies connectivity."""
    try:
        conn = get_conn()
        conn.close()
        logger.info('Supabase DB connected OK')
    except Exception as e:
        logger.error(f'DB connection failed: {e}')
        raise

def upsert_post(post: dict):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('''
                    INSERT INTO posts
                    (id, subreddit, title, body, author, permalink, post_type, ai_classified,
                     ups, num_comments, created_utc, fetched_at, notified)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0)
                    ON CONFLICT (id) DO UPDATE SET
                        ups = EXCLUDED.ups,
                        num_comments = EXCLUDED.num_comments,
                        post_type = CASE WHEN posts.ai_classified=0 THEN EXCLUDED.post_type ELSE posts.post_type END,
                        ai_classified = CASE WHEN posts.ai_classified=0 THEN EXCLUDED.ai_classified ELSE posts.ai_classified END
                ''', (
                    post['id'], post['subreddit'], post['title'], post['body'],
                    post['author'], post['permalink'], post['post_type'],
                    post.get('ai_classified', 0),
                    post['ups'], post['num_comments'], post['created_utc'],
                    int(datetime.utcnow().timestamp())
                ))
    finally:
        conn.close()

def get_unnotified_posts():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute('SELECT * FROM posts WHERE notified=0 ORDER BY created_utc DESC')
            return [dict(r) for r in c.fetchall()]
    finally:
        conn.close()

def mark_notified(post_id: str):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET notified=1 WHERE id=%s', (post_id,))
    finally:
        conn.close()

def mark_contacted(post_id: str):
    conn = get_conn()
    now = int(datetime.utcnow().timestamp())
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET contacted=1, contacted_at=%s WHERE id=%s', (now, post_id))
                c.execute('INSERT INTO activity_log (timestamp, action, post_id) VALUES (%s,%s,%s)', (now, 'contacted', post_id))
    finally:
        conn.close()

def unmark_contacted(post_id: str):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET contacted=0, contacted_at=NULL WHERE id=%s', (post_id,))
    finally:
        conn.close()

def get_posts(limit=200, offset=0, post_type=None, hide_contacted=False):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            query = 'SELECT * FROM posts WHERE 1=1'
            params = []
            if post_type and post_type != 'all':
                query += ' AND post_type=%s'
                params.append(post_type)
            if hide_contacted:
                query += ' AND contacted=0'
            query += ' ORDER BY created_utc DESC LIMIT %s OFFSET %s'
            params += [limit, offset]
            c.execute(query, params)
            return [dict(r) for r in c.fetchall()]
    finally:
        conn.close()

def get_stats():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute('SELECT COUNT(*) as cnt FROM posts'); total = c.fetchone()['cnt']
            c.execute('SELECT COUNT(*) as cnt FROM posts WHERE contacted=1'); contacted = c.fetchone()['cnt']
            c.execute("SELECT COUNT(*) as cnt FROM posts WHERE post_type='sell'"); sell = c.fetchone()['cnt']
            c.execute("SELECT COUNT(*) as cnt FROM posts WHERE post_type='buy'"); buy = c.fetchone()['cnt']
            c.execute("SELECT COUNT(*) as cnt FROM posts WHERE post_type='unclear'"); unclear = c.fetchone()['cnt']

            c.execute('SELECT AVG(contacted_at - created_utc) as avg FROM posts WHERE contacted=1 AND contacted_at IS NOT NULL')
            avg_row = c.fetchone()['avg']
            avg_response = int(avg_row) if avg_row else None

            c.execute('SELECT subreddit, COUNT(*) as cnt FROM posts GROUP BY subreddit ORDER BY cnt DESC LIMIT 8')
            top_subs = [dict(r) for r in c.fetchall()]

            c.execute('''SELECT a.timestamp, a.action, p.title, p.author, p.subreddit
                         FROM activity_log a LEFT JOIN posts p ON a.post_id=p.id
                         ORDER BY a.timestamp DESC LIMIT 10''')
            activity = [dict(r) for r in c.fetchall()]

            today_start = int(datetime.utcnow().replace(hour=0, minute=0, second=0).timestamp())
            c.execute('SELECT COUNT(*) as cnt FROM posts WHERE fetched_at >= %s', (today_start,))
            today_new = c.fetchone()['cnt']

            c.execute('SELECT subreddit, COUNT(*) as cnt FROM posts GROUP BY subreddit')
            sub_counts = {r['subreddit']: r['cnt'] for r in c.fetchall()}

            return {
                'total': total, 'contacted': contacted, 'sell': sell,
                'buy': buy, 'unclear': unclear,
                'avg_response_seconds': avg_response,
                'top_subreddits': top_subs,
                'activity': activity,
                'today_new': today_new,
                'sub_counts': sub_counts
            }
    finally:
        conn.close()

def get_setting(key, default=None):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute('SELECT value FROM settings WHERE key=%s', (key,))
            row = c.fetchone()
            return row[0] if row else default
    finally:
        conn.close()

def set_setting(key, value):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('INSERT INTO settings (key, value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value',
                          (key, str(value)))
    finally:
        conn.close()

DEFAULT_SUBS = [
    {'name': 'ConcertTicketsIndia',   'priority': 'high'},
    {'name': 'ticketresellingindia',  'priority': 'high'},
    {'name': 'TicketResale',          'priority': 'high'},
    {'name': 'ConcertResale',         'priority': 'high'},
    {'name': 'concerts_india',        'priority': 'medium'},
    {'name': 'ConcertsIndia_',        'priority': 'medium'},
    {'name': 'IndianHipHopHeads',     'priority': 'medium'},
    {'name': 'Concerts',              'priority': 'medium'},
    {'name': 'delhi',                 'priority': 'low'},
    {'name': 'delhi_marketplace',     'priority': 'low'},
    {'name': 'chandigarhmarketplace', 'priority': 'low'},
    {'name': 'Tickets',               'priority': 'low'},
]

def get_subreddits():
    val = get_setting('subreddits_v2')
    if val:
        return json.loads(val)
    old_val = get_setting('subreddits')
    if old_val:
        old_list = json.loads(old_val)
        migrated = [{'name': s, 'priority': 'medium'} for s in old_list]
        save_subreddits(migrated)
        return migrated
    return DEFAULT_SUBS

def save_subreddits(subs: list):
    set_setting('subreddits_v2', json.dumps(subs))

def get_subreddit_names():
    return [s['name'] for s in get_subreddits()]

def get_event_keywords():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute('SELECT * FROM event_keywords ORDER BY created_at DESC')
            return [dict(r) for r in c.fetchall()]
    finally:
        conn.close()

def add_event_keywords(names: list):
    conn = get_conn()
    now = int(datetime.utcnow().timestamp())
    try:
        with conn:
            with conn.cursor() as c:
                for name in names:
                    name = name.strip()
                    if name:
                        c.execute('INSERT INTO event_keywords (name, created_at) VALUES (%s,%s)', (name, now))
    finally:
        conn.close()

def delete_event_keyword(kid: int):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('DELETE FROM event_keywords WHERE id=%s', (kid,))
    finally:
        conn.close()

def get_all_extra_keywords():
    events = get_event_keywords()
    keywords = []
    for e in events:
        name = e['name'].lower()
        words = [w.strip('|,.-()') for w in name.split() if len(w.strip('|,.-()')) > 3]
        keywords.extend(words)
        keywords.append(name)
    return list(set(keywords))

import json
import logging
from datetime import datetime
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from config import DATABASE_URL

logger = logging.getLogger(__name__)

# ── Connection Pool ──
db_pool = None

def init_db_pool():
    """Initialize the connection pool (call once at startup)"""
    global db_pool
    if not DATABASE_URL:
        logger.error('DATABASE_URL is not set')
        return False
    
    try:
        db_pool = pool.SimpleConnectionPool(
            1, 10,  # min 1, max 10 connections
            DATABASE_URL,
            connect_timeout=5,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        logger.info('✓ Database connection pool created')
        return True
    except Exception as e:
        logger.error(f'✗ Failed to create connection pool: {e}')
        return False

def get_conn():
    """Get a connection from the pool"""
    if not DATABASE_URL:
        raise RuntimeError('DATABASE_URL is not set')
    
    if db_pool:
        try:
            return db_pool.getconn()
        except Exception as e:
            logger.error(f'Failed to get connection from pool: {e}')
            # Fall back to direct connection
            return psycopg2.connect(DATABASE_URL, connect_timeout=10)
    else:
        return psycopg2.connect(DATABASE_URL, connect_timeout=10)

def return_conn(conn):
    """Return a connection to the pool"""
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            logger.error(f'Failed to return connection to pool: {e}')
            conn.close()
    elif conn:
        conn.close()

def init_db():
    """Verify DB connectivity on startup. Warns but does NOT crash the app."""
    try:
        conn = get_conn()
        return_conn(conn)
        logger.info('✓ Supabase DB connected OK')
        return True
    except Exception as e:
        logger.error(f'✗ DB connection failed: {e}')
        logger.error('  → Check DATABASE_URL env var')
        logger.error('  → Must use session pooler for Render (IPv4):')
        logger.error('  → postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:5432/postgres')
        return False

# ── Posts ──

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
        return_conn(conn)


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
        return_conn(conn)


def get_stats():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            # Use multiple queries but same connection
            c.execute('SELECT COUNT(*) as cnt FROM posts')
            total = c.fetchone()['cnt']
            
            c.execute('SELECT COUNT(*) as cnt FROM posts WHERE contacted=1')
            contacted = c.fetchone()['cnt']
            
            c.execute("SELECT COUNT(*) as cnt FROM posts WHERE post_type='sell'")
            sell = c.fetchone()['cnt']
            
            c.execute("SELECT COUNT(*) as cnt FROM posts WHERE post_type='buy'")
            buy = c.fetchone()['cnt']
            
            c.execute("SELECT COUNT(*) as cnt FROM posts WHERE post_type='unclear'")
            unclear = c.fetchone()['cnt']

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
        return_conn(conn)


def get_unnotified_posts():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute('SELECT * FROM posts WHERE notified=0 ORDER BY created_utc DESC')
            return [dict(r) for r in c.fetchall()]
    finally:
        return_conn(conn)


def mark_notified(post_id: str):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET notified=1 WHERE id=%s', (post_id,))
    finally:
        return_conn(conn)


def mark_contacted(post_id: str):
    conn = get_conn()
    now = int(datetime.utcnow().timestamp())
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET contacted=1, contacted_at=%s WHERE id=%s', (now, post_id))
                c.execute('INSERT INTO activity_log (timestamp, action, post_id) VALUES (%s,%s,%s)', (now, 'contacted', post_id))
    finally:
        return_conn(conn)


def unmark_contacted(post_id: str):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET contacted=0, contacted_at=NULL WHERE id=%s', (post_id,))
    finally:
        return_conn(conn)

# ── Settings ──

def get_setting(key, default=None):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute('SELECT value FROM settings WHERE key=%s', (key,))
            row = c.fetchone()
            return row[0] if row else default
    finally:
        return_conn(conn)


def set_setting(key, value):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('INSERT INTO settings (key, value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value',
                          (key, str(value)))
    finally:
        return_conn(conn)

# ── Subreddits ──

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

# ── Events / Keywords ──

def get_event_keywords():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute('SELECT * FROM event_keywords ORDER BY created_at DESC')
            return [dict(r) for r in c.fetchall()]
    finally:
        return_conn(conn)


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
        return_conn(conn)


def delete_event_keyword(kid: int):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as c:
                c.execute('DELETE FROM event_keywords WHERE id=%s', (kid,))
    finally:
        return_conn(conn)


def get_all_extra_keywords():
    events = get_event_keywords()
    keywords = []
    for e in events:
        name = e['name'].lower()
        words = [w.strip('|,.-()') for w in name.split() if len(w.strip('|,.-()')) > 3]
        keywords.extend(words)
        keywords.append(name)
    return list(set(keywords))
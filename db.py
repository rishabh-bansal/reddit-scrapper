import json
import logging
import threading
from datetime import datetime
import psycopg2
import psycopg2.extras
import psycopg2.pool
from config import DATABASE_URL

logger = logging.getLogger(__name__)

# ── Connection pool (max 5 connections — safe for Supabase free tier) ──
_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool():
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        if not DATABASE_URL:
            raise RuntimeError('DATABASE_URL is not set')
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DATABASE_URL,
            connect_timeout=10,
            sslmode='require',
            sslcompression=0,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        logger.info('DB connection pool created (max 5 connections)')
        return _pool


def get_conn():
    """Get a live connection from the pool. Tests it first; replaces if dead."""
    p = _get_pool()
    conn = p.getconn()
    try:
        conn.cursor().execute('SELECT 1')
        return conn
    except Exception:
        # Connection is stale/dead — discard and open a fresh one
        try:
            p.putconn(conn, close=True)
        except Exception:
            pass
        return psycopg2.connect(
            DATABASE_URL,
            connect_timeout=10,
            sslmode='require',
            sslcompression=0,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )


def put_conn(conn):
    """Return a connection to the pool. Discards broken connections."""
    try:
        if conn.closed:
            _get_pool().putconn(conn, close=True)
        else:
            _get_pool().putconn(conn)
    except Exception:
        pass


class _PoolConn:
    """Context manager: borrows a connection from pool and returns it on exit."""
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = get_conn()
        return self.conn

    def __exit__(self, exc_type, *_):
        if self.conn:
            if exc_type:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
            put_conn(self.conn)
            self.conn = None


def init_db() -> bool:
    """Verify DB connectivity on startup. Returns True/False — never raises."""
    try:
        with _PoolConn() as conn:
            with conn.cursor() as c:
                c.execute('SELECT 1')
        logger.info('✓ Supabase DB connected OK')
        return True
    except Exception as e:
        logger.error(f'✗ DB connection failed: {e}')
        logger.error('  → Check DATABASE_URL env var')
        logger.error('  → Must use session pooler (IPv4):')
        logger.error('  → postgresql://postgres.PROJECT_REF:PASSWORD@aws-1-ap-south-1.pooler.supabase.com:5432/postgres')
        return False


# ── Posts ──

def upsert_post(post: dict):
    try:
        with _PoolConn() as conn:
            with conn:
                with conn.cursor() as c:
                    c.execute('''
                        INSERT INTO posts
                        (id, subreddit, title, body, author, permalink, post_type, ai_classified,
                         ups, num_comments, created_utc, fetched_at, notified)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0)
                        ON CONFLICT (id) DO UPDATE SET
                            ups            = EXCLUDED.ups,
                            num_comments   = EXCLUDED.num_comments,
                            post_type      = CASE WHEN posts.ai_classified=0
                                             THEN EXCLUDED.post_type ELSE posts.post_type END,
                            ai_classified  = CASE WHEN posts.ai_classified=0
                                             THEN EXCLUDED.ai_classified ELSE posts.ai_classified END
                    ''', (
                        post['id'], post['subreddit'], post['title'], post['body'],
                        post['author'], post['permalink'], post['post_type'],
                        post.get('ai_classified', 0),
                        post['ups'], post['num_comments'], post['created_utc'],
                        int(datetime.utcnow().timestamp())
                    ))
    except Exception as e:
        logger.error(f'upsert_post error: {e}')
        raise


def get_posts(limit=100, offset=0, post_type=None, hide_contacted=False):
    try:
        with _PoolConn() as conn:
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
    except Exception as e:
        logger.error(f'get_posts error: {e}')
        raise


def get_stats():
    try:
        with _PoolConn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                # All counts in one query
                c.execute('''
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN contacted=1 THEN 1 ELSE 0 END) as contacted,
                        SUM(CASE WHEN post_type='sell' THEN 1 ELSE 0 END) as sell,
                        SUM(CASE WHEN post_type='buy'  THEN 1 ELSE 0 END) as buy,
                        SUM(CASE WHEN post_type='unclear' THEN 1 ELSE 0 END) as unclear,
                        AVG(CASE WHEN contacted=1 AND contacted_at IS NOT NULL
                            THEN contacted_at - created_utc END) as avg_response
                    FROM posts
                ''')
                row = c.fetchone()
                total     = row['total'] or 0
                contacted = row['contacted'] or 0
                sell      = row['sell'] or 0
                buy       = row['buy'] or 0
                unclear   = row['unclear'] or 0
                avg_response = int(row['avg_response']) if row['avg_response'] else None

                c.execute('''
                    SELECT subreddit, COUNT(*) as cnt
                    FROM posts GROUP BY subreddit ORDER BY cnt DESC LIMIT 8
                ''')
                top_subs = [dict(r) for r in c.fetchall()]

                c.execute('''
                    SELECT a.timestamp, a.action, p.title, p.author, p.subreddit
                    FROM activity_log a
                    LEFT JOIN posts p ON a.post_id = p.id
                    ORDER BY a.timestamp DESC LIMIT 10
                ''')
                activity = [dict(r) for r in c.fetchall()]

                today_start = int(datetime.utcnow().replace(
                    hour=0, minute=0, second=0, microsecond=0).timestamp())
                c.execute('SELECT COUNT(*) as cnt FROM posts WHERE fetched_at >= %s', (today_start,))
                today_new = c.fetchone()['cnt']

                c.execute('SELECT subreddit, COUNT(*) as cnt FROM posts GROUP BY subreddit')
                sub_counts = {r['subreddit']: r['cnt'] for r in c.fetchall()}

                return {
                    'total': total,
                    'contacted': contacted,
                    'sell': sell,
                    'buy': buy,
                    'unclear': unclear,
                    'avg_response_seconds': avg_response,
                    'top_subreddits': top_subs,
                    'activity': activity,
                    'today_new': today_new,
                    'sub_counts': sub_counts,
                }
    except Exception as e:
        logger.error(f'get_stats error: {e}')
        raise


def get_unnotified_posts(max_age_hours: int = 2):
    """Only return posts that are recent (default: last 2 hours) to avoid Telegram spam on restart."""
    cutoff = int(datetime.utcnow().timestamp()) - (max_age_hours * 3600)
    try:
        with _PoolConn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                c.execute(
                    'SELECT * FROM posts WHERE notified=0 AND fetched_at >= %s ORDER BY created_utc DESC',
                    (cutoff,)
                )
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f'get_unnotified_posts error: {e}')
        return []


def mark_notified(post_id: str):
    try:
        with _PoolConn() as conn:
            with conn:
                with conn.cursor() as c:
                    c.execute('UPDATE posts SET notified=1 WHERE id=%s', (post_id,))
    except Exception as e:
        logger.error(f'mark_notified error: {e}')


def mark_contacted(post_id: str):
    now = int(datetime.utcnow().timestamp())
    try:
        with _PoolConn() as conn:
            with conn:
                with conn.cursor() as c:
                    c.execute('UPDATE posts SET contacted=1, contacted_at=%s WHERE id=%s', (now, post_id))
                    c.execute('INSERT INTO activity_log (timestamp, action, post_id) VALUES (%s,%s,%s)',
                              (now, 'contacted', post_id))
    except Exception as e:
        logger.error(f'mark_contacted error: {e}')


def unmark_contacted(post_id: str):
    try:
        with _PoolConn() as conn:
            with conn:
                with conn.cursor() as c:
                    c.execute('UPDATE posts SET contacted=0, contacted_at=NULL WHERE id=%s', (post_id,))
    except Exception as e:
        logger.error(f'unmark_contacted error: {e}')


# ── Settings ──

def get_setting(key, default=None):
    try:
        with _PoolConn() as conn:
            with conn.cursor() as c:
                c.execute('SELECT value FROM settings WHERE key=%s', (key,))
                row = c.fetchone()
                return row[0] if row else default
    except Exception as e:
        logger.error(f'get_setting error: {e}')
        return default


def set_setting(key, value):
    try:
        with _PoolConn() as conn:
            with conn:
                with conn.cursor() as c:
                    c.execute(
                        'INSERT INTO settings (key,value) VALUES (%s,%s) '
                        'ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value',
                        (key, str(value))
                    )
    except Exception as e:
        logger.error(f'set_setting error: {e}')


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
    try:
        with _PoolConn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                c.execute('SELECT * FROM event_keywords ORDER BY created_at DESC')
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f'get_event_keywords error: {e}')
        return []


def add_event_keywords(names: list):
    now = int(datetime.utcnow().timestamp())
    try:
        with _PoolConn() as conn:
            with conn:
                with conn.cursor() as c:
                    for name in names:
                        name = name.strip()
                        if name:
                            c.execute('INSERT INTO event_keywords (name, created_at) VALUES (%s,%s)',
                                      (name, now))
    except Exception as e:
        logger.error(f'add_event_keywords error: {e}')


def delete_event_keyword(kid: int):
    try:
        with _PoolConn() as conn:
            with conn:
                with conn.cursor() as c:
                    c.execute('DELETE FROM event_keywords WHERE id=%s', (kid,))
    except Exception as e:
        logger.error(f'delete_event_keyword error: {e}')


def get_all_extra_keywords():
    events = get_event_keywords()
    keywords = []
    for e in events:
        name = e['name'].lower()
        words = [w.strip('|,.-()') for w in name.split() if len(w.strip('|,.-()')) > 3]
        keywords.extend(words)
        keywords.append(name)
    return list(set(keywords))

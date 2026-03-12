import json
import logging
from datetime import datetime
import psycopg2
import psycopg2.extras
from config import DATABASE_URL

logger = logging.getLogger(__name__)

def get_conn():
    """Get a direct database connection with proper SSL"""
    if not DATABASE_URL:
        raise RuntimeError('DATABASE_URL is not set')
    
    # Parse the DATABASE_URL to ensure it's using the pooler
    # Render requires the session pooler URL for IPv4 compatibility
    if 'pooler.supabase.com' not in DATABASE_URL:
        logger.warning('DATABASE_URL may not be using the session pooler. Should be: postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:5432/postgres')
    
    try:
        # Simple connection with SSL parameters that work with Supabase
        conn = psycopg2.connect(
            DATABASE_URL,
            connect_timeout=10,
            sslmode='require',
            sslcompression=0,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        return conn
    except Exception as e:
        logger.error(f"Connection error: {e}")
        raise

def init_db():
    """Verify DB connectivity on startup."""
    try:
        conn = get_conn()
        # Test the connection with a simple query
        with conn.cursor() as c:
            c.execute('SELECT 1')
        conn.close()
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
    conn = None
    try:
        conn = get_conn()
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
    except Exception as e:
        logger.error(f'upsert_post error: {e}')
        raise
    finally:
        if conn:
            conn.close()


def get_posts(limit=200, offset=0, post_type=None, hide_contacted=False):
    conn = None
    try:
        conn = get_conn()
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
            results = [dict(r) for r in c.fetchall()]
            logger.debug(f"get_posts found {len(results)} posts")
            return results
    except Exception as e:
        logger.error(f'get_posts error: {e}')
        raise
    finally:
        if conn:
            conn.close()


def get_stats():
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            # Get all stats in a single query for efficiency
            c.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN contacted=1 THEN 1 ELSE 0 END) as contacted,
                    SUM(CASE WHEN post_type='sell' THEN 1 ELSE 0 END) as sell,
                    SUM(CASE WHEN post_type='buy' THEN 1 ELSE 0 END) as buy,
                    SUM(CASE WHEN post_type='unclear' THEN 1 ELSE 0 END) as unclear,
                    AVG(CASE WHEN contacted=1 AND contacted_at IS NOT NULL 
                        THEN contacted_at - created_utc END) as avg_response
                FROM posts
            ''')
            row = c.fetchone()
            
            total = row['total'] or 0
            contacted = row['contacted'] or 0
            sell = row['sell'] or 0
            buy = row['buy'] or 0
            unclear = row['unclear'] or 0
            avg_response = int(row['avg_response']) if row['avg_response'] else None

            # Get top subreddits
            c.execute('''
                SELECT subreddit, COUNT(*) as cnt 
                FROM posts 
                GROUP BY subreddit 
                ORDER BY cnt DESC 
                LIMIT 8
            ''')
            top_subs = [dict(r) for r in c.fetchall()]

            # Get recent activity
            c.execute('''
                SELECT a.timestamp, a.action, p.title, p.author, p.subreddit
                FROM activity_log a 
                LEFT JOIN posts p ON a.post_id = p.id
                ORDER BY a.timestamp DESC 
                LIMIT 10
            ''')
            activity = [dict(r) for r in c.fetchall()]

            # Get today's new posts
            today_start = int(datetime.utcnow().replace(hour=0, minute=0, second=0).timestamp())
            c.execute('SELECT COUNT(*) as cnt FROM posts WHERE fetched_at >= %s', (today_start,))
            today_new = c.fetchone()['cnt']

            # Get all subreddit counts for sidebar
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
                'sub_counts': sub_counts
            }
    except Exception as e:
        logger.error(f'get_stats error: {e}')
        raise
    finally:
        if conn:
            conn.close()


def get_unnotified_posts():
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute('SELECT * FROM posts WHERE notified=0 ORDER BY created_utc DESC')
            return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f'get_unnotified_posts error: {e}')
        return []
    finally:
        if conn:
            conn.close()


def mark_notified(post_id: str):
    conn = None
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET notified=1 WHERE id=%s', (post_id,))
    except Exception as e:
        logger.error(f'mark_notified error: {e}')
    finally:
        if conn:
            conn.close()


def mark_contacted(post_id: str):
    conn = None
    now = int(datetime.utcnow().timestamp())
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET contacted=1, contacted_at=%s WHERE id=%s', (now, post_id))
                c.execute('INSERT INTO activity_log (timestamp, action, post_id) VALUES (%s,%s,%s)', (now, 'contacted', post_id))
    except Exception as e:
        logger.error(f'mark_contacted error: {e}')
    finally:
        if conn:
            conn.close()


def unmark_contacted(post_id: str):
    conn = None
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as c:
                c.execute('UPDATE posts SET contacted=0, contacted_at=NULL WHERE id=%s', (post_id,))
    except Exception as e:
        logger.error(f'unmark_contacted error: {e}')
    finally:
        if conn:
            conn.close()

# ── Settings ──

def get_setting(key, default=None):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as c:
            c.execute('SELECT value FROM settings WHERE key=%s', (key,))
            row = c.fetchone()
            return row[0] if row else default
    except Exception as e:
        logger.error(f'get_setting error: {e}')
        return default
    finally:
        if conn:
            conn.close()


def set_setting(key, value):
    conn = None
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as c:
                c.execute('INSERT INTO settings (key, value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value',
                          (key, str(value)))
    except Exception as e:
        logger.error(f'set_setting error: {e}')
    finally:
        if conn:
            conn.close()

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
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute('SELECT * FROM event_keywords ORDER BY created_at DESC')
            return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f'get_event_keywords error: {e}')
        return []
    finally:
        if conn:
            conn.close()


def add_event_keywords(names: list):
    conn = None
    now = int(datetime.utcnow().timestamp())
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as c:
                for name in names:
                    name = name.strip()
                    if name:
                        c.execute('INSERT INTO event_keywords (name, created_at) VALUES (%s,%s)', (name, now))
    except Exception as e:
        logger.error(f'add_event_keywords error: {e}')
    finally:
        if conn:
            conn.close()


def delete_event_keyword(kid: int):
    conn = None
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as c:
                c.execute('DELETE FROM event_keywords WHERE id=%s', (kid,))
    except Exception as e:
        logger.error(f'delete_event_keyword error: {e}')
    finally:
        if conn:
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
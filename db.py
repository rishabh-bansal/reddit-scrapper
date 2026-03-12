import sqlite3
import os
import json
from datetime import datetime

DB_PATH = os.environ.get('DB_PATH', 'reticket.db')

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            subreddit TEXT,
            title TEXT,
            body TEXT,
            author TEXT,
            permalink TEXT,
            post_type TEXT,
            ai_classified INTEGER DEFAULT 0,
            ups INTEGER DEFAULT 0,
            num_comments INTEGER DEFAULT 0,
            created_utc INTEGER,
            fetched_at INTEGER,
            contacted INTEGER DEFAULT 0,
            contacted_at INTEGER,
            notified INTEGER DEFAULT 0
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            action TEXT,
            subreddit TEXT,
            post_id TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS event_keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def upsert_post(post: dict):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
        INSERT OR IGNORE INTO posts
        (id, subreddit, title, body, author, permalink, post_type, ai_classified,
         ups, num_comments, created_utc, fetched_at, notified)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)
    ''', (
        post['id'], post['subreddit'], post['title'], post['body'],
        post['author'], post['permalink'], post['post_type'], post.get('ai_classified', 0),
        post['ups'], post['num_comments'], post['created_utc'],
        int(datetime.utcnow().timestamp())
    ))
    c.execute('''
        UPDATE posts SET ups=?, num_comments=?, post_type=?, ai_classified=?
        WHERE id=? AND ai_classified=0
    ''', (post['ups'], post['num_comments'], post['post_type'], post.get('ai_classified', 0), post['id']))
    conn.commit()
    conn.close()

def get_unnotified_posts():
    conn = get_conn()
    rows = conn.execute('SELECT * FROM posts WHERE notified=0 ORDER BY created_utc DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]

def mark_notified(post_id: str):
    conn = get_conn()
    conn.execute('UPDATE posts SET notified=1 WHERE id=?', (post_id,))
    conn.commit()
    conn.close()

def mark_contacted(post_id: str):
    conn = get_conn()
    now = int(datetime.utcnow().timestamp())
    conn.execute('UPDATE posts SET contacted=1, contacted_at=? WHERE id=?', (now, post_id))
    conn.execute('INSERT INTO activity_log (timestamp, action, post_id) VALUES (?,?,?)', (now, 'contacted', post_id))
    conn.commit()
    conn.close()

def unmark_contacted(post_id: str):
    conn = get_conn()
    conn.execute('UPDATE posts SET contacted=0, contacted_at=NULL WHERE id=?', (post_id,))
    conn.commit()
    conn.close()

def get_posts(limit=200, offset=0, post_type=None, hide_contacted=False):
    conn = get_conn()
    query = 'SELECT * FROM posts WHERE 1=1'
    params = []
    if post_type and post_type != 'all':
        query += ' AND post_type=?'
        params.append(post_type)
    if hide_contacted:
        query += ' AND contacted=0'
    query += ' ORDER BY created_utc DESC LIMIT ? OFFSET ?'
    params += [limit, offset]
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_stats():
    conn = get_conn()
    total = conn.execute('SELECT COUNT(*) FROM posts').fetchone()[0]
    contacted = conn.execute('SELECT COUNT(*) FROM posts WHERE contacted=1').fetchone()[0]
    sell = conn.execute("SELECT COUNT(*) FROM posts WHERE post_type='sell'").fetchone()[0]
    buy = conn.execute("SELECT COUNT(*) FROM posts WHERE post_type='buy'").fetchone()[0]
    unclear = conn.execute("SELECT COUNT(*) FROM posts WHERE post_type='unclear'").fetchone()[0]

    avg_row = conn.execute(
        'SELECT AVG(contacted_at - created_utc) FROM posts WHERE contacted=1 AND contacted_at IS NOT NULL'
    ).fetchone()[0]
    avg_response = int(avg_row) if avg_row else None

    top_subs = conn.execute(
        'SELECT subreddit, COUNT(*) as cnt FROM posts GROUP BY subreddit ORDER BY cnt DESC LIMIT 8'
    ).fetchall()

    activity = conn.execute(
        '''SELECT a.timestamp, a.action, p.title, p.author, p.subreddit
           FROM activity_log a LEFT JOIN posts p ON a.post_id=p.id
           ORDER BY a.timestamp DESC LIMIT 10'''
    ).fetchall()

    today_start = int(datetime.utcnow().replace(hour=0, minute=0, second=0).timestamp())
    today_new = conn.execute('SELECT COUNT(*) FROM posts WHERE fetched_at >= ?', (today_start,)).fetchone()[0]

    all_sub_counts = conn.execute(
        'SELECT subreddit, COUNT(*) as cnt FROM posts GROUP BY subreddit'
    ).fetchall()

    conn.close()
    return {
        'total': total,
        'contacted': contacted,
        'sell': sell,
        'buy': buy,
        'unclear': unclear,
        'avg_response_seconds': avg_response,
        'top_subreddits': [dict(r) for r in top_subs],
        'activity': [dict(r) for r in activity],
        'today_new': today_new,
        'sub_counts': {r['subreddit']: r['cnt'] for r in all_sub_counts}
    }

def get_setting(key, default=None):
    conn = get_conn()
    row = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
    conn.close()
    return row['value'] if row else default

def set_setting(key, value):
    conn = get_conn()
    conn.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)', (key, str(value)))
    conn.commit()
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
    """Returns list of {name, priority} dicts."""
    val = get_setting('subreddits_v2')
    if val:
        return json.loads(val)
    # Migrate old plain list if it exists
    old_val = get_setting('subreddits')
    if old_val:
        old_list = json.loads(old_val)
        migrated = [{'name': s, 'priority': 'medium'} for s in old_list]
        save_subreddits(migrated)
        return migrated
    return DEFAULT_SUBS

def save_subreddits(subs: list):
    """subs: list of {name, priority} dicts."""
    set_setting('subreddits_v2', json.dumps(subs))

def get_subreddit_names():
    """Flat list of names only, for scraper."""
    return [s['name'] for s in get_subreddits()]

def get_event_keywords():
    conn = get_conn()
    rows = conn.execute('SELECT * FROM event_keywords ORDER BY created_at DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]

def add_event_keywords(names: list):
    conn = get_conn()
    now = int(datetime.utcnow().timestamp())
    for name in names:
        name = name.strip()
        if name:
            conn.execute('INSERT INTO event_keywords (name, created_at) VALUES (?,?)', (name, now))
    conn.commit()
    conn.close()

def delete_event_keyword(kid: int):
    conn = get_conn()
    conn.execute('DELETE FROM event_keywords WHERE id=?', (kid,))
    conn.commit()
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

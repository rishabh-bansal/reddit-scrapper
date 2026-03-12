import requests
import os
import time
import logging
from db import upsert_post, get_subreddits

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

BUY_KEYWORDS = [
    'wtb', 'want to buy', 'looking to buy', 'need ticket', 'need tickets',
    'looking for ticket', 'looking for tickets', 'iso ticket', 'iso tickets',
    'anyone selling', 'can anyone sell', 'wants to buy', 'need passes',
    'looking for passes', 'need entry', 'need 1', 'need 2', 'need 3'
]
SELL_KEYWORDS = [
    'wts', 'want to sell', 'selling ticket', 'selling tickets', 'for sale',
    'have ticket', 'have tickets', 'extra ticket', 'extra tickets',
    'anyone buying', 'selling my ticket', 'selling my tickets',
    'selling passes', 'selling entry', 'available ticket', 'selling 1',
    'selling 2', 'selling 3', 'b24', 'b32', 'b48', 'pit ticket'
]
TICKET_KEYWORDS = [
    'ticket', 'tickets', 'concert', 'show', 'fest', 'festival', 'event',
    'gig', 'entry', 'pass', 'passes', 'wtb', 'wts', 'selling', 'fanpit',
    'pit', 'standing', 'seated', 'vip', 'floor', 'balcony', 'stall',
    'honey singh', 'diljit', 'ap dhillon', 'arijit', 'badshah', 'divine',
    'nucleya', 'sunburn', 'lollapalooza', 'nh7', 'vh1', 'weekender',
    'kanye', 'coldplay', 'ed sheeran', 'b24', 'fanpit'
]

TICKET_FOCUSED_SUBS = {
    'concertticketsindia', 'ticketresellingindia', 'concertresale',
    'ticketresale', 'concerts_india', 'concertsindia_', 'transactiontickets'
}

HEADERS = {'User-Agent': 'ReticketLeads/1.0 (personal ticket lead tool)'}


def is_ticket_post(post: dict, subreddit: str) -> bool:
    if subreddit.lower() in TICKET_FOCUSED_SUBS:
        return True
    text = (post.get('title', '') + ' ' + post.get('selftext', '')).lower()
    return any(k in text for k in TICKET_KEYWORDS)


def classify_keyword(post: dict) -> str:
    text = (post.get('title', '') + ' ' + post.get('selftext', '')).lower()
    is_buy = any(k in text for k in BUY_KEYWORDS)
    is_sell = any(k in text for k in SELL_KEYWORDS)
    if is_buy and not is_sell:
        return 'buy'
    if is_sell and not is_buy:
        return 'sell'
    return 'unclear'


def classify_with_ai(posts: list) -> dict:
    """Call Claude API to classify a batch of unclear posts. Returns {id: type}"""
    if not ANTHROPIC_API_KEY or not posts:
        return {}
    batch = posts[:20]
    prompt = (
        'Classify each Reddit post as "buy" (person wants to buy tickets), '
        '"sell" (person wants to sell tickets), or "skip" (not about buying/selling tickets).\n\n'
        'Posts:\n' +
        str([{'id': p['id'], 'text': (p['title'] + ' ' + p.get('selftext', ''))[:200]} for p in batch]) +
        '\n\nReply ONLY with a JSON object like: {"id1": "buy", "id2": "sell", ...}'
    )
    try:
        res = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json'
            },
            json={
                'model': 'claude-haiku-4-5-20251001',
                'max_tokens': 500,
                'messages': [{'role': 'user', 'content': prompt}]
            },
            timeout=15
        )
        import json, re
        text = res.json()['content'][0]['text']
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception as e:
        logger.warning(f'AI classification failed: {e}')
    return {}


def fetch_subreddit(subreddit: str) -> list:
    """Fetch latest posts from a subreddit. Returns list of new post dicts."""
    url = f'https://www.reddit.com/r/{subreddit}/new.json?limit=100'
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code == 404:
            logger.warning(f'r/{subreddit} not found (404)')
            return []
        if res.status_code == 403:
            logger.warning(f'r/{subreddit} private/banned (403)')
            return []
        res.raise_for_status()
        data = res.json()
        posts = [c['data'] for c in data.get('data', {}).get('children', [])]
        ticket_posts = [p for p in posts if is_ticket_post(p, subreddit)]

        # Classify
        unclear = []
        result = []
        for p in ticket_posts:
            ptype = classify_keyword(p)
            post_dict = {
                'id': p['id'],
                'subreddit': subreddit,
                'title': p.get('title', ''),
                'body': p.get('selftext', '')[:500],
                'author': p.get('author', '[deleted]'),
                'permalink': p.get('permalink', ''),
                'post_type': ptype,
                'ai_classified': 0,
                'ups': p.get('ups', 0),
                'num_comments': p.get('num_comments', 0),
                'created_utc': int(p.get('created_utc', 0))
            }
            if ptype == 'unclear':
                unclear.append(post_dict)
            result.append(post_dict)

        # AI classify unclear ones
        if unclear and ANTHROPIC_API_KEY:
            ai_results = classify_with_ai(unclear)
            for post_dict in result:
                if post_dict['id'] in ai_results and ai_results[post_dict['id']] != 'skip':
                    post_dict['post_type'] = ai_results[post_dict['id']]
                    post_dict['ai_classified'] = 1

        return result

    except requests.RequestException as e:
        logger.warning(f'Failed to fetch r/{subreddit}: {e}')
        return []


def scrape_all() -> list:
    """Scrape all subreddits. Returns list of newly inserted post dicts."""
    subreddits = get_subreddits()
    new_posts = []

    for sub in subreddits:
        posts = fetch_subreddit(sub)
        for post in posts:
            upsert_post(post)
            new_posts.append(post)
        time.sleep(0.5)  # be polite to Reddit

    logger.info(f'Scraped {len(new_posts)} ticket posts from {len(subreddits)} subreddits')
    return new_posts

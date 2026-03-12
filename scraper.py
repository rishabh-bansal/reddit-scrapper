import requests
import os
import time
import random
import logging
from db import upsert_post, get_subreddits, get_all_extra_keywords

# Re-export so app.py can import it from here
__all__ = ['fetch_subreddit', 'scrape_all', 'upsert_post']

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

BUY_KEYWORDS = [
    'wtb', 'want to buy', 'looking to buy', 'need ticket', 'need tickets',
    'looking for ticket', 'looking for tickets', 'iso ticket', 'iso tickets',
    'anyone selling', 'can anyone sell', 'wants to buy', 'need passes',
    'looking for passes', 'need entry', 'buying ticket', 'buying tickets',
]
SELL_KEYWORDS = [
    'wts', 'want to sell', 'selling ticket', 'selling tickets', 'for sale',
    'have ticket', 'have tickets', 'extra ticket', 'extra tickets',
    'selling passes', 'selling entry', 'available ticket',
    'selling 1', 'selling 2', 'selling 3', 'b24', 'b32', 'b48',
    'pit ticket', 'pit pass', 'selling my ticket', 'selling my tickets',
]

# STRICT ticket keywords — must be present for general subs
STRICT_TICKET_KEYWORDS = [
    'ticket', 'tickets', 'pass', 'passes', 'entry pass',
    'wtb', 'wts', 'fanpit', 'fan pit',
]

# Extra context keywords — only count if combined with strict ones
CONTEXT_KEYWORDS = [
    'concert', 'show', 'fest', 'festival', 'gig', 'tour', 'live',
    'nh7', 'lollapalooza', 'sunburn', 'vh1', 'weekender',
    'honey singh', 'diljit', 'ap dhillon', 'arijit', 'badshah', 'divine',
    'nucleya', 'kanye', 'coldplay', 'ed sheeran', 'calvin harris',
    'black coffee', 'keinemusik', 'ar rahman', 'sonu nigam',
]

# These subs are 100% ticket-focused — show ALL posts
TICKET_FOCUSED_SUBS = {
    'concertticketsindia', 'ticketresellingindia', 'concertresale',
    'ticketresale', 'concerts_india', 'concertsindia_', 'transactiontickets',
    'ticketexchange', 'edmtickets',
}

# These subs need strict filtering — don't show everything
GENERAL_SUBS = {
    'delhi', 'delhi_marketplace', 'chandigarhmarketplace', 'mumbai',
    'bangalore', 'pune', 'hyderabad', 'india', 'indiamarketplace',
    'creditcardsindia', 'indianhiphopheads', 'concerts', 'tickets',
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
]

def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
    }


def is_ticket_post(post: dict, subreddit: str, extra_keywords: list) -> bool:
    sub_lower = subreddit.lower()
    text = (post.get('title', '') + ' ' + post.get('selftext', '')).lower()

    # Ticket-focused subs: show all posts
    if sub_lower in TICKET_FOCUSED_SUBS:
        return True

    # General subs: require a strict ticket keyword
    has_strict = any(k in text for k in STRICT_TICKET_KEYWORDS)
    if not has_strict:
        return False

    # Also require at least one context keyword OR a buy/sell keyword
    has_context = any(k in text for k in CONTEXT_KEYWORDS + extra_keywords)
    has_buy_sell = any(k in text for k in BUY_KEYWORDS + SELL_KEYWORDS)

    return has_context or has_buy_sell


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
    import json, re
    if not ANTHROPIC_API_KEY or not posts:
        return {}
    batch = posts[:20]
    prompt = (
        'You are filtering Reddit posts for a concert ticket marketplace in India.\n'
        'Classify each post as:\n'
        '- "buy": person wants to BUY concert/event tickets\n'
        '- "sell": person wants to SELL concert/event tickets\n'
        '- "skip": NOT about buying or selling tickets (general discussion, unrelated items, questions about concerts in general)\n\n'
        'Be strict: "Why are Indian concerts chaotic?" = skip. "Selling phone cooler" = skip. '
        '"WTS Calvin Harris ticket" = sell. "Need 2 Diljit tickets" = buy.\n\n'
        'Posts:\n' +
        json.dumps([{'id': p['id'], 'text': (p['title'] + ' ' + p.get('body', ''))[:200]} for p in batch]) +
        '\n\nReply ONLY with a valid JSON object like: {"abc123": "buy", "xyz456": "skip"}'
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
            timeout=20
        )
        rdata = res.json()
        # Handle API errors gracefully
        if res.status_code != 200:
            logger.warning(f'AI API error {res.status_code}: {rdata.get("error", {}).get("message", rdata)}')
            return {}
        if 'content' not in rdata or not rdata['content']:
            logger.warning(f'AI response missing content: {rdata}')
            return {}
        text = rdata['content'][0]['text']
        match = re.search(r'\{.*?\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        logger.warning(f'AI response had no JSON: {text[:200]}')
    except Exception as e:
        logger.warning(f'AI classification failed: {e}')
    return {}


def fetch_subreddit(subreddit: str, extra_keywords: list) -> list:
    url = f'https://www.reddit.com/r/{subreddit}/new.json?limit=100'
    # Random jitter so requests never look like a robot clock
    time.sleep(random.uniform(2, 5))
    try:
        res = requests.get(url, headers=get_headers(), timeout=15)
        if res.status_code == 404:
            logger.warning(f'r/{subreddit} not found (404)')
            return []
        if res.status_code == 403:
            logger.warning(f'r/{subreddit} private/banned (403)')
            return []
        if res.status_code == 429:
            retry_after = int(res.headers.get('Retry-After', 120))
            logger.warning(f'r/{subreddit} rate limited — sleeping {retry_after}s')
            time.sleep(retry_after)
            return []
        res.raise_for_status()
        data = res.json()
        posts = [c['data'] for c in data.get('data', {}).get('children', [])]
        ticket_posts = [p for p in posts if is_ticket_post(p, subreddit, extra_keywords)]
        logger.info(f'r/{subreddit}: {len(posts)} raw posts, {len(ticket_posts)} matched ticket filter')

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

        # AI classify unclear ones — mark "skip" posts for removal
        if unclear and ANTHROPIC_API_KEY:
            ai_results = classify_with_ai(unclear)
            keep = []
            for post_dict in result:
                ai_label = ai_results.get(post_dict['id'])
                if ai_label == 'skip':
                    continue  # drop irrelevant posts
                if ai_label in ('buy', 'sell'):
                    post_dict['post_type'] = ai_label
                    post_dict['ai_classified'] = 1
                keep.append(post_dict)
            result = keep

        return result

    except requests.RequestException as e:
        logger.warning(f'Failed to fetch r/{subreddit}: {e}')
        return []


def scrape_all() -> list:
    subreddits = get_subreddits()
    extra_keywords = get_all_extra_keywords()
    new_posts = []

    for sub in subreddits:
        posts = fetch_subreddit(sub, extra_keywords)
        for post in posts:
            upsert_post(post)
            new_posts.append(post)
        time.sleep(1)

    logger.info(f'Scraped {len(new_posts)} ticket posts from {len(subreddits)} subreddits')
    return new_posts

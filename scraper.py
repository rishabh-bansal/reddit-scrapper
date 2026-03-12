import time
import random
import logging
import requests
import classifier

logger = logging.getLogger(__name__)

# ── Keyword lists ──

BROAD_KEYWORDS = [
    # ticket words
    'ticket', 'tickets', 'pass', 'passes', 'entry', 'fanpit', 'fan pit',
    # buy signals
    'wtb', 'want to buy', 'looking to buy', 'looking for', 'need ticket',
    'need tickets', 'iso ticket', 'iso tickets', 'anyone selling', 'need passes',
    'buying ticket', 'buying tickets', 'anyone have', 'anyone got',
    # sell signals
    'wts', 'want to sell', 'selling ticket', 'selling tickets', 'for sale',
    'have ticket', 'have tickets', 'extra ticket', 'extra tickets',
    'selling passes', 'available ticket', 'selling my ticket', 'pit ticket',
    'selling 1', 'selling 2', 'selling 3',
    # concert/event words
    'concert', 'festival', 'fest', 'gig', 'show', 'tour',
    'nh7', 'lollapalooza', 'sunburn', 'weekender',
    'diljit', 'coldplay', 'calvin harris', 'ap dhillon', 'arijit',
    'badshah', 'divine', 'ar rahman', 'honey singh', 'nucleya',
]

TICKET_FOCUSED_SUBS = {
    'concertticketsindia', 'ticketresellingindia', 'concertresale',
    'ticketresale', 'concerts_india', 'concertsindia_', 'transactiontickets',
    'ticketexchange', 'edmtickets',
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
]


def _headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
    }


def _broad_filter(post: dict, subreddit: str, extra_keywords: list) -> bool:
    """Stage 1: loose net — decides what gets sent to AI."""
    sub_lower = subreddit.lower()
    text = (post.get('title', '') + ' ' + post.get('selftext', '')).lower()
    all_kw = BROAD_KEYWORDS + [k.lower() for k in extra_keywords]

    if sub_lower in TICKET_FOCUSED_SUBS:
        return any(k in text for k in all_kw)
    else:
        # General subs — require a ticket/buy/sell word to avoid noise
        ticket_words = ['ticket', 'tickets', 'pass', 'passes', 'entry pass',
                        'wtb', 'wts', 'fanpit', 'fan pit', 'for sale',
                        'want to sell', 'want to buy', 'extra ticket', 'selling ticket']
        has_ticket = any(k in text for k in ticket_words)
        has_event = any(k in text for k in all_kw)
        return has_ticket and has_event


def fetch_subreddit(subreddit: str, extra_keywords: list, fast_mode: bool = False) -> list:
    """
    3-stage pipeline:
      Stage 1 — broad keyword filter (local, free)
      Stage 2 — Gemini AI classification (all candidates, batched)
      Stage 3 — keep only buy/sell, discard skip

    fast_mode=True: paginate up to 5 pages (first-boot)
    fast_mode=False: single page (normal scrape)
    """
    all_results = []
    after = None
    pages = 5 if fast_mode else 1

    for page in range(pages):
        url = f'https://www.reddit.com/r/{subreddit}/new.json?limit=100'
        if after:
            url += f'&after={after}'

        time.sleep(random.uniform(0.3, 0.8) if fast_mode else random.uniform(0.5, 1.5))

        try:
            res = requests.get(url, headers=_headers(), timeout=15)

            if res.status_code == 404:
                logger.warning(f'r/{subreddit} not found (404)')
                break
            if res.status_code == 403:
                logger.warning(f'r/{subreddit} private/banned (403)')
                break
            if res.status_code == 429:
                wait = int(res.headers.get('Retry-After', 60))
                logger.warning(f'r/{subreddit} rate limited — waiting {wait}s')
                time.sleep(wait)
                break
            res.raise_for_status()

            data = res.json()
            children = data.get('data', {}).get('children', [])
            if not children:
                break

            raw_posts = [c['data'] for c in children]

            # Stage 1
            candidates = [p for p in raw_posts if _broad_filter(p, subreddit, extra_keywords)]
            logger.info(f'r/{subreddit} p{page+1}: {len(raw_posts)} raw → {len(candidates)} candidates')

            if not candidates:
                after = data.get('data', {}).get('after')
                if not after:
                    break
                continue

            # Build post dicts
            post_dicts = [{
                'id': p['id'],
                'subreddit': subreddit,
                'title': p.get('title', ''),
                'body': p.get('selftext', '')[:500],
                'author': p.get('author', '[deleted]'),
                'permalink': p.get('permalink', ''),
                'post_type': 'unclear',
                'ai_classified': 0,
                'ups': p.get('ups', 0),
                'num_comments': p.get('num_comments', 0),
                'created_utc': int(p.get('created_utc', 0)),
            } for p in candidates]

            # Stage 2 — AI
            ai_results = classifier.classify_batch(post_dicts, extra_keywords)

            # Stage 3 — filter
            kept, skipped = [], 0
            for pd in post_dicts:
                label = ai_results.get(pd['id'], 'unclear')
                if label == 'skip':
                    skipped += 1
                    continue
                pd['post_type'] = label if label in ('buy', 'sell') else 'unclear'
                pd['ai_classified'] = 1 if label in ('buy', 'sell') else 0
                kept.append(pd)

            logger.info(f'r/{subreddit} p{page+1}: kept {len(kept)}, skipped {skipped}')
            all_results.extend(kept)

            after = data.get('data', {}).get('after')
            if not after:
                break

        except requests.RequestException as e:
            logger.warning(f'Failed to fetch r/{subreddit}: {e}')
            break

    return all_results

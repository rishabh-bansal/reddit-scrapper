import re
import time
import random
import logging
import requests
import classifier
from config import REDDIT_USER_AGENTS, MAX_POSTS_PER_SUBREDDIT, REQUEST_TIMEOUT, DELAY_BETWEEN_REQUESTS

logger = logging.getLogger(__name__)

# ── Broad keyword filter (Stage 1) ──
BROAD_KEYWORDS = [
    # ticket words
    'ticket', 'tickets', 'pass', 'passes', 'entry', 'fanpit', 'fan pit', 'pit ticket',
    # buy signals
    'wtb', 'want to buy', 'looking to buy', 'looking for', 'need ticket', 'need tickets',
    'iso ticket', 'iso tickets', 'anyone selling', 'need passes', 'buying ticket',
    'buying tickets', 'anyone have', 'anyone got',
    # sell signals
    'wts', 'want to sell', 'selling ticket', 'selling tickets', 'for sale',
    'have ticket', 'have tickets', 'extra ticket', 'extra tickets',
    'selling passes', 'available ticket', 'selling my ticket', 'pit ticket',
    'selling 1', 'selling 2', 'selling 3', 'selling two', 'selling three',
    # concert/event words
    'concert', 'festival', 'fest', 'gig', 'show', 'tour', 'live',
    'nh7', 'lollapalooza', 'sunburn', 'weekender', 'edm', 'music festival',
    # Indian artists/events
    'diljit', 'coldplay', 'calvin harris', 'ap dhillon', 'arijit', 'badshah',
    'divine', 'ar rahman', 'honey singh', 'nucleya', 'martin garrix', 'alan walker',
    'marshmello', 'chainsmokers', 'karan aujla', 'sidhu moose wala', 'diljit dosanjh',
]

TICKET_FOCUSED_SUBS = {
    'concertticketsindia', 'ticketresellingindia', 'concertresale',
    'ticketresale', 'concerts_india', 'concertsindia_', 'transactiontickets',
    'ticketexchange', 'edmtickets', 'tickets', 'concerttickets',
}

# FIX: These must be REGEX patterns used with re.search(), not plain string matches
# DeepSeek's version had them as plain strings with r'' prefix but used `in` operator —
# now correctly compiled and used with re.search()
_SKIP_PATTERNS_RAW = [
    r'planning an? event', r'organizing an? event', r'hosting an? event',
    r'looking for venue', r'suggestions for.*event', r'need recommendations for',
    r'help me plan', r'how to organize', r'event management', r'event planning',
    r'budget place', r'good place for', r'vehicle suggestions',
    # Non-ticket items — use word boundaries to avoid false positives
    r'selling.*\bmonitor\b', r'selling.*\blaptop\b', r'selling.*\bphone\b',
    r'selling.*\bcamera\b', r'selling.*\bfurniture\b',
    r'wts.*\bmonitor\b', r'wts.*\blaptop\b', r'wts.*\bphone\b',
    r'for sale.*\bmonitor\b', r'for sale.*\blaptop\b',
    r'\b4k monitor\b', r'\bgaming monitor\b', r'\bgaming laptop\b',
    # General discussion
    r'what do you think', r'your thoughts', r'opinion on',
    r'has anyone been', r'anyone attended', r'review of', r'experience with',
    r'how was the', r'did you go', r'who is going', r"who's going",
    # Poetry/creative
    r'\bpoem\b', r'\bpoetry\b', r'creative writing', r'original work',
    # Price questions (not transactions)
    r'how to get tickets', r'where to buy tickets', r'booking open',
    r'when will tickets release',
]
_SKIP_RE = [re.compile(p, re.IGNORECASE) for p in _SKIP_PATTERNS_RAW]


def _headers():
    return {
        'User-Agent': random.choice(REDDIT_USER_AGENTS),
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    }


def _should_skip(text: str) -> bool:
    """Stage 1 pre-filter: skip obvious non-ticket posts using compiled regex."""
    for pat in _SKIP_RE:
        if pat.search(text):
            return True
    return False


def _broad_filter(post: dict, subreddit: str, extra_keywords: list) -> bool:
    """
    Stage 1: Decide if post is worth sending to AI.
    Ticket-focused subs: lenient (any ticket/buy/sell word).
    General subs: strict (must have ticket word AND buy/sell/event context).
    """
    sub_lower = subreddit.lower()
    text = (post.get('title', '') + ' ' + post.get('selftext', '')).lower()

    # Hard skip — obvious non-ticket content
    if _should_skip(text):
        return False

    all_kw = BROAD_KEYWORDS + [k.lower() for k in extra_keywords]

    has_ticket_word = any(w in text for w in
        ['ticket', 'tickets', 'pass', 'passes', 'entry', 'fanpit', 'pit'])
    has_buy_sell = any(w in text for w in
        ['wtb', 'wts', 'buy', 'sell', 'selling', 'buying', 'for sale', 'looking for'])
    has_event = any(k in text for k in all_kw)

    if sub_lower in TICKET_FOCUSED_SUBS:
        return has_ticket_word or has_buy_sell or has_event
    else:
        return has_ticket_word and (has_buy_sell or has_event)


def _extract_post_data(post: dict, subreddit: str) -> dict:
    return {
        'id': post['id'],
        'subreddit': subreddit,
        'title': post.get('title', ''),
        'body': post.get('selftext', '')[:500],
        'author': post.get('author', '[deleted]'),
        'permalink': post.get('permalink', ''),
        'post_type': 'unclear',
        'ai_classified': 0,
        'ups': post.get('ups', 0),
        'num_comments': post.get('num_comments', 0),
        'created_utc': int(post.get('created_utc', 0)),
    }


def fetch_subreddit(subreddit: str, extra_keywords: list, fast_mode: bool = False) -> list:
    """
    3-stage pipeline:
      Stage 1 — broad keyword filter (local, fast)
      Stage 2 — Groq AI classification (batched 10/call)
      Stage 3 — keep buy/sell, discard skip

    fast_mode=True: up to 3 pages (first boot)
    fast_mode=False: 1 page (normal scrape)
    """
    all_results = []
    after = None
    pages = 3 if fast_mode else 1

    for page in range(pages):
        url = f'https://www.reddit.com/r/{subreddit}/new.json?limit={MAX_POSTS_PER_SUBREDDIT}'
        if after:
            url += f'&after={after}'

        delay = random.uniform(0.5, 1.5) if fast_mode else random.uniform(1, 2)
        time.sleep(delay)

        try:
            res = requests.get(url, headers=_headers(), timeout=REQUEST_TIMEOUT)

            if res.status_code == 429:
                retry_after = int(res.headers.get('Retry-After', 60))
                logger.warning(f'r/{subreddit} rate limited — waiting {retry_after}s')
                time.sleep(retry_after)
                continue
            if res.status_code == 404:
                logger.warning(f'r/{subreddit} not found (404)')
                break
            if res.status_code == 403:
                logger.warning(f'r/{subreddit} private/banned (403)')
                break
            if res.status_code != 200:
                logger.warning(f'r/{subreddit} returned {res.status_code}')
                break

            data = res.json()
            children = data.get('data', {}).get('children', [])
            if not children:
                break

            raw_posts = [c['data'] for c in children]

            # Stage 1
            candidates = []
            for post in raw_posts:
                try:
                    if _broad_filter(post, subreddit, extra_keywords):
                        candidates.append(post)
                except Exception as e:
                    logger.debug(f'Broad filter error: {e}')

            logger.info(f'r/{subreddit} p{page+1}: {len(raw_posts)} raw → {len(candidates)} candidates')

            if not candidates:
                after = data.get('data', {}).get('after')
                if not after:
                    break
                continue

            post_dicts = [_extract_post_data(p, subreddit) for p in candidates]

            # Stage 2
            ai_results = classifier.classify_batch(post_dicts, extra_keywords)

            # Stage 3
            kept, skipped = 0, 0
            for pd in post_dicts:
                label = ai_results.get(pd['id'], 'unclear')
                if label == 'skip':
                    skipped += 1
                    continue
                pd['post_type'] = label if label in ('buy', 'sell') else 'unclear'
                pd['ai_classified'] = 1 if label in ('buy', 'sell') else 0
                kept += 1
                all_results.append(pd)

            logger.info(f'r/{subreddit} p{page+1}: kept {kept}, skipped {skipped}')

            after = data.get('data', {}).get('after')
            if not after:
                break

        except requests.exceptions.Timeout:
            logger.warning(f'Timeout fetching r/{subreddit}')
            break
        except requests.exceptions.ConnectionError:
            logger.warning(f'Connection error fetching r/{subreddit}')
            break
        except Exception as e:
            logger.warning(f'Unexpected error for r/{subreddit}: {e}')
            break

    time.sleep(DELAY_BETWEEN_REQUESTS)
    logger.info(f'r/{subreddit} complete: found {len(all_results)} relevant posts')
    return all_results

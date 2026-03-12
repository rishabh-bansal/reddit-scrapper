import requests
import os
import time
import random
import logging
import json
import re
from db import upsert_post, get_subreddits, get_subreddit_names, get_all_extra_keywords

__all__ = ['fetch_subreddit', 'scrape_all', 'upsert_post']

logger = logging.getLogger(__name__)

# ── Keyword lists ──
# Stage 1: broad net — if ANY of these appear, send to AI for final verdict
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
    # concert/event words (enough signal on their own in ticket subs)
    'concert', 'festival', 'fest', 'gig', 'show', 'tour',
    'nh7', 'lollapalooza', 'sunburn', 'weekender',
    'diljit', 'coldplay', 'calvin harris', 'ap dhillon', 'arijit',
    'badshah', 'divine', 'ar rahman', 'honey singh', 'nucleya',
]

# Ticket-focused subs — pass everything with any broad keyword to AI
TICKET_FOCUSED_SUBS = {
    'concertticketsindia', 'ticketresellingindia', 'concertresale',
    'ticketresale', 'concerts_india', 'concertsindia_', 'transactiontickets',
    'ticketexchange', 'edmtickets',
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


def broad_filter(post: dict, subreddit: str, extra_keywords: list) -> bool:
    """
    Stage 1: loose filter — casts a wide net to send candidates to AI.
    Ticket-focused subs: pass if any broad keyword OR event keyword matches.
    General subs: must have ticket/buy/sell word to avoid spammy city subs.
    """
    sub_lower = subreddit.lower()
    text = (post.get('title', '') + ' ' + post.get('selftext', '')).lower()

    all_keywords = BROAD_KEYWORDS + [k.lower() for k in extra_keywords]

    if sub_lower in TICKET_FOCUSED_SUBS:
        return any(k in text for k in all_keywords)
    else:
        # General subs: require a ticket/buy/sell word — avoids passing general city posts
        ticket_words = ['ticket', 'tickets', 'pass', 'passes', 'entry pass',
                        'wtb', 'wts', 'fanpit', 'fan pit', 'for sale', 'want to sell',
                        'want to buy', 'extra ticket', 'selling ticket']
        has_ticket = any(k in text for k in ticket_words)
        has_event = any(k in text for k in all_keywords)
        return has_ticket and has_event


def classify_batch_with_gemini(posts: list, event_keywords: list) -> dict:
    """
    Stage 2: send ALL Stage-1 candidates to Gemini for final buy/sell/skip classification.
    Batches 20 posts per API call. Returns {post_id: 'buy'|'sell'|'skip'}.
    Includes event context so Gemini knows what we care about.
    """
    gemini_key = os.environ.get('GEMINI_API_KEY', '')
    if not gemini_key or not posts:
        # No AI key — fall back to keyword-only classification, keep all
        return {p['id']: _keyword_classify(p) for p in posts}

    results = {}
    batch_size = 20
    events_hint = ', '.join(event_keywords[:15]) if event_keywords else 'any concert/event tickets'

    for i in range(0, len(posts), batch_size):
        batch = posts[i:i + batch_size]
        prompt = (
            f'You are filtering Reddit posts for a concert ticket resale marketplace in India.\n'
            f'Current events we care about: {events_hint}\n\n'
            f'Classify each post as:\n'
            f'- "buy": person wants to BUY tickets for a concert/event\n'
            f'- "sell": person wants to SELL tickets for a concert/event\n'
            f'- "skip": NOT about buying or selling tickets (poems, rants, general questions, '
            f'unrelated items, general concert discussion without ticket transaction)\n\n'
            f'Examples:\n'
            f'"Poem by me: Scores to Settle" = skip\n'
            f'"Feeling guilty about missing the show" = skip\n'
            f'"WTS 2 Diljit pit tickets Delhi" = sell\n'
            f'"Anyone got extra Coldplay passes?" = buy\n'
            f'"Looking for 1 ticket to NH7" = buy\n'
            f'"Have 3 Calvin Harris tickets, selling below MRP" = sell\n\n'
            f'Posts:\n' +
            json.dumps([{'id': p['id'], 'text': (p['title'] + ' ' + p.get('body', ''))[:200]}
                        for p in batch]) +
            f'\n\nReply ONLY with a valid JSON object: {{"id1": "buy", "id2": "skip", ...}}'
        )
        try:
            res = requests.post(
                f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={gemini_key}',
                json={'contents': [{'parts': [{'text': prompt}]}],
                      'generationConfig': {'temperature': 0.1}},
                timeout=25
            )
            if res.status_code != 200:
                logger.warning(f'Gemini error {res.status_code}: {res.text[:150]}')
                # Fallback: keyword classify this batch
                for p in batch:
                    results[p['id']] = _keyword_classify(p)
                continue
            rdata = res.json()
            text = rdata['candidates'][0]['content']['parts'][0]['text']
            match = re.search(r'\{.*?\}', text, re.DOTALL)
            if match:
                batch_results = json.loads(match.group())
                results.update(batch_results)
            else:
                logger.warning(f'Gemini no JSON in response: {text[:150]}')
                for p in batch:
                    results[p['id']] = _keyword_classify(p)
        except Exception as e:
            logger.warning(f'Gemini batch {i//batch_size + 1} failed: {e}')
            for p in batch:
                results[p['id']] = _keyword_classify(p)
        # Small pause between batches — Gemini free tier is 15 req/min
        if i + batch_size < len(posts):
            time.sleep(1)

    return results


def _keyword_classify(post: dict) -> str:
    """Fallback keyword classifier when AI is unavailable."""
    buy_kw = ['wtb', 'want to buy', 'looking to buy', 'need ticket', 'need tickets',
              'looking for ticket', 'looking for tickets', 'iso ticket', 'anyone selling',
              'need passes', 'looking for passes', 'buying ticket', 'anyone got', 'anyone have']
    sell_kw = ['wts', 'want to sell', 'selling ticket', 'selling tickets', 'for sale',
               'have ticket', 'have tickets', 'extra ticket', 'extra tickets',
               'selling passes', 'available ticket', 'pit ticket', 'selling my ticket']
    text = (post.get('title', '') + ' ' + post.get('body', '')).lower()
    is_buy = any(k in text for k in buy_kw)
    is_sell = any(k in text for k in sell_kw)
    if is_buy and not is_sell:
        return 'buy'
    if is_sell and not is_buy:
        return 'sell'
    return 'unclear'


def fetch_subreddit(subreddit: str, extra_keywords: list, fast_mode: bool = False) -> list:
    """
    Fetch posts from a subreddit using the 3-stage pipeline:
      Stage 1 — broad_filter(): wide keyword net (local, free)
      Stage 2 — classify_batch_with_gemini(): AI final verdict on all candidates
      Stage 3 — store only buy/sell, discard skip

    fast_mode=True: paginate up to 5 pages (first-boot bulk load)
    fast_mode=False: single page of /new (normal scrape)
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
            res = requests.get(url, headers=get_headers(), timeout=15)
            if res.status_code == 404:
                logger.warning(f'r/{subreddit} not found (404)')
                break
            if res.status_code == 403:
                logger.warning(f'r/{subreddit} private/banned (403)')
                break
            if res.status_code == 429:
                retry_after = int(res.headers.get('Retry-After', 60))
                logger.warning(f'r/{subreddit} rate limited — sleeping {retry_after}s')
                time.sleep(retry_after)
                break
            res.raise_for_status()

            data = res.json()
            children = data.get('data', {}).get('children', [])
            if not children:
                break

            raw_posts = [c['data'] for c in children]

            # Stage 1: broad filter
            candidates = [p for p in raw_posts if broad_filter(p, subreddit, extra_keywords)]
            logger.info(f'r/{subreddit} p{page+1}: {len(raw_posts)} raw → {len(candidates)} Stage-1 candidates')

            if not candidates:
                after = data.get('data', {}).get('after')
                if not after:
                    break
                continue

            # Build post dicts for AI
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

            # Stage 2: AI classification (all candidates, batched)
            ai_results = classify_batch_with_gemini(post_dicts, extra_keywords)

            # Stage 3: keep only buy/sell
            kept = []
            skipped = 0
            for pd in post_dicts:
                label = ai_results.get(pd['id'], 'unclear')
                if label == 'skip':
                    skipped += 1
                    continue
                pd['post_type'] = label if label in ('buy', 'sell') else 'unclear'
                pd['ai_classified'] = 1 if label in ('buy', 'sell') else 0
                kept.append(pd)

            logger.info(f'r/{subreddit} p{page+1}: {len(kept)} kept, {skipped} skipped by AI')
            all_results.extend(kept)

            after = data.get('data', {}).get('after')
            if not after:
                break

        except requests.RequestException as e:
            logger.warning(f'Failed to fetch r/{subreddit}: {e}')
            break

    return all_results


def scrape_all() -> list:
    """Legacy — used for manual full scrape."""
    subreddits = get_subreddit_names()
    extra_keywords = get_all_extra_keywords()
    new_posts = []
    for sub in subreddits:
        posts = fetch_subreddit(sub, extra_keywords)
        for post in posts:
            upsert_post(post)
            new_posts.append(post)
        time.sleep(1)
    logger.info(f'Scraped {len(new_posts)} posts from {len(subreddits)} subreddits')
    return new_posts

import json
import re
import time
import logging
import requests
from config import GEMINI_API_KEY

logger = logging.getLogger(__name__)

GEMINI_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent'


def is_available() -> bool:
    return bool(GEMINI_API_KEY)


def classify_batch(posts: list, event_keywords: list) -> dict:
    """
    Classify a list of post dicts as 'buy', 'sell', or 'skip'.
    Batches 20 posts per Gemini call.
    Falls back to keyword classification if Gemini is unavailable.
    Returns {post_id: label}
    """
    if not is_available() or not posts:
        logger.warning('Gemini unavailable — using keyword fallback for all posts')
        return {p['id']: _keyword_fallback(p) for p in posts}

    results = {}
    batch_size = 20
    events_hint = ', '.join(event_keywords[:15]) if event_keywords else 'any concert/event tickets'

    for i in range(0, len(posts), batch_size):
        batch = posts[i:i + batch_size]
        batch_results = _call_gemini(batch, events_hint)
        results.update(batch_results)
        if i + batch_size < len(posts):
            time.sleep(1)  # Stay under 15 req/min free tier

    return results


def _call_gemini(batch: list, events_hint: str) -> dict:
    prompt = (
        f'You are filtering Reddit posts for a concert ticket resale marketplace in India.\n'
        f'Current events we care about: {events_hint}\n\n'
        f'Classify each post as:\n'
        f'- "buy": person wants to BUY tickets for a concert/event\n'
        f'- "sell": person wants to SELL tickets for a concert/event\n'
        f'- "skip": NOT about buying or selling tickets (poems, rants, general questions, '
        f'general concert discussion without a ticket transaction)\n\n'
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
        f'\n\nReply ONLY with valid JSON: {{"id1": "buy", "id2": "skip", ...}}'
    )
    try:
        res = requests.post(
            f'{GEMINI_URL}?key={GEMINI_API_KEY}',
            json={'contents': [{'parts': [{'text': prompt}]}],
                  'generationConfig': {'temperature': 0.1}},
            timeout=25
        )
        if res.status_code != 200:
            logger.warning(f'Gemini error {res.status_code}: {res.text[:200]}')
            return {p['id']: _keyword_fallback(p) for p in batch}

        text = res.json()['candidates'][0]['content']['parts'][0]['text']
        match = re.search(r'\{.*?\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        logger.warning(f'Gemini returned no JSON: {text[:200]}')
    except Exception as e:
        logger.warning(f'Gemini call failed: {e}')

    return {p['id']: _keyword_fallback(p) for p in batch}


def _keyword_fallback(post: dict) -> str:
    """Simple keyword classification used when Gemini is unavailable."""
    buy_kw = ['wtb', 'want to buy', 'looking to buy', 'need ticket', 'need tickets',
              'iso ticket', 'anyone selling', 'need passes', 'buying ticket',
              'anyone got', 'anyone have', 'looking for ticket']
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

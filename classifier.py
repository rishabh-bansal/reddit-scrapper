import json
import re
import time
import logging
import requests
from config import GEMINI_API_KEY

logger = logging.getLogger(__name__)

GEMINI_MODEL = 'gemini-2.5-flash'
GEMINI_URL = f'https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent'

BUY_KEYWORDS = [
    'wtb', 'want to buy', 'looking to buy', 'looking for', 'need ticket', 'need tickets',
    'iso ticket', 'iso tickets', 'anyone selling', 'need passes', 'buying ticket',
    'buying tickets', 'anyone got', 'anyone have', 'looking for ticket', 'looking for tickets',
    'need 1 ticket', 'need 2 ticket', 'in search of', 'dm me price', 'dm price',
    "what's the price", 'how much for', 'available for purchase', 'want to purchase',
    'interested in buying', 'can i get', 'can i buy', 'want to get', 'ticket wanted',
    'want 1 ticket', 'want 2 tickets', 'need a ticket', 'need two tickets',
]

SELL_KEYWORDS = [
    'wts', 'want to sell', 'selling ticket', 'selling tickets', 'for sale', 'for sale:',
    'have ticket', 'have tickets', 'extra ticket', 'extra tickets', 'selling passes',
    'available ticket', 'pit ticket', 'selling my ticket', 'selling 1', 'selling 2',
    'selling 3', 'dm to buy', 'dm if interested', 'ticket available', 'tickets available',
    'selling below', 'below mrp', 'below bookmyshow', 'face value', 'at cost',
    'selling at', 'ticket for sale', 'tickets for sale', 'have 1 ticket', 'have 2 tickets',
    'have 3 tickets', 'selling two', 'selling one', 'dm for price', 'price negotiable',
]

# These are real regex patterns — used with re.search()
SKIP_PATTERNS = [
    # Event planning / organisation
    r'planning an? event', r'organizing an? event', r'hosting an? event',
    r'looking for venue', r'suggestions for.*event', r'need recommendations for',
    r'help me plan', r'how to organize', r'event management', r'event planning',
    r'budget place', r'good place for', r'vehicle suggestions',
    # Non-ticket items for sale
    r'selling.*\bmonitor\b', r'selling.*\blaptop\b', r'selling.*\bphone\b',
    r'selling.*\bcamera\b', r'selling.*\bfurniture\b', r'selling.*\bbike\b',
    r'selling.*\bcar\b', r'selling.*\bhouse\b',
    r'wts.*\bmonitor\b', r'wts.*\blaptop\b', r'wts.*\bphone\b',
    r'for sale.*\bmonitor\b', r'for sale.*\blaptop\b',
    r'used.*\bmonitor\b', r'used.*\blaptop\b', r'\b4k monitor\b',
    # General discussion
    r'what do you think', r'your thoughts', r'opinion on', r'is it worth',
    r'has anyone been', r'anyone attended', r'review of', r'experience with',
    r'how was the', r'did you go', r'who is going', r"who's going",
    r'any updates on', r'any news about', r'when is the',
    # Poetry/creative
    r'\bpoem\b', r'\bpoetry\b', r'\bstory\b', r'\bfiction\b',
    r'creative writing', r'original work',
    # Questions about tickets (not buying/selling)
    r'how to get tickets', r'where to buy tickets', r'ticket price',
    r'cost of tickets', r'is it sold out', r'booking open',
    r'when will tickets release',
]

# Pre-compile for speed
_SKIP_RE = [re.compile(p, re.IGNORECASE) for p in SKIP_PATTERNS]
_TICKET_WORDS = {'ticket', 'tickets', 'pass', 'passes', 'entry', 'fanpit', 'pit'}


def is_available() -> bool:
    return bool(GEMINI_API_KEY)


def classify_batch(posts: list, event_keywords: list) -> dict:
    """
    Classify posts as 'buy', 'sell', or 'skip'.
    Batches 20 posts per Gemini call with 4s delay to stay under 15 req/min.
    Falls back to keyword classification on failure.
    Returns {post_id: label}
    """
    if not is_available() or not posts:
        logger.warning('Gemini unavailable — using keyword fallback')
        return {p['id']: _keyword_filter(p) for p in posts}

    results = {}
    batch_size = 20
    events_hint = ', '.join(event_keywords[:15]) if event_keywords else 'any concert/event tickets'

    for i in range(0, len(posts), batch_size):
        batch = posts[i:i + batch_size]
        results.update(_call_gemini(batch, events_hint))
        if i + batch_size < len(posts):
            time.sleep(4)  # stay under 15 req/min free tier

    return results


def _call_gemini(batch: list, events_hint: str) -> dict:
    prompt = (
        f'You are filtering Reddit posts for a concert ticket resale marketplace in India.\n'
        f'Current events we care about: {events_hint}\n\n'
        f'Classify each post as:\n'
        f'- "buy": person wants to BUY tickets for a concert/event\n'
        f'- "sell": person wants to SELL tickets for a concert/event\n'
        f'- "skip": NOT about buying/selling tickets (general discussion, event planning, '
        f'selling non-ticket items, poems, questions, reviews)\n\n'
        f'Examples:\n'
        f'"WTS 2 Diljit pit tickets Delhi" = sell\n'
        f'"Anyone got extra Coldplay passes?" = buy\n'
        f'"Looking for 1 ticket to NH7" = buy\n'
        f'"Have 3 Calvin Harris tickets, selling below MRP" = sell\n'
        f'"4K Monitor for sale" = skip\n'
        f'"Planning an event, suggestions for a budget venue?" = skip\n'
        f'"Poem by me: Scores to Settle" = skip\n'
        f'"How was the Diljit concert?" = skip\n\n'
        f'Posts:\n' +
        json.dumps([{'id': p['id'], 'text': (p['title'] + ' ' + p.get('body', ''))[:300]}
                    for p in batch]) +
        f'\n\nReply ONLY with valid JSON: {{"id1": "buy", "id2": "skip", ...}}'
    )

    for attempt in range(3):
        try:
            res = requests.post(
                f'{GEMINI_URL}?key={GEMINI_API_KEY}',
                json={'contents': [{'parts': [{'text': prompt}]}],
                      'generationConfig': {'temperature': 0.1, 'maxOutputTokens': 512}},
                timeout=30
            )
            if res.status_code == 429:
                wait = 15 * (attempt + 1)
                logger.warning(f'Gemini 429 — waiting {wait}s (attempt {attempt+1}/3)')
                time.sleep(wait)
                continue
            if res.status_code != 200:
                logger.warning(f'Gemini error {res.status_code}: {res.text[:300]}')
                break

            text = res.json()['candidates'][0]['content']['parts'][0]['text']
            # Strip markdown fences if present
            text = re.sub(r'```(?:json)?\s*|\s*```', '', text).strip()
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                parsed = json.loads(match.group())
                # Validate — only keep known IDs and valid labels
                valid = {'buy', 'sell', 'skip'}
                ids = {p['id'] for p in batch}
                return {k: v for k, v in parsed.items() if k in ids and v in valid}
            logger.warning(f'Gemini returned no JSON: {text[:200]}')
            break

        except json.JSONDecodeError as e:
            logger.warning(f'Gemini JSON parse error: {e}')
            break
        except Exception as e:
            logger.warning(f'Gemini call failed: {e}')
            break

    return {p['id']: _keyword_filter(p) for p in batch}


def _keyword_filter(post: dict) -> str:
    """
    Keyword-based classification. Used as fallback when Gemini is unavailable.
    """
    text = (post.get('title', '') + ' ' + post.get('body', '')).lower()

    # Check skip patterns first (these are real regex)
    for pat in _SKIP_RE:
        if pat.search(text):
            # Override skip only if post has strong ticket + buy/sell signals
            has_ticket = any(w in text for w in _TICKET_WORDS)
            has_transaction = any(k in text for k in ['wts', 'wtb', 'selling ticket', 'buying ticket'])
            if not (has_ticket and has_transaction):
                return 'skip'

    has_ticket_word = any(w in text for w in _TICKET_WORDS)

    is_buy = any(kw in text for kw in BUY_KEYWORDS)
    is_sell = any(kw in text for kw in SELL_KEYWORDS)

    if not has_ticket_word:
        return 'unclear'

    if is_buy and not is_sell:
        return 'buy'
    if is_sell and not is_buy:
        return 'sell'
    if is_buy and is_sell:
        # FIX: was `sell_count > sell_count` (always False) — now correct
        buy_count = sum(1 for kw in BUY_KEYWORDS if kw in text)
        sell_count = sum(1 for kw in SELL_KEYWORDS if kw in text)
        if buy_count > sell_count:
            return 'buy'
        if sell_count > buy_count:
            return 'sell'
        return 'unclear'

    return 'unclear'


# Alias for any callers using old name
_keyword_fallback = _keyword_filter

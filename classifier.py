import json
import re
import time
import logging
import requests
from config import GEMINI_API_KEY

logger = logging.getLogger(__name__)

# Use the correct model that's enabled in your account
GEMINI_MODEL = 'gemini-2.5-flash'  # Changed from gemini-2.0-flash
GEMINI_URL = f'https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent'

# Expanded keyword lists for better filtering
BUY_KEYWORDS = [
    'wtb', 'want to buy', 'looking to buy', 'looking for', 'need ticket', 'need tickets',
    'iso ticket', 'iso tickets', 'anyone selling', 'need passes', 'buying ticket',
    'buying tickets', 'anyone got', 'anyone have', 'looking for ticket', 'looking for tickets',
    'need 1 ticket', 'need 2 ticket', 'in search of', 'dm me price', 'dm price',
    'what\'s the price', 'how much for', 'available for purchase', 'want to purchase',
    'interested in buying', 'can i get', 'can i buy', 'want to get', 'ticket wanted',
    'want 1 ticket', 'want 2 tickets', 'need a ticket', 'need two tickets'
]

SELL_KEYWORDS = [
    'wts', 'want to sell', 'selling ticket', 'selling tickets', 'for sale', 'for sale:',
    'have ticket', 'have tickets', 'extra ticket', 'extra tickets', 'selling passes',
    'available ticket', 'pit ticket', 'selling my ticket', 'selling 1', 'selling 2',
    'selling 3', 'dm to buy', 'dm if interested', 'ticket available', 'tickets available',
    'selling below', 'below mrp', 'below bookmyshow', 'face value', 'at cost',
    'selling at', 'ticket for sale', 'tickets for sale', 'have 1 ticket', 'have 2 tickets',
    'have 3 tickets', 'selling two', 'selling one', 'dm for price', 'price negotiable'
]

SKIP_PATTERNS = [
    # Event planning / organization
    r'planning an? event', r'organizing an? event', r'hosting an? event',
    r'looking for venue', r'suggestions for.*event', r'need recommendations for',
    r'help me plan', r'how to organize', r'event management', r'event planning',
    r'budget place', r'good place for', r'vehicle suggestions',
    
    # Non-ticket items for sale
    r'selling.*monitor', r'selling.*laptop', r'selling.*phone', r'selling.*camera',
    r'selling.*furniture', r'selling.*bike', r'selling.*car', r'selling.*house',
    r'wts.*monitor', r'wts.*laptop', r'wts.*phone', r'for sale.*monitor',
    r'for sale.*laptop', r'used.*monitor', r'used.*laptop', r'4k monitor',
    
    # General discussion
    r'what do you think', r'your thoughts', r'opinion on', r'is it worth',
    r'has anyone been', r'anyone attended', r'review of', r'experience with',
    r'how was the', r'did you go', r'who is going', r'who\'s going',
    r'any updates on', r'any news about', r'when is the',
    
    # Poetry/creative
    r'poem', r'poetry', r'story', r'fiction', r'creative writing', r'original work',
    
    # Questions about tickets (not buying/selling)
    r'how to get tickets', r'where to buy tickets', r'ticket price', r'cost of tickets',
    r'is it sold out', r'are tickets available', r'booking open', r'when will tickets release',
]

def is_available() -> bool:
    return bool(GEMINI_API_KEY)


def classify_batch(posts: list, event_keywords: list) -> dict:
    """
    Classify a list of post dicts as 'buy', 'sell', or 'skip'.
    Batches 20 posts per Gemini call.
    Falls back to enhanced keyword classification if Gemini is unavailable.
    Returns {post_id: label}
    """
    if not is_available() or not posts:
        logger.warning('Gemini unavailable — using enhanced keyword fallback')
        return {p['id']: _enhanced_keyword_filter(p) for p in posts}

    results = {}
    batch_size = 20
    events_hint = ', '.join(event_keywords[:15]) if event_keywords else 'any concert/event tickets'

    for i in range(0, len(posts), batch_size):
        batch = posts[i:i + batch_size]
        batch_results = _call_gemini(batch, events_hint)
        results.update(batch_results)
        if i + batch_size < len(posts):
            time.sleep(1)  # Stay under rate limits

    return results


def _call_gemini(batch: list, events_hint: str) -> dict:
    prompt = (
        f'You are filtering Reddit posts for a concert ticket resale marketplace in India.\n'
        f'Current events we care about: {events_hint}\n\n'
        f'Classify each post as:\n'
        f'- "buy": person wants to BUY tickets for a concert/event\n'
        f'- "sell": person wants to SELL tickets for a concert/event\n'
        f'- "skip": NOT about buying or selling tickets (poems, rants, general questions, '
        f'event planning, selling non-ticket items, general concert discussion without a ticket transaction)\n\n'
        f'Examples:\n'
        f'"Poem by me: Scores to Settle" = skip\n'
        f'"Feeling guilty about missing the show" = skip\n'
        f'"Planning an event, suggestions for a good budget place?" = skip\n'
        f'"4K Monitor for sale" = skip (not tickets)\n'
        f'"WTS 2 Diljit pit tickets Delhi" = sell\n'
        f'"Anyone got extra Coldplay passes?" = buy\n'
        f'"Looking for 1 ticket to NH7" = buy\n'
        f'"Have 3 Calvin Harris tickets, selling below MRP" = sell\n\n'
        f'Posts:\n' +
        json.dumps([{'id': p['id'], 'text': (p['title'] + ' ' + p.get('body', ''))[:300]}
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
            return {p['id']: _enhanced_keyword_filter(p) for p in batch}

        text = res.json()['candidates'][0]['content']['parts'][0]['text']
        match = re.search(r'\{.*?\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        logger.warning(f'Gemini returned no JSON: {text[:200]}')
    except Exception as e:
        logger.warning(f'Gemini call failed: {e}')

    return {p['id']: _enhanced_keyword_filter(p) for p in batch}


def _enhanced_keyword_filter(post: dict) -> str:
    """
    Enhanced keyword classification with better pattern matching.
    Used when Gemini is unavailable or fails.
    """
    title = post.get('title', '').lower()
    body = post.get('body', '').lower()
    text = title + ' ' + body
    
    # First check: Skip patterns (non-ticket items, event planning, etc.)
    for pattern in SKIP_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            # Double-check: If it has strong ticket keywords, override skip
            ticket_keywords = ['ticket', 'tickets', 'pass', 'passes', 'wts', 'wtb', 'selling', 'buying']
            has_ticket = any(kw in text for kw in ticket_keywords)
            if not has_ticket:
                return 'skip'
    
    # Check for buy/sell signals
    is_buy = False
    is_sell = False
    
    # Check buy keywords
    for kw in BUY_KEYWORDS:
        if kw in text:
            is_buy = True
            break
    
    # Check sell keywords
    for kw in SELL_KEYWORDS:
        if kw in text:
            is_sell = True
            break
    
    # Must have ticket-related words to be considered
    has_ticket_word = any(word in text for word in ['ticket', 'tickets', 'pass', 'passes', 'entry'])
    
    if not has_ticket_word:
        # If no ticket word, only classify if strong buy/sell signal
        if (is_buy and not is_sell) or (is_sell and not is_buy):
            # Still return unclear to be safe
            return 'unclear'
        return 'skip'
    
    # Classify based on signals
    if is_buy and not is_sell:
        return 'buy'
    if is_sell and not is_buy:
        return 'sell'
    if is_buy and is_sell:
        # Ambiguous - check which one is stronger
        buy_count = sum(1 for kw in BUY_KEYWORDS if kw in text)
        sell_count = sum(1 for kw in SELL_KEYWORDS if kw in text)
        if buy_count > sell_count:
            return 'buy'
        if sell_count > sell_count:
            return 'sell'
        return 'unclear'
    
    return 'unclear'


def _keyword_fallback(post: dict) -> str:
    """Original simple keyword fallback - kept for compatibility"""
    return _enhanced_keyword_filter(post)
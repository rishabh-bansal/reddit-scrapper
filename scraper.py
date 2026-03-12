import time
import random
import logging
import requests
import classifier
from config import REDDIT_USER_AGENTS, MAX_POSTS_PER_SUBREDDIT, REQUEST_TIMEOUT, DELAY_BETWEEN_REQUESTS

logger = logging.getLogger(__name__)

# ── Enhanced keyword lists ──

BROAD_KEYWORDS = [
    # ticket words
    'ticket', 'tickets', 'pass', 'passes', 'entry', 'fanpit', 'fan pit', 'pit ticket',
    # buy signals
    'wtb', 'want to buy', 'looking to buy', 'looking for', 'need ticket',
    'need tickets', 'iso ticket', 'iso tickets', 'anyone selling', 'need passes',
    'buying ticket', 'buying tickets', 'anyone have', 'anyone got',
    # sell signals
    'wts', 'want to sell', 'selling ticket', 'selling tickets', 'for sale',
    'have ticket', 'have tickets', 'extra ticket', 'extra tickets',
    'selling passes', 'available ticket', 'selling my ticket', 'pit ticket',
    'selling 1', 'selling 2', 'selling 3', 'selling two', 'selling three',
    # concert/event words
    'concert', 'festival', 'fest', 'gig', 'show', 'tour', 'live',
    'nh7', 'lollapalooza', 'sunburn', 'weekender', 'edm', 'music festival',
    # Indian artists/events
    'diljit', 'coldplay', 'calvin harris', 'ap dhillon', 'arijit',
    'badshah', 'divine', 'ar rahman', 'honey singh', 'nucleya',
    'martin garrix', 'alan walker', 'marshmello', 'chainsmokers',
    'karan aujla', 'sidhu moose wala', 'diljit dosanjh',
]

# Subreddits that are specifically for ticket selling/buying
TICKET_FOCUSED_SUBS = {
    'concertticketsindia', 'ticketresellingindia', 'concertresale',
    'ticketresale', 'concerts_india', 'concertsindia_', 'transactiontickets',
    'ticketexchange', 'edmtickets', 'tickets', 'concerttickets',
}

# Skip patterns for irrelevant posts
SKIP_PATTERNS = [
    # Event planning / organization
    'planning an event', 'organizing an event', 'hosting an event',
    'looking for venue', 'suggestions for event', 'need recommendations for',
    'help me plan', 'how to organize', 'event management', 'event planning',
    'budget place', 'good place for', 'venue suggestions',
    
    # Non-ticket items for sale
    'monitor for sale', 'laptop for sale', 'phone for sale', 'camera for sale',
    'furniture for sale', 'bike for sale', 'car for sale', 'house for sale',
    'selling my monitor', 'selling my laptop', 'selling my phone',
    '4k monitor', 'gaming monitor', 'gaming laptop', 'smartphone',
    
    # General discussion
    'what do you think', 'your thoughts', 'opinion on', 'is it worth',
    'has anyone been', 'anyone attended', 'review of', 'experience with',
    'how was the', 'did you go', 'who is going', 'who\'s going',
    'any updates on', 'any news about', 'when is the',
    
    # Poetry/creative
    'poem', 'poetry', 'story', 'fiction', 'creative writing', 'original work',
    
    # Questions about tickets (not buying/selling)
    'how to get tickets', 'where to buy tickets', 'ticket price', 'cost of tickets',
    'is it sold out', 'are tickets available', 'booking open', 'when will tickets release',
]

def _headers():
    """Generate random headers to avoid detection"""
    return {
        'User-Agent': random.choice(REDDIT_USER_AGENTS),
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Connection': 'keep-alive',
    }


def _should_skip(text: str) -> bool:
    """Check if post matches skip patterns"""
    text_lower = text.lower()
    for pattern in SKIP_PATTERNS:
        if pattern in text_lower:
            return True
    return False


def _broad_filter(post: dict, subreddit: str, extra_keywords: list) -> bool:
    """
    Stage 1: broad keyword filter - decides what gets sent to AI.
    Returns True if post should be considered for further processing.
    """
    sub_lower = subreddit.lower()
    title = post.get('title', '')
    selftext = post.get('selftext', '')
    text = (title + ' ' + selftext).lower()
    
    # Skip obvious non-ticket posts first
    if _should_skip(text):
        logger.debug(f"Skipping post due to pattern match: {title[:50]}...")
        return False
    
    # Combine broad keywords with event keywords
    all_kw = BROAD_KEYWORDS + [k.lower() for k in extra_keywords]
    
    # Check for ticket-related keywords
    has_ticket_word = any(word in text for word in 
        ['ticket', 'tickets', 'pass', 'passes', 'entry', 'fanpit', 'pit'])
    
    # Check for buy/sell keywords
    has_buy_sell = any(word in text for word in 
        ['wtb', 'wts', 'buy', 'sell', 'selling', 'buying', 'for sale', 'looking for'])
    
    # Check for event keywords
    has_event = any(k in text for k in all_kw)
    
    if sub_lower in TICKET_FOCUSED_SUBS:
        # In ticket-focused subs, be more lenient
        return has_ticket_word or has_buy_sell or has_event
    else:
        # In general subs, require both ticket word AND buy/sell/event
        return has_ticket_word and (has_buy_sell or has_event)


def _extract_post_data(post: dict, subreddit: str) -> dict:
    """Extract relevant data from Reddit post"""
    return {
        'id': post['id'],
        'subreddit': subreddit,
        'title': post.get('title', ''),
        'body': post.get('selftext', '')[:500],  # Limit body length
        'author': post.get('author', '[deleted]'),
        'permalink': post.get('permalink', ''),
        'post_type': 'unclear',  # Will be updated by AI
        'ai_classified': 0,
        'ups': post.get('ups', 0),
        'num_comments': post.get('num_comments', 0),
        'created_utc': int(post.get('created_utc', 0)),
    }


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
    pages = 3 if fast_mode else 1  # Reduced from 5 to 3 for faster first boot
    
    logger.info(f"Fetching r/{subreddit} (fast_mode={fast_mode})")
    
    for page in range(pages):
        url = f'https://www.reddit.com/r/{subreddit}/new.json?limit={MAX_POSTS_PER_SUBREDDIT}'
        if after:
            url += f'&after={after}'

        # Random delay to avoid rate limiting
        delay = random.uniform(0.5, 1.5) if fast_mode else random.uniform(1, 2)
        time.sleep(delay)

        try:
            res = requests.get(url, headers=_headers(), timeout=REQUEST_TIMEOUT)

            # Handle rate limiting
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
                
            res.raise_for_status()

            data = res.json()
            children = data.get('data', {}).get('children', [])
            
            if not children:
                logger.debug(f'r/{subreddit} page {page+1}: no posts')
                break

            raw_posts = [c['data'] for c in children]

            # Stage 1: Broad filter
            candidates = []
            for post in raw_posts:
                try:
                    if _broad_filter(post, subreddit, extra_keywords):
                        candidates.append(post)
                except Exception as e:
                    logger.debug(f"Error in broad filter: {e}")
                    continue
                    
            logger.info(f'r/{subreddit} p{page+1}: {len(raw_posts)} raw → {len(candidates)} candidates')

            if not candidates:
                after = data.get('data', {}).get('after')
                if not after:
                    break
                continue

            # Build post dicts for candidates
            post_dicts = [_extract_post_data(p, subreddit) for p in candidates]

            # Stage 2: AI classification
            ai_results = classifier.classify_batch(post_dicts, extra_keywords)

            # Stage 3: Filter results
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

            # Get next page token
            after = data.get('data', {}).get('after')
            if not after:
                break

        except requests.exceptions.Timeout:
            logger.warning(f'Timeout fetching r/{subreddit}')
            break
        except requests.exceptions.ConnectionError:
            logger.warning(f'Connection error fetching r/{subreddit}')
            break
        except requests.exceptions.RequestException as e:
            logger.warning(f'Request failed for r/{subreddit}: {e}')
            break
        except Exception as e:
            logger.warning(f'Unexpected error for r/{subreddit}: {e}')
            break

    # Small delay between subreddits
    time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # FIXED: This line had the syntax error - now properly closed
    logger.info(f'r/{subreddit} complete: found {len(all_results)} relevant posts')
    return all_results


def fetch_multiple_subreddits(subreddits: list, extra_keywords: list, fast_mode: bool = False) -> list:
    """
    Fetch posts from multiple subreddits
    """
    all_posts = []
    for sub in subreddits:
        try:
            posts = fetch_subreddit(sub, extra_keywords, fast_mode)
            all_posts.extend(posts)
        except Exception as e:
            logger.error(f"Error fetching r/{sub}: {e}")
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    return all_posts
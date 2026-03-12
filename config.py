import os
import logging

logger = logging.getLogger(__name__)

# ── Database ──
DATABASE_URL = os.environ.get('DATABASE_URL', '')

# ── API Keys ──
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

# ── App ──
DASHBOARD_URL = os.environ.get('RENDER_EXTERNAL_URL', 'http://localhost:5000')
PORT = int(os.environ.get('PORT', 5000))

# ── Scraper intervals (seconds) - INCREASED to avoid rate limits ──
PRIORITY_INTERVALS = {
    'high':   15 * 60,  # 15 minutes
    'medium': 30 * 60,  # 30 minutes
    'low':    60 * 60,  # 60 minutes
}

# ── Reddit API settings ──
REDDIT_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
]

# ── Scraper settings ──
MAX_POSTS_PER_SUBREDDIT = 100  # Number of posts to fetch per subreddit
REQUEST_TIMEOUT = 15  # Seconds to wait for Reddit API
DELAY_BETWEEN_REQUESTS = 1  # Seconds between subreddit scrapes

# ── Validate on import — warn but never crash ──
def validate():
    missing = []
    if not DATABASE_URL:
        missing.append('DATABASE_URL')
    if not GEMINI_API_KEY:
        logger.warning('GEMINI_API_KEY not set - will use keyword fallback only')
    if not TELEGRAM_TOKEN:
        logger.warning('TELEGRAM_TOKEN not set - Telegram alerts disabled')
    if not TELEGRAM_CHAT_ID:
        logger.warning('TELEGRAM_CHAT_ID not set - Telegram alerts disabled')
    
    if missing:
        logger.info(f'Missing optional env vars: {", ".join(missing)} — some features will be disabled')
    else:
        logger.info('All required env vars present ✓')

validate()
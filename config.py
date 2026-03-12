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

# ── Scraper intervals (seconds) ──
PRIORITY_INTERVALS = {
    'high':   5 * 60,
    'medium': 15 * 60,
    'low':    45 * 60,
}

# ── Validate on import — warn but never crash ──
def validate():
    missing = []
    if not DATABASE_URL:
        missing.append('DATABASE_URL')
    if not GEMINI_API_KEY:
        missing.append('GEMINI_API_KEY')
    if not TELEGRAM_TOKEN:
        missing.append('TELEGRAM_TOKEN')
    if missing:
        logger.warning(f'Missing env vars: {", ".join(missing)} — some features will be disabled')
    else:
        logger.info('All env vars present ✓')

validate()

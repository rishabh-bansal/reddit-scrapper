import os
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
BASE_URL = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}'


def send_message(text: str, parse_mode='HTML', disable_web_page_preview=True):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning('Telegram not configured')
        return False
    try:
        res = requests.post(f'{BASE_URL}/sendMessage', json={
            'chat_id': TELEGRAM_CHAT_ID,
            'text': text,
            'parse_mode': parse_mode,
            'disable_web_page_preview': disable_web_page_preview
        }, timeout=10)
        return res.ok
    except Exception as e:
        logger.warning(f'Telegram send failed: {e}')
        return False


def format_post_alert(post: dict) -> str:
    type_emoji = {'buy': '🟢', 'sell': '🔴', 'unclear': '🟡'}.get(post['post_type'], '⚪')
    type_label = {'buy': 'WTB', 'sell': 'WTS', 'unclear': '?'}.get(post['post_type'], '?')
    ai_tag = ' <i>(AI)</i>' if post.get('ai_classified') else ''
    age = time_ago(post['created_utc'])
    dm_url = f"https://www.reddit.com/message/compose/?to={post['author']}&subject=Ticket%20enquiry"
    post_url = f"https://reddit.com{post['permalink']}"

    body_preview = post.get('body', '').strip()[:120]
    body_line = f'\n<i>{body_preview}...</i>' if body_preview else ''

    return (
        f"{type_emoji} <b>{type_label}{ai_tag}</b> · r/{post['subreddit']} · {age}\n"
        f"<b>{post['title']}</b>"
        f"{body_line}\n\n"
        f"👤 u/{post['author']}\n"
        f"<a href='{dm_url}'>✉️ DM User</a>  |  <a href='{post_url}'>View Post</a>"
    )


def send_post_alert(post: dict):
    msg = format_post_alert(post)
    return send_message(msg)


def send_daily_summary(stats: dict, new_today: list):
    now = datetime.now().strftime('%d %b %Y')
    buy_today = sum(1 for p in new_today if p['post_type'] == 'buy')
    sell_today = sum(1 for p in new_today if p['post_type'] == 'sell')

    top_subs = '\n'.join(
        f"  • r/{s['subreddit']}: {s['cnt']} posts"
        for s in stats['top_subreddits'][:5]
    ) or '  No data yet'

    msg = (
        f"📊 <b>Daily Summary — {now}</b>\n\n"
        f"🆕 New posts today: <b>{stats['today_new']}</b>\n"
        f"  🟢 WTB: {buy_today}   🔴 WTS: {sell_today}\n\n"
        f"📬 Total contacted: <b>{stats['contacted']}</b>\n"
        f"⏱ Avg response time: <b>{format_duration(stats['avg_response_seconds'])}</b>\n\n"
        f"🔥 Most active subreddits:\n{top_subs}"
    )
    return send_message(msg)


def send_weekly_stats(stats: dict):
    avg = format_duration(stats['avg_response_seconds'])
    contact_rate = round(stats['contacted'] / stats['total'] * 100) if stats['total'] else 0

    top_subs = '\n'.join(
        f"  {i+1}. r/{s['subreddit']}: {s['cnt']} posts"
        for i, s in enumerate(stats['top_subreddits'][:5])
    ) or '  No data yet'

    msg = (
        f"📈 <b>Weekly Stats</b>\n\n"
        f"📦 Total posts found: <b>{stats['total']}</b>\n"
        f"  🟢 Buyers (WTB): {stats['buy']}\n"
        f"  🔴 Sellers (WTS): {stats['sell']}\n"
        f"  🟡 Unclear: {stats['unclear']}\n\n"
        f"✅ Total contacted: <b>{stats['contacted']}</b>\n"
        f"📊 Contact rate: <b>{contact_rate}%</b>\n"
        f"⏱ Avg time to respond: <b>{avg}</b>\n\n"
        f"🔥 Top subreddits:\n{top_subs}"
    )
    return send_message(msg)


def send_startup_message(dashboard_url: str):
    msg = (
        f"🚀 <b>Reticket is live!</b>\n\n"
        f"Scraping Reddit every 61 seconds for concert ticket leads.\n\n"
        f"🌐 Dashboard: {dashboard_url}\n\n"
        f"You'll get instant alerts for new WTB/WTS posts here."
    )
    return send_message(msg)


def format_duration(seconds):
    if not seconds:
        return '—'
    m = seconds // 60
    if m < 60:
        return f'{m}m'
    h = m // 60
    if h < 24:
        return f'{h}h'
    return f'{h // 24}d'


def time_ago(utc):
    secs = int(datetime.utcnow().timestamp()) - utc
    if secs < 60:
        return f'{secs}s ago'
    if secs < 3600:
        return f'{secs // 60}m ago'
    if secs < 86400:
        return f'{secs // 3600}h ago'
    return f'{secs // 86400}d ago'

# Reticket Backend — Setup Guide

A Reddit scraper + Telegram bot for concert ticket leads. Runs 24/7 on Render.

---

## Step 1 — Create a Telegram Bot (2 minutes)

1. Open Telegram, search for **@BotFather**
2. Send `/newbot`
3. Give it a name: `Reticket Leads`
4. Give it a username: `reticket_yourname_bot`
5. Copy the **bot token** it gives you (looks like `7123456789:AAF...`)

Then get your Chat ID:
1. Send any message to your new bot
2. Open this URL in your browser (replace YOUR_TOKEN):
   `https://api.telegram.org/botYOUR_TOKEN/getUpdates`
3. Find `"chat":{"id":XXXXXXX}` — that number is your **Chat ID**

---

## Step 2 — Deploy to Render (5 minutes)

1. Push this folder to a **GitHub repo** (can be private)
2. Go to [render.com](https://render.com) → New → Web Service
3. Connect your GitHub repo
4. Render will auto-detect `render.yaml`
5. Add these **Environment Variables** in Render dashboard:

| Variable | Value |
|---|---|
| `TELEGRAM_TOKEN` | Your bot token from Step 1 |
| `TELEGRAM_CHAT_ID` | Your chat ID from Step 1 |
| `ANTHROPIC_API_KEY` | (Optional) Your Claude API key for AI classification |

6. Click **Deploy**

Your app will be live at `https://reticket-backend.onrender.com` (or similar)

---

## Step 3 — Test it

Send `/start` or any message to your Telegram bot.
Within a minute you should get a startup message with your dashboard URL.

---

## What happens automatically

- **Every 61 seconds**: Scrapes all subreddits, sends Telegram alert for each new WTB/WTS post
- **Every day at 8am**: Sends a daily digest summary
- **Every Monday at 9am**: Sends weekly stats
- **Every 10 minutes**: Self-pings to prevent Render free tier from sleeping

---

## Free tier note

Render's free tier **spins down after 15 minutes of no web traffic**.
The self-ping every 10 minutes is designed to prevent this, but it's not 100% guaranteed.

For a guaranteed always-on setup, upgrade to Render's **Starter plan ($7/mo)** or use **Railway** which has a more generous free tier.

---

## Adding more subreddits

Just type in the dashboard and click "+ Add Subreddit". Changes are saved to the database instantly.

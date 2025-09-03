# bot.py — full-featured Telegram bot for your arbitrage scanner (PTB v22.1)

import os
import asyncio
from typing import Set, Tuple, Dict

import config
from main import App, load_symbols_universe, make_pairs, fmt_full, age_sec
from get_all_coins import write_full_universe

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ========= Env config =========
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")             # set in PowerShell
ALLOWED_IDS = {s.strip() for s in (os.getenv("TELEGRAM_ADMIN_CHAT_ID") or "").split(",") if s.strip()}

# compute/alert cadence
POLL_SECS = 2.0  # was 0.5 → less CPU + avoids job skipping spam
TOP_N = 15       # rows to show in /active and /long

# ========= Helpers =========
def _allowed(chat_id: int) -> bool:
    """Check if user is allowed (admin)."""
    return not ALLOWED_IDS or str(chat_id) in ALLOWED_IDS

def _format_op(op: dict, i: int) -> str:
    buy_px  = fmt_full(op.get("buy_price_str"),  op.get("buy_price"))
    sell_px = fmt_full(op.get("sell_price_str"), op.get("sell_price"))
    ages    = f"{op.get('buy_age',0):.1f}s/{op.get('sell_age',0):.1f}s"
    long_tag = " 🔶LONG" if op.get("long") else ""
    return (
        f"<b>{i:>2}</b> {op['pair']}  "
        f"🟢 <b>{op['buy_mkt']}</b>@{buy_px} → "
        f"🔴 <b>{op['sell_mkt']}</b>@{sell_px}  "
        f"Δ <b>{op['profit_pct']:.4f}%</b>  "
        f"sz:{fmt_full(None, op['exec_qty'])}  age:{ages}{long_tag}"
    )

# ========= Background setup & monitor =========
async def _setup_core(context: ContextTypes.DEFAULT_TYPE):
    """Runs once on startup: prepare universe, load markets, start streams."""
    app = App()

    # ensure coins_universe.json exists / refresh if enabled
    if getattr(config, "REFRESH_UNIVERSE_ENABLED", True):
        try:
            write_full_universe(config.COINS_UNIVERSE_FILE, config.COINPAPRIKA_TIMEOUT)
        except Exception as e:
            print(f"Universe refresh failed: {e}")
    else:
        if not os.path.exists(config.COINS_UNIVERSE_FILE):
            raise SystemExit("coins_universe.json missing. Run get_all_coins.py once.")

    await app.load_markets()

    bases = load_symbols_universe(
        config.COINS_UNIVERSE_FILE,
        config.COINS_RANK_RANGE,
        config.EXTRA_BASE_SYMBOLS,
    )
    desired = make_pairs(bases, config.QUOTE_CURRENCIES)
    await app.discover(desired)
    await app.start_markets()

    # === Store state for commands/monitor (typed locals, no squiggles) ===
    prev_keys: Set[Tuple[str, str, str]] = set()
    prev_longs: Set[Tuple[str, str, str]] = set()
    last_alert_ts: Dict[Tuple[str, str, str], float] = {}

    context.application.bot_data["app"] = app
    context.application.bot_data["prev_keys"] = prev_keys
    context.application.bot_data["prev_longs"] = prev_longs
    context.application.bot_data["last_alert_ts"] = last_alert_ts

    # Notify first admin (if any)
    if ALLOWED_IDS:
        try:
            aid = int(next(iter(ALLOWED_IDS)))
            await context.bot.send_message(
                aid,
                "✅ Markets started. Use /active /long /stale /status",
                disable_web_page_preview=True,
            )
        except Exception as e:
            print(f"Notify admin failed: {e}")

async def _monitor_tick(context: ContextTypes.DEFAULT_TYPE):
    """Periodic compute + alerting (new entries & LONG events)."""
    app: App = context.application.bot_data.get("app")
    if not app:
        return

    try:
        ops = app.compute_arbitrages()
        prev_keys: Set[Tuple[str,str,str]] = context.application.bot_data["prev_keys"]
        prev_longs: Set[Tuple[str,str,str]] = context.application.bot_data["prev_longs"]
        cur_keys, cur_longs = set(), set()
        new_alerts, long_alerts = [], []

        for op in ops:
            key = (op["pair"], op["buy_mkt"], op["sell_mkt"])
            cur_keys.add(key)
            if op.get("long"):
                cur_longs.add(key)
            if key not in prev_keys:
                new_alerts.append(op)

        for key in cur_longs:
            if key not in prev_longs:
                for op in ops:
                    if (op["pair"], op["buy_mkt"], op["sell_mkt"]) == key:
                        long_alerts.append(op); break

        # update state
        context.application.bot_data["prev_keys"]  = cur_keys
        context.application.bot_data["prev_longs"] = cur_longs

        if ALLOWED_IDS and (new_alerts or long_alerts):
            lines = []
            if new_alerts:
                lines.append("🚨 <b>New arbitrage(s) entered</b>")
                for i, op in enumerate(new_alerts[:5], 1):
                    lines.append(_format_op(op, i))
            if long_alerts:
                lines.append("⏱️ <b>LONG arbitrage(s)</b>")
                for i, op in enumerate(long_alerts[:5], 1):
                    lines.append(_format_op(op, i))

            for aid in ALLOWED_IDS:
                try:
                    await context.bot.send_message(
                        int(aid),
                        "\n".join(lines),
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                except Exception as e:
                    print(f"Send alert to {aid} failed: {e}")

    except Exception as e:
        for aid in ALLOWED_IDS:
            try:
                await context.bot.send_message(int(aid), f"⚠️ monitor error: {e}")
            except:
                pass

# ========= Commands =========
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not _allowed(chat_id):
        await update.message.reply_text("Unauthorized"); return
    await update.message.reply_html(
        f"Hello! Your chat id is <code>{chat_id}</code>\n"
        "Commands: /active /long /stale /status"
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized"); return
    await update.message.reply_html(
        "<b>Commands</b>\n"
        "/active — top arbs (by profit)\n"
        "/long — only long arbs\n"
        "/stale — markets/pairs with no updates\n"
        "/status — short status summary"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized"); return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment."); return

    mkts = list(app.markets.keys())
    sup_counts = {m: len(app.supported.get(m, set())) for m in mkts}
    total_pairs = sum(sup_counts.values())

    await update.message.reply_html(
        "<b>Status</b>\n"
        f"Markets: <code>{', '.join(mkts) or '-'}</code>\n"
        f"Supported pairs (sum): <b>{total_pairs}</b>\n"
        f"Enter ≥ <b>{getattr(config,'THRESH_ENTER_PCT',0):.2f}%</b>, "
        f"Exit &lt; <b>{getattr(config,'THRESH_EXIT_PCT',0):.2f}%</b>, "
        f"Long: <b>{int(getattr(config,'LONG_SECS',0)/60)} min</b>"
    )

async def cmd_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized"); return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment."); return
    ops = app.compute_arbitrages()
    if not ops:
        await update.message.reply_text("No active arbs right now."); return
    lines = ["<b>Top active arbs</b>"]
    for i, op in enumerate(ops[:TOP_N], 1):
        lines.append(_format_op(op, i))
    await update.message.reply_html("\n".join(lines))

async def cmd_long(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized"); return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment."); return
    ops = [op for op in app.compute_arbitrages() if op.get("long")]
    if not ops:
        await update.message.reply_text("No long arbs yet."); return
    lines = ["<b>Long arbs</b>"]
    for i, op in enumerate(ops[:TOP_N], 1):
        lines.append(_format_op(op, i))
    await update.message.reply_html("\n".join(lines))

async def cmd_stale(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized"); return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment."); return
    items = app.list_stale()
    if not items:
        await update.message.reply_text("No stale markets/pairs."); return
    lines = ["<b>Stale (no update)</b>"]
    for i, (mkt, pair, a, q) in enumerate(items[:20], 1):
        lines.append(
            f"<b>{i:>2}</b> {mkt} {pair}  age:{a:.1f}s  "
            f"bid:{fmt_full(q.bid_str, q.bid)} ask:{fmt_full(q.ask_str, q.ask)}"
        )
    await update.message.reply_html("\n".join(lines))

# ========= Main =========
def main():
    if not TOKEN:
        raise SystemExit('Set TELEGRAM_BOT_TOKEN first:  $env:TELEGRAM_BOT_TOKEN = "YOUR_TOKEN"')

    application = Application.builder().token(TOKEN).build()

    # commands
    application.add_handler(CommandHandler("start",  cmd_start))
    application.add_handler(CommandHandler("help",   cmd_help))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("active", cmd_active))
    application.add_handler(CommandHandler("long",   cmd_long))
    application.add_handler(CommandHandler("stale",  cmd_stale))

    # background jobs
    if application.job_queue is None:
        raise SystemExit(
            'JobQueue missing. Install extra: pip install "python-telegram-bot[job-queue]==22.1" apscheduler'
        )
    application.job_queue.run_once(_setup_core, when=0)
    application.job_queue.run_repeating(
        _monitor_tick,
        interval=POLL_SECS,
        first=POLL_SECS,
        name="monitor",
        job_kwargs={"max_instances": 1, "coalesce": True, "misfire_grace_time": 10},
    )

    print("✅ Bot is running. Press Ctrl+C to stop.")
    application.run_polling()

if __name__ == "__main__":
    main()

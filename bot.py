
# # bot.py — Telegram bot for the arbitrage scanner (PTB v22.1)

# import os
# import asyncio
# import contextlib
# from html import escape
# from typing import Set, Tuple, Dict, cast, List

# import config
# from main import App, load_symbols_universe, make_pairs, fmt_full, age_sec
# from get_all_coins import write_full_universe

# from telegram import Update
# from telegram.constants import ParseMode
# from telegram.ext import Application, CommandHandler, ContextTypes

# # ========= Env/config =========
# TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# # Allow multiple admin chat IDs: "id1,id2,id3"
# ALLOWED_IDS: Set[str] = {
#     s.strip() for s in (os.getenv("TELEGRAM_ADMIN_CHAT_ID") or "").split(",") if s.strip()
# }

# # Bot cadence/settings
# POLL_SECS = 2.0  # compute/alert loop; 2s avoids scheduler spam
# TOP_N = 15       # rows to show in /active and /long

# # Live board settings
# BOARD_INTERVAL = 5  # seconds between live board refresh
# BOARD_ROWS = 20     # number of rows in the board


# # ========= Helpers =========
# def _allowed(chat_id: int) -> bool:
#     return not ALLOWED_IDS or str(chat_id) in ALLOWED_IDS


# def _format_op(op: dict, i: int) -> str:
#     buy_px = fmt_full(op.get("buy_price_str"), op.get("buy_price"))
#     sell_px = fmt_full(op.get("sell_price_str"), op.get("sell_price"))
#     ages = f"{op.get('buy_age', 0):.1f}s/{op.get('sell_age', 0):.1f}s"
#     long_tag = " 🔶LONG" if op.get("long") else ""
#     return (
#         f"<b>{i:>2}</b> {op['pair']}  "
#         f"🟢 <b>{op['buy_mkt']}</b>@{buy_px} → "
#         f"🔴 <b>{op['sell_mkt']}</b>@{sell_px}  "
#         f"Δ <b>{op['profit_pct']:.4f}%</b>  "
#         f"sz:{fmt_full(None, op['exec_qty'])}  age:{ages}{long_tag}"
#     )


# # ========= Background setup & monitor =========
# async def _setup_core(context: ContextTypes.DEFAULT_TYPE):
#     """
#     Runs once on startup: prepare universe, load markets, start streams.
#     """
#     app = App()

#     # Ensure coins_universe.json exists / refresh if enabled
#     try:
#         if getattr(config, "REFRESH_UNIVERSE_ENABLED", True):
#             write_full_universe(config.COINS_UNIVERSE_FILE, config.COINPAPRIKA_TIMEOUT)
#         else:
#             if not os.path.exists(config.COINS_UNIVERSE_FILE):
#                 raise SystemExit("coins_universe.json missing. Run get_all_coins.py once.")
#     except Exception as e:
#         print(f"Universe refresh failed: {e}")

#     await app.load_markets()

#     bases = load_symbols_universe(
#         config.COINS_UNIVERSE_FILE,
#         config.COINS_RANK_RANGE,
#         config.EXTRA_BASE_SYMBOLS,
#     )
#     desired = make_pairs(bases, config.QUOTE_CURRENCIES)
#     await app.discover(desired)
#     await app.start_markets()

#     # Store for commands/monitor
#     context.application.bot_data["app"] = app
#     context.application.bot_data["prev_keys"] = cast(Set[Tuple[str, str, str]], set())
#     context.application.bot_data["prev_longs"] = cast(Set[Tuple[str, str, str]], set())
#     context.application.bot_data["last_alert_ts"] = cast(Dict[Tuple[str, str, str], float], {})

#     # Notify first admin (if any)
#     if ALLOWED_IDS:
#         try:
#             aid = int(next(iter(ALLOWED_IDS)))
#             await context.bot.send_message(
#                 aid,
#                 "✅ Markets started. Use /active /long /stale /status",
#                 disable_web_page_preview=True,
#             )
#         except Exception as e:
#             print(f"Notify admin failed: {e}")


# async def _monitor_tick(context: ContextTypes.DEFAULT_TYPE):
#     """
#     Periodic compute + alerting (new entries & LONG events).
#     """
#     app: App = context.application.bot_data.get("app")
#     if not app:
#         return

#     try:
#         ops = app.compute_arbitrages()

#         prev_keys: Set[Tuple[str, str, str]] = context.application.bot_data["prev_keys"]
#         prev_longs: Set[Tuple[str, str, str]] = context.application.bot_data["prev_longs"]

#         cur_keys: Set[Tuple[str, str, str]] = set()
#         cur_longs: Set[Tuple[str, str, str]] = set()
#         new_alerts: List[dict] = []
#         long_alerts: List[dict] = []

#         for op in ops:
#             key = (op["pair"], op["buy_mkt"], op["sell_mkt"])
#             cur_keys.add(key)
#             if op.get("long"):
#                 cur_longs.add(key)
#             if key not in prev_keys:
#                 new_alerts.append(op)

#         for key in cur_longs:
#             if key not in prev_longs:
#                 for op in ops:
#                     if (op["pair"], op["buy_mkt"], op["sell_mkt"]) == key:
#                         long_alerts.append(op)
#                         break

#         # update state
#         context.application.bot_data["prev_keys"] = cur_keys
#         context.application.bot_data["prev_longs"] = cur_longs

#         if (new_alerts or long_alerts) and ALLOWED_IDS:
#             lines = []
#             if new_alerts:
#                 lines.append("🚨 <b>New arbitrage(s) entered</b>")
#                 for i, op in enumerate(new_alerts[:5], 1):
#                     lines.append(_format_op(op, i))
#             if long_alerts:
#                 lines.append("⏱️ <b>LONG arbitrage(s)</b>")
#                 for i, op in enumerate(long_alerts[:5], 1):
#                     lines.append(_format_op(op, i))

#             text = "\n".join(lines)
#             for cid in ALLOWED_IDS:
#                 with contextlib.suppress(Exception):
#                     await context.bot.send_message(
#                         int(cid), text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
#                     )

#     except Exception as e:
#         for cid in ALLOWED_IDS:
#             with contextlib.suppress(Exception):
#                 await context.bot.send_message(int(cid), f"⚠️ monitor error: {e}")


# # ========= Commands =========
# async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     chat_id = update.effective_chat.id
#     if not _allowed(chat_id):
#         await update.message.reply_text("Unauthorized")
#         return
#     await update.message.reply_html(
#         f"Hello! Your chat id is <code>{chat_id}</code>\n"
#         "Commands: /active /long /stale /status /board_on /board_off"
#     )


# async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     if not _allowed(update.effective_chat.id):
#         await update.message.reply_text("Unauthorized")
#         return
#     await update.message.reply_html(
#         "<b>Commands</b>\n"
#         "/active — top arbs (by profit)\n"
#         "/long — only long arbs\n"
#         "/stale — markets/pairs with no updates\n"
#         "/status — short status summary\n"
#         "/board_on — live-updating table\n"
#         "/board_off — stop the live board"
#     )


# async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     if not _allowed(update.effective_chat.id):
#         await update.message.reply_text("Unauthorized")
#         return
#     app: App = context.application.bot_data.get("app")
#     if not app:
#         await update.message.reply_text("Starting markets… try again in a moment.")
#         return

#     mkts = list(app.markets.keys())
#     sup_counts = {m: len(app.supported.get(m, set())) for m in mkts}
#     total_pairs = sum(sup_counts.values())

#     await update.message.reply_html(
#         "<b>Status</b>\n"
#         f"Markets: <code>{', '.join(mkts) or '-'}</code>\n"
#         f"Supported pairs (sum): <b>{total_pairs}</b>\n"
#         f"Enter ≥ <b>{getattr(config,'THRESH_ENTER_PCT',0):.2f}%</b>, "
#         f"Exit &lt; <b>{getattr(config,'THRESH_EXIT_PCT',0):.2f}%</b>, "
#         f"Long: <b>{int(getattr(config,'LONG_SECS',0)/60)} min</b>"
#     )


# async def cmd_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     if not _allowed(update.effective_chat.id):
#         await update.message.reply_text("Unauthorized")
#         return
#     app: App = context.application.bot_data.get("app")
#     if not app:
#         await update.message.reply_text("Starting markets… try again in a moment.")
#         return
#     ops = app.compute_arbitrages()
#     if not ops:
#         await update.message.reply_text("No active arbs right now.")
#         return
#     lines = ["<b>Top active arbs</b>"]
#     for i, op in enumerate(ops[:TOP_N], 1):
#         lines.append(_format_op(op, i))
#     await update.message.reply_html("\n".join(lines))


# async def cmd_long(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     if not _allowed(update.effective_chat.id):
#         await update.message.reply_text("Unauthorized")
#         return
#     app: App = context.application.bot_data.get("app")
#     if not app:
#         await update.message.reply_text("Starting markets… try again in a moment.")
#         return
#     ops = [op for op in app.compute_arbitrages() if op.get("long")]
#     if not ops:
#         await update.message.reply_text("No long arbs yet.")
#         return
#     lines = ["<b>Long arbs</b>"]
#     for i, op in enumerate(ops[:TOP_N], 1):
#         lines.append(_format_op(op, i))
#     await update.message.reply_html("\n".join(lines))


# async def cmd_stale(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     if not _allowed(update.effective_chat.id):
#         await update.message.reply_text("Unauthorized")
#         return
#     app: App = context.application.bot_data.get("app")
#     if not app:
#         await update.message.reply_text("Starting markets… try again in a moment.")
#         return
#     items = app.list_stale()
#     if not items:
#         await update.message.reply_text("No stale markets/pairs.")
#         return
#     lines = ["<b>Stale (no update)</b>"]
#     for i, (mkt, pair, a, q) in enumerate(items[:20], 1):
#         lines.append(
#             f"<b>{i:>2}</b> {mkt} {pair}  age:{a:.1f}s  "
#             f"bid:{fmt_full(q.bid_str, q.bid)} ask:{fmt_full(q.ask_str, q.ask)}"
#         )
#     await update.message.reply_html("\n".join(lines))


# # ========= Live Board (/board_on, /board_off) =========
# def _cut(s: str, n: int) -> str:
#     s = str(s)
#     return s[:n] if len(s) <= n else s[:n - 1] + "…"


# def _pad(s: str, n: int) -> str:
#     s = str(s)
#     return s.ljust(n)[:n]


# def _fmt_board(ops: list) -> str:
#     cols = [
#         ("#", 3),
#         ("PAIR", 12),
#         ("BUY@PX", 18),
#         ("SELL@PX", 18),
#         ("Δ%", 8),
#         ("SZ", 8),
#         ("AGE(s)", 10),
#         ("LONG", 5),
#     ]
#     header = " ".join(_pad(c, w) for c, w in cols)
#     sep = "-" * len(header)

#     lines = [header, sep]
#     for i, op in enumerate(ops[:BOARD_ROWS], 1):
#         buy = f"{op['buy_mkt']}@{fmt_full(op.get('buy_price_str'), op.get('buy_price'))}"
#         sell = f"{op['sell_mkt']}@{fmt_full(op.get('sell_price_str'), op.get('sell_price'))}"
#         row = [
#             _pad(i, 3),
#             _pad(_cut(op['pair'], 12), 12),
#             _pad(_cut(buy, 18), 18),
#             _pad(_cut(sell, 18), 18),
#             _pad(f"{op['profit_pct']:.4f}", 8),
#             _pad(fmt_full(None, op['exec_qty']), 8),
#             _pad(f"{op.get('buy_age', 0):.1f}/{op.get('sell_age', 0):.1f}", 10),
#             _pad("YES" if op.get("long") else "", 5),
#         ]
#         lines.append(" ".join(row))

#     table = "\n".join(lines)
#     return f"<b>Opportunities (top {min(BOARD_ROWS, len(ops))})</b>\n<pre>{escape(table)}</pre>"


# async def _board_tick(context: ContextTypes.DEFAULT_TYPE):
#     chat_id = context.job.chat_id
#     app: App = context.application.bot_data.get("app")
#     if not app:
#         return
#     ops = app.compute_arbitrages()
#     text = _fmt_board(ops)
#     mid = context.chat_data.get("board_mid")

#     try:
#         if mid:
#             await context.bot.edit_message_text(
#                 chat_id=chat_id,
#                 message_id=mid,
#                 text=text,
#                 parse_mode=ParseMode.HTML,
#                 disable_web_page_preview=True,
#             )
#         else:
#             msg = await context.bot.send_message(
#                 chat_id, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
#             )
#             context.chat_data["board_mid"] = msg.message_id
#     except Exception:
#         # If edit fails (deleted/too large), send a fresh one
#         msg = await context.bot.send_message(
#             chat_id, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
#         )
#         context.chat_data["board_mid"] = msg.message_id


# async def cmd_board_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     if not _allowed(update.effective_chat.id):
#         await update.message.reply_text("Unauthorized")
#         return
#     # Start (or restart) per-chat board job
#     if job := context.chat_data.get("board_job"):
#         job.schedule_removal()
#     job = context.application.job_queue.run_repeating(
#         _board_tick,
#         interval=BOARD_INTERVAL,
#         first=0,
#         chat_id=update.effective_chat.id,
#         name=f"board-{update.effective_chat.id}",
#         job_kwargs={"max_instances": 1, "coalesce": True, "misfire_grace_time": 10},
#     )
#     context.chat_data["board_job"] = job
#     await update.message.reply_text("📊 Live board started. Use /board_off to stop.")


# async def cmd_board_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     if not _allowed(update.effective_chat.id):
#         await update.message.reply_text("Unauthorized")
#         return
#     if job := context.chat_data.pop("board_job", None):
#         job.schedule_removal()
#     # Optionally delete the board message
#     if mid := context.chat_data.pop("board_mid", None):
#         with contextlib.suppress(Exception):
#             await context.bot.delete_message(update.effective_chat.id, mid)
#     await update.message.reply_text("🛑 Live board stopped.")


# # ========= Main =========
# def main():
#     if not TOKEN:
#         raise SystemExit('Set TELEGRAM_BOT_TOKEN first:  $env:TELEGRAM_BOT_TOKEN = "YOUR_TOKEN"')

#     application = Application.builder().token(TOKEN).build()

#     # commands
#     application.add_handler(CommandHandler("start", cmd_start))
#     application.add_handler(CommandHandler("help", cmd_help))
#     application.add_handler(CommandHandler("status", cmd_status))
#     application.add_handler(CommandHandler("active", cmd_active))
#     application.add_handler(CommandHandler("long", cmd_long))
#     application.add_handler(CommandHandler("stale", cmd_stale))
#     application.add_handler(CommandHandler("board_on", cmd_board_on))
#     application.add_handler(CommandHandler("board_off", cmd_board_off))

#     # background jobs (requires PTB job-queue extra)
#     if application.job_queue is None:
#         raise SystemExit(
#             'JobQueue missing. Install: pip install "python-telegram-bot[job-queue]==22.1" apscheduler'
#         )

#     application.job_queue.run_once(_setup_core, when=0)
#     application.job_queue.run_repeating(
#         _monitor_tick,
#         interval=POLL_SECS,
#         first=POLL_SECS,
#         name="monitor",
#         job_kwargs={"max_instances": 1, "coalesce": True, "misfire_grace_time": 10},
#     )

#     print("✅ Bot is running. Press Ctrl+C to stop.")
#     application.run_polling()


# if __name__ == "__main__":
#     main()


# # if __name__ == "__main__":
# #     main()


# bot.py — Telegram bot for the arbitrage scanner (PTB v22.1)

import os
import re
import asyncio
import contextlib
from html import escape
from typing import Set, Tuple, Dict, cast, List

import config
from main import App, load_symbols_universe, make_pairs, fmt_full, age_sec
from get_all_coins import write_full_universe

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# ========= Env/config =========
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# Allow multiple admin chat IDs: "id1,id2,id3"
ALLOWED_IDS: Set[str] = {
    s.strip() for s in (os.getenv("TELEGRAM_ADMIN_CHAT_ID") or "").split(",") if s.strip()
}

# Bot cadence/settings
POLL_SECS = 2.0  # compute/alert loop; 2s avoids scheduler spam
TOP_N = 15       # rows to show in /active and /long

# Live board settings
BOARD_INTERVAL = 5  # seconds between live board refresh
BOARD_ROWS = 20     # number of rows in the board


# ========= Per-chat preferences =========
# Stored under application.bot_data["prefs"][chat_id] as:
# {
#   "watch_markets": set[str],   # empty => all
#   "watch_quotes":  set[str],   # empty => all; matches pair's QUOTE (e.g. USDT)
#   "watch_bases":   set[str],   # empty => all; matches pair's BASE  (e.g. BTC)
#   "min_profit":    float       # percentage threshold
# }
def _prefs_for_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> Dict:
    prefs_all = context.application.bot_data.setdefault("prefs", {})
    if chat_id not in prefs_all:
        prefs_all[chat_id] = {
            "watch_markets": set(),
            "watch_quotes": set(),
            "watch_bases": set(),
            "min_profit": 0.0,
        }
    return prefs_all[chat_id]


def _pair_base_quote(pair: str) -> Tuple[str, str]:
    # expected like "ETH/USDT"
    if "/" in pair:
        b, q = pair.split("/", 1)
        return b.upper(), q.upper()
    return pair.upper(), ""


def _passes_filters(op: dict, prefs: Dict) -> bool:
    # Markets filter: both sides must be in the allowed set (if any)
    if prefs["watch_markets"]:
        if (op["buy_mkt"] not in prefs["watch_markets"]) or (op["sell_mkt"] not in prefs["watch_markets"]):
            return False

    base, quote = _pair_base_quote(op["pair"])

    # Quote filter (e.g., USDT-only)
    if prefs["watch_quotes"] and quote not in prefs["watch_quotes"]:
        return False

    # Base filter (restrict specific coins)
    if prefs["watch_bases"] and base not in prefs["watch_bases"]:
        return False

    # Min profit
    if op.get("profit_pct", 0.0) < float(prefs.get("min_profit", 0.0)):
        return False

    return True


def _filter_ops_for_chat(ops: List[dict], prefs: Dict) -> List[dict]:
    return [op for op in ops if _passes_filters(op, prefs)]


# ========= Helpers =========
def _allowed(chat_id: int) -> bool:
    return not ALLOWED_IDS or str(chat_id) in ALLOWED_IDS


def _format_op(op: dict, i: int) -> str:
    buy_px = fmt_full(op.get("buy_price_str"), op.get("buy_price"))
    sell_px = fmt_full(op.get("sell_price_str"), op.get("sell_price"))
    ages = f"{op.get('buy_age', 0):.1f}s/{op.get('sell_age', 0):.1f}s"
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
    """
    Runs once on startup: prepare universe, load markets, start streams.
    """
    app = App()

    # Ensure coins_universe.json exists / refresh if enabled
    try:
        if getattr(config, "REFRESH_UNIVERSE_ENABLED", True):
            write_full_universe(config.COINS_UNIVERSE_FILE, config.COINPAPRIKA_TIMEOUT)
        else:
            if not os.path.exists(config.COINS_UNIVERSE_FILE):
                raise SystemExit("coins_universe.json missing. Run get_all_coins.py once.")
    except Exception as e:
        print(f"Universe refresh failed: {e}")

    await app.load_markets()

    bases = load_symbols_universe(
        config.COINS_UNIVERSE_FILE,
        config.COINS_RANK_RANGE,
        config.EXTRA_BASE_SYMBOLS,
    )
    desired = make_pairs(bases, config.QUOTE_CURRENCIES)
    await app.discover(desired)
    await app.start_markets()

    # Store for commands/monitor
    context.application.bot_data["app"] = app
    context.application.bot_data["prev_keys"] = cast(Set[Tuple[str, str, str]], set())
    context.application.bot_data["prev_longs"] = cast(Set[Tuple[str, str, str]], set())
    context.application.bot_data["last_alert_ts"] = cast(Dict[Tuple[str, str, str], float], {})
    context.application.bot_data.setdefault("prefs", {})  # per-chat prefs container

    # Notify first admin (if any)
    if ALLOWED_IDS:
        try:
            aid = int(next(iter(ALLOWED_IDS)))
            await context.bot.send_message(
                aid,
                "✅ Markets started. Use /active /long /stale /status\n"
                "Use /help to see filtering commands.",
                disable_web_page_preview=True,
            )
        except Exception as e:
            print(f"Notify admin failed: {e}")


async def _monitor_tick(context: ContextTypes.DEFAULT_TYPE):
    """
    Periodic compute + alerting (new entries & LONG events).
    """
    app: App = context.application.bot_data.get("app")
    if not app:
        return

    try:
        ops = app.compute_arbitrages()

        prev_keys: Set[Tuple[str, str, str]] = context.application.bot_data["prev_keys"]
        prev_longs: Set[Tuple[str, str, str]] = context.application.bot_data["prev_longs"]

        cur_keys: Set[Tuple[str, str, str]] = set()
        cur_longs: Set[Tuple[str, str, str]] = set()
        new_alerts: List[dict] = []
        long_alerts: List[dict] = []

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
                        long_alerts.append(op)
                        break

        # update state (global)
        context.application.bot_data["prev_keys"] = cur_keys
        context.application.bot_data["prev_longs"] = cur_longs

        # Send per-chat (respecting prefs)
        if ALLOWED_IDS and (new_alerts or long_alerts):
            for cid_str in ALLOWED_IDS:
                try:
                    cid = int(cid_str)
                except:
                    continue
                prefs = _prefs_for_chat(context, cid)

                per_new = _filter_ops_for_chat(new_alerts, prefs)
                per_long = _filter_ops_for_chat(long_alerts, prefs)
                if not per_new and not per_long:
                    continue

                lines = []
                if per_new:
                    lines.append("🚨 <b>New arbitrage(s) entered</b>")
                    for i, op in enumerate(per_new[:5], 1):
                        lines.append(_format_op(op, i))
                if per_long:
                    lines.append("⏱️ <b>LONG arbitrage(s)</b>")
                    for i, op in enumerate(per_long[:5], 1):
                        lines.append(_format_op(op, i))

                await context.bot.send_message(
                    cid,
                    "\n".join(lines),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

    except Exception as e:
        for cid in ALLOWED_IDS:
            with contextlib.suppress(Exception):
                await context.bot.send_message(int(cid), f"⚠️ monitor error: {e}")


# ========= Commands =========
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not _allowed(chat_id):
        await update.message.reply_text("Unauthorized")
        return
    await update.message.reply_html(
        f"Hello! Your chat id is <code>{chat_id}</code>\n"
        "Commands: /active /long /stale /status /board_on /board_off\n"
        "Filters: /markets /watchmarkets /watchall /setprofit /quotes /quotes_clear /bases /bases_clear /flow /prefs"
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    await update.message.reply_html(
        "<b>Commands</b>\n"
        "/active — top arbs (by profit)\n"
        "/long — only long arbs\n"
        "/stale — markets/pairs with no updates\n"
        "/status — short status summary\n"
        "/board_on — live-updating table\n"
        "/board_off — stop the live board\n\n"
        "<b>Filters</b>\n"
        "/markets — list supported exchanges\n"
        "/watchmarkets binance,okx — only alert for these\n"
        "/watchall — clear market filter\n"
        "/setprofit 2 — alert only if profit ≥ 2%\n"
        "/quotes USDT,USDC — restrict quotes\n"
        "/quotes_clear — clear quote filter\n"
        "/bases BTC,ETH — restrict bases\n"
        "/bases_clear — clear base filter\n"
        "/flow usdt->usdt — convenience for USDT-only\n"
        "/prefs — show current filters"
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment.")
        return

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
        await update.message.reply_text("Unauthorized")
        return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment.")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    ops = _filter_ops_for_chat(app.compute_arbitrages(), prefs)
    if not ops:
        await update.message.reply_text("No active arbs right now.")
        return
    lines = ["<b>Top active arbs</b>"]
    for i, op in enumerate(ops[:TOP_N], 1):
        lines.append(_format_op(op, i))
    await update.message.reply_html("\n".join(lines))


async def cmd_long(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment.")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    ops = [op for op in app.compute_arbitrages() if op.get("long")]
    ops = _filter_ops_for_chat(ops, prefs)
    if not ops:
        await update.message.reply_text("No long arbs yet.")
        return
    lines = ["<b>Long arbs</b>"]
    for i, op in enumerate(ops[:TOP_N], 1):
        lines.append(_format_op(op, i))
    await update.message.reply_html("\n".join(lines))


async def cmd_stale(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment.")
        return
    items = app.list_stale()
    if not items:
        await update.message.reply_text("No stale markets/pairs.")
        return
    lines = ["<b>Stale (no update)</b>"]
    for i, (mkt, pair, a, q) in enumerate(items[:20], 1):
        lines.append(
            f"<b>{i:>2}</b> {mkt} {pair}  age:{a:.1f}s  "
            f"bid:{fmt_full(q.bid_str, q.bid)} ask:{fmt_full(q.ask_str, q.ask)}"
        )
    await update.message.reply_html("\n".join(lines))


# ---- Markets list & filters ----
async def cmd_markets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment.")
        return
    mkts = sorted(app.markets.keys())
    await update.message.reply_html("<b>Supported markets</b>\n" + ", ".join(mkts))


def _parse_csv_arg(args: List[str]) -> List[str]:
    if not args:
        return []
    raw = " ".join(args).strip()
    parts = [p.strip() for p in re.split(r"[,\s]+", raw) if p.strip()]
    return parts


async def cmd_watchmarkets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    app: App = context.application.bot_data.get("app")
    if not app:
        await update.message.reply_text("Starting markets… try again in a moment.")
        return

    prefs = _prefs_for_chat(context, update.effective_chat.id)
    want = {p.lower() for p in _parse_csv_arg(context.args)}
    if not want:
        await update.message.reply_text("Usage: /watchmarkets binance,okx,bybit")
        return

    # Validate against available
    have = set(app.markets.keys())
    bad = [m for m in want if m not in have]
    if bad:
        await update.message.reply_text(f"Unknown market(s): {', '.join(bad)}")
        return

    prefs["watch_markets"] = want
    await update.message.reply_text("✅ Watch list set: " + ", ".join(sorted(want)))


async def cmd_watchall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    prefs["watch_markets"] = set()
    await update.message.reply_text("✅ Market filter cleared (all markets allowed).")


def _parse_percent_arg(args: List[str]) -> float:
    if not args:
        return 0.0
    raw = args[0].strip().lower()
    raw = raw.rstrip("%")
    try:
        return float(raw)
    except:
        return 0.0


async def cmd_setprofit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    val = _parse_percent_arg(context.args)
    prefs["min_profit"] = max(0.0, val)
    await update.message.reply_text(f"✅ Min profit set to {prefs['min_profit']:.2f}%.")


async def cmd_quotes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    qs = {p.upper() for p in _parse_csv_arg(context.args)}
    if not qs:
        await update.message.reply_text("Usage: /quotes USDT or /quotes USDT,USDC")
        return
    prefs["watch_quotes"] = qs
    await update.message.reply_text("✅ Quote filter set: " + ", ".join(sorted(qs)))


async def cmd_quotes_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    prefs["watch_quotes"] = set()
    await update.message.reply_text("✅ Quote filter cleared.")


async def cmd_bases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    bs = {p.upper() for p in _parse_csv_arg(context.args)}
    if not bs:
        await update.message.reply_text("Usage: /bases BTC or /bases BTC,ETH")
        return
    prefs["watch_bases"] = bs
    await update.message.reply_text("✅ Base filter set: " + ", ".join(sorted(bs)))


async def cmd_bases_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    prefs["watch_bases"] = set()
    await update.message.reply_text("✅ Base filter cleared.")


async def cmd_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Convenience: /flow usdt->usdt   => restrict quotes to {USDT}.
    Since cross-exchange arb uses the same pair on both sides, quote constraint is enough.
    """
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    if not context.args:
        await update.message.reply_text("Usage: /flow usdt->usdt")
        return
    expr = context.args[0].strip().upper()
    m = re.match(r"([A-Z0-9]+)\s*->\s*([A-Z0-9]+)", expr)
    if not m:
        await update.message.reply_text("Format must be like: /flow USDT->USDT")
        return
    q1, q2 = m.groups()
    if q1 != q2:
        await update.message.reply_text("For cross-exchange same-pair arb, use same quote on both sides (e.g. USDT->USDT).")
        return
    prefs["watch_quotes"] = {q1}
    await update.message.reply_text(f"✅ Flow set to {q1}->{q2} (quote filter = {q1}).")


async def cmd_prefs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    prefs = _prefs_for_chat(context, update.effective_chat.id)
    def fmt_set(s): return ", ".join(sorted(s)) if s else "ALL"
    base = (
        "<b>Your preferences</b>\n"
        f"Markets: <code>{fmt_set(prefs['watch_markets'])}</code>\n"
        f"Quotes:  <code>{fmt_set(prefs['watch_quotes'])}</code>\n"
        f"Bases:   <code>{fmt_set(prefs['watch_bases'])}</code>\n"
        f"Min profit: <b>{prefs['min_profit']:.2f}%</b>"
    )
    await update.message.reply_html(base)


# ========= Live Board (/board_on, /board_off) =========
def _cut(s: str, n: int) -> str:
    s = str(s)
    return s[:n] if len(s) <= n else s[:n - 1] + "…"


def _pad(s: str, n: int) -> str:
    s = str(s)
    return s.ljust(n)[:n]


def _fmt_board(ops: list) -> str:
    cols = [
        ("#", 3),
        ("PAIR", 12),
        ("BUY@PX", 18),
        ("SELL@PX", 18),
        ("Δ%", 8),
        ("SZ", 8),
        ("AGE(s)", 10),
        ("LONG", 5),
    ]
    header = " ".join(_pad(c, w) for c, w in cols)
    sep = "-" * len(header)

    lines = [header, sep]
    for i, op in enumerate(ops[:BOARD_ROWS], 1):
        buy = f"{op['buy_mkt']}@{fmt_full(op.get('buy_price_str'), op.get('buy_price'))}"
        sell = f"{op['sell_mkt']}@{fmt_full(op.get('sell_price_str'), op.get('sell_price'))}"
        row = [
            _pad(i, 3),
            _pad(_cut(op['pair'], 12), 12),
            _pad(_cut(buy, 18), 18),
            _pad(_cut(sell, 18), 18),
            _pad(f"{op['profit_pct']:.4f}", 8),
            _pad(fmt_full(None, op['exec_qty']), 8),
            _pad(f"{op.get('buy_age', 0):.1f}/{op.get('sell_age', 0):.1f}", 10),
            _pad("YES" if op.get("long") else "", 5),
        ]
        lines.append(" ".join(row))

    table = "\n".join(lines)
    return f"<b>Opportunities (top {min(BOARD_ROWS, len(ops))})</b>\n<pre>{escape(table)}</pre>"


async def _board_tick(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    app: App = context.application.bot_data.get("app")
    if not app:
        return
    prefs = _prefs_for_chat(context, chat_id)
    ops = _filter_ops_for_chat(app.compute_arbitrages(), prefs)
    text = _fmt_board(ops)
    mid = context.chat_data.get("board_mid")

    try:
        if mid:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=mid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        else:
            msg = await context.bot.send_message(
                chat_id, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
            context.chat_data["board_mid"] = msg.message_id
    except Exception:
        # If edit fails (deleted/too large), send a fresh one
        msg = await context.bot.send_message(
            chat_id, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
        context.chat_data["board_mid"] = msg.message_id


async def cmd_board_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    # Start (or restart) per-chat board job
    if job := context.chat_data.get("board_job"):
        job.schedule_removal()
    job = context.application.job_queue.run_repeating(
        _board_tick,
        interval=BOARD_INTERVAL,
        first=0,
        chat_id=update.effective_chat.id,
        name=f"board-{update.effective_chat.id}",
        job_kwargs={"max_instances": 1, "coalesce": True, "misfire_grace_time": 10},
    )
    context.chat_data["board_job"] = job
    await update.message.reply_text("📊 Live board started. Use /board_off to stop.")


async def cmd_board_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update.effective_chat.id):
        await update.message.reply_text("Unauthorized")
        return
    if job := context.chat_data.pop("board_job", None):
        job.schedule_removal()
    if mid := context.chat_data.pop("board_mid", None):
        with contextlib.suppress(Exception):
            await context.bot.delete_message(update.effective_chat.id, mid)
    await update.message.reply_text("🛑 Live board stopped.")


# ========= Main =========
def main():
    if not TOKEN:
        raise SystemExit('Set TELEGRAM_BOT_TOKEN first:  $env:TELEGRAM_BOT_TOKEN = "YOUR_TOKEN"')

    application = Application.builder().token(TOKEN).build()

    # commands
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("active", cmd_active))
    application.add_handler(CommandHandler("long", cmd_long))
    application.add_handler(CommandHandler("stale", cmd_stale))

    # new filter commands
    application.add_handler(CommandHandler("markets", cmd_markets))
    application.add_handler(CommandHandler("watchmarkets", cmd_watchmarkets))
    application.add_handler(CommandHandler("watchall", cmd_watchall))
    application.add_handler(CommandHandler("setprofit", cmd_setprofit))
    application.add_handler(CommandHandler("quotes", cmd_quotes))
    application.add_handler(CommandHandler("quotes_clear", cmd_quotes_clear))
    application.add_handler(CommandHandler("bases", cmd_bases))
    application.add_handler(CommandHandler("bases_clear", cmd_bases_clear))
    application.add_handler(CommandHandler("flow", cmd_flow))
    application.add_handler(CommandHandler("prefs", cmd_prefs))

    # board commands
    application.add_handler(CommandHandler("board_on", cmd_board_on))
    application.add_handler(CommandHandler("board_off", cmd_board_off))

    # background jobs (requires PTB job-queue extra)
    if application.job_queue is None:
        raise SystemExit(
            'JobQueue missing. Install: pip install "python-telegram-bot[job-queue]==22.1" apscheduler'
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


# bot.py
"""
Telegram schedule bot — full feature set (sheets-based, .env config)
Собирает конфигурацию из .env (dotenv). Авторизация Google Sheets через
service account: путь к JSON в GOOGLE_CREDS_JSON_PATH или JSON в GOOGLE_CREDS_JSON_CONTENT.
"""
import os
import json
import glob
import asyncio
import logging
import hashlib
import contextlib
from typing import Any, Dict, List, Optional, Iterable
from datetime import datetime, timedelta, date

from dotenv import load_dotenv
load_dotenv()  # loads .env from current working directory

import gspread
from google.oauth2.service_account import Credentials as GoogleCredentials

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, BotCommandScopeDefault,
)
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# -------------------- Configuration (.env) --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
# Accept SPREADSHEET_ID or older GSHEET_ID name
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("GSHEET_ID")
GOOGLE_CREDS_JSON_PATH = os.getenv("GOOGLE_CREDS_JSON_PATH")
GOOGLE_CREDS_JSON_CONTENT = os.getenv("GOOGLE_CREDS_JSON_CONTENT")
# sheet names
GS_SCHEDULE_SHEET = os.getenv("GS_SCHEDULE_SHEET", "schedule")
GS_EXAMS_SHEET = os.getenv("GS_EXAMS_SHEET", "exams")
GS_SUBS_SHEET = os.getenv("GS_SUBS_SHEET", "subs")
# timezone
TZ_NAME = os.getenv("TZ_NAME", "Europe/Samara")
# groups (comma separated)
GROUPS = [g.strip() for g in os.getenv("GROUPS", "10,11").split(",") if g.strip()]
# watcher interval (seconds)
WATCH_INTERVAL = int(os.getenv("WATCH_INTERVAL", "60"))

# admins / superadmins env format: comma-separated integers
ADMINS_RAW = os.getenv("ADMINS", "")
SUPERADMINS_RAW = os.getenv("SUPERADMINS", "")

def parse_id_list(raw: str) -> set:
    s = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            s.add(int(p))
        except Exception:
            logging.warning("Invalid id in list skipped: %s", p)
    return s

ADMINS: set[int] = parse_id_list(ADMINS_RAW)
SUPERADMINS: set[int] = parse_id_list(SUPERADMINS_RAW)

# -------------------- Logging & TZ --------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("schedule-bot")

# TZ resolution
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    def resolve_tz(name: str, default: str = "UTC"):
        try:
            return ZoneInfo(name)
        except ZoneInfoNotFoundError:
            try:
                import tzdata  # type: ignore
                return ZoneInfo(name)
            except Exception:
                logging.warning("Часовой пояс %s недоступен. Фолбэк на %s.", name, default)
                return ZoneInfo(default)
    tz = resolve_tz(TZ_NAME, "UTC")
except Exception:
    class _FakeTZ: pass
    tz = _FakeTZ()

def now_local() -> datetime:
    try:
        return datetime.now(tz)  # type: ignore[arg-type]
    except Exception:
        return datetime.utcnow()

# -------------------- Google Sheets helpers (env-based) --------------------
_GS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def _load_service_account_credentials() -> Optional[GoogleCredentials]:
    """Load credentials from a file path or JSON content (both from .env)."""
    try:
        if GOOGLE_CREDS_JSON_PATH and os.path.isfile(GOOGLE_CREDS_JSON_PATH):
            log.info("Loading Google credentials from file: %s", GOOGLE_CREDS_JSON_PATH)
            return GoogleCredentials.from_service_account_file(GOOGLE_CREDS_JSON_PATH, scopes=_GS_SCOPES)
        if GOOGLE_CREDS_JSON_CONTENT:
            log.info("Loading Google credentials from JSON content in env")
            info = json.loads(GOOGLE_CREDS_JSON_CONTENT)
            return GoogleCredentials.from_service_account_info(info, scopes=_GS_SCOPES)
        log.warning("No Google credentials provided. Set GOOGLE_CREDS_JSON_PATH or GOOGLE_CREDS_JSON_CONTENT in .env")
        return None
    except Exception as e:
        log.exception("Failed to load Google service account credentials: %s", e)
        return None

def get_gspread_client():
    creds = _load_service_account_credentials()
    if not creds:
        raise RuntimeError("Google credentials not available. Check GOOGLE_CREDS_JSON_PATH or GOOGLE_CREDS_JSON_CONTENT.")
    try:
        client = gspread.authorize(creds)
        try:
            # log service account email if possible (helpful for sharing sheet)
            sa_email = creds.service_account_email
            log.info("Authorized Google client. Service account email: %s", sa_email)
        except Exception:
            log.info("Authorized Google client.")
        return client
    except Exception as e:
        log.exception("Error authorizing gspread client: %s", e)
        raise

def sh_open():
    if not SPREADSHEET_ID or "PASTE_YOUR_SHEET_ID" in SPREADSHEET_ID:
        raise RuntimeError("Заполни SPREADSHEET_ID в .env (ID таблицы между /d/ и /edit в URL).")
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)

def ws_open(sheet_name: str):
    try:
        return sh_open().worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sh = sh_open()
        ws = sh.add_worksheet(title=sheet_name, rows=100, cols=10)
        return ws

def gs_read_all(sheet_name: str) -> List[Dict[str, Any]]:
    ws = ws_open(sheet_name)
    return ws.get_all_records()

def gs_append_rows(sheet_name: str, rows: List[List[Any]]):
    if not rows:
        return
    ws = ws_open(sheet_name)
    ws.append_rows(rows, value_input_option="RAW")

def gs_clear_rows_by_indices(sheet_name: str, row_indices_desc: Iterable[int]):
    ws = ws_open(sheet_name)
    for idx in sorted(row_indices_desc, reverse=True):
        ws.delete_rows(idx)

def ensure_subs_sheet_headers():
    ws = ws_open(GS_SUBS_SHEET)
    values = ws.get_all_values()
    want = ["user_id", "group", "added_at"]
    if not values:
        ws.update([want])
        return
    if values[0] != want:
        ws.update([want] + values[1:])

# -------------------- Data / formatting --------------------
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
WEEKDAYS_RU = {
    "Mon": "Понедельник", "Tue": "Вторник", "Wed": "Среда",
    "Thu": "Четверг", "Fri": "Пятница", "Sat": "Суббота", "Sun": "Воскресенье"
}

def monday_of_week(dt: datetime) -> datetime:
    d = dt
    return (d - timedelta(days=d.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

def build_schedule_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    res: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for r in rows:
        g = str(r.get("group", "")).strip()
        wd = str(r.get("weekday", "")).strip()
        tm = str(r.get("time", "")).strip()
        subj = str(r.get("subject", "")).strip()
        teacher = str(r.get("teacher", "")).strip()
        room = str(r.get("room", "")).strip()
        if g in GROUPS and wd in WEEKDAYS and tm and subj:
            item = {"time": tm, "subject": subj}
            if teacher:
                item["teacher"] = teacher
            if room:
                item["room"] = room
            res.setdefault(g, {}).setdefault(wd, []).append(item)

    def start_minutes(s: str) -> int:
        try:
            hh, mm = s.split("-")[0].split(":")
            return int(hh) * 60 + int(mm)
        except Exception:
            return 0

    for g, days in res.items():
        for wd, items in days.items():
            items.sort(key=lambda x: start_minutes(x["time"]))
    return res

def read_schedule_map() -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    return build_schedule_map(gs_read_all(GS_SCHEDULE_SHEET))

def read_exams_map() -> Dict[str, List[Dict[str, Any]]]:
    rows = gs_read_all(GS_EXAMS_SHEET)
    res: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        g = str(r.get("group", "")).strip()
        ds = str(r.get("date", "")).strip()
        tm = str(r.get("time", "")).strip()
        subj = str(r.get("subject", "")).strip()
        note = str(r.get("note", "")).strip()
        if g in GROUPS and ds and subj:
            try:
                d = datetime.strptime(ds, "%Y-%m-%d").date()
            except Exception:
                try:
                    d = datetime.fromisoformat(ds).date()
                except Exception:
                    continue
            res.setdefault(g, []).append({
                "date": d.isoformat(), "time": tm, "subject": subj, "note": note
            })
    for g in res:
        res[g].sort(key=lambda x: (x["date"], x.get("time", "")))
    return res

def format_lessons(lessons: List[Dict[str, Any]]) -> str:
    if not lessons:
        return "Пар нет 🎉"
    out = []
    for i, x in enumerate(lessons, start=1):
        extra = []
        if x.get("teacher"):
            extra.append(x["teacher"])
        if x.get("room"):
            extra.append(f"ауд. {x['room']}")
        tail = f" ({', '.join(extra)})" if extra else ""
        out.append(f"{i}. {x['time']} — {x['subject']}{tail}")
    return "\n".join(out)

def exams_for_range(exams: List[Dict[str, Any]], start: date, end: date) -> List[Dict[str, Any]]:
    out = []
    for x in exams:
        try:
            d = datetime.strptime(x["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if start <= d <= end:
            out.append(x)
    return sorted(out, key=lambda x: (x["date"], x.get("time", "")))

def week_range_str(monday: date) -> str:
    return f"{monday:%d.%m}–{(monday + timedelta(days=6)):%d.%m}"

# -------------------- Keyboards --------------------
def groups_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=g, callback_data=f"pick_group:{g}") for g in GROUPS]
    ])

def main_menu_kb(is_admin_user: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="Сегодня", callback_data="menu:today"),
         InlineKeyboardButton(text="Завтра", callback_data="menu:tomorrow")],
        [InlineKeyboardButton(text="Неделя", callback_data="menu:week"),
         InlineKeyboardButton(text="След. неделя", callback_data="menu:nextweek")],
        [InlineKeyboardButton(text="Контрольные", callback_data="menu:exams")],
        [InlineKeyboardButton(text="Контр. неделя", callback_data="menu:exams_week"),
         InlineKeyboardButton(text="Контр. след. неделя", callback_data="menu:exams_nextweek")],
        [InlineKeyboardButton(text="Сменить группу", callback_data="menu:change_group")],
    ]
    if is_admin_user:
        rows.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin:panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="ℹ️ Инфо", callback_data="admin:info"),
         InlineKeyboardButton(text="🔄 Reload", callback_data="admin:reload")],
        [InlineKeyboardButton(text="📣 Рассылка", callback_data="admin:broadcast")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back")],
    ])

def broadcast_pick_group_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="10 класс", callback_data="broadcast:grp:10"),
         InlineKeyboardButton(text="11 класс", callback_data="broadcast:grp:11")],
        [InlineKeyboardButton(text="10+11", callback_data="broadcast:grp:both")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast:cancel")],
    ])

# -------------------- FSM --------------------
class Form(StatesGroup):
    group = State()

class BroadcastFSM(StatesGroup):
    wait_group = State()
    wait_text = State()

# -------------------- Helpers --------------------
def is_admin(uid: int) -> bool:
    return uid in ADMINS or uid in SUPERADMINS

def is_superadmin(uid: int) -> bool:
    return uid in SUPERADMINS

async def ack(cq: CallbackQuery, text: Optional[str] = None):
    try:
        await cq.answer(text or "", cache_time=0)
    except TelegramBadRequest:
        pass

async def ensure_group(m: Message | CallbackQuery, state: FSMContext) -> Optional[str]:
    data = await state.get_data()
    group = data.get("group")
    if not group:
        if isinstance(m, Message):
            await m.answer("Сначала выберите группу:", reply_markup=groups_kb())
        else:
            await m.message.answer("Сначала выберите группу:", reply_markup=groups_kb())
        return None
    return group

# -------------------- Router / Handlers --------------------
router = Router()

# Subscriptions
@router.message(Command("subscribe"))
async def cmd_subscribe(m: Message, state: FSMContext):
    await m.answer(
        "Выберите группу для подписки на уведомления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=g + " класс", callback_data=f"subs:add:{g}") for g in GROUPS]
        ])
    )

@router.message(Command("unsubscribe"))
async def cmd_unsubscribe(m: Message, state: FSMContext):
    buttons = [[InlineKeyboardButton(text=g + " класс", callback_data=f"subs:del:{g}") for g in GROUPS]]
    buttons.append([InlineKeyboardButton(text="От всего", callback_data="subs:del:all")])
    await m.answer("От какой группы отписаться?", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("subs:add:"))
async def on_subs_add(cq: CallbackQuery):
    await ack(cq)
    group = cq.data.split(":")[2]
    if group not in GROUPS:
        await cq.message.answer("Некорректная группа.")
        return
    ensure_subs_sheet_headers()
    ws = ws_open(GS_SUBS_SHEET)
    rows = ws.get_all_records()
    for r in rows:
        if str(r.get("user_id")) == str(cq.from_user.id) and str(r.get("group")) == group:
            await cq.message.answer(f"Вы уже подписаны на {group} класс.")
            break
    else:
        gs_append_rows(GS_SUBS_SHEET, [[str(cq.from_user.id), group, now_local().isoformat()]])
        await cq.message.answer(f"Готово! Подписка на уведомления группы {group} оформлена.")

@router.callback_query(F.data.startswith("subs:del:"))
async def on_subs_del(cq: CallbackQuery):
    await ack(cq)
    which = cq.data.split(":")[2]
    ensure_subs_sheet_headers()
    ws = ws_open(GS_SUBS_SHEET)
    values = ws.get_all_values()
    to_delete = []
    for idx, row in enumerate(values, start=1):
        if idx == 1:
            continue
        uid = (row + [""])[0]
        grp = (row + ["", ""])[1]
        cond = str(uid) == str(cq.from_user.id) and (which == "all" or grp == which)
        if cond:
            to_delete.append(idx)
    if not to_delete:
        await cq.message.answer("Нечего удалять — вы не подписаны.")
    else:
        gs_clear_rows_by_indices(GS_SUBS_SHEET, to_delete)
        await cq.message.answer("Подписка удалена." if which != "all" else "Все подписки удалены.")

# Start / group
@router.message(CommandStart())
async def start_cmd(m: Message, state: FSMContext):
    await state.clear()
    log.info("User started: id=%s, name=%s", m.from_user.id, m.from_user.full_name)
    await m.answer(
        "Привет! Выберите свой класс:",
        reply_markup=groups_kb()
    )

@router.message(Command("group"))
async def cmd_group(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Выберите группу:", reply_markup=groups_kb())

@router.callback_query(F.data.startswith("pick_group:"))
async def on_pick_group(cq: CallbackQuery, state: FSMContext):
    await ack(cq)
    group = cq.data.split(":")[1]
    if group not in GROUPS:
        await cq.message.answer("Неизвестная группа.")
        return
    await state.update_data(group=group)
    admin_flag = is_admin(cq.from_user.id)
    await cq.message.answer(
        f"Группа установлена: <b>{group}</b>",
        parse_mode="HTML",
        reply_markup=main_menu_kb(admin_flag)
    )

# Schedule commands
@router.message(Command("today"))
async def cmd_today(m: Message, state: FSMContext):
    group = await ensure_group(m, state)
    if not group:
        return
    sched = read_schedule_map()
    dt = now_local()
    wd = WEEKDAYS[dt.weekday()]
    items = sched.get(group, {}).get(wd, [])
    await m.answer(
        f"<b>{WEEKDAYS_RU[wd]} ({dt:%d.%m.%Y})</b>\n{format_lessons(items)}",
        parse_mode="HTML",
        reply_markup=main_menu_kb(is_admin(m.from_user.id))
    )

@router.message(Command("tomorrow"))
async def cmd_tomorrow(m: Message, state: FSMContext):
    group = await ensure_group(m, state)
    if not group:
        return
    sched = read_schedule_map()
    dt = now_local() + timedelta(days=1)
    wd = WEEKDAYS[dt.weekday()]
    items = sched.get(group, {}).get(wd, [])
    await m.answer(
        f"<b>{WEEKDAYS_RU[wd]} ({dt:%d.%m.%Y})</b>\n{format_lessons(items)}",
        parse_mode="HTML",
        reply_markup=main_menu_kb(is_admin(m.from_user.id))
    )

@router.message(Command("week"))
async def cmd_week(m: Message, state: FSMContext):
    group = await ensure_group(m, state)
    if not group:
        return
    sched = read_schedule_map()
    start = monday_of_week(now_local())
    chunks = []
    d = start
    for _ in range(7):
        wd = WEEKDAYS[d.weekday()]
        items = sched.get(group, {}).get(wd, [])
        chunks.append(f"<b>{WEEKDAYS_RU[wd]} ({d:%d.%m})</b>\n{format_lessons(items)}")
        d += timedelta(days=1)
    await m.answer("\n\n".join(chunks), parse_mode="HTML", reply_markup=main_menu_kb(is_admin(m.from_user.id)))

@router.message(Command("nextweek"))
async def cmd_nextweek(m: Message, state: FSMContext):
    group = await ensure_group(m, state)
    if not group:
        return
    sched = read_schedule_map()
    start = monday_of_week(now_local()) + timedelta(days=7)
    chunks = []
    d = start
    for _ in range(7):
        wd = WEEKDAYS[d.weekday()]
        items = sched.get(group, {}).get(wd, [])
        chunks.append(f"<b>{WEEKDAYS_RU[wd]} ({d:%d.%m})</b>\n{format_lessons(items)}")
        d += timedelta(days=1)
    await m.answer("\n\n".join(chunks), parse_mode="HTML", reply_markup=main_menu_kb(is_admin(m.from_user.id)))

@router.message(Command("date"))
async def cmd_date(m: Message, state: FSMContext):
    group = await ensure_group(m, state)
    if not group:
        return
    parts = (m.text or "").split()
    if len(parts) != 2:
        await m.answer("Использование: /date YYYY-MM-DD")
        return
    try:
        dt = datetime.strptime(parts[1], "%Y-%m-%d")
    except ValueError:
        await m.answer("Неверный формат даты (нужно YYYY-MM-DD).")
        return
    sched = read_schedule_map()
    wd = WEEKDAYS[dt.weekday()]
    items = sched.get(group, {}).get(wd, [])
    await m.answer(
        f"<b>{WEEKDAYS_RU[wd]} ({dt:%d.%m.%Y})</b>\n{format_lessons(items)}",
        parse_mode="HTML",
        reply_markup=main_menu_kb(is_admin(m.from_user.id))
    )

# Exams
def format_exams(items: List[Dict[str, Any]], title: str) -> str:
    if not items:
        return title + "\nНет запланированных контрольных."
    parts = [title]
    for i, x in enumerate(items, start=1):
        t = f" — {x['time']}" if x.get("time") else ""
        note = f"\n   ⤷ {x['note']}" if x.get("note") else ""
        parts.append(f"{i}. {x['date']}{t}: {x['subject']}{note}")
    return "\n".join(parts)

@router.message(Command("exams"))
async def cmd_exams(m: Message, state: FSMContext):
    group = await ensure_group(m, state)
    if not group:
        return
    exmap = read_exams_map()
    items = exams_for_range(
        exmap.get(group, []),
        start=now_local().date(),
        end=now_local().date() + timedelta(days=90)
    )
    await m.answer(
        format_exams(items, f"📌 Контрольные (ближайшие), группа {group}"),
        reply_markup=main_menu_kb(is_admin(m.from_user.id))
    )

@router.message(Command("exams_week"))
async def cmd_exams_week(m: Message, state: FSMContext):
    group = await ensure_group(m, state)
    if not group:
        return
    exmap = read_exams_map()
    start = monday_of_week(now_local()).date()
    items = exams_for_range(exmap.get(group, []), start=start, end=start + timedelta(days=6))
    await m.answer(
        format_exams(items, f"📅 Контрольные на неделю ({week_range_str(start)}), группа {group}"),
        reply_markup=main_menu_kb(is_admin(m.from_user.id))
    )

@router.message(Command("exams_nextweek"))
async def cmd_exams_nextweek(m: Message, state: FSMContext):
    group = await ensure_group(m, state)
    if not group:
        return
    exmap = read_exams_map()
    start = (monday_of_week(now_local()) + timedelta(days=7)).date()
    items = exams_for_range(exmap.get(group, []), start=start, end=start + timedelta(days=6))
    await m.answer(
        format_exams(items, f"📅 Контрольные на след. неделю ({week_range_str(start)}), группа {group}"),
        reply_markup=main_menu_kb(is_admin(m.from_user.id))
    )

# Menu callbacks
@router.callback_query(F.data.startswith("menu:"))
async def on_menu(cq: CallbackQuery, state: FSMContext):
    await ack(cq)
    data = await state.get_data()
    if cq.data == "menu:change_group":
        await state.clear()
        await cq.message.answer("Выберите группу:", reply_markup=groups_kb())
        return

    group = data.get("group")
    if not group:
        await cq.message.answer("Сначала выберите группу:", reply_markup=groups_kb())
        return

    action = cq.data.split(":", 1)[1]
    if action in ("today", "tomorrow", "week", "nextweek"):
        sched = read_schedule_map()
        if action == "today":
            dt = now_local()
            wd = WEEKDAYS[dt.weekday()]
            items = sched.get(group, {}).get(wd, [])
            await cq.message.answer(f"<b>{WEEKDAYS_RU[wd]} ({dt:%d.%m.%Y})</b>\n{format_lessons(items)}", parse_mode="HTML", reply_markup=main_menu_kb(is_admin(cq.from_user.id)))
            return
        if action == "tomorrow":
            dt = now_local() + timedelta(days=1)
            wd = WEEKDAYS[dt.weekday()]
            items = sched.get(group, {}).get(wd, [])
            await cq.message.answer(f"<b>{WEEKDAYS_RU[wd]} ({dt:%d.%m.%Y})</b>\n{format_lessons(items)}", parse_mode="HTML", reply_markup=main_menu_kb(is_admin(cq.from_user.id)))
            return
        if action == "week":
            start = monday_of_week(now_local())
            d = start
            chunks = []
            for _ in range(7):
                wd = WEEKDAYS[d.weekday()]
                items = sched.get(group, {}).get(wd, [])
                chunks.append(f"<b>{WEEKDAYS_RU[wd]} ({d:%d.%m})</b>\n{format_lessons(items)}")
                d += timedelta(days=1)
            await cq.message.answer("\n\n".join(chunks), parse_mode="HTML", reply_markup=main_menu_kb(is_admin(cq.from_user.id)))
            return
        if action == "nextweek":
            start = monday_of_week(now_local()) + timedelta(days=7)
            d = start
            chunks = []
            for _ in range(7):
                wd = WEEKDAYS[d.weekday()]
                items = sched.get(group, {}).get(wd, [])
                chunks.append(f"<b>{WEEKDAYS_RU[wd]} ({d:%d.%m})</b>\n{format_lessons(items)}")
                d += timedelta(days=1)
            await cq.message.answer("\n\n".join(chunks), parse_mode="HTML", reply_markup=main_menu_kb(is_admin(cq.from_user.id)))
            return

    if action in ("exams", "exams_week", "exams_nextweek"):
        exmap = read_exams_map()
        ex = exmap.get(group, [])
        if action == "exams":
            items = exams_for_range(ex, start=now_local().date(), end=now_local().date() + timedelta(days=90))
            await cq.message.answer(format_exams(items, f"📌 Контрольные (ближайшие), группа {group}"), reply_markup=main_menu_kb(is_admin(cq.from_user.id)))
        elif action == "exams_week":
            start = monday_of_week(now_local()).date()
            items = exams_for_range(ex, start=start, end=start + timedelta(days=6))
            await cq.message.answer(format_exams(items, f"📅 Контрольные на неделю ({week_range_str(start)}), группа {group}"), reply_markup=main_menu_kb(is_admin(cq.from_user.id)))
        else:
            start = (monday_of_week(now_local()) + timedelta(days=7)).date()
            items = exams_for_range(ex, start=start, end=start + timedelta(days=6))
            await cq.message.answer(format_exams(items, f"📅 Контрольные на след. неделю ({week_range_str(start)}), группа {group}"), reply_markup=main_menu_kb(is_admin(cq.from_user.id)))

# Admin panel and broadcast
@router.message(Command("admin"))
async def cmd_admin(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администраторам.")
        return
    await m.answer("⚙️ Админ-панель", reply_markup=admin_panel_kb())

@router.message(Command("admin_info"))
async def cmd_admin_info(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администраторам.")
        return
    try:
        cnt_sched = len(gs_read_all(GS_SCHEDULE_SHEET))
        cnt_exams = len(gs_read_all(GS_EXAMS_SHEET))
        ensure_subs_sheet_headers()
        subs_ws = ws_open(GS_SUBS_SHEET)
        subs = subs_ws.get_all_records()
        # count by groups
        counts = {g: sum(1 for r in subs if str(r.get("group")) == g) for g in GROUPS}
        parts = "\n".join(f"{g}={counts[g]}" for g in GROUPS)
        await m.answer(
            "ℹ️ Информация:\n"
            f"• TZ: {TZ_NAME}\n"
            f"• Schedule rows: {cnt_sched}\n"
            f"• Exams rows: {cnt_exams}\n"
            f"• Подписчики: {parts}\n"
            f"• Google Sheet ID: {SPREADSHEET_ID}"
        )
    except Exception as e:
        await m.answer(f"Ошибка доступа к Google Sheets: {e!r}")

@router.message(Command("admin_reload"))
async def cmd_admin_reload(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администраторам.")
        return
    try:
        _ = read_schedule_map()
        _ = read_exams_map()
        await m.answer("✅ Данные перечитаны из Google Sheets (готово).")
    except Exception as e:
        await m.answer(f"❌ Ошибка при обновлении: {e!r}")

# Broadcast flow
@router.message(Command("broadcast"))
async def cmd_broadcast(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администраторам.")
        return
    await state.set_state(BroadcastFSM.wait_group)
    await state.update_data(broadcast_group=None, broadcast_text=None)
    await m.answer("Выберите, кому отправить объявление:", reply_markup=broadcast_pick_group_kb())

@router.callback_query(F.data.startswith("broadcast:"))
async def on_broadcast_flow(cq: CallbackQuery, state: FSMContext):
    await ack(cq)
    if not is_admin(cq.from_user.id):
        await cq.message.answer("Только для администраторов.")
        return
    parts = cq.data.split(":")
    action = parts[1]
    if action == "cancel":
        await state.clear()
        await cq.message.answer("Рассылка отменена.")
        return
    if action == "grp":
        grp = parts[2]
        if grp not in tuple(GROUPS) + ("both",):
            await cq.message.answer("Некорректный выбор группы.")
            return
        await state.set_state(BroadcastFSM.wait_text)
        await state.update_data(broadcast_group=grp)
        await cq.message.answer(
            f"Введите текст объявления для: {'+'.join(GROUPS) if grp=='both' else grp}\n\n"
            "Отправьте одним сообщением. Для отмены — /cancel"
        )

@router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    cur = await state.get_state()
    if cur in (BroadcastFSM.wait_group, BroadcastFSM.wait_text):
        await state.clear()
        await m.answer("Операция отменена.")

@router.message(BroadcastFSM.wait_text)
async def on_broadcast_text(m: Message, state: FSMContext, bot: Bot):
    if not is_admin(m.from_user.id):
        await state.clear()
        await m.answer("Только для администраторов.")
        return
    data = await state.get_data()
    grp = data.get("broadcast_group")
    text = (m.text or "").strip()
    if not text:
        await m.answer("Пустое сообщение. Введите текст или /cancel.")
        return
    try:
        ensure_subs_sheet_headers()
        ws = ws_open(GS_SUBS_SHEET)
        subs = ws.get_all_records()
        if grp in GROUPS:
            targets = [int(r["user_id"]) for r in subs if str(r.get("group")) == grp]
        else:
            targets = [int(r["user_id"]) for r in subs if str(r.get("group")) in GROUPS]
        targets = sorted(set(targets))
    except Exception as e:
        await state.clear()
        await m.answer(f"❌ Ошибка чтения подписчиков: {e!r}")
        return
    if not targets:
        await state.clear()
        await m.answer("Подписчиков не найдено для выбранной группы.")
        return
    ok, fail = 0, 0
    for uid in targets:
        try:
            await bot.send_message(uid, f"📣 Объявление для группы {grp if grp!='both' else '+'.join(GROUPS)}:\n\n{text}")
            ok += 1
        except Exception as e:
            log.warning("Не удалось отправить %s: %r", uid, e)
            fail += 1
            await asyncio.sleep(0.05)
    await state.clear()
    await m.answer(f"Готово. Разослано: {ok}, ошибок: {fail}.")

@router.callback_query(F.data.startswith("admin:"))
async def on_admin_panel(cq: CallbackQuery, state: FSMContext):
    await ack(cq)
    if not is_admin(cq.from_user.id):
        await cq.message.answer("Только для администраторов.")
        return
    action = cq.data.split(":")[1]
    if action == "panel":
        await cq.message.answer("⚙️ Админ-панель", reply_markup=admin_panel_kb())
    elif action == "info":
        await cmd_admin_info(cq.message)
    elif action == "reload":
        await cmd_admin_reload(cq.message)
    elif action == "back":
        data = await state.get_data()
        isadm = is_admin(cq.from_user.id)
        await cq.message.answer("Главное меню", reply_markup=main_menu_kb(isadm))
    elif action == "broadcast":
        await cmd_broadcast(cq.message, state)

# -------------------- Auto-notify about changes --------------------
def hash_group_data_for_changes(sched_map: Dict[str, Dict[str, List[Dict[str, Any]]]],
                                exams_map: Dict[str, List[Dict[str, Any]]],
                                group: str) -> str:
    payload = {
        "schedule": sched_map.get(group, {}),
        "exams": exams_map.get(group, []),
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

async def notify_subscribers(bot: Bot, group: str, text: str):
    try:
        ensure_subs_sheet_headers()
        ws = ws_open(GS_SUBS_SHEET)
        subs = ws.get_all_records()
        targets = [int(r["user_id"]) for r in subs if str(r.get("group")) == group]
        if not targets:
            return
        for uid in sorted(set(targets)):
            try:
                await bot.send_message(uid, text)
            except Exception as e:
                log.warning("Notify %s failed: %r", uid, e)
                await asyncio.sleep(0.05)
    except Exception as e:
        log.error("notify_subscribers error: %r", e)

async def watch_changes(bot: Bot, stop_event: asyncio.Event):
    # store last hashes by group
    last_hash: Dict[str, str] = {}
    # init
    try:
        sched = read_schedule_map()
        exams = read_exams_map()
        for g in GROUPS:
            last_hash[g] = hash_group_data_for_changes(sched, exams, g)
    except Exception as e:
        log.warning("Initial read of Google Sheets failed: %r", e)

    log.info("Watchdog started, interval=%ss", WATCH_INTERVAL)

    while not stop_event.is_set():
        try:
            await asyncio.sleep(WATCH_INTERVAL)
            sched = read_schedule_map()
            exams = read_exams_map()
            for g in GROUPS:
                cur = hash_group_data_for_changes(sched, exams, g)
                if last_hash.get(g) and last_hash[g] != cur:
                    last_hash[g] = cur
                    msg = (
                        f"🔔 Обновления в расписании для группы {g}\n"
                        f"• Время: {now_local():%d.%m.%Y %H:%M}\n"
                        f"Откройте меню бота: /today /week /exams"
                    )
                    await notify_subscribers(bot, g, msg)
                elif g not in last_hash:
                    last_hash[g] = cur
        except Exception as e:
            log.error("watch_changes loop error: %r", e)

# -------------------- Commands & Main --------------------
GENERAL_CMDS = [
    BotCommand(command="start", description="Запуск"),
    BotCommand(command="group", description="Сменить группу"),
    BotCommand(command="today", description="Расписание на сегодня"),
    BotCommand(command="tomorrow", description="Расписание на завтра"),
    BotCommand(command="week", description="Расписание на неделю"),
    BotCommand(command="nextweek", description="Расписание на следующую неделю"),
    BotCommand(command="date", description="Расписание на дату YYYY-MM-DD"),
    BotCommand(command="exams", description="Контрольные (ближайшие)"),
    BotCommand(command="exams_week", description="Контрольные этой недели"),
    BotCommand(command="exams_nextweek", description="Контрольные следующей недели"),
    BotCommand(command="subscribe", description="Подписка на уведомления"),
    BotCommand(command="unsubscribe", description="Отписка от уведомлений"),
    BotCommand(command="admin", description="Админ-панель"),
    BotCommand(command="admin_info", description="Админ: информация"),
    BotCommand(command="admin_reload", description="Админ: перезагрузка"),
    BotCommand(command="broadcast", description="Админ: рассылка"),
]

async def main():
    # validations
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable required (set in .env).")
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID environment variable required (set in .env).")
    # warn if no credentials; errors will occur on sheet access
    if not (GOOGLE_CREDS_JSON_PATH or GOOGLE_CREDS_JSON_CONTENT):
        log.warning("Google Sheets credentials not provided (GOOGLE_CREDS_JSON_PATH or GOOGLE_CREDS_JSON_CONTENT).")

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.set_my_commands(GENERAL_CMDS, scope=BotCommandScopeDefault())
    await bot.delete_webhook(drop_pending_updates=True)

    # ensure subs sheet header exists
    try:
        ensure_subs_sheet_headers()
    except Exception as e:
        log.warning("Could not verify/create subs sheet headers: %r", e)

    # start watcher task
    stop_event = asyncio.Event()
    watcher_task = asyncio.create_task(watch_changes(bot, stop_event))

    log.info("Bot started. TZ=%s | GSheet=%s [schedule=%s, exams=%s, subs=%s]",
             TZ_NAME, SPREADSHEET_ID, GS_SCHEDULE_SHEET, GS_EXAMS_SHEET, GS_SUBS_SHEET)

    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"], skip_updates=True)
    finally:
        stop_event.set()
        with contextlib.suppress(Exception):
            await watcher_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Stopped.")


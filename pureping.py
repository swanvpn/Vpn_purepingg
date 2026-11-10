import asyncio
import html
import io
import json
import logging
import hashlib
import hmac
import ipaddress
import os
import random
import re
import secrets
import socket
import time
import traceback
import weakref
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote, urljoin, urlsplit

import jdatetime
from aiohttp import ClientSession, ClientTimeout, web

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Integer,
    String,
    Text,
    create_engine,
    func,
    inspect,
    select,
    or_,
    text,
)
from sqlalchemy import event as sa_event
from sqlalchemy.orm import declarative_base, sessionmaker

from aiogram import BaseMiddleware, Bot, Dispatcher, F, types
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import BaseFilter, Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReactionTypeEmoji,
    ReplyKeyboardMarkup,
)

# ---------------------------------------------------------------------------
# تنظیمات اولیه
# هیچ اطلاعات حساسی داخل کد نیست؛ همه از متغیرهای محیطی (Environment) خوانده می‌شوند:
#   BOT_TOKEN, ADMIN_ID            -> الزامی
#   CARD_NUMBER, CARD_HOLDER       -> الزامی فقط برای اولین اجرا (بعدش از پنل ادمین قابل تغییر است)
#   DATABASE_URL                   -> آدرس دیتابیس Postgres (اگر نباشد SQLite محلی)
#   SUPPORT_INFO                   -> اختیاری
# ---------------------------------------------------------------------------


def _require_env(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(
            f"Missing required environment variable: {name}. "
            "Set it in your hosting dashboard (Render -> Environment) and redeploy."
        )
    return value


TOKEN = _require_env("BOT_TOKEN")
try:
    ADMIN_ID = int(_require_env("ADMIN_ID"))
except ValueError:
    raise SystemExit("ADMIN_ID must be a numeric Telegram user id.")

database_url = os.environ.get("DATABASE_URL")
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

if database_url:
    engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=240, pool_size=5, max_overflow=5)
else:
    engine = create_engine("sqlite:///bot_data.db")

Base = declarative_base()
SessionLocal = sessionmaker(bind=engine)


# --- ابزار تشخیص کندی: کوئری‌های کند در لاگ ثبت می‌شوند ---
SLOW_QUERY_MS = 150


@sa_event.listens_for(engine, "before_cursor_execute")
def _query_start(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault("_qstart", []).append(time.perf_counter())


@sa_event.listens_for(engine, "after_cursor_execute")
def _query_end(conn, cursor, statement, parameters, context, executemany):
    starts = conn.info.get("_qstart")
    if starts:
        ms = (time.perf_counter() - starts.pop()) * 1000
        if ms > SLOW_QUERY_MS:
            logging.warning("SLOW QUERY %.0fms: %s", ms, " ".join(statement.split())[:140])


def measure_db_latency(rounds=5):
    """میانگین زمان رفت‌وبرگشت به دیتابیس (میلی‌ثانیه)."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # اتصال اولیه
        t0 = time.perf_counter()
        for _ in range(rounds):
            conn.execute(text("SELECT 1"))
        return (time.perf_counter() - t0) * 1000 / rounds

dp = Dispatcher(storage=MemoryStorage())

esc = html.escape


def err_text(exc, limit=300):
    """متن کوتاه‌شدهٔ خطا برای ارسال در تلگرام (حد پیام ۴۰۹۶ کاراکتر است؛ خطای SQL می‌تواند بسیار بلند باشد)."""
    text_ = str(exc)
    if len(text_) > limit:
        text_ = text_[:limit] + "…"
    return html.escape(text_)
ARROW = "⬅️"  # جهت فلش: از قیمت اصلی به قیمت تخفیف‌خورده (متن راست‌به‌چپ)

# تسک‌های پس‌زمینه باید ارجاع قوی داشته باشند، وگرنه ممکن است وسط اجرا توسط GC حذف شوند.
_bg_tasks = set()


def spawn(coro):
    """اجرای امن تسک پس‌زمینه (fire-and-forget) با نگه‌داشتن ارجاع."""
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task


# قفل سبک برای هر کاربر؛ جلوی ثبت دوباره‌ی سفارش با کلیک/ارسال همزمان را می‌گیرد.
_user_locks = weakref.WeakValueDictionary()


def user_lock(user_id):
    lock = _user_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _user_locks[user_id] = lock
    return lock


BTN_BUY = "🛒 خرید سرویس"
BTN_MY = "📦 سرویس‌های من"
BTN_TARIFFS = "📈 تعرفه سرویس‌ها"
BTN_WALLET = "💳 شارژ کیف پول"
BTN_ACCOUNT = "👤 حساب کاربری"
BTN_SUPPORT = "🎧 پشتیبانی"
MENU_TEXTS = {BTN_BUY, BTN_MY, BTN_TARIFFS, BTN_WALLET, BTN_ACCOUNT, BTN_SUPPORT}

CATEGORY_LABELS = {"economic": "اقتصادی", "vip": "VIP", "gaming": "گیمینگ"}
MAX_DISCOUNT = 90
MAX_BALANCE = 1_000_000_000  # ستون Integer در Postgres ۳۲ بیتی است؛ سقف امن


# ---------------------------------------------------------------------------
# مدل‌های دیتابیس
# ---------------------------------------------------------------------------
class User(Base):
    __tablename__ = "users"
    user_id = Column(BigInteger, primary_key=True, autoincrement=False)
    username = Column(String)
    first_name = Column(String)
    balance = Column(Integer, default=0)
    is_banned = Column(Boolean, default=False)
    is_new_user = Column(Boolean, default=True)
    purchase_count_current_month = Column(Integer, default=0)
    last_purchase_month = Column(String, default="None")
    has_monthly_discount = Column(Boolean, default=False)
    note = Column(Text)
    referred_by = Column(BigInteger)
    referral_rewarded = Column(Boolean, default=False)
    joined_at = Column(String)
    trial_used = Column(Boolean, default=False)
    points = Column(Integer, default=0)  # امتیاز باشگاه مشتریان
    src = Column(String)  # منبع جذب (/start src_xxx)
    promo_off = Column(Boolean, default=False)
    blocked = Column(Boolean, default=False)


class Service(Base):
    __tablename__ = "services"
    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String)
    name = Column(String)
    price = Column(Integer)


class UserConfig(Base):
    __tablename__ = "user_configs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    config_name = Column(String)
    config_details = Column(String)
    service_id = Column(Integer)
    expires_at = Column(String)  # YYYY-MM-DD (میلادی)
    reminded = Column(Boolean, default=False)
    remind_stage = Column(Integer, default=0)


class Request(Base):
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    req_type = Column(String)  # buy | renew
    plan_name = Column(String)
    status = Column(String, default="unanswered")
    created_at = Column(String)
    answered_at = Column(String)
    receipt_file_id = Column(String)
    price = Column(Integer, default=0)
    paid_wallet = Column(Boolean, default=False)
    config_id = Column(Integer)
    service_id = Column(Integer)
    discount_code = Column(String)
    created_iso = Column(String)  # YYYY-MM-DDTHH:MM به وقت ایران (برای یادآوری ادمین)
    last_remind_iso = Column(String)
    remind_count = Column(Integer, default=0)
    custom_days = Column(Integer)  # مدت پلن دلخواه (روز)
    pay_amount = Column(Integer)  # مبلغ دقیق واریزی (قیمت ± عدد تصادفی)


class KV(Base):
    __tablename__ = "kv_settings"
    key = Column(String, primary_key=True)
    value = Column(Text)


class DiscountCode(Base):
    __tablename__ = "discount_codes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True)
    percent = Column(Integer, default=0)
    expires_at = Column(String)  # YYYY-MM-DD یا خالی
    max_uses = Column(Integer, default=0)  # 0 = نامحدود
    used_count = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)


class CodeUse(Base):
    __tablename__ = "code_uses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code_id = Column(Integer)
    user_id = Column(BigInteger)


class Ticket(Base):
    __tablename__ = "tickets"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    status = Column(String, default="open")
    created_at = Column(String)


class TicketMessage(Base):
    __tablename__ = "ticket_messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticket_id = Column(Integer)
    sender = Column(String)  # user | staff
    text = Column(Text)
    photo_id = Column(String)
    created_at = Column(String)


class AdminLog(Base):
    __tablename__ = "admin_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(BigInteger)
    action = Column(Text)
    created_at = Column(String)


class Rating(Base):
    __tablename__ = "ratings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    request_id = Column(Integer)
    score = Column(Integer)
    created_at = Column(String)


class ReceiptHash(Base):
    __tablename__ = "receipt_hashes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    sha256 = Column(String)
    unique_id = Column(String)
    user_id = Column(BigInteger)
    request_id = Column(Integer)
    created_at = Column(String)


class ScheduledBroadcast(Base):
    __tablename__ = "scheduled_broadcasts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    segment = Column(String, default="all")
    chat_id = Column(BigInteger)  # چتی که پیام اصلی در آن است (ادمین)
    message_id = Column(BigInteger)
    run_at = Column(String)  # YYYY-MM-DDTHH:MM به وقت ایران
    status = Column(String, default="pending")  # pending | sending | sent | cancelled
    created_by = Column(BigInteger)
    created_at = Column(String)
    result = Column(String)


class Category(Base):
    """دسته‌بندی پلن‌ها؛ key ثابت می‌ماند و فقط برچسب/ایموجی/ترتیب عوض می‌شود."""

    __tablename__ = "categories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, unique=True)
    label = Column(String)
    emoji = Column(String, default="")
    sort_order = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)


class CustomButton(Base):
    """دکمه‌های شیشه‌ای دلخواه که ادمین از «دکمه‌ساز» می‌سازد (درختی و چندمرحله‌ای).

    parent_id = 0 یعنی دکمه زیر منوی اصلی؛ در غیر این صورت دکمه‌ای در «مرحله بعدِ» دکمه‌ی والد.
    kind: msg = نمایش پیام (+عکس) و دکمه‌های مرحله بعد | url = باز کردن لینک
    """

    __tablename__ = "custom_buttons"
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, default=0)
    kind = Column(String, default="msg")
    label = Column(String)
    text = Column(Text)
    photo_id = Column(String)
    url = Column(String)
    sort_order = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)


DEFAULT_CATEGORIES = [
    ("economic", "اقتصادی", "🌱"),
    ("vip", "VIP", "💎"),
    ("gaming", "گیمینگ", "🎮"),
]

DEFAULT_PLANS = [
    ("economic", "100 گیگ - ۲ کاربر", 298000),
    ("economic", "100 گیگ - نامحدود", 398000),
    ("vip", "10 گیگ", 190000),
    ("vip", "20 گیگ", 320000),
    ("vip", "30 گیگ", 490000),
    ("vip", "50 گیگ", 790000),
    ("vip", "100 گیگ", 1330000),
    ("gaming", "10 گیگ", 220000),
    ("gaming", "20 گیگ", 360000),
    ("gaming", "30 گیگ", 530000),
    ("gaming", "50 گیگ", 850000),
]

DEFAULT_FAQ = (
    "❓ <b>سوالات متداول</b>\n\n"
    "🔹 <b>چطور وصل شوم؟</b>\nساب‌لینک را در برنامه V2Box اضافه کنید (آموزش داخل کانال).\n\n"
    "🔹 <b>سرویس‌ها چند روزه‌اند؟</b>\nهر سرویس ۳۰ روزه است و قبل از پایان یادآوری می‌گیرید.\n\n"
    "🔹 <b>چطور تمدید کنم؟</b>\nاز بخش «سرویس‌های من» روی سرویس بزنید و دکمه تمدید را انتخاب کنید."
)

# ---------------------------------------------------------------------------
# تنظیمات قابل تغییر از پنل (جدول key-value)
# ---------------------------------------------------------------------------
DEFAULTS = {
    "card_number": "",
    "card_holder": "",
    "support_info": "",
    "discount_percent": "0",
    "first_on": "1",
    "first_percent": "10",
    "loyalty_on": "1",
    "loyalty_bronze_percent": "5",
    "loyalty_silver_percent": "10",
    "loyalty_gold_percent": "15",
    "rating_on": "1",
    "rating_low_max": "2",
    "monitor_on": "0",
    "monitor_urls": "",
    "monitor_fails": "3",
    "monitor_notify_users": "1",
    "outage_text": "⚠️ در حال حاضر یکی از سرورهای ما مشکل دارد و تیم فنی در حال رفع آن است. به‌محض برطرف شدن اطلاع می‌دهیم. از صبر شما سپاسگزاریم 🙏",
    "recovery_text": "✅ مشکل سرور برطرف شد و سرویس‌ها به حالت عادی برگشتند. ممنون از صبوری شما 🙏",
    "post_on": "0",
    "post_channel": "",
    "post_times": "10:00,20:00",
    "post_title": "",
    "maintenance_on": "0",
    "maintenance_text": "🔧 ربات در حال بروزرسانی است. لطفاً کمی بعد دوباره تلاش کنید.",
    "welcome_text": "",
    "welcome_banner": "",
    "channel_link": "",
    "sticker_welcome": "",
    "sticker_purchase": "",
    "sticker_charge": "",
    "sticker_config": "",
    "charge_amounts": "200000,400000,600000,800000,1000000,1500000",
    "min_charge": "50000",
    "referral_on": "1",
    "referral_reward": "20000",
    "reminder_days": "3",
    "report_on": "1",
    "backup_on": "1",
    "faq_text": DEFAULT_FAQ,
    "sub_admins": "",
    "post_photo": "",
    "pending_text": "سفارشت ثبت شد ✅ تا ۳۰ دقیقه دیگه آماده می‌شه، خبرت می‌کنم. وضعیتش رو هم می‌تونی با دکمه زیر ببینی.",
    "connect_guide": "📲 آموزش اتصال\n\n1️⃣ برنامه را نصب کنید: iOS ← V2Box | اندروید ← v2rayNG | ویندوز ← v2rayN\n2️⃣ ساب‌لینک سرویس را کپی کنید.\n3️⃣ داخل برنامه گزینه افزودن از کلیپ‌بورد (Subscription) را بزنید.\n4️⃣ سرور را انتخاب کنید و وصل شوید.\n\nآموزش تصویری کامل داخل کانال هست.",
    "config_extra_text": "",
    "qr_on": "1",
    "admin_remind_on": "1",
    "admin_remind_minutes": "30",
    "trial_on": "0",
    "trial_days": "1",
    "custom_on": "1",
    "custom_price_per_gb": "22000",
    "custom_base_days": "30",
    "custom_min_gb": "1",
    "custom_max_gb": "1000",
    "custom_min_days": "1",
    "custom_max_days": "365",
    # --- مبلغ تصادفی پرداخت (کارت‌به‌کارت) ---
    "payrand_on": "1",
    "payrand_min": "2000",
    "payrand_max": "3999",
    "payrand_down": "1",
    # --- باشگاه مشتریان و پاداش شارژ ---
    "club_on": "1",
    "club_earn_per": "10000",
    "club_point_value": "500",
    "club_min_redeem": "20",
    "bonus_on": "0",
    "bonus_tiers": "500000:5,1000000:8,2000000:10",
    # --- دعوت پله‌ای ---
    "ref_tiers_on": "0",
    "ref_tiers": "3:30000,10:150000,25:500000",
    # --- عضویت اجباری کانال ---
    "force_join_on": "0",
    "force_join_channel": "",
    "force_join_text": "🔒 برای استفاده از ربات ابتدا در کانال ما عضو شوید، سپس دکمه «✅ عضو شدم» را بزنید.",
    # --- بکاپ، گزارش، نگهداری، خطا ---
    "backup_channel": "",
    "weekly_report_on": "1",
    "cleanup_on": "1",
    "log_keep_days": "90",
    "err_report_on": "1",
    # --- امنیت ---
    "twostep_on": "1",
    # --- فروشگاه: پرفروش و راهنمای انتخاب ---
    "bestseller_on": "1",
    "bestseller_min": "3",
    "bestseller_label": "⭐ پرفروش",
    "pick_on": "1",
    # --- مصرف از ساب‌لینک ---
    "usage_on": "1",
    "usage_alerts_on": "1",
    "usage_warn_percent": "80",
    "usage_sync_expiry_on": "1",
    "usage_admin_report_on": "1",
    "testlink_on": "1",
    # --- آموزش اتصال به تفکیک دستگاه ---
    "guide_android": "📱 آموزش اتصال در اندروید\n\n1️⃣ برنامه v2rayNG را نصب کنید.\n2️⃣ ساب‌لینک سرویس را کپی کنید.\n3️⃣ داخل برنامه روی علامت + بزنید و گزینه افزودن از کلیپ‌بورد/اشتراک (Subscription) را انتخاب کنید.\n4️⃣ سرور را انتخاب و دکمه اتصال را بزنید.",
    "guide_ios": "🍎 آموزش اتصال در آیفون (iOS)\n\n1️⃣ برنامه V2Box یا Streisand را از App Store نصب کنید.\n2️⃣ ساب‌لینک سرویس را کپی کنید.\n3️⃣ داخل برنامه وارد بخش Configs شوید و گزینه افزودن اشتراک (Subscription) را بزنید.\n4️⃣ سرور را انتخاب و وصل شوید.",
    "guide_windows": "💻 آموزش اتصال در ویندوز\n\n1️⃣ برنامه v2rayN را نصب کنید.\n2️⃣ ساب‌لینک سرویس را کپی کنید.\n3️⃣ از منوی Subscription گزینه Add subscription را بزنید و لینک را وارد کنید.\n4️⃣ سرور را انتخاب و حالت System Proxy را فعال کنید.",
    "guide_mac": "🖥 آموزش اتصال در مک\n\n1️⃣ برنامه V2Box یا FoXray را از App Store نصب کنید.\n2️⃣ ساب‌لینک سرویس را کپی کنید.\n3️⃣ داخل برنامه اشتراک (Subscription) جدید اضافه کنید و لینک را وارد کنید.\n4️⃣ سرور را انتخاب و وصل شوید.",
    # --- متن‌های قابل ویرایش پیام‌های ربات ---
    "t_wallet_paid": "✅ پرداخت از کیف پولت انجام شد. ممنون 🙏",
    "t_receipt_ok": "✅ رسیدت تاریخ {date} ثبت شد. ممنون از خریدت! 🙏",
    "t_charge_receipt_ok": "🙏 رسیدت رسید دستمون! بعد از بررسی ادمین، کیف پولت شارژ می‌شه.",
    "t_charge_ok": "🎉 تبریک!\n\nمبلغ {amount} تومان با موفقیت به کیف پولت اضافه شد.",
    "t_renew_ok": "✅ سرویس {plan} با موفقیت تمدید شد.\n📅 تاریخ پایان جدید: {end}",
    "t_rejected": "⚠️ متأسفانه پرداخت/درخواستت توسط ادمین تایید نشد.",
    "t_config_ready": "🎉 سفارشت تایید و آماده شد!\n\n📦 سرویس: {plan}\n📅 تاریخ شروع: {start}\n📅 تاریخ پایان: {end} ({days} روزه)\n\n📋 جزئیات/کانفیگ:\n\n{config}\n\nلطفاً ساب‌لینکت رو طبق آموزش داخل کانال وارد برنامه V2Box کن.\nمرسی که با ما هستی 🙏",
}

# متادیتای تنظیماتی که از منوی «تنظیمات ربات» ویرایش می‌شوند
SETTINGS = {
    "discount_percent": dict(page="disc", type="num", label="تخفیف عمومی (٪)", min=0, max=MAX_DISCOUNT),
    "first_on": dict(page="disc", type="toggle", label="تخفیف خرید اول"),
    "first_percent": dict(page="disc", type="num", label="درصد تخفیف خرید اول", min=0, max=50),
    "loyalty_on": dict(page="disc", type="toggle", label="تخفیف وفاداری"),
    "loyalty_bronze_percent": dict(page="disc", type="num", label="🥉 برنزی (۲ خرید در ماه) ٪", min=0, max=50),
    "loyalty_silver_percent": dict(page="disc", type="num", label="🥈 نقره‌ای (۴ خرید در ماه) ٪", min=0, max=50),
    "loyalty_gold_percent": dict(page="disc", type="num", label="🥇 طلایی (۶ خرید در ماه) ٪", min=0, max=50),
    "rating_on": dict(page="auto", type="toggle", label="نظرسنجی بعد از تحویل"),
    "rating_low_max": dict(page="auto", type="num", label="ارسال به ادمین اگر امتیاز ≤", min=1, max=4),
    "monitor_on": dict(page="mon", type="toggle", label="مانیتور خودکار سرور"),
    "monitor_urls": dict(page="mon", type="text", label="آدرس‌های مانیتور (http/https، با کاما جدا کنید)"),
    "monitor_fails": dict(page="mon", type="num", label="تعداد خطای پشت‌سرهم تا اعلان قطعی", min=1, max=20),
    "monitor_notify_users": dict(page="mon", type="toggle", label="اعلان قطعی/وصلی برای همه کاربران"),
    "outage_text": dict(page="mon", type="text", label="متن اعلان قطعی"),
    "recovery_text": dict(page="mon", type="text", label="متن اعلان وصل شدن"),
    "post_on": dict(page="chpost", type="toggle", label="پست خودکار تعرفه در کانال"),
    "post_channel": dict(page="chpost", type="text", label="کانال (@username یا آیدی عددی -100...)"),
    "post_times": dict(page="chpost", type="text", label="ساعت‌های پست (مثلاً 10:00,20:00)"),
    "post_title": dict(page="chpost", type="text", label="عنوان پست (اختیاری)"),
    "post_photo": dict(page="chpost", type="photo", label="عکس پست کانال (اختیاری)"),
    "card_number": dict(page="pay", type="text", label="شماره کارت"),
    "card_holder": dict(page="pay", type="text", label="نام صاحب کارت"),
    "charge_amounts": dict(page="pay", type="text", label="مبالغ شارژ (با کاما جدا کنید)"),
    "min_charge": dict(page="pay", type="num", label="حداقل مبلغ شارژ (تومان)", min=0, max=100000000),
    "maintenance_on": dict(page="maint", type="toggle", label="حالت تعمیر"),
    "maintenance_text": dict(page="maint", type="text", label="پیام حالت تعمیر"),
    "welcome_text": dict(page="start", type="text", label="متن خوش‌آمدگویی ({name} = اسم کاربر)"),
    "welcome_banner": dict(page="start", type="photo", label="بنر پیام شروع"),
    "channel_link": dict(page="start", type="text", label="لینک کانال"),
    "sticker_welcome": dict(page="start", type="sticker", label="استیکر خوش‌آمدگویی"),
    "sticker_purchase": dict(page="start", type="sticker", label="استیکر ثبت سفارش"),
    "sticker_charge": dict(page="start", type="sticker", label="استیکر شارژ موفق"),
    "sticker_config": dict(page="start", type="sticker", label="استیکر تحویل کانفیگ"),
    "support_info": dict(page="support", type="text", label="اطلاعات پشتیبانی (آیدی ادمین)"),
    "faq_text": dict(page="support", type="text", label="متن سوالات متداول"),
    "referral_on": dict(page="ref", type="toggle", label="سیستم دعوت دوستان"),
    "referral_reward": dict(page="ref", type="num", label="پاداش دعوت (تومان)", min=0, max=100000000),
    "reminder_days": dict(page="auto", type="num", label="یادآوری تمدید (روز مانده)", min=1, max=30),
    "report_on": dict(page="auto", type="toggle", label="گزارش روزانه برای ادمین"),
    "backup_on": dict(page="auto", type="toggle", label="بکاپ شبانه خودکار"),
    "pending_text": dict(page="deliv", type="text", label="پیام «در حال آماده‌سازی» بعد از ثبت سفارش (زمان تخمینی)"),
    "connect_guide": dict(page="deliv", type="text", label="متن آموزش اتصال"),
    "config_extra_text": dict(page="deliv", type="text", label="متن ثابت زیر کانفیگ (اختیاری)"),
    "qr_on": dict(page="deliv", type="toggle", label="ارسال QR همراه کانفیگ"),
    "admin_remind_on": dict(page="deliv", type="toggle", label="یادآوری سفارش معطل‌مانده به ادمین"),
    "admin_remind_minutes": dict(page="deliv", type="num", label="یادآوری بعد از (دقیقه)", min=5, max=1440),
    "trial_on": dict(page="deliv", type="toggle", label="سرویس تست رایگان"),
    "trial_days": dict(page="deliv", type="num", label="مدت سرویس تست (روز)", min=1, max=30),
    "custom_on": dict(page="custom", type="toggle", label="پلن دلخواه (حجم و روز)"),
    "custom_price_per_gb": dict(page="custom", type="num", label="قیمت هر گیگ (تومان)", min=0, max=100000000),
    "custom_base_days": dict(page="custom", type="num", label="قیمت گیگ برای چند روز است؟ (۰ = مستقل از روز)", min=0, max=365),
    "custom_min_gb": dict(page="custom", type="num", label="حداقل حجم (گیگ)", min=1, max=10000),
    "custom_max_gb": dict(page="custom", type="num", label="حداکثر حجم (گیگ)", min=1, max=10000),
    "custom_min_days": dict(page="custom", type="num", label="حداقل روز", min=1, max=3650),
    "custom_max_days": dict(page="custom", type="num", label="حداکثر روز", min=1, max=3650),
    # --- مبلغ تصادفی پرداخت ---
    "payrand_on": dict(page="pay", type="toggle", label="مبلغ تصادفی انتهای پرداخت کارت‌به‌کارت"),
    "payrand_min": dict(page="pay", type="num", label="حداقل عدد تصادفی (تومان)", min=1, max=100000),
    "payrand_max": dict(page="pay", type="num", label="حداکثر عدد تصادفی (تومان؛ ۰ = خاموش)", min=0, max=100000),
    "payrand_down": dict(page="pay", type="toggle", label="اجازه‌ی مبلغ کمتر از قیمت (بالا و پایین)"),
    # --- باشگاه و پاداش شارژ ---
    "club_on": dict(page="club", type="toggle", label="باشگاه مشتریان (امتیاز)"),
    "club_earn_per": dict(page="club", type="num", label="هر چند تومان خرید = ۱ امتیاز", min=1000, max=100000000),
    "club_point_value": dict(page="club", type="num", label="ارزش هر امتیاز (تومان اعتبار)", min=0, max=1000000),
    "club_min_redeem": dict(page="club", type="num", label="حداقل امتیاز برای تبدیل به اعتبار", min=1, max=1000000),
    "bonus_on": dict(page="club", type="toggle", label="پاداش شارژ کیف پول"),
    "bonus_tiers": dict(page="club", type="text", label="پله‌های پاداش شارژ (مبلغ:درصد، مثل 500000:5,1000000:8)"),
    # --- دعوت پله‌ای ---
    "ref_tiers_on": dict(page="ref", type="toggle", label="پاداش پله‌ای دعوت"),
    "ref_tiers": dict(page="ref", type="text", label="پله‌ها (تعداد دعوت:پاداش تومان، مثل 3:30000,10:150000)"),
    # --- عضویت اجباری ---
    "force_join_on": dict(page="join", type="toggle", label="عضویت اجباری در کانال"),
    "force_join_channel": dict(page="join", type="text", label="کانال (@username یا آیدی عددی -100...؛ ربات باید ادمین کانال باشد)"),
    "force_join_text": dict(page="join", type="text", label="متن درخواست عضویت"),
    # --- بکاپ، گزارش، نگهداری، خطا ---
    "backup_channel": dict(page="auto", type="text", label="کانال خصوصی بکاپ (@username یا -100...؛ ربات باید ادمین باشد)"),
    "weekly_report_on": dict(page="auto", type="toggle", label="گزارش هفتگی خودکار (شنبه‌ها)"),
    "cleanup_on": dict(page="auto", type="toggle", label="پاکسازی خودکار آمار و لاگ قدیمی"),
    "log_keep_days": dict(page="auto", type="num", label="نگهداری لاگ و تیکت بسته (روز)", min=7, max=3650),
    "err_report_on": dict(page="auto", type="toggle", label="ارسال خطای ربات به تلگرام ادمین"),
    # --- امنیت ---
    "twostep_on": dict(page="sec", type="toggle", label="تأیید دومرحله‌ای کارهای حساس (بازیابی بکاپ، حذف ادمین)"),
    # --- فروشگاه ---
    "bestseller_on": dict(page="shop", type="toggle", label="برچسب روی پرفروش‌ترین پلن"),
    "bestseller_label": dict(page="shop", type="text", label="متن برچسب (مثلاً ⭐ پرفروش یا 👌 پیشنهاد ما)"),
    "bestseller_min": dict(page="shop", type="num", label="حداقل فروش برای نمایش برچسب", min=1, max=1000),
    "pick_on": dict(page="shop", type="toggle", label="دکمه «نمی‌دونم کدوم رو بخرم؟»"),
    # --- مصرف از ساب‌لینک ---
    "usage_on": dict(page="usage", type="toggle", label="نمایش مصرف از ساب‌لینک"),
    "usage_alerts_on": dict(page="usage", type="toggle", label="هشدار خودکار حجم برای کاربر"),
    "usage_warn_percent": dict(page="usage", type="num", label="هشدار وقتی مصرف رسید به (٪)", min=50, max=99),
    "usage_sync_expiry_on": dict(page="usage", type="toggle", label="همگام‌سازی تاریخ انقضا با پنل"),
    "usage_admin_report_on": dict(page="usage", type="toggle", label="گزارش لینک خراب/تموم‌شده به ادمین"),
    "testlink_on": dict(page="usage", type="toggle", label="تست لینک هنگام تحویل سرویس (ادمین تصمیم می‌گیرد)"),
    # --- آموزش اتصال ---
    "guide_android": dict(page="guide", type="text", label="آموزش اندروید"),
    "guide_ios": dict(page="guide", type="text", label="آموزش آیفون (iOS)"),
    "guide_windows": dict(page="guide", type="text", label="آموزش ویندوز"),
    "guide_mac": dict(page="guide", type="text", label="آموزش مک"),
    # --- متن‌های ربات ---
    "t_wallet_paid": dict(page="texts", type="text", label="پیام پرداخت موفق از کیف پول"),
    "t_receipt_ok": dict(page="texts", type="text", label="پیام دریافت رسید خرید ({date})"),
    "t_charge_receipt_ok": dict(page="texts", type="text", label="پیام دریافت رسید شارژ"),
    "t_charge_ok": dict(page="texts", type="text", label="پیام تایید شارژ ({amount})"),
    "t_renew_ok": dict(page="texts", type="text", label="پیام تمدید موفق ({plan} {end})"),
    "t_rejected": dict(page="texts", type="text", label="پیام رد درخواست"),
    "t_config_ready": dict(page="texts", type="text", label="پیام تحویل کانفیگ ({plan} {start} {end} {days} {config})"),
}

SETTING_PAGES = {
    "disc": "🏷 تخفیف‌ها",
    "pay": "💳 پرداخت و شارژ",
    "maint": "🔧 حالت تعمیر",
    "start": "👋 پیام شروع و استیکرها",
    "support": "🎧 پشتیبانی و سوالات متداول",
    "ref": "🎁 دعوت دوستان",
    "auto": "🔔 یادآوری، گزارش و نظرسنجی",
    "mon": "📡 مانیتور سرور و اعلان قطعی",
    "custom": "✨ پلن دلخواه (حجم و روز)",
    "deliv": "📦 تحویل سرویس، QR و تست رایگان",
    "chpost": "📣 پست خودکار کانال",
    "flash": "🔥 تخفیف زمان‌دار",
    "club": "🏆 باشگاه مشتریان و پاداش شارژ",
    "join": "🔒 عضویت اجباری کانال",
    "guide": "📲 آموزش اتصال (به تفکیک دستگاه)",
    "texts": "📝 متن‌های ربات",
    "sec": "🔐 امنیت و تأیید دومرحله‌ای",
    "shop": "🛍 فروشگاه و راهنمای انتخاب",
    "usage": "📊 مصرف و هشدار حجم (ساب‌لینک)",
}

_cache = {}


def get_setting(key):
    if key in _cache:
        return _cache[key]
    with SessionLocal() as s:
        row = s.get(KV, key)
        value = row.value if row is not None and row.value is not None else DEFAULTS.get(key, "")
    _cache[key] = value
    return value


def set_setting(key, value):
    value = "" if value is None else str(value)
    with SessionLocal() as s:
        row = s.get(KV, key)
        if row is None:
            s.add(KV(key=key, value=value))
        else:
            row.value = value
        s.commit()
    _cache[key] = value


def get_int(key):
    try:
        return int(get_setting(key))
    except (TypeError, ValueError):
        return 0


def get_bool(key):
    return get_setting(key) in ("1", "true", "True")


# --- کیبورد پایین صفحه (قابل ویرایش توسط ادمین) ---
KB_ITEMS = [
    dict(key="buy", label=BTN_BUY, default_on=True),
    dict(key="my", label=BTN_MY, default_on=True),
    dict(key="support", label=BTN_SUPPORT, default_on=True),
    dict(key="wallet", label=BTN_WALLET, default_on=False),
    dict(key="account", label=BTN_ACCOUNT, default_on=False),
]


def kb_find(key):
    return next((i for i in KB_ITEMS if i["key"] == key), None)


def kb_label(it):
    return get_setting(f"kb_label_{it['key']}").strip() or it["label"]


def kb_visible(it):
    v = get_setting(f"kb_show_{it['key']}")
    return (v == "1") if v in ("0", "1") else it["default_on"]


def kb_tokens():
    keys = [i["key"] for i in KB_ITEMS]
    res = list(dict.fromkeys(t for t in get_setting("uorder_kbd").split(",") if t in keys))
    for k in keys:
        if k not in res:
            res.append(k)
    return res


def kb_cols():
    v = get_setting("kb_cols").strip()
    return int(v) if v in ("1", "2", "3") else 3


def build_reply_keyboard():
    btns = []
    for k in kb_tokens():
        it = kb_find(k)
        if kb_visible(it):
            btns.append(KeyboardButton(text=kb_label(it)))
    if not btns:
        btns = [KeyboardButton(text=BTN_BUY)]
    cols = kb_cols()
    rows = [btns[i : i + cols] for i in range(0, len(btns), cols)]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def refresh_menu_texts():
    """MENU_TEXTS را درجا به‌روز می‌کند (فیلترهای FSM به همین set اشاره دارند)."""
    texts = {BTN_TARIFFS}
    for it in KB_ITEMS:
        texts.add(it["label"])
        texts.add(kb_label(it))
    MENU_TEXTS.clear()
    MENU_TEXTS.update(texts)


class KbBtn(BaseFilter):
    """متن دکمه‌ی کیبورد پایین (نام فعلی یا پیش‌فرض) را تشخیص می‌دهد."""

    def __init__(self, key: str):
        self.key = key

    async def __call__(self, event) -> bool:
        t = getattr(event, "text", None)
        it = kb_find(self.key)
        return bool(t and it and (t == kb_label(it) or t == it["label"]))


# ---------------------------------------------------------------------------
# ساخت و مهاجرت دیتابیس
# ---------------------------------------------------------------------------
def _migrate_columns():
    wanted = {
        "users": {
            "username": "VARCHAR",
            "first_name": "VARCHAR",
            "is_new_user": "BOOLEAN DEFAULT TRUE",
            "purchase_count_current_month": "INTEGER DEFAULT 0",
            "last_purchase_month": "VARCHAR",
            "has_monthly_discount": "BOOLEAN DEFAULT FALSE",
            "note": "TEXT",
            "referred_by": "BIGINT",
            "referral_rewarded": "BOOLEAN DEFAULT FALSE",
            "joined_at": "VARCHAR",
            "trial_used": "BOOLEAN DEFAULT FALSE",
            "points": "INTEGER DEFAULT 0",
            "src": "VARCHAR",
            "promo_off": "BOOLEAN DEFAULT FALSE",
            "blocked": "BOOLEAN DEFAULT FALSE",
        },
        "user_configs": {
            "service_id": "INTEGER",
            "expires_at": "VARCHAR",
            "reminded": "BOOLEAN DEFAULT FALSE",
            "remind_stage": "INTEGER DEFAULT 0",
        },
        "requests": {
            "receipt_file_id": "VARCHAR",
            "price": "INTEGER DEFAULT 0",
            "paid_wallet": "BOOLEAN DEFAULT FALSE",
            "config_id": "INTEGER",
            "service_id": "INTEGER",
            "discount_code": "VARCHAR",
            "created_iso": "VARCHAR",
            "last_remind_iso": "VARCHAR",
            "remind_count": "INTEGER DEFAULT 0",
            "custom_days": "INTEGER",
            "pay_amount": "INTEGER",
        },
    }
    insp = inspect(engine)
    with engine.begin() as conn:
        for table, cols in wanted.items():
            existing = {c["name"].lower() for c in insp.get_columns(table)}
            for name, ddl in cols.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))

    # ایندکس‌ها برای کوئری‌های پرتکرار (روی جدول‌های قدیمی هم اعمال می‌شود)
    indexes = (
        ("ix_user_configs_user_id", "user_configs", "user_id"),
        ("ix_requests_user_id", "requests", "user_id"),
        ("ix_requests_status", "requests", "status"),
        ("ix_code_uses_code_user", "code_uses", "code_id, user_id"),
        ("ix_receipt_hashes_sha256", "receipt_hashes", "sha256"),
        ("ix_receipt_hashes_unique_id", "receipt_hashes", "unique_id"),
        ("ix_ticket_messages_ticket_id", "ticket_messages", "ticket_id"),
    )
    for name, table, cols in indexes:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({cols})"))
        except Exception as e:  # noqa: BLE001
            logging.warning("index %s skipped: %s", name, e)

    if engine.dialect.name == "postgresql":
        for table in ("users", "requests", "user_configs"):
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN user_id TYPE BIGINT"))
            except Exception as e:  # noqa: BLE001
                logging.warning("BIGINT migration skipped for %s: %s", table, e)


def ensure_categories(s):
    """دسته‌های پیش‌فرض را می‌سازد و دسته‌های استفاده‌شده در پلن‌ها را هم ثبت می‌کند."""
    if s.query(Category).count() == 0:
        for i, (k, label, emoji) in enumerate(DEFAULT_CATEGORIES):
            s.add(Category(key=k, label=label, emoji=emoji, sort_order=i, is_active=True))
        s.flush()
    known = {c.key for c in s.query(Category).all()}
    order = s.query(func.max(Category.sort_order)).scalar() or 0
    for (k,) in s.query(Service.category).distinct().all():
        if k and k not in known:
            order += 1
            s.add(Category(key=k, label=CATEGORY_LABELS.get(k, k), emoji="", sort_order=order, is_active=True))
            known.add(k)
    s.commit()


def category_map():
    with SessionLocal() as s:
        return {c.key: (c.label or c.key, c.emoji or "") for c in s.query(Category).all()}


def cat_label(key, cmap=None, emoji=False):
    cmap = cmap if cmap is not None else category_map()
    label, em = cmap.get(key, (key, ""))
    return f"{em} {label}".strip() if emoji else label


def init_db():
    Base.metadata.create_all(engine)
    _migrate_columns()
    with SessionLocal() as s:
        if s.query(Service).count() == 0:
            s.add_all([Service(category=c, name=n, price=p) for c, n, p in DEFAULT_PLANS])
        s.flush()
        ensure_categories(s)

        for key, env in (("card_number", "CARD_NUMBER"), ("card_holder", "CARD_HOLDER")):
            if s.get(KV, key) is None:
                value = os.environ.get(env, "").strip()
                if not value:
                    raise SystemExit(
                        f"Missing required environment variable: {env} "
                        "(needed on first run; later it can be changed from the admin panel)."
                    )
                s.add(KV(key=key, value=value))

        if s.get(KV, "support_info") is None:
            s.add(KV(key="support_info", value=os.environ.get("SUPPORT_INFO", "").strip()))
        s.commit()
    _cache.clear()


# ---------------------------------------------------------------------------
# وضعیت‌های FSM
# ---------------------------------------------------------------------------
class PaymentState(StatesGroup):
    waiting_for_receipt = State()
    waiting_for_code = State()
    waiting_for_charge_receipt = State()
    waiting_for_custom_amount = State()
    waiting_for_admin_config = State()
    broadcasting = State()
    adjusting_balance = State()
    editing_setting = State()
    editing_card_pair = State()
    broadcast_time = State()
    private_message = State()
    editing_note = State()
    searching_user = State()
    editing_plan_price = State()
    editing_plan_name = State()
    adding_plan = State()
    creating_code = State()
    adding_subadmin = State()
    ticket_waiting_text = State()
    ticket_staff_reply = State()
    restore_wait_file = State()
    adding_category = State()
    renaming_category = State()
    editing_cat_emoji = State()
    editing_expiry = State()
    custom_gb = State()
    custom_days = State()
    cb_label = State()
    cb_text = State()
    cb_photo = State()
    cb_url = State()
    cb_edit = State()
    confirm_code = State()
    setting_pin = State()
    flash_percent_in = State()
    flash_hours_in = State()
    ub_rename = State()
    kb_rename = State()


# ---------------------------------------------------------------------------
# توابع کمکی عمومی
# ---------------------------------------------------------------------------
iran_timezone = timezone(timedelta(hours=3, minutes=30))


def now_iran():
    return datetime.now(iran_timezone)


def today_iran():
    return now_iran().date()


def jalali_today():
    return jdatetime.date.fromgregorian(date=today_iran())


def get_current_jalali():
    return f"{jalali_today().strftime('%Y/%m/%d')} - {now_iran().strftime('%H:%M:%S')}"


def jalali_from_iso(iso_date):
    try:
        d = date.fromisoformat(iso_date)
        return jdatetime.date.fromgregorian(date=d).strftime("%Y/%m/%d")
    except Exception:  # noqa: BLE001
        return "-"


def log_action(admin_id, action):
    try:
        with SessionLocal() as s:
            s.add(AdminLog(admin_id=admin_id, action=action, created_at=get_current_jalali()))
            s.commit()
    except Exception as e:  # noqa: BLE001
        logging.warning("log_action failed: %s", e)


def sub_admins():
    ids = []
    for part in get_setting("sub_admins").split(","):
        part = part.strip()
        if part.isdigit():
            ids.append(int(part))
    return ids


def is_main(user_id):
    return user_id == ADMIN_ID


def is_staff(user_id):
    return user_id == ADMIN_ID or user_id in sub_admins()


def staff_ids():
    ids = [ADMIN_ID]
    for i in sub_admins():
        if i not in ids:
            ids.append(i)
    return ids


class MainAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        u = getattr(event, "from_user", None)
        return bool(u and is_main(u.id))


class Staff(BaseFilter):
    async def __call__(self, event) -> bool:
        u = getattr(event, "from_user", None)
        return bool(u and is_staff(u.id))


# بخش‌هایی که ادمین اصلی می‌تواند جداگانه به ادمین فرعی بدهد (رسیدها و تیکت‌ها همیشه مجازند)
ADMIN_PERMS = {
    "users": "👥 مدیریت کاربران",
    "balance": "💰 تغییر موجودی کاربران",
    "plans": "📦 پلن‌ها و دسته‌بندی‌ها",
    "codes": "🏷 کدهای تخفیف",
    "broadcast": "📢 پیام همگانی",
    "stats": "📊 آمار، نمودار و لاگ",
}


def staff_perms(user_id):
    return {p for p in get_setting(f"perm_{user_id}").split(",") if p in ADMIN_PERMS}


def has_perm(user_id, perm):
    return is_main(user_id) or (user_id in sub_admins() and perm in staff_perms(user_id))


class Perm(BaseFilter):
    def __init__(self, perm: str):
        self.perm = perm

    async def __call__(self, event) -> bool:
        u = getattr(event, "from_user", None)
        return bool(u and has_perm(u.id, self.perm))


def user_display(u):
    """نام قابل نمایش کاربر (بدون escape)."""
    parts = []
    if u.first_name:
        parts.append(u.first_name)
    if u.username:
        parts.append(f"@{u.username}")
    return " ".join(parts) if parts else str(u.user_id)


def user_label(tg_user):
    if tg_user.username:
        return f"@{esc(tg_user.username)}"
    return f"آیدی: {tg_user.id}"


def get_or_create_user(session, user_id):
    user = session.get(User, user_id)
    if user is None:
        user = User(
            user_id=user_id,
            balance=0,
            is_banned=False,
            is_new_user=True,
            purchase_count_current_month=0,
            last_purchase_month="None",
            has_monthly_discount=False,
            referral_rewarded=False,
            joined_at=today_iran().isoformat(),
        )
        session.add(user)
        session.flush()
    return user


_ban_cache = {}  # user_id -> (is_banned, زمان)؛ هر پیام یک کوئری دیتابیس نمی‌زند
BAN_CACHE_TTL = 30


def is_banned(user_id):
    now = time.time()
    hit = _ban_cache.get(user_id)
    if hit and now - hit[1] < BAN_CACHE_TTL:
        return hit[0]
    with SessionLocal() as s:
        user = s.get(User, user_id)
        banned = bool(user and user.is_banned)
    if len(_ban_cache) > 5000:
        _ban_cache.clear()
    _ban_cache[user_id] = (banned, now)
    return banned


# ---------------------------------------------------------------------------
# تخفیف‌ها (جمعی) و قیمت‌گذاری
# ---------------------------------------------------------------------------
# سطح‌های وفاداری بر اساس تعداد خرید در ماه (از بالا به پایین)
LOYALTY_TIERS = [
    (6, "gold", "🥇 طلایی"),
    (4, "silver", "🥈 نقره‌ای"),
    (2, "bronze", "🥉 برنزی"),
]
TIER_RANK = {"bronze": 1, "silver": 2, "gold": 3}


def tier_for_count(count):
    """(کلید, برچسب, درصد) سطح مربوط به تعداد خرید ماه، یا None."""
    for need, key, label in LOYALTY_TIERS:
        if (count or 0) >= need:
            return key, label, get_int(f"loyalty_{key}_percent")
    return None


def discount_breakdown(user_id):
    """(درصد کل, [(درصد, توضیح), ...]) برای کاربر؛ تخفیف‌ها جمع می‌شوند."""
    with SessionLocal() as s:
        user = s.get(User, user_id)
        is_new = bool(user.is_new_user) if user else True
        has_monthly = bool(user.has_monthly_discount) if user else False
        month_count = (user.purchase_count_current_month or 0) if user else 0

    first = get_int("first_percent") if (get_bool("first_on") and is_new) else 0
    tier = tier_for_count(month_count) if (get_bool("loyalty_on") and has_monthly) else None
    loyal = tier[2] if tier else 0
    loyal_label = f"تخفیف وفاداری {tier[1]}" if tier else "تخفیف وفاداری"

    parts = []
    # «خرید اول» یا «وفاداری»؛ اگر هر دو بود، بزرگ‌تر اعمال می‌شود
    if first or loyal:
        if first >= loyal:
            parts.append((first, "تخفیف خرید اول"))
        else:
            parts.append((loyal, loyal_label))
    general = get_int("discount_percent")
    if general > 0:
        parts.append((general, "تخفیف ویژه"))
    flash = flash_percent_now()
    if flash > 0:
        parts.append((flash, "🔥 تخفیف زمان‌دار"))
    total = min(sum(p for p, _ in parts), MAX_DISCOUNT)
    return total, parts


def price_with_percent(base, percent):
    return int(base * (100 - percent) / 100)


def price_line(name, base, final, badge=""):
    tag = f" {esc(badge)}" if badge else ""
    tag += per_gb_note(name, final)
    if final != base:
        return f"🔹 <s>{base:,}</s> {ARROW} <b>{final:,}</b> تومان ({esc(name)}){tag}\n"
    return f"🔹 {base:,} تومان ({esc(name)}){tag}\n"


def breakdown_text(total, parts, extra=None):
    if total <= 0:
        return ""
    items = [f"{p}٪ {label}" for p, label in parts]
    if extra:
        items.append(extra)
    return f"🎁 <b>تخفیف شما: {total}٪</b> ({' + '.join(items)})"


# ---------------------------------------------------------------------------
# کدهای تخفیف
# ---------------------------------------------------------------------------
def validate_code(session, code_str, user_id):
    """(DiscountCode | None, پیام خطا)"""
    code_str = (code_str or "").strip().upper()
    if not code_str:
        return None, "کد وارد نشده است."
    dc = session.query(DiscountCode).filter(DiscountCode.code == code_str).first()
    if dc is None or not dc.is_active:
        return None, "این کد کار نکرد 😅 یه بار دیگه چک کن"
    if dc.expires_at:
        try:
            if date.fromisoformat(dc.expires_at) < today_iran():
                return None, "این کد دیگه منقضی شده 😅"
        except ValueError:
            pass
    if (dc.max_uses or 0) > 0 and (dc.used_count or 0) >= dc.max_uses:
        return None, "ظرفیت استفاده از این کد تمام شده است."
    used = (
        session.query(CodeUse).filter(CodeUse.code_id == dc.id, CodeUse.user_id == user_id).first()
    )
    if used:
        return None, "شما قبلاً از این کد استفاده کرده‌اید."
    return dc, ""


def consume_code(session, code_str, user_id):
    dc, err = validate_code(session, code_str, user_id)
    if dc is None:
        raise ValueError(err)
    dc.used_count = (dc.used_count or 0) + 1
    session.add(CodeUse(code_id=dc.id, user_id=user_id))


def release_code(session, code_str, user_id):
    if not code_str:
        return
    dc = session.query(DiscountCode).filter(DiscountCode.code == code_str).first()
    if dc is None:
        return
    use = session.query(CodeUse).filter(CodeUse.code_id == dc.id, CodeUse.user_id == user_id).first()
    if use:
        session.delete(use)
        dc.used_count = max(0, (dc.used_count or 0) - 1)


# ---------------------------------------------------------------------------
# توابع کمکی پیام‌رسانی
# ---------------------------------------------------------------------------
async def edit_or_send(call: types.CallbackQuery, text_, markup=None):
    try:
        await call.message.edit_text(text_, reply_markup=markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return
        await call.message.answer(text_, reply_markup=markup)
    except AttributeError:  # پیام قدیمی/غیرقابل‌دسترس
        await call.bot.send_message(chat_id=call.from_user.id, text=text_, reply_markup=markup)


async def reply_to(target, text_, markup=None):
    """پیام را برای Message ارسال یا برای CallbackQuery ویرایش می‌کند."""
    if isinstance(target, types.CallbackQuery):
        await edit_or_send(target, text_, markup)
    else:
        await target.answer(text_, reply_markup=markup)


async def append_status(call: types.CallbackQuery, line, markup=None):
    if call.message.caption is not None:
        base = call.message.caption
    else:
        base = call.message.text or ""
    new_text = f"{base}\n\n{line}"
    try:
        if call.message.photo:
            await call.message.edit_caption(caption=new_text, reply_markup=markup, parse_mode=None)
        else:
            await call.message.edit_text(new_text, reply_markup=markup, parse_mode=None)
    except TelegramBadRequest as e:
        logging.warning("append_status failed: %s", e)


async def notify_staff(bot, text_, markup=None, photo=None):
    for cid in staff_ids():
        try:
            if photo:
                await bot.send_photo(chat_id=cid, photo=photo, caption=text_, reply_markup=markup)
            else:
                await bot.send_message(chat_id=cid, text=text_, reply_markup=markup)
        except Exception as e:  # noqa: BLE001
            logging.warning("notify_staff failed for %s: %s", cid, e)


async def send_sticker(bot, chat_id, key):
    sticker_id = get_setting(key).strip()
    if not sticker_id:
        return
    try:
        await bot.send_sticker(chat_id=chat_id, sticker=sticker_id)
    except Exception as e:  # noqa: BLE001
        logging.warning("sticker %s failed: %s", key, e)


async def relay(bot, chat_id, message: types.Message, header, markup=None):
    """متن یا عکس پیام را با یک سرتیتر برای chat_id می‌فرستد."""
    body = message.caption if message.photo else message.text
    full = f"{header}{esc(body or '')}"
    if message.photo:
        await bot.send_photo(chat_id=chat_id, photo=message.photo[-1].file_id, caption=full, reply_markup=markup)
    else:
        await bot.send_message(chat_id=chat_id, text=full, reply_markup=markup)


# ---------------------------------------------------------------------------
# کیبوردها
# ---------------------------------------------------------------------------
def get_user_keyboard():
    return build_reply_keyboard()


def main_inline_markup(user_id=None):
    """دکمه‌های شیشه‌ای منوی اصلی؛ از «دکمه‌های کاربر» در پنل ادمین قابل تنظیم است."""
    return InlineKeyboardMarkup(inline_keyboard=ub_build_rows("main", user_id))


def back_home_markup():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")]]
    )


def iso_now_minute():
    return now_iran().strftime("%Y-%m-%dT%H:%M")


def pending_notice():
    return esc(get_setting("pending_text").strip() or DEFAULTS["pending_text"])


def status_markup(req_id):
    b = InlineKeyboardButton
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="📋 وضعیت سفارش من", callback_data=f"order_status_{req_id}")],
            [b(text="🏠 منوی اصلی", callback_data="main_menu")],
        ]
    )


def delivery_markup():
    rows = ub_build_rows("deliv")
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


LINK_RE = re.compile(r"(?:vless|vmess|trojan|ss|hysteria2?|tuic|https?)://\S+", re.I)


def make_qr_png(data):
    """QR را به‌صورت PNG برمی‌گرداند؛ اگر کتابخانه qrcode نصب نباشد None."""
    try:
        import qrcode
    except ImportError:
        return None
    try:
        img = qrcode.make(data)
        buf = io.BytesIO()
        img.save(buf)
        return buf.getvalue()
    except Exception as e:  # noqa: BLE001
        logging.warning("qr failed: %s", e)
        return None


async def send_config_qr(bot, chat_id, text_):
    if not get_bool("qr_on"):
        return
    m = LINK_RE.search(text_ or "")
    if not m:
        return
    png = make_qr_png(m.group(0))
    if png:
        await bot.send_photo(
            chat_id=chat_id, photo=BufferedInputFile(png, filename="qr.png"), caption="📷 QR کد اتصال (با دوربین یا از داخل برنامه اسکن کنید)"
        )


def channel_url():
    ch = get_setting("channel_link").strip()
    if ch.startswith("@"):
        return f"https://t.me/{ch[1:]}"
    if ch.startswith("http://") or ch.startswith("https://"):
        return ch
    if ch.startswith("t.me/"):
        return f"https://{ch}"
    return None


def trial_available(user_id):
    if user_id is None or not get_bool("trial_on"):
        return False
    with SessionLocal() as s:
        u = s.get(User, user_id)
        return bool(u is not None and not u.trial_used and u.is_new_user)


def get_admin_main_keyboard(user_id=None):
    b = InlineKeyboardButton
    top = [
        b(text="🟢 پاسخ داده نشده", callback_data="admin_unanswered"),
        b(text="🔴 پاسخ داده شده", callback_data="admin_answered"),
    ]
    if user_id is not None and not is_main(user_id):
        # ادمین فرعی: همیشه رسیدها و تیکت‌ها؛ بقیه فقط اگر ادمین اصلی اجازه داده باشد
        perms = staff_perms(user_id)
        optional = [
            ("users", b(text="👥 مدیریت کاربران", callback_data="admin_users")),
            ("stats", b(text="📊 آمار کلی فروش", callback_data="admin_sales_stats")),
            ("plans", b(text="📦 مدیریت پلن‌ها", callback_data="admin_plans")),
            ("codes", b(text="🏷 کدهای تخفیف", callback_data="admin_codes")),
            ("broadcast", b(text="📢 پیام همگانی", callback_data="admin_broadcast_start")),
        ]
        buttons = [b(text="🎫 تیکت‌ها", callback_data="admin_tickets")] + [btn for p, btn in optional if p in perms]
        rows = [top] + [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
        return InlineKeyboardMarkup(inline_keyboard=rows)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            top,
            [b(text="👥 مدیریت کاربران", callback_data="admin_users"), b(text="🎫 تیکت‌ها", callback_data="admin_tickets")],
            [b(text="📊 آمار کلی فروش", callback_data="admin_sales_stats"), b(text="🧾 لاگ فعالیت‌ها", callback_data="admin_logs")],
            [b(text="📦 مدیریت پلن‌ها", callback_data="admin_plans"), b(text="🏷 کدهای تخفیف", callback_data="admin_codes")],
            [b(text="📢 پیام همگانی", callback_data="admin_broadcast_start"), b(text="🧰 بکاپ", callback_data="admin_backup")],
            [b(text="🎛 دکمه‌های کاربر", callback_data="ubh"), b(text="⚙️ تنظیمات ربات", callback_data="admin_settings")],
        ]
    )


def admin_request_markup(req_id):
    b = InlineKeyboardButton
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                b(text="✅ تایید", callback_data=f"admin_confirm_{req_id}"),
                b(text="❌ رد درخواست", callback_data=f"admin_reject_{req_id}"),
            ],
            [b(text="⚙️ پنل مدیریت", callback_data="admin_home")],
        ]
    )


# ---------------------------------------------------------------------------
# میدل‌ور: ثبت کاربر، مسدودسازی، حالت تعمیر، ریاکشن
# ---------------------------------------------------------------------------
_user_seen = {}  # user_id -> (username, first_name, زمان آخرین ثبت)


def touch_user(tg_user):
    """کاربر را ثبت/به‌روزرسانی می‌کند. اگر کاربر تازه ساخته شد True برمی‌گرداند."""
    now = time.time()
    hit = _user_seen.get(tg_user.id)
    if hit and hit[0] == tg_user.username and hit[1] == tg_user.first_name and now - hit[2] < 600:
        return False  # کاربر شناخته‌شده و بدون تغییر؛ نیازی به نوشتن در دیتابیس نیست
    try:
        with SessionLocal() as s:
            created = s.get(User, tg_user.id) is None
            u = get_or_create_user(s, tg_user.id)
            u.username = tg_user.username
            u.first_name = tg_user.first_name
            u.blocked = False
            s.commit()
        if len(_user_seen) > 5000:
            _user_seen.clear()
        _user_seen[tg_user.id] = (tg_user.username, tg_user.first_name, now)
        return created
    except Exception as e:  # noqa: BLE001
        logging.warning("touch_user failed: %s", e)
        return False


async def _react(event):
    try:
        if isinstance(event, types.Message):
            if event.photo or (event.text and event.text in MENU_TEXTS):
                await event.bot.set_message_reaction(
                    chat_id=event.chat.id, message_id=event.message_id,
                    reaction=[ReactionTypeEmoji(emoji="👍")],
                )
        elif isinstance(event, types.CallbackQuery) and event.message:
            await event.bot.set_message_reaction(
                chat_id=event.message.chat.id, message_id=event.message.message_id,
                reaction=[ReactionTypeEmoji(emoji="👍")],
            )
    except Exception:  # noqa: BLE001
        pass


class AccessMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is None or user.is_bot:
            return await handler(event, data)

        data["is_new_user_row"] = touch_user(user)

        if not is_staff(user.id):
            if is_banned(user.id):
                if isinstance(event, types.CallbackQuery):
                    await event.answer("دسترسی شما مسدود شده است.", show_alert=True)
                return None
            if get_bool("maintenance_on"):
                msg = get_setting("maintenance_text") or "ربات در حال بروزرسانی است."
                if isinstance(event, types.CallbackQuery):
                    await event.answer(msg[:190], show_alert=True)
                else:
                    await event.answer(esc(msg))
                return None
            if get_bool("force_join_on") and not (
                isinstance(event, types.CallbackQuery) and event.data == "fj_check"
            ):
                if not await is_channel_member(event.bot, user.id):
                    # لینک دعوت را قبل از قفل‌شدن ثبت می‌کنیم تا بعد از عضویت از دست نرود
                    if (
                        isinstance(event, types.Message)
                        and event.text
                        and event.text.startswith("/start ")
                        and data.get("is_new_user_row")
                        and get_bool("referral_on")
                    ):
                        _bind_referral(user.id, event.text.split(" ", 1)[1].strip())
                    await send_join_prompt(event)
                    return None
            spawn(_react(event))
        return await handler(event, data)


class TimingMiddleware(BaseMiddleware):
    """هر پردازشی که بیش از ۱ ثانیه طول بکشد را با نام دکمه/پیام در لاگ می‌نویسد."""

    async def __call__(self, handler, event, data):
        t0 = time.perf_counter()
        try:
            return await handler(event, data)
        finally:
            ms = (time.perf_counter() - t0) * 1000
            if ms > 1000:
                label = getattr(event, "data", None) or (getattr(event, "text", None) or "")[:30] or type(event).__name__
                logging.warning("SLOW HANDLER %.0fms: %s", ms, label)


async def loop_lag_monitor():
    """اگر حلقهٔ asyncio بیش از نیم‌ثانیه بلاک شود (کار سنگین همگام)، گزارش می‌دهد."""
    while True:
        t0 = time.perf_counter()
        await asyncio.sleep(1)
        lag = (time.perf_counter() - t0 - 1) * 1000
        if lag > 500:
            logging.warning("EVENT LOOP BLOCKED ~%.0fms", lag)


dp.message.outer_middleware(TimingMiddleware())
dp.callback_query.outer_middleware(TimingMiddleware())
dp.message.outer_middleware(AccessMiddleware())
dp.callback_query.outer_middleware(AccessMiddleware())

BOT_USERNAME = ""


# ---------------------------------------------------------------------------
# پیام شروع و منوی اصلی
# ---------------------------------------------------------------------------
def expiring_list(session, user_id, days):
    out = []
    rows = (
        session.query(UserConfig)
        .filter(UserConfig.user_id == user_id, UserConfig.expires_at.isnot(None))
        .all()
    )
    for c in rows:
        try:
            left = (date.fromisoformat(c.expires_at) - today_iran()).days
        except (TypeError, ValueError):
            continue
        if left <= days:
            out.append((c.config_name, left))
    return out


def build_welcome(user_id, first_name):
    name = first_name or "دوست عزیز"
    custom = get_setting("welcome_text").strip()
    with SessionLocal() as s:
        u = s.get(User, user_id)
        balance = (u.balance if u else 0) or 0
        is_new = bool(u.is_new_user) if u else True
        active = s.query(func.count(UserConfig.id)).filter(UserConfig.user_id == user_id).scalar()
        expiring = expiring_list(s, user_id, get_int("reminder_days"))

    if custom:
        body = esc(custom.replace("{name}", name))
    elif is_new:
        body = f"سلام {esc(name)} جان 👋\nخوش اومدی! امروز چی می‌خوای؟"
    else:
        body = (
            f"خوش برگشتی {esc(name)} جان 😊 دلمون برات تنگ شده بود\n\n"
            f"💳 موجودی: {balance:,} تومان\n📦 سرویس‌های فعال: {active}"
        )

    total, parts = discount_breakdown(user_id)
    if total > 0:
        body += "\n\n" + breakdown_text(total, parts)

    for cname, left in expiring[:3]:
        if left >= 0:
            body += f"\n⏰ سرویس «{esc(cname or '')}» {left} روز دیگر تمام می‌شود."
        else:
            body += f"\n⚠️ سرویس «{esc(cname or '')}» منقضی شده است."

    body += "\n\nبرای برگشت به منوی اصلی /start رو بزن."
    return body


@dp.message(Command("start"))
async def show_main_menu(
    message: types.Message, state: FSMContext, command: CommandObject, is_new_user_row: bool = False
):
    await state.clear()

    # دعوت دوستان: لینک /start ref_<id>
    if is_new_user_row and get_bool("referral_on") and command.args and command.args.startswith("ref_"):
        try:
            ref_id = int(command.args[4:])
        except ValueError:
            ref_id = 0
        if ref_id and ref_id != message.from_user.id:
            with SessionLocal() as s:
                if s.get(User, ref_id) is not None:
                    me = s.get(User, message.from_user.id)
                    if me is not None and not me.referred_by:
                        me.referred_by = ref_id
                        s.commit()

    if is_new_user_row and command.args and command.args.startswith("src_"):
        bind_source(message.from_user.id, command.args)
    await send_sticker(message.bot, message.chat.id, "sticker_welcome")
    body = build_welcome(message.from_user.id, message.from_user.first_name)
    markup = main_inline_markup(message.from_user.id)
    banner = get_setting("welcome_banner").strip()
    sent = False
    if banner:
        try:
            await message.answer_photo(photo=banner, caption=body, reply_markup=markup)
            sent = True
        except Exception as e:  # noqa: BLE001
            logging.warning("banner send failed: %s", e)
    if not sent:
        await message.answer(body, reply_markup=markup)
    await message.answer("🔽 منوی پایین هم همیشه در دسترسته.", reply_markup=get_user_keyboard())


@dp.callback_query(F.data == "main_menu")
async def main_menu_handler_call(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await edit_or_send(
        call, build_welcome(call.from_user.id, call.from_user.first_name), main_inline_markup(call.from_user.id)
    )


# ---------------------------------------------------------------------------
# تعرفه‌ها
# ---------------------------------------------------------------------------
def shop_view(user_id, cat_key=None):
    """تعرفه و خرید در یک صفحه؛ دسته‌ها به‌شکل تب."""
    total, parts = discount_breakdown(user_id)
    with SessionLocal() as s:
        cats = [
            (c.key, c.label, c.emoji or "")
            for c in s.query(Category)
            .filter(Category.is_active.is_(True))
            .order_by(Category.sort_order, Category.id)
            .all()
        ]
        if not cats:
            return "فعلاً پلنی برای فروش ثبت نشده است.", back_home_markup()
        keys = [c[0] for c in cats]
        if cat_key not in keys:
            cat_key = keys[0]
        services = [
            (r.id, r.name, r.price)
            for r in s.query(Service).filter(Service.category == cat_key).order_by(Service.id).all()
        ]

    b = InlineKeyboardButton
    cur = next(c for c in cats if c[0] == cat_key)
    out = "🛒 <b>خرید سرویس</b> | مرحله ۱ از ۳\nیه دسته انتخاب کن، بعدش پلن مورد نظرت رو بزن 👇\n"
    if total > 0:
        out += "\n" + breakdown_text(total, parts) + "\n"
    left = flash_remaining()
    if left is not None:
        out += f"⏳ تخفیف زمان‌دار {fmt_remaining(left)} دیگر ادامه دارد!\n"
    out += f"\n📂 <b>{esc((cur[2] + ' ' + cur[1]).strip())}</b>\n"
    keyboard = []
    tabs = [
        b(text=("• " if k == cat_key else "") + f"{em} {label}".strip(), callback_data=f"shop_{k}")
        for k, label, em in cats
    ]
    keyboard.extend(tabs[i : i + 3] for i in range(0, len(tabs), 3))
    best = bestseller_id()
    badge = get_setting("bestseller_label").strip() or "⭐ پرفروش"
    for sid, name, price in services:
        final = price_with_percent(price, total)
        is_best = best is not None and sid == best
        out += price_line(name, price, final, badge if is_best else "")
        keyboard.append(
            [b(text=f"🛒 {name} — {final:,} تومان" + (f" {badge}" if is_best else ""), callback_data=f"buy_{sid}")]
        )
    if not services:
        out += "فعلاً پلنی در این دسته وجود ندارد.\n"
    if get_bool("loyalty_on"):
        out += (
            "\n💎 باشگاه وفاداری (خرید در هر ماه): "
            f"۲ خرید 🥉 {get_int('loyalty_bronze_percent')}٪ | "
            f"۴ خرید 🥈 {get_int('loyalty_silver_percent')}٪ | "
            f"۶ خرید 🥇 {get_int('loyalty_gold_percent')}٪"
        )
    if get_bool("custom_on") and get_int("custom_price_per_gb") > 0:
        keyboard.append(
            [b(text=f"✨ پلن دلخواه (هر گیگ {get_int('custom_price_per_gb'):,} تومان)", callback_data="custom_start")]
        )
    if get_bool("pick_on"):
        keyboard.append([b(text="🤔 نمی‌دونم کدوم رو بخرم؟", callback_data="pk_start")])
    keyboard.append([b(text="💳 شارژ کیف پول", callback_data="wallet_menu")])
    keyboard.append([b(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")])
    return out, InlineKeyboardMarkup(inline_keyboard=keyboard)


async def _show_shop(target, user_id, cat_key=None):
    out, markup = shop_view(user_id, cat_key)
    await reply_to(target, out, markup)


@dp.message(KbBtn("buy"))
@dp.message(F.text == BTN_TARIFFS)
async def message_buy_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await _show_shop(message, message.from_user.id)


@dp.callback_query(F.data.in_({"main_buy", "tariffs"}))
async def main_buy_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await _show_shop(call, call.from_user.id)


@dp.callback_query(F.data.startswith("shop_"))
@dp.callback_query(F.data.startswith("cat_"))
async def category_handler(call: types.CallbackQuery):
    await call.answer()
    await _show_shop(call, call.from_user.id, call.data.split("_", 1)[1])


def custom_limits():
    gmin = max(1, get_int("custom_min_gb"))
    gmax = max(gmin, get_int("custom_max_gb"))
    dmin = max(1, get_int("custom_min_days"))
    dmax = max(dmin, get_int("custom_max_days"))
    return gmin, gmax, dmin, dmax


def custom_price(gb, days):
    """قیمت = حجم × قیمت هر گیگ (برای مدت پایه)، متناسب با تعداد روز؛ گرد به هزار تومان."""
    price = gb * get_int("custom_price_per_gb")
    base_days = get_int("custom_base_days")
    if base_days > 0:
        price = price * days / base_days
    return max(1000, int(round(price / 1000.0)) * 1000)


def parse_int_input(raw):
    t = (raw or "").translate(DIGITS_FA).replace(",", "").replace("،", "").strip()
    return int(t) if t.isdigit() else None


def compute_checkout(user_id, data):
    sid = data.get("service_id")
    gb, days = data.get("custom_gb"), data.get("custom_days")
    is_custom = bool(gb and days)
    if not sid and not is_custom:
        return None
    custom_days = None
    with SessionLocal() as s:
        if is_custom:
            sid = None
            label = f"✨ پلن دلخواه - {gb} گیگ - {days} روزه"
            base = custom_price(gb, days)
            custom_days = days
        else:
            svc = s.get(Service, sid)
            if svc is None:
                return None
            label = f"{cat_label(svc.category)} - {svc.name}"
            base = svc.price
        code, code_pct = None, 0
        if data.get("code"):
            dc, _err = validate_code(s, data["code"], user_id)
            if dc is not None:
                code, code_pct = dc.code, dc.percent or 0
    total, parts = discount_breakdown(user_id)
    percent = min(total + code_pct, MAX_DISCOUNT)
    return dict(
        service_id=sid, label=label, base=base, percent=percent, parts=parts,
        code=code, code_pct=code_pct, final=price_with_percent(base, percent),
        renew_cfg_id=data.get("renew_cfg_id"), custom_days=custom_days,
    )


def checkout_view(co, balance=0):
    if co["final"] != co["base"]:
        price_display = f"<s>{co['base']:,}</s> {ARROW} <b>{co['final']:,}</b> تومان"
    else:
        price_display = f"{co['base']:,} تومان"
    title = "🔄 تمدید سرویس" if co["renew_cfg_id"] else "🛒 سفارش شما"
    out = f"<b>مرحله ۲ از ۳ | پرداخت</b>\n\n{title}: {esc(co['label'])}\n💰 قیمت: {price_display}\n"
    if co["percent"] > 0:
        extra = f"{co['code_pct']}٪ کد تخفیف" if co["code"] else None
        out += breakdown_text(co["percent"], co["parts"], extra) + "\n"
    out += f"\n💳 موجودی کیف پولت: {balance:,} تومان\n"
    b = InlineKeyboardButton
    rows = []
    if balance >= co["final"]:
        out += "\nروش پرداخت رو انتخاب کن:"
        rows.append([b(text="💳 پرداخت از کیف پول (سریع‌تر)", callback_data="pay_with_wallet")])
        rows.append([b(text="🏦 کارت‌به‌کارت", callback_data="confirm_payment")])
    else:
        shortage = co["final"] - balance
        out += f"\nفقط {shortage:,} تومان کم داری! با یه شارژ سریع، خریدت تموم میشه 💪\nروش پرداخت رو انتخاب کن:"
        rows.append([b(text=f"➕ شارژ و خرید ({shortage:,} تومان کم)", callback_data="short_charge")])
        rows.append([b(text="🏦 کارت‌به‌کارت", callback_data="confirm_payment")])
    rows.append(
        [
            b(text="❌ حذف کد تخفیف", callback_data="remove_code")
            if co["code"]
            else b(text="🏷 کد تخفیف دارم", callback_data="use_code")
        ]
    )
    rows.append([b(text="⬅️ انصراف و بازگشت", callback_data="main_buy")])
    return out, InlineKeyboardMarkup(inline_keyboard=rows)


async def send_checkout(target, state, user_id):
    data = await state.get_data()
    co = compute_checkout(user_id, data)
    if co is None:
        await state.clear()
        await reply_to(target, "این سفارش دیگه معتبر نیست، دوباره از خرید شروع کن 🔄", back_home_markup())
        return
    with SessionLocal() as s:
        u = s.get(User, user_id)
        balance = (u.balance if u else 0) or 0
    out, markup = checkout_view(co, balance)
    await reply_to(target, out, markup)


@dp.callback_query(F.data == "custom_start")
async def custom_start_handler(call: types.CallbackQuery, state: FSMContext):
    if not get_bool("custom_on") or get_int("custom_price_per_gb") <= 0:
        await call.answer("پلن دلخواه فعلاً غیرفعال است.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    gmin, gmax, _dmin, _dmax = custom_limits()
    await state.set_state(PaymentState.custom_gb)
    out = (
        "✨ <b>پلن دلخواه | حجم</b>\n\n"
        f"💰 قیمت هر گیگ: {get_int('custom_price_per_gb'):,} تومان\n\n"
        f"حجم موردنظر را به <b>گیگ</b> و به‌صورت عدد بفرستید ({gmin} تا {gmax}):"
    )
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ انصراف", callback_data="main_buy")]])
    await edit_or_send(call, out, markup)


@dp.message(PaymentState.custom_gb, ~F.text.in_(MENU_TEXTS))
async def custom_gb_entered(message: types.Message, state: FSMContext):
    gmin, gmax, dmin, dmax = custom_limits()
    n = parse_int_input(message.text)
    if n is None or not (gmin <= n <= gmax):
        await message.answer(f"یک عدد بین {gmin} تا {gmax} بفرستید.")
        return
    await state.update_data(custom_gb=n)
    await state.set_state(PaymentState.custom_days)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ تغییر حجم", callback_data="custom_start")],
            [InlineKeyboardButton(text="⬅️ انصراف", callback_data="main_buy")],
        ]
    )
    await message.answer(
        f"✨ <b>پلن دلخواه | مدت</b>\n\n📦 حجم انتخابی: {n} گیگ\n\n"
        f"تعداد <b>روز</b> را به‌صورت عدد بفرستید ({dmin} تا {dmax}):",
        reply_markup=markup,
    )


@dp.message(PaymentState.custom_days, ~F.text.in_(MENU_TEXTS))
async def custom_days_entered(message: types.Message, state: FSMContext):
    _gmin, _gmax, dmin, dmax = custom_limits()
    n = parse_int_input(message.text)
    if n is None or not (dmin <= n <= dmax):
        await message.answer(f"یک عدد بین {dmin} تا {dmax} بفرستید.")
        return
    await state.update_data(service_id=None, renew_cfg_id=None, code=None, custom_days=n)
    await state.set_state(PaymentState.waiting_for_receipt)
    await send_checkout(message, state, message.from_user.id)


@dp.callback_query(F.data.startswith("buy_"))
async def buy_plan_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    try:
        service_id = int(call.data.split("_", 1)[1])
    except ValueError:
        return
    await state.clear()
    await state.set_state(PaymentState.waiting_for_receipt)
    await state.update_data(service_id=service_id, renew_cfg_id=None, code=None)
    await send_checkout(call, state, call.from_user.id)


@dp.callback_query(F.data.startswith("renew_"))
async def renew_handler(call: types.CallbackQuery, state: FSMContext):
    try:
        cfg_id = int(call.data.split("_", 1)[1])
    except ValueError:
        return
    with SessionLocal() as s:
        cfg = s.get(UserConfig, cfg_id)
        if cfg is None or cfg.user_id != call.from_user.id:
            await call.answer("سرویس یافت نشد.", show_alert=True)
            return
        service_id = cfg.service_id
        exists = bool(service_id and s.get(Service, service_id))
    if not exists:
        await call.answer("تمدید خودکار برای این سرویس ممکن نیست؛ با پشتیبانی تماس بگیرید.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    await state.set_state(PaymentState.waiting_for_receipt)
    await state.update_data(service_id=service_id, renew_cfg_id=cfg_id, code=None)
    await send_checkout(call, state, call.from_user.id)


@dp.callback_query(F.data == "use_code")
async def use_code_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    if not (data.get("service_id") or data.get("custom_gb")):
        await edit_or_send(call, "این سفارش دیگه معتبر نیست، دوباره از خرید شروع کن 🔄", back_home_markup())
        return
    await state.set_state(PaymentState.waiting_for_code)
    await edit_or_send(call, "🏷 کد تخفیف خود را وارد کنید:")


_code_failures = {}  # user_id -> [زمان تلاش‌های ناموفق]
CODE_MAX_FAILS, CODE_WINDOW = 5, 600


@dp.message(PaymentState.waiting_for_code, ~F.text.in_(MENU_TEXTS))
async def code_entered_handler(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    now = time.time()
    recent = [t for t in _code_failures.get(uid, []) if now - t < CODE_WINDOW]
    if len(recent) >= CODE_MAX_FAILS:
        _code_failures[uid] = recent
        await message.answer("⛔ تلاش‌های ناموفق زیاد بود. چند دقیقه دیگر دوباره امتحان کنید.")
        return
    with SessionLocal() as s:
        dc, err = validate_code(s, message.text or "", uid)
        code = dc.code if dc else None
    if code is None:
        recent.append(now)
        _code_failures[uid] = recent
        if len(_code_failures) > 5000:
            _code_failures.clear()
        await message.answer(f"❌ {err}\nدوباره وارد کنید یا از منوی پایین خارج شوید.")
        return
    await state.update_data(code=code)
    await state.set_state(PaymentState.waiting_for_receipt)
    await send_checkout(message, state, message.from_user.id)


@dp.callback_query(F.data == "remove_code")
async def remove_code_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.update_data(code=None)
    await send_checkout(call, state, call.from_user.id)


def build_request(session, user_id, co, paid_wallet, receipt_id):
    if co["code"]:
        consume_code(session, co["code"], user_id)  # ValueError اگر دیگر معتبر نباشد
    req = Request(
        user_id=user_id,
        req_type="renew" if co["renew_cfg_id"] else "buy",
        plan_name=co["label"],
        status="unanswered",
        created_at=get_current_jalali(),
        created_iso=iso_now_minute(),
        receipt_file_id=receipt_id,
        price=co["final"],
        paid_wallet=paid_wallet,
        config_id=co["renew_cfg_id"],
        service_id=co["service_id"],
        custom_days=co.get("custom_days"),
        discount_code=co["code"],
    )
    session.add(req)
    session.flush()
    return req


def request_caption(req_id, tg_user, co, req_type, via_wallet, pay_amount=None):
    kind = "تمدید" if req_type == "renew" else "خرید"
    way = "با کیف پول" if via_wallet else "با رسید"
    out = (
        f"🚨 درخواست {kind} جدید {way} (کد #{req_id})\n"
        f"👤 کاربر: {user_label(tg_user)} ({tg_user.id})\n"
        f"📦 پلن: {esc(co['label'])}\n"
        f"💰 مبلغ: {co['final']:,} تومان\n"
    )
    if pay_amount and pay_amount != co["final"]:
        out += f"💳 مبلغ واریزی دقیق (با عدد تصادفی): <b>{pay_amount:,}</b> تومان\n"
    if co["code"]:
        out += f"🏷 کد تخفیف: {esc(co['code'])}\n"
    return out + f"⏱ زمان ثبت: {get_current_jalali()}" + sms_badge(pay_amount)


@dp.callback_query(F.data == "pay_with_wallet")
async def pay_with_wallet_handler(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    co = compute_checkout(call.from_user.id, data)
    if co is None:
        await call.answer("این سفارش دیگه معتبر نیست، دوباره از خرید شروع کن 🔄", show_alert=True)
        return

    user_id = call.from_user.id
    with SessionLocal() as s:
        user = get_or_create_user(s, user_id)
        balance = user.balance or 0
        if balance < co["final"]:
            await call.answer(
                f"موجودی شما ({balance:,} تومان) برای پرداخت این سرویس کافی نیست.", show_alert=True
            )
            return
        try:
            req = build_request(s, user_id, co, True, "None")
        except ValueError as e:
            await call.answer(str(e), show_alert=True)
            return
        user.balance = balance - co["final"]
        req_id, req_type = req.id, req.req_type
        s.commit()

    await state.clear()  # قبل از هر await شبکه‌ای، تا کلیک دوباره سفارش تکراری نسازد
    await call.answer()
    await edit_or_send(
        call,
        fill_text("t_wallet_paid") + "\n\n" + pending_notice(),
        status_markup(req_id),
    )
    await send_sticker(call.bot, call.message.chat.id, "sticker_purchase")
    await notify_staff(call.bot, request_caption(req_id, call.from_user, co, req_type, True), admin_request_markup(req_id))


@dp.callback_query(F.data == "confirm_payment")
async def confirm_payment_handler(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    co = compute_checkout(call.from_user.id, data)
    if co is None:
        await call.answer("این سفارش دیگه معتبر نیست، دوباره از خرید شروع کن 🔄", show_alert=True)
        return
    await call.answer()
    await state.set_state(PaymentState.waiting_for_receipt)
    amount = await ensure_pay_amount(state, co["final"])
    record_cart(call.from_user.id, co["label"], co["final"])
    out = (
        "<b>مرحله ۳ از ۳ | واریز و ارسال رسید</b>\n\n"
        f"📦 {esc(co['label'])}\n💰 مبلغ قابل پرداخت: <b>{amount:,}</b> تومان\n"
        + pay_note(co["final"], amount)
        + "\n"
        f"💳 شماره کارت:\n<code>{esc(pick_card(call.from_user.id)[0])}</code>\n"
        f"به نام: {esc(pick_card(call.from_user.id)[1])}\n\n"
        "بعد از واریز، <b>عکس رسیدت</b> رو همینجا بفرست 📸\n⏱ بعد از ارسال رسید، معمولاً ظرف ۳۰ دقیقه آماده می‌شه."
    )
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت به پرداخت", callback_data="checkout_back")]]
    )
    await edit_or_send(call, out, markup)


@dp.callback_query(F.data == "checkout_back")
async def checkout_back_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PaymentState.waiting_for_receipt)
    await send_checkout(call, state, call.from_user.id)


@dp.callback_query(F.data == "short_charge")
async def short_charge_handler(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    co = compute_checkout(call.from_user.id, data)
    if co is None:
        await call.answer("این سفارش دیگه معتبر نیست، دوباره از خرید شروع کن 🔄", show_alert=True)
        return
    with SessionLocal() as s:
        u = s.get(User, call.from_user.id)
        balance = (u.balance if u else 0) or 0
    amount = min(max(co["final"] - balance, get_int("min_charge"), 1), 100000000)
    await call.answer()
    await send_charge_instructions(
        call, state, amount, note="\n\n✅ بعد از تایید شارژ، دوباره از «خرید سرویس» پلن را انتخاب و از کیف پول پرداخت کنید."
    )


async def receipt_fingerprint(bot, photo):
    """(file_unique_id, sha256 محتوای عکس) برای تشخیص رسید تکراری."""
    uniq = getattr(photo, "file_unique_id", None)
    digest = None
    size = getattr(photo, "file_size", None)
    if size and size > 10 * 1024 * 1024:  # از دانلود فایل‌های حجیم برای هش صرف‌نظر می‌شود
        return uniq, None
    try:
        buf = io.BytesIO()
        await bot.download(photo.file_id, destination=buf)
        digest = hashlib.sha256(buf.getvalue()).hexdigest()
    except Exception as e:  # noqa: BLE001
        logging.warning("receipt hash failed: %s", e)
    return uniq, digest


def find_duplicate_receipt(session, uniq, digest):
    """(شماره درخواست قبلی, آیدی کاربر قبلی) اگر همین رسید قبلاً ثبت شده و رد نشده باشد."""
    conds = []
    if uniq:
        conds.append(ReceiptHash.unique_id == uniq)
    if digest:
        conds.append(ReceiptHash.sha256 == digest)
    if not conds:
        return None
    for rh in session.query(ReceiptHash).filter(or_(*conds)).all():
        prev = session.get(Request, rh.request_id)
        if prev is not None and prev.status != "rejected":
            return prev.id, prev.user_id
    return None


def save_receipt_fingerprint(session, uniq, digest, user_id, req_id):
    session.add(
        ReceiptHash(
            sha256=digest, unique_id=uniq, user_id=user_id, request_id=req_id, created_at=get_current_jalali()
        )
    )


IMG_RECEIPT = F.photo | (F.document & F.document.mime_type.startswith("image/"))


def receipt_media(message: types.Message):
    """آبجکت فایل رسید (عکس یا فایل تصویری)."""
    if message.photo:
        return message.photo[-1]
    return message.document


def safe_find_duplicate(s, uniq, digest):
    try:
        return find_duplicate_receipt(s, uniq, digest)
    except Exception as e:  # noqa: BLE001
        logging.exception("duplicate check failed: %s", e)
        try:
            s.rollback()
        except Exception:  # noqa: BLE001
            pass
        return None


def safe_save_fingerprint(s, uniq, digest, user_id, req_id):
    try:
        with s.begin_nested():
            save_receipt_fingerprint(s, uniq, digest, user_id, req_id)
    except Exception as e:  # noqa: BLE001
        logging.exception("save fingerprint failed: %s", e)


async def handle_duplicate_receipt(message: types.Message, dup):
    prev_req, prev_user = dup
    await message.answer(
        "⚠️ این رسید قبلاً در سیستم ثبت شده است و نمی‌توان دوباره از آن استفاده کرد.\n"
        "اگر رسید جدیدی دارید همان را بفرستید؛ در صورت اشتباه از بخش پشتیبانی تیکت بزنید."
    )
    await notify_staff(
        message.bot,
        "🚫 <b>تلاش برای ارسال رسید تکراری</b>\n"
        f"👤 کاربر: {user_label(message.from_user)} (<code>{message.from_user.id}</code>)\n"
        f"🔁 رسید قبلاً برای درخواست #{prev_req} (کاربر <code>{prev_user}</code>) ثبت شده بود.",
    )


async def receipt_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    co = compute_checkout(message.from_user.id, data)
    if co is None:
        await state.clear()
        await message.answer("این سفارش دیگه معتبر نیست، دوباره از خرید شروع کن 🔄")
        return

    user_id = message.from_user.id
    media = receipt_media(message)
    photo_id = media.file_id
    logging.info("receipt received from %s", user_id)
    uniq, digest = await receipt_fingerprint(message.bot, media)
    dup = None
    with SessionLocal() as s:
        get_or_create_user(s, user_id)
        dup = safe_find_duplicate(s, uniq, digest)
        if dup is None:
            try:
                req = build_request(s, user_id, co, False, photo_id)
            except ValueError as e:
                await message.answer(f"❌ {e}")
                return
            if data.get("pay_base") == co["final"] and data.get("pay_amount"):
                req.pay_amount = int(data["pay_amount"])
            safe_save_fingerprint(s, uniq, digest, user_id, req.id)
            req_id, req_type, pay_amt = req.id, req.req_type, req.pay_amount
            s.commit()
    if dup is not None:
        await handle_duplicate_receipt(message, dup)
        return

    await state.clear()
    await message.answer(
        fill_text("t_receipt_ok", date=get_current_jalali()) + "\n\n" + pending_notice(),
        reply_markup=status_markup(req_id),
    )
    await send_sticker(message.bot, message.chat.id, "sticker_purchase")
    await notify_staff(
        message.bot, request_caption(req_id, message.from_user, co, req_type, False, pay_amt),
        admin_request_markup(req_id), photo=photo_id,
    )


BUILD_TAG = "growth-pack-v12b"


def has_receipt_media(message: types.Message) -> bool:
    if message.photo:
        return True
    doc = message.document
    return bool(doc and (doc.mime_type or "").startswith("image/"))


@dp.message(PaymentState.waiting_for_receipt)
@dp.message(PaymentState.waiting_for_charge_receipt)
async def receipt_router(message: types.Message, state: FSMContext):
    """همه پیام‌های مرحله رسید از اینجا رد می‌شوند و داخل تابع تصمیم‌گیری می‌شود."""
    cur = await state.get_state()
    logging.info(
        "receipt_router user=%s state=%s type=%s photo=%s doc=%s",
        message.from_user.id, cur, message.content_type, bool(message.photo), bool(message.document),
    )
    if message.text and message.text in MENU_TEXTS:
        raise SkipHandler()
    if has_receipt_media(message):
        async with user_lock(message.from_user.id):
            cur = await state.get_state()
            if cur is None:  # عکس دوم آلبوم/ارسال همزمان: سفارش قبلاً ثبت شده
                return
            if cur == PaymentState.waiting_for_charge_receipt.state:
                await process_charge_receipt_handler(message, state)
            else:
                await receipt_handler(message, state)
        return
    if message.document:
        await message.answer("رسید رو به‌صورت «عکس» بفرست (فایل تصویری هم قبوله، PDF نه) 📸")
        return
    await message.answer("عکس رسیدت رو همینجا بفرست، خودم می‌رسم بهش 🙌")


# ---------------------------------------------------------------------------
# کیف پول
# ---------------------------------------------------------------------------
def charge_amounts():
    out = []
    for part in re.split(r"[,\s،]+", get_setting("charge_amounts")):
        if part.isdigit() and int(part) > 0:
            out.append(int(part))
    return out or [200000, 400000, 600000]


async def _render_wallet(target):
    with SessionLocal() as s:
        user = s.get(User, target.from_user.id)
        balance = (user.balance if user else 0) or 0
    out = (
        "💳 <b>شارژ کیف پول Swan VPN</b>\n\n"
        f"💰 موجودی فعلی: {balance:,} تومان\n\n"
        + bonus_hint_text()
        + "مبلغ مورد نظرت رو انتخاب کن (یا «مبلغ دلخواه» رو بزن) 👇"
    )
    b = InlineKeyboardButton
    amounts = [a for a in charge_amounts() if a >= get_int("min_charge")]
    buttons = [b(text=f"{a:,} تومان", callback_data=f"charge_{a}") for a in amounts]
    rows = [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
    rows.append([b(text="✍️ مبلغ دلخواه", callback_data="charge_custom")])
    rows.append([b(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")])
    await reply_to(target, out, InlineKeyboardMarkup(inline_keyboard=rows))


@dp.message(KbBtn("wallet"))
async def wallet_menu_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await _render_wallet(message)


@dp.callback_query(F.data == "wallet_menu")
async def wallet_menu_callback(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await _render_wallet(call)


async def send_charge_instructions(target, state, amount, note=""):
    await state.update_data(charge_amount=amount)
    pay = await ensure_pay_amount(state, amount, key="chg")
    bonus, bonus_pct = charge_bonus_for(amount)
    bonus_line = f"🎁 پاداش شارژ: {bonus:,} تومان ({bonus_pct}٪) بعد از تایید به اعتبار شما اضافه می‌شود.\n" if bonus else ""
    pay_line = f"💳 مبلغ قابل واریز: <b>{pay:,}</b> تومان (اعتبار شما {amount:,} تومان ثبت می‌شود)\n" if pay != amount else ""
    note = pay_note(amount, pay) + note
    out = (
        "💳 <b>درخواست شارژ کیف پول</b>\n"
        f"💰 مبلغ شارژ: {amount:,} تومان\n"
        + pay_line
        + bonus_line
        + "\n"
        "لطفاً مبلغ را به شماره کارت زیر واریز نمایید:\n"
        f"💳 <code>{esc(pick_card(target.from_user.id)[0])}</code>\n"
        f"به نام: {esc(pick_card(target.from_user.id)[1])}\n\n"
        "پس از واریز، روی دکمه زیر کلیک کرده و رسید خود را ارسال کنید."
        + note
    )
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="📤 ارسال رسید واریز", callback_data="send_charge_receipt")],
            [b(text="✍️ مبلغ دلخواه دیگه می‌خوام", callback_data="charge_custom")],
            [b(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")],
        ]
    )
    await reply_to(target, out, markup)


@dp.callback_query(F.data == "charge_custom")
async def charge_custom_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PaymentState.waiting_for_custom_amount)
    await edit_or_send(
        call,
        f"✍️ مبلغی که دوست داری شارژ کنی رو به تومان و به‌صورت عدد بفرست (حداقل {get_int('min_charge'):,} تومان):",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ برگشت", callback_data="wallet_menu")]]),
    )


@dp.message(PaymentState.waiting_for_custom_amount, ~F.text.in_(MENU_TEXTS))
async def charge_custom_amount(message: types.Message, state: FSMContext):
    amount = parse_int_input(message.text)
    if amount is None:
        await message.answer("فقط عدد بفرست 🙂")
        return
    if amount < get_int("min_charge") or amount > 100000000:
        await message.answer(f"مبلغ باید بین {get_int('min_charge'):,} تا ۱۰۰,۰۰۰,۰۰۰ تومان باشه.")
        return
    await state.set_state(None)
    await send_charge_instructions(message, state, amount)


@dp.callback_query(F.data.regexp(r"^charge_\d+$"))
async def select_charge_amount_handler(call: types.CallbackQuery, state: FSMContext):
    amount = int(call.data.split("_")[1])
    if amount < get_int("min_charge"):
        await call.answer("این مبلغ کمتر از حداقل شارژ است.", show_alert=True)
        return
    await call.answer()
    await send_charge_instructions(call, state, amount)


@dp.callback_query(F.data == "send_charge_receipt")
async def prompt_charge_receipt_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    if not data.get("charge_amount"):
        await edit_or_send(call, "لطفاً ابتدا مبلغ شارژ را انتخاب کنید.", back_home_markup())
        return
    await state.set_state(PaymentState.waiting_for_charge_receipt)
    await edit_or_send(call, "عکس رسید واریزیت رو همینجا بفرست، خودم می‌رسم بهش 🙌")


async def process_charge_receipt_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    amount = data.get("charge_amount", 0)
    if not amount:
        await state.clear()
        await message.answer("درخواست منقضی شده است. لطفاً دوباره مبلغ شارژ را انتخاب کنید.")
        return

    user_id = message.from_user.id
    media = receipt_media(message)
    photo_id = media.file_id
    logging.info("receipt received from %s", user_id)
    uniq, digest = await receipt_fingerprint(message.bot, media)
    dup = None
    with SessionLocal() as s:
        get_or_create_user(s, user_id)
        dup = safe_find_duplicate(s, uniq, digest)
        if dup is None:
            req = Request(
                user_id=user_id, req_type="charge", plan_name="شارژ کیف پول", status="unanswered",
                created_at=get_current_jalali(), created_iso=iso_now_minute(), receipt_file_id=photo_id,
                price=amount, paid_wallet=False,
                pay_amount=(int(data["chg_amount"]) if data.get("chg_base") == amount and data.get("chg_amount") else None),
            )
            s.add(req)
            s.flush()
            safe_save_fingerprint(s, uniq, digest, user_id, req.id)
            req_id, pay_amt = req.id, req.pay_amount
            s.commit()
    if dup is not None:
        await handle_duplicate_receipt(message, dup)
        return

    await state.clear()
    await message.answer(
        fill_text("t_charge_receipt_ok"),
        reply_markup=status_markup(req_id),
    )
    caption = (
        f"🚨 <b>درخواست جدید شارژ کیف پول</b> (کد #{req_id})\n"
        f"👤 کاربر: {user_label(message.from_user)} ({user_id})\n"
        f"💰 مبلغ: {amount:,} تومان\n"
        + (f"💳 مبلغ واریزی دقیق (با عدد تصادفی): <b>{pay_amt:,}</b> تومان\n" if pay_amt and pay_amt != amount else "")
        + f"⏱ زمان ارسال: {get_current_jalali()}"
    )
    await notify_staff(message.bot, caption, admin_request_markup(req_id), photo=photo_id)


# ---------------------------------------------------------------------------
# سرویس‌های من
# ---------------------------------------------------------------------------
def _my_services_view(user_id):
    with SessionLocal() as s:
        configs = [
            (c.id, c.config_name, c.expires_at, c.service_id)
            for c in s.query(UserConfig).filter(UserConfig.user_id == user_id).all()
        ]
    if not configs:
        return "❌ فعلاً سرویس فعالی نداری.", InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🛒 خرید سرویس", callback_data="main_buy")],
                [InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")],
            ]
        )

    remind = get_int("reminder_days")
    buttons, renew_rows = [], []
    for cfg_id, name, exp, sid in configs:
        suffix, icon, soon = "", "🌐", False
        if exp:
            try:
                left = (date.fromisoformat(exp) - today_iran()).days
                suffix = f" ({left} روز)" if left >= 0 else " (منقضی)"
                soon = left <= remind
            except ValueError:
                pass
        if soon:
            icon = "⚠️"
        buttons.append(InlineKeyboardButton(text=f"{icon} {name}{suffix}", callback_data=f"view_cfg_{cfg_id}"))
        if soon and sid:
            renew_rows.append(
                [InlineKeyboardButton(text=f"🔄 تمدید سریع: {name}"[:60], callback_data=f"renew_{cfg_id}")]
            )
    rows = [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
    rows.extend(renew_rows)
    rows.append([InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")])
    return (
        "📦 <b>سرویس‌های تو</b> (برای دیدن جزئیات و لینک، روی سرویس بزن 👇):",
        InlineKeyboardMarkup(inline_keyboard=rows),
    )


@dp.message(KbBtn("my"))
async def my_services_handler(message: types.Message, state: FSMContext):
    await state.clear()
    out, markup = _my_services_view(message.from_user.id)
    await message.answer(out, reply_markup=markup)


@dp.callback_query(F.data == "my_services_callback")
async def my_services_callback_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    out, markup = _my_services_view(call.from_user.id)
    await edit_or_send(call, out, markup)


@dp.callback_query(F.data.startswith("view_cfg_"))
async def view_config_detail_handler(call: types.CallbackQuery):
    cfg_id = int(call.data.split("_")[2])
    with SessionLocal() as s:
        cfg = s.get(UserConfig, cfg_id)
        if cfg is None or cfg.user_id != call.from_user.id:
            await call.answer("سرویس یافت نشد.", show_alert=True)
            return
        name, details, exp, sid = cfg.config_name, cfg.config_details, cfg.expires_at, cfg.service_id

    await call.answer()
    usage = None
    sub = find_sub_url(details) if get_bool("usage_on") else None
    if sub:
        usage = await fetch_usage(sub, timeout=6)
        if usage.get("ok") and usage.get("has_info") and get_bool("usage_sync_expiry_on"):
            new_exp = sync_real_expiry(cfg_id, usage)
            if new_exp:
                exp = new_exp

    out = f"📦 <b>{esc(name or '')}</b>\n"
    left = None
    if exp:
        try:
            left = (date.fromisoformat(exp) - today_iran()).days
        except ValueError:
            left = None
    if left is not None:
        if left < 0:
            status = "🔴 منقضی شده"
        elif usage_is_finished(usage):
            status = "🔴 حجم تموم شده"
        elif left <= get_int("reminder_days"):
            status = "🟡 نزدیک به پایان"
        else:
            status = "🟢 فعال"
        total = max(30, left)
        filled = 0 if left <= 0 else max(1, round(10 * min(left, total) / total))
        out += f"{status}\n{'▰' * filled}{'▱' * (10 - filled)}\n"
        if left >= 0:
            out += f"⏳ {left} روز مانده | 📅 پایان: {jalali_from_iso(exp)}\n"
        else:
            out += f"📅 پایان: {jalali_from_iso(exp)}\n"
    ul = usage_lines(usage)
    if ul:
        out += "\n" + ul
    out += f"\n🔗 <b>ساب‌لینک / کانفیگ</b> (برای کپی، روی متن بزن):\n<code>{esc((details or '')[:3000])}</code>"
    ctx = {"id": cfg_id, "sid": sid, "has_link": bool(LINK_RE.search(details or ""))}
    rows = ub_build_rows("svc", call.from_user.id, ctx)
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=rows))


# ---------------------------------------------------------------------------
# حساب کاربری، تاریخچه، دعوت دوستان
# ---------------------------------------------------------------------------
def _account_view(user_id):
    with SessionLocal() as s:
        user = s.get(User, user_id)
        balance = (user.balance if user else 0) or 0
        count = (user.purchase_count_current_month if user else 0) or 0
        has_monthly = bool(user and user.has_monthly_discount)
        active_count = s.query(func.count(UserConfig.id)).filter(UserConfig.user_id == user_id).scalar()

    tier = tier_for_count(count) if has_monthly else None
    next_need = next((n for n, _k, _l in reversed(LOYALTY_TIERS) if n > count), None)
    if tier:
        status_text = f"سطح {tier[1]} ({tier[2]}٪ تخفیف)"
        if next_need:
            status_text += f" - تا سطح بعد {next_need - count} خرید دیگر"
    else:
        status_text = f"⏳ {count} خرید در این ماه (برای سطح برنزی ۲ خرید لازم است)"

    out = (
        "👤 <b>حساب کاربری</b>\n\n"
        f"🆔 آیدی عددی: <code>{user_id}</code>\n"
        f"💳 موجودی کیف پول: {balance:,} تومان\n"
        f"🟢 تعداد سرویس‌های فعال: {active_count}\n"
        f"🎁 وضعیت آفر وفاداری: {status_text}"
    )
    markup = InlineKeyboardMarkup(inline_keyboard=ub_build_rows("account", user_id))
    return out, markup


@dp.message(KbBtn("account"))
async def account_handler(message: types.Message, state: FSMContext):
    await state.clear()
    out, markup = _account_view(message.from_user.id)
    await message.answer(out, reply_markup=markup)


@dp.callback_query(F.data == "back_to_account")
async def back_to_account_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    out, markup = _account_view(call.from_user.id)
    await edit_or_send(call, out, markup)


TYPE_LABELS = {"buy": "خرید", "renew": "تمدید", "charge": "شارژ", "trial": "تست"}


def render_history(user_id, limit=15):
    with SessionLocal() as s:
        reqs = [
            (r.id, r.req_type, r.plan_name, r.status, r.created_at, r.price or 0)
            for r in s.query(Request).filter(Request.user_id == user_id).order_by(Request.id.desc()).limit(limit).all()
        ]
    lines = []
    for req_id, rtype, plan, status, created, price in reqs:
        emoji = {"answered": "✅", "rejected": "❌"}.get(status, "⏳")
        lines.append(
            f"🔹 #{req_id} | {TYPE_LABELS.get(rtype, rtype or '')} | {esc(plan or '')} | {price:,} | {emoji}\n"
            f"⏱ {created}\n-----------------------------------"
        )
    return lines


@dp.callback_query(F.data == "tx_history")
async def transaction_history_handler(call: types.CallbackQuery):
    lines = render_history(call.from_user.id)
    if not lines:
        await call.answer("هیچ فاکتور یا درخواستی ثبت نشده است.", show_alert=True)
        return
    await call.answer()
    out = "📜 <b>تاریخچه تراکنش‌ها و درخواست‌های شما:</b>\n\n" + "\n".join(lines)
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="⬅️ برگشت به حساب کاربری", callback_data="back_to_account")],
            [b(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")],
        ]
    )
    await edit_or_send(call, out, markup)


@dp.callback_query(F.data.startswith("order_status_"))
async def order_status_handler(call: types.CallbackQuery):
    try:
        rid = int(call.data.split("_")[2])
    except (IndexError, ValueError):
        await call.answer()
        return
    with SessionLocal() as s:
        r = s.get(Request, rid)
        if r is None or r.user_id != call.from_user.id:
            await call.answer("سفارش یافت نشد.", show_alert=True)
            return
        status, plan, created, rtype = r.status, r.plan_name, r.created_at, r.req_type
    await call.answer()
    pending = status not in ("answered", "rejected")
    if status == "answered":
        line = "✅ انجام شد"
    elif status == "rejected":
        line = "❌ رد شد؛ در صورت نیاز از بخش پشتیبانی پیگیری کنید"
    else:
        line = "⏳ در انتظار بررسی"
    out = (
        f"📋 <b>سفارش #{rid}</b> ({TYPE_LABELS.get(rtype, rtype or '')})\n"
        f"📦 {esc(plan or '')}\n🔹 وضعیت: {line}\n⏱ ثبت: {created}\n"
        f"🔄 آخرین بررسی: {get_current_jalali()}"
    )
    if pending:
        out += "\n\n" + pending_notice()
    b = InlineKeyboardButton
    rows = []
    if pending:
        rows.append([b(text="🔄 بروزرسانی وضعیت", callback_data=f"order_status_{rid}")])
    rows.append([b(text="📦 سرویس‌های من", callback_data="my_services_callback")])
    rows.append([b(text="🏠 منوی اصلی", callback_data="main_menu")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=rows))


@dp.callback_query(F.data == "trial_request")
async def trial_request_handler(call: types.CallbackQuery):
    uid = call.from_user.id
    if not get_bool("trial_on"):
        await call.answer("سرویس تست فعلاً غیرفعال است.", show_alert=True)
        return
    with SessionLocal() as s:
        user = get_or_create_user(s, uid)
        if user.trial_used or not user.is_new_user:
            await call.answer("سرویس تست فقط یک‌بار و برای کاربران بدون خرید فعال است.", show_alert=True)
            return
        user.trial_used = True
        req = Request(
            user_id=uid, req_type="trial", plan_name="🎁 سرویس تست رایگان", status="unanswered",
            created_at=get_current_jalali(), created_iso=iso_now_minute(), receipt_file_id="None",
            price=0, paid_wallet=False,
        )
        s.add(req)
        s.flush()
        req_id = req.id
        s.commit()
    await call.answer()
    await edit_or_send(call, "🎁 درخواست سرویس تست شما ثبت شد.\n\n" + pending_notice(), status_markup(req_id))
    await notify_staff(
        call.bot,
        f"🎁 درخواست سرویس تست (کد #{req_id})\n👤 کاربر: {user_label(call.from_user)} ({uid})\n"
        f"⏱ زمان: {get_current_jalali()}",
        admin_request_markup(req_id),
    )


@dp.callback_query(F.data == "referral_menu")
async def referral_menu_handler(call: types.CallbackQuery):
    await call.answer()
    if not get_bool("referral_on"):
        await edit_or_send(call, "سیستم دعوت دوستان فعلاً غیرفعال است.", back_home_markup())
        return
    uid = call.from_user.id
    with SessionLocal() as s:
        invited = s.query(func.count(User.user_id)).filter(User.referred_by == uid).scalar()
        rewarded = (
            s.query(func.count(User.user_id)).filter(User.referred_by == uid, User.referral_rewarded.is_(True)).scalar()
        )
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{uid}" if BOT_USERNAME else "—"
    out = (
        "🎁 <b>دعوت دوستان</b>\n\n"
        f"با هر دوستی که با لینک شما وارد شود و اولین خریدش تایید شود، "
        f"<b>{get_int('referral_reward'):,} تومان</b> به کیف پول شما اضافه می‌شود.\n\n"
        f"🔗 لینک اختصاصی شما:\n<code>{esc(link)}</code>\n\n"
        f"👥 دعوت‌شده‌ها: {invited}\n✅ پاداش گرفته‌شده: {rewarded}"
    )
    if get_bool("ref_tiers_on"):
        tiers = parse_tiers(get_setting("ref_tiers"))
        if tiers:
            out += "\n\n🏅 <b>پاداش پله‌ای (علاوه بر پاداش بالا):</b>\n"
            out += "\n".join(
                f"{'✅' if rewarded >= n else '⬜'} {n} دعوت موفق ← {amt:,} تومان" for n, amt in tiers
            )
            nxt = next((n for n, _a in tiers if n > rewarded), None)
            if nxt:
                out += f"\n🎯 تا پله بعدی: {nxt - rewarded} دعوت دیگر"
    b = InlineKeyboardButton
    rows = []
    if BOT_USERNAME:
        share_text = "با لینک من در Swan VPN ثبت‌نام کن و سرویس بگیر 🎁"
        share_url = f"https://t.me/share/url?url={quote(link, safe='')}&text={quote(share_text, safe='')}"
        rows.append([b(text="📤 ارسال برای دوستان", url=share_url)])
    if get_bool("club_on"):
        rows.append([b(text="🏆 باشگاه مشتریان", callback_data="club_menu")])
    rows.append([b(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=rows))


# ---------------------------------------------------------------------------
# پشتیبانی، سوالات متداول و تیکت (سمت کاربر)
# ---------------------------------------------------------------------------
def support_view():
    support = get_setting("support_info").strip()
    out = "🎧 <b>پشتیبانی Swan VPN</b>\n\n"
    if support:
        out += f"برای ارتباط مستقیم با ادمین به آیدی زیر پیام دهید:\n👉 {esc(support)}\n\n"
    out += "یا از دکمه زیر تیکت بفرستید تا همین‌جا جواب بگیرید."
    markup = InlineKeyboardMarkup(inline_keyboard=ub_build_rows("support"))
    return out, markup


@dp.message(KbBtn("support"))
async def support_handler(message: types.Message, state: FSMContext):
    await state.clear()
    out, markup = support_view()
    await message.answer(out, reply_markup=markup)


@dp.callback_query(F.data == "support_menu")
async def support_callback(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    out, markup = support_view()
    await edit_or_send(call, out, markup)


@dp.callback_query(F.data == "faq_show")
async def faq_show(call: types.CallbackQuery):
    await call.answer()
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="🎫 ارسال تیکت", callback_data="ticket_new")],
            [b(text="⬅️ بازگشت", callback_data="support_menu")],
        ]
    )
    faq = get_setting("faq_text")
    # متن پیش‌فرض HTML است؛ متن ساخته‌شده توسط ادمین escape می‌شود
    await edit_or_send(call, faq if faq == DEFAULT_FAQ else esc(faq), markup)


@dp.callback_query(F.data == "ticket_new")
async def ticket_new(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    with SessionLocal() as s:
        open_count = (
            s.query(func.count(Ticket.id))
            .filter(Ticket.user_id == call.from_user.id, Ticket.status == "open")
            .scalar()
        )
    if open_count >= 3:
        await edit_or_send(call, "شما ۳ تیکت باز دارید. لطفاً تا پاسخ پشتیبانی صبر کنید.", back_home_markup())
        return
    await state.set_state(PaymentState.ticket_waiting_text)
    await state.update_data(ticket_id=None)
    await edit_or_send(call, "✍️ پیام یا مشکل خود را بنویسید (می‌توانید عکس هم بفرستید):")


@dp.callback_query(F.data.startswith("ticket_ureply_"))
async def ticket_user_reply(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    tid = int(call.data.split("_")[2])
    with SessionLocal() as s:
        t = s.get(Ticket, tid)
        if t is None or t.user_id != call.from_user.id:
            return
    await state.set_state(PaymentState.ticket_waiting_text)
    await state.update_data(ticket_id=tid)
    await call.message.answer(f"✍️ پاسخ شما برای تیکت #{tid} را بنویسید:")


_ticket_times = {}  # user_id -> [زمان ساخت تیکت‌های جدید]
TICKET_MAX_PER_HOUR = 5


@dp.message(PaymentState.ticket_waiting_text, ~F.text.in_(MENU_TEXTS))
async def ticket_text_received(message: types.Message, state: FSMContext):
    body = message.caption if message.photo else message.text
    if not body and not message.photo:
        await message.answer("لطفاً متن یا عکس بفرستید.")
        return
    data = await state.get_data()
    tid = data.get("ticket_id")
    if not tid and not is_staff(message.from_user.id):  # محدودیت ساخت تیکت جدید (ضد اسپم)
        now = time.time()
        recent = [t for t in _ticket_times.get(message.from_user.id, []) if now - t < 3600]
        if len(recent) >= TICKET_MAX_PER_HOUR:
            _ticket_times[message.from_user.id] = recent
            await message.answer("⛔ تعداد تیکت‌های ارسالی شما در یک ساعت گذشته زیاد بوده؛ لطفاً کمی بعد دوباره تلاش کنید.")
            return
        recent.append(now)
        _ticket_times[message.from_user.id] = recent
        if len(_ticket_times) > 5000:
            _ticket_times.clear()
    photo_id = message.photo[-1].file_id if message.photo else None
    with SessionLocal() as s:
        if tid:
            t = s.get(Ticket, tid)
            if t is None or t.user_id != message.from_user.id:
                await state.clear()
                return
            t.status = "open"
        else:
            t = Ticket(user_id=message.from_user.id, status="open", created_at=get_current_jalali())
            s.add(t)
            s.flush()
            tid = t.id
        s.add(TicketMessage(ticket_id=tid, sender="user", text=body or "", photo_id=photo_id, created_at=get_current_jalali()))
        s.commit()

    await state.clear()
    await message.answer(f"✅ تیکت #{tid} ثبت شد. پاسخ پشتیبانی همین‌جا برایتان ارسال می‌شود.")

    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                b(text="💬 پاسخ", callback_data=f"ticket_reply_{tid}"),
                b(text="✅ بستن تیکت", callback_data=f"ticket_close_{tid}"),
            ]
        ]
    )
    header = f"🎫 تیکت #{tid} از {user_label(message.from_user)} ({message.from_user.id}):\n\n"
    for cid in staff_ids():
        try:
            await relay(message.bot, cid, message, header, markup)
        except Exception as e:  # noqa: BLE001
            logging.warning("ticket notify failed: %s", e)


# ---------------------------------------------------------------------------
# پنل ادمین: ورود
# ---------------------------------------------------------------------------
@dp.message(Command("admin"), Staff())
async def admin_panel_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        admin_dashboard_text(message.from_user.id), reply_markup=get_admin_main_keyboard(message.from_user.id)
    )


@dp.callback_query(F.data == "admin_home", Staff())
async def admin_home_callback(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    markup = get_admin_main_keyboard(call.from_user.id)
    dash = admin_dashboard_text(call.from_user.id)
    try:
        await call.message.edit_text(dash, reply_markup=markup)
    except TelegramBadRequest:
        await call.message.answer(dash, reply_markup=markup)


# ---------------------------------------------------------------------------
# پنل ادمین: صف درخواست‌ها
# ---------------------------------------------------------------------------
@dp.callback_query(F.data == "admin_unanswered", Staff())
async def admin_unanswered_handler(call: types.CallbackQuery):
    with SessionLocal() as s:
        reqs = [
            (r.id, r.user_id, r.req_type, r.plan_name, r.created_at)
            for r in s.query(Request).filter(Request.status == "unanswered").order_by(Request.id).limit(15).all()
        ]
    if not reqs:
        await call.answer("درخواست پاسخ‌داده‌نشده‌ای وجود ندارد.", show_alert=True)
        return

    await call.answer()
    out = "🔴 <b>درخواست‌های در انتظار پاسخ:</b>\n\n"
    keyboard = []
    for req_id, u_id, rtype, plan, created in reqs:
        out += (
            f"🆔 #{req_id} | {TYPE_LABELS.get(rtype, rtype or '')} | کاربر: {u_id}\n"
            f"📦 {esc(plan or '')} | {created}\n-----------------------------------\n"
        )
        keyboard.append(
            [InlineKeyboardButton(text=f"💬 بررسی درخواست #{req_id}", callback_data=f"admin_detail_{req_id}")]
        )
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data == "admin_answered", Staff())
async def admin_answered_handler(call: types.CallbackQuery):
    with SessionLocal() as s:
        reqs = [
            (r.id, r.user_id, r.req_type, r.plan_name, r.created_at, r.answered_at)
            for r in s.query(Request).filter(Request.status != "unanswered").order_by(Request.id.desc()).limit(10).all()
        ]
    if not reqs:
        await call.answer("درخواست پاسخ‌داده‌شده‌ای یافت نشد.", show_alert=True)
        return

    await call.answer()
    out = "🟢 <b>آخرین درخواست‌های بررسی‌شده:</b>\n\n"
    for req_id, u_id, rtype, plan, created, answered in reqs:
        out += (
            f"🆔 #{req_id} | {TYPE_LABELS.get(rtype, rtype or '')} | کاربر: {u_id}\n"
            f"📦 {esc(plan or '')}\n⏱ ثبت: {created}\n✅ بررسی: {answered}\n"
            "-----------------------------------\n"
        )
    buttons = [InlineKeyboardButton(text=f"💬 #{r[0]}", callback_data=f"admin_detail_{r[0]}") for r in reqs]
    rows = [buttons[i : i + 3] for i in range(0, len(buttons), 3)]
    rows.append([InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=rows))


@dp.callback_query(F.data.startswith("admin_detail_"), Staff())
async def admin_detail_handler(call: types.CallbackQuery):
    req_id = int(call.data.split("_")[2])
    with SessionLocal() as s:
        r = s.get(Request, req_id)
        if r is None:
            await call.answer("درخواست یافت نشد.", show_alert=True)
            return
        u_id, r_type, plan = r.user_id, r.req_type, r.plan_name
        status, created, answered = r.status, r.created_at, r.answered_at
        receipt_id, price, code = r.receipt_file_id, r.price or 0, r.discount_code
        pay_amt = r.pay_amount

    await call.answer()
    status_text = {"answered": "پاسخ داده شده 🟢", "rejected": "رد شده ❌"}.get(status, "در انتظار بررسی 🔴")
    out = (
        f"📋 <b>جزئیات کامل درخواست #{req_id}</b>\n\n"
        f"👤 آیدی کاربر: <code>{u_id}</code>\n"
        f"📦 نوع: {TYPE_LABELS.get(r_type, r_type or '')} - {esc(plan or '')}\n"
        f"💰 مبلغ: {price:,} تومان\n"
    )
    if pay_amt and pay_amt != price:
        out += f"💳 مبلغ واریزی دقیق: {pay_amt:,} تومان\n"
    if code:
        out += f"🏷 کد تخفیف: {esc(code)}\n"
    out += f"📌 وضعیت: {status_text}\n⏱ زمان ثبت: {created}\n"
    if answered:
        out += f"✅ زمان بررسی: {answered}\n"

    b = InlineKeyboardButton
    back_cb = "admin_unanswered" if status == "unanswered" else "admin_answered"
    keyboard = [
        [b(text="⬅️ بازگشت به لیست", callback_data=back_cb)],
        [b(text="🏠 پنل اصلی", callback_data="admin_home")],
    ]
    if status == "unanswered":
        keyboard.insert(
            0,
            [
                b(text="✅ تایید", callback_data=f"admin_confirm_{req_id}"),
                b(text="❌ رد درخواست", callback_data=f"admin_reject_{req_id}"),
            ],
        )
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    try:
        if receipt_id and receipt_id != "None":
            try:
                await call.message.delete()
            except Exception:  # noqa: BLE001
                pass
            await call.bot.send_photo(chat_id=call.message.chat.id, photo=receipt_id, caption=out, reply_markup=markup)
        else:
            await edit_or_send(call, out, markup)
    except Exception as e:  # noqa: BLE001
        logging.error("Error showing request details: %s", e)


def register_purchase(s, user_id, price=0):
    """شمارنده خرید/وفاداری/امتیاز/دعوت را به‌روز می‌کند. (پیام وفاداری؟, (آیدی معرف, مبلغ, پله‌ها) | None)"""
    user = get_or_create_user(s, user_id)
    current_month = jalali_today().strftime("%Y/%m")
    was_new = bool(user.is_new_user)
    count = user.purchase_count_current_month or 0
    same_month = user.last_purchase_month == current_month
    prev_tier = tier_for_count(count) if same_month else None
    new_count = (count + 1) if same_month else 1
    user.is_new_user = False
    user.purchase_count_current_month = new_count
    user.last_purchase_month = current_month
    user.has_monthly_discount = new_count >= 2
    new_tier = tier_for_count(new_count)
    monthly_notif = None
    if new_tier and get_bool("loyalty_on"):
        if prev_tier is None or TIER_RANK[new_tier[0]] > TIER_RANK[prev_tier[0]]:
            monthly_notif = new_tier

    earn = club_points_for(price)
    if earn:
        user.points = (user.points or 0) + earn

    reward = None
    if was_new and user.referred_by and not user.referral_rewarded and get_bool("referral_on"):
        amount = get_int("referral_reward")
        ref = s.get(User, user.referred_by)
        if ref is not None and (amount > 0 or get_bool("ref_tiers_on")):
            user.referral_rewarded = True
            hits = referral_tier_hits(s, ref.user_id)
            ref.balance = (ref.balance or 0) + amount + sum(a for _n, a in hits)
            reward = (ref.user_id, amount, hits)
            inv = get_int("ref_invitee_reward")
            if inv > 0:
                user.balance = min(MAX_BALANCE, (user.balance or 0) + inv)
                _INVITEE_NOTICE[user_id] = inv
    return monthly_notif, reward


async def post_purchase_notifications(bot, user_id, monthly_notif, reward):
    inv_amount = _INVITEE_NOTICE.pop(user_id, 0)
    if inv_amount:
        await gsend(bot, user_id, f"🎁 هدیهٔ دعوت: {inv_amount:,} تومان به کیف پولت اضافه شد. ممنون که با دعوت دوستت اومدی!")
    if monthly_notif:
        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"🔥 <b>تبریک! به سطح {monthly_notif[1]} باشگاه وفاداری رسیدید و "
                f"{monthly_notif[2]}٪ تخفیف وفاداری برای خریدهای بعدی شما فعال شد.</b>",
            )
        except Exception:  # noqa: BLE001
            pass
    if reward:
        base = reward[1]
        hits = reward[2] if len(reward) > 2 else []
        lines = []
        if base > 0:
            lines.append(f"🎉 دوست دعوت‌شده شما اولین خریدش را انجام داد و {base:,} تومان به کیف پول شما اضافه شد!")
        else:
            lines.append("🎉 دوست دعوت‌شده شما اولین خریدش را انجام داد!")
        for n, amt in hits:
            lines.append(f"🏅 به پله {n} دعوت موفق رسیدید: {amt:,} تومان پاداش ویژه به کیف پول شما اضافه شد!")
        try:
            await bot.send_message(chat_id=reward[0], text="\n".join(lines))
        except Exception:  # noqa: BLE001
            pass


async def send_rating_request(bot, user_id, req_id):
    """نظرسنجی ۱ تا ۵ بعد از تحویل سرویس."""
    if not get_bool("rating_on"):
        return
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[b(text=f"{n} ⭐", callback_data=f"rate_{req_id}_{n}") for n in range(1, 6)]]
    )
    try:
        await bot.send_message(
            chat_id=user_id,
            text="⭐ <b>نظر شما برای ما مهم است!</b>\nاز ۱ تا ۵ به تجربه خرید و تحویل سرویس امتیاز بدهید:",
            reply_markup=markup,
        )
    except Exception as e:  # noqa: BLE001
        logging.info("rating request to %s failed: %s", user_id, e)


@dp.callback_query(F.data.regexp(r"^rate_\d+_[1-5]$"))
async def rating_handler(call: types.CallbackQuery):
    _, req_id_s, score_s = call.data.split("_")
    req_id, score = int(req_id_s), int(score_s)
    uid = call.from_user.id
    with SessionLocal() as s:
        r = s.get(Request, req_id)
        if r is None or r.user_id != uid:
            await call.answer("سفارش یافت نشد.", show_alert=True)
            return
        if s.query(Rating).filter(Rating.request_id == req_id).first():
            await call.answer("امتیاز این سفارش قبلاً ثبت شده است.", show_alert=True)
            return
        s.add(Rating(user_id=uid, request_id=req_id, score=score, created_at=get_current_jalali()))
        plan = r.plan_name
        s.commit()

    await call.answer()
    low = score <= get_int("rating_low_max")
    if low:
        b = InlineKeyboardButton
        markup = InlineKeyboardMarkup(inline_keyboard=[[b(text="🎫 ارسال تیکت", callback_data="ticket_new")]])
        await edit_or_send(
            call,
            f"🙏 ممنون از امتیاز شما ({score} ⭐). متأسفیم که تجربه خوبی نداشتید؛ "
            "اگر مشکلی هست از طریق تیکت بگویید تا سریع بررسی کنیم.",
            markup,
        )
        text_ = (
            f"⚠️ <b>امتیاز پایین: {score}/5</b>\n"
            f"👤 کاربر: {user_label(call.from_user)} (<code>{uid}</code>)\n"
            f"📦 سفارش #{req_id}: {esc(plan or '')}\n"
            f"⏱ {get_current_jalali()}"
        )
        for cid in staff_ids():
            markup_admin = None
            if is_main(cid):
                markup_admin = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(text="✉️ پیام به کاربر", callback_data=f"pm_{uid}"),
                            InlineKeyboardButton(text="👤 مدیریت کاربر", callback_data=f"manage_user_{uid}"),
                        ]
                    ]
                )
            try:
                await call.bot.send_message(chat_id=cid, text=text_, reply_markup=markup_admin)
            except Exception as e:  # noqa: BLE001
                logging.warning("low rating notify failed for %s: %s", cid, e)
    else:
        await edit_or_send(call, f"🙏 ممنون از امتیاز شما ({score} ⭐)!")


@dp.callback_query(F.data.startswith("admin_confirm_"), Staff())
async def admin_confirm_action(call: types.CallbackQuery, state: FSMContext):
    req_id = int(call.data.split("_")[2])
    result = None  # برای شارژ و تمدید
    with SessionLocal() as s:
        r = s.get(Request, req_id)
        if r is None:
            await call.answer("درخواست یافت نشد.", show_alert=True)
            return
        if r.status != "unanswered":
            await call.answer("این درخواست قبلاً بررسی شده است.", show_alert=True)
            return
        rtype, uid, plan, price = r.req_type, r.user_id, r.plan_name, r.price or 0

        if rtype == "charge":
            user = get_or_create_user(s, uid)
            bonus, bonus_pct = charge_bonus_for(price)
            user.balance = (user.balance or 0) + price + bonus
            r.status = "answered"
            r.answered_at = get_current_jalali()
            s.commit()
            result = ("charge", uid, price, (bonus, bonus_pct), None)

        elif rtype == "renew":
            cfg = s.get(UserConfig, r.config_id) if r.config_id else None
            if cfg is None:
                await call.answer("سرویس مربوط به این تمدید پیدا نشد. درخواست را رد کنید.", show_alert=True)
                return
            try:
                current = date.fromisoformat(cfg.expires_at) if cfg.expires_at else today_iran()
            except ValueError:
                current = today_iran()
            new_exp = max(current, today_iran()) + timedelta(days=30)
            cfg.expires_at = new_exp.isoformat()
            cfg.reminded = False
            r.status = "answered"
            r.answered_at = get_current_jalali()
            monthly_notif, reward = register_purchase(s, uid, price)
            cfg_id_for_hold = cfg.id
            s.commit()
            set_setting(f"renewhold_{cfg_id_for_hold}", (today_iran() + timedelta(days=3)).isoformat())
            result = ("renew", uid, plan, new_exp.isoformat(), (monthly_notif, reward))
        else:
            sid = r.service_id

    await call.answer()
    markup = get_admin_main_keyboard(call.from_user.id)

    if result is None:  # خرید جدید: متن کانفیگ را از ادمین می‌گیریم
        await state.update_data(target_user_id=uid, plan_name=plan, req_id=req_id, service_id=sid)
        await state.set_state(PaymentState.waiting_for_admin_config)
        await append_status(
            call, "✅ درخواست تایید شد.\n⬇️ متن کانفیگ یا ساب‌لینک را برای ارسال به کاربر بفرستید:", None
        )
        return

    kind, uid, a, b_, extra = result
    log_action(call.from_user.id, f"تایید درخواست #{req_id} ({kind})")
    if kind == "charge":
        bonus_note = f" (+{b_[0]:,} تومان پاداش شارژ)" if b_ and b_[0] else ""
        await append_status(call, f"✅ وضعیت: واریز تایید شد و حساب کاربر شارژ گردید{bonus_note}.", markup)
        try:
            user_text = fill_text("t_charge_ok", amount=f"{a:,}")
            if b_ and b_[0]:
                user_text += f"\n\n🎁 پاداش شارژ: {b_[0]:,} تومان ({b_[1]}٪) هم به اعتبار شما اضافه شد."
            await call.bot.send_message(chat_id=uid, text=user_text)
            await send_sticker(call.bot, uid, "sticker_charge")
        except Exception as e:  # noqa: BLE001
            logging.error("notify charge failed: %s", e)
    else:
        await append_status(call, "✅ وضعیت: تمدید انجام شد.", markup)
        try:
            await call.bot.send_message(
                chat_id=uid,
                text=fill_text("t_renew_ok", plan=f"<b>{esc(a or '')}</b>", end=jalali_from_iso(b_)),
            )
            await send_sticker(call.bot, uid, "sticker_config")
        except Exception as e:  # noqa: BLE001
            logging.error("notify renew failed: %s", e)
        await post_purchase_notifications(call.bot, uid, extra[0], extra[1])
        await send_rating_request(call.bot, uid, req_id)


@dp.callback_query(F.data.startswith("admin_reject_"), Staff())
async def admin_reject_action(call: types.CallbackQuery):
    req_id = int(call.data.split("_")[2])
    refunded = 0
    with SessionLocal() as s:
        r = s.get(Request, req_id)
        if r is None:
            await call.answer("درخواست یافت نشد.", show_alert=True)
            return
        if r.status != "unanswered":
            await call.answer("این درخواست قبلاً بررسی شده است.", show_alert=True)
            return
        r.status = "rejected"
        r.answered_at = get_current_jalali()
        target_user = r.user_id
        if r.paid_wallet and (r.price or 0) > 0:
            user = get_or_create_user(s, target_user)
            user.balance = (user.balance or 0) + r.price
            refunded = r.price
        release_code(s, r.discount_code, target_user)
        if r.req_type == "trial":
            tu = s.get(User, target_user)
            if tu is not None:
                tu.trial_used = False
        s.commit()

    await call.answer("درخواست رد شد.")
    log_action(call.from_user.id, f"رد درخواست #{req_id}")
    await append_status(call, "❌ وضعیت: درخواست رد شد.", get_admin_main_keyboard(call.from_user.id))
    msg = fill_text("t_rejected")
    if refunded:
        msg += f"\n💰 مبلغ {refunded:,} تومان به کیف پول شما برگشت داده شد."
    try:
        await call.bot.send_message(chat_id=target_user, text=msg)
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# پنل ادمین: مدیریت کاربران
# ---------------------------------------------------------------------------
USERS_PER_PAGE = 10


def users_filter_row(current):
    b = InlineKeyboardButton
    names = [("all", "همه"), ("banned", "🛑 مسدودها"), ("svc", "📦 دارای سرویس")]
    return [
        b(text=("• " if k == current else "") + label, callback_data=f"ausers_{k}_1")
        for k, label in names
    ]


@dp.callback_query(F.data == "admin_users", Perm("users"))
@dp.callback_query(F.data.startswith("ausers_"), Perm("users"))
async def admin_users_list_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    flt, page = "all", 1
    if call.data.startswith("ausers_"):
        _, flt, p = call.data.split("_")
        page = max(1, int(p))
    offset = (page - 1) * USERS_PER_PAGE

    with SessionLocal() as s:
        q = s.query(User)
        if flt == "banned":
            q = q.filter(User.is_banned.is_(True))
        elif flt == "svc":
            q = q.filter(User.user_id.in_(select(UserConfig.user_id)))
        total = q.count()
        users = q.order_by(User.user_id).offset(offset).limit(USERS_PER_PAGE).all()
        rows = [(u.user_id, user_display(u), u.balance or 0, bool(u.is_banned)) for u in users]

    out = f"👥 <b>کاربران ({total} نفر) - صفحه {page}:</b>\n\n"
    keyboard = [users_filter_row(flt)]
    for u_id, name, bal, banned in rows:
        icon = "🛑" if banned else "🟢"
        out += f"{icon} <code>{u_id}</code> | {esc(name)} | {bal:,}\n"
        keyboard.append(
            [InlineKeyboardButton(text=f"👤 {name[:28]}", callback_data=f"manage_user_{u_id}")]
        )
    if not rows:
        out += "کاربری یافت نشد."

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️ قبلی", callback_data=f"ausers_{flt}_{page - 1}"))
    if offset + USERS_PER_PAGE < total:
        nav.append(InlineKeyboardButton(text="بعدی ➡️", callback_data=f"ausers_{flt}_{page + 1}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton(text="🔎 جستجوی کاربر", callback_data="admin_user_search")])
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data == "admin_user_search", Perm("users"))
async def admin_user_search_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PaymentState.searching_user)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data="admin_users")]]
    )
    await edit_or_send(call, "🔎 آیدی عددی، یوزرنیم یا بخشی از اسم کاربر را بفرستید:", markup)


@dp.message(PaymentState.searching_user, Perm("users"))
async def admin_user_search_result(message: types.Message, state: FSMContext):
    q = (message.text or "").strip().lstrip("@")
    if not q:
        await message.answer("متن جستجو را بفرستید.")
        return
    with SessionLocal() as s:
        query = s.query(User)
        if q.isdigit():
            query = query.filter(User.user_id == int(q))
        else:
            like = f"%{q}%"
            query = query.filter(or_(User.username.ilike(like), User.first_name.ilike(like)))
        rows = [(u.user_id, user_display(u), u.balance or 0) for u in query.limit(10).all()]
    await state.clear()

    keyboard = []
    out = f"🔎 نتیجه جستجو برای «{esc(q)}»:\n\n"
    for u_id, name, bal in rows:
        out += f"<code>{u_id}</code> | {esc(name)} | {bal:,}\n"
        keyboard.append([InlineKeyboardButton(text=f"👤 {name[:28]}", callback_data=f"manage_user_{u_id}")])
    if not rows:
        out += "کاربری پیدا نشد."
    keyboard.append([InlineKeyboardButton(text="🔙 لیست کاربران", callback_data="admin_users")])
    await message.answer(out, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


async def _render_user_management(target, user_id: int):
    with SessionLocal() as s:
        user = s.get(User, user_id)
        if user is None:
            await reply_to(target, "کاربر یافت نشد.", None)
            return
        name = user_display(user)
        balance = user.balance or 0
        banned = bool(user.is_banned)
        count = user.purchase_count_current_month or 0
        has_monthly = bool(user.has_monthly_discount)
        note = user.note or ""
        referred_by = user.referred_by
        configs = [
            (c.id, c.config_name, c.expires_at)
            for c in s.query(UserConfig).filter(UserConfig.user_id == user_id).all()
        ]

    status_text = "✅ فعال" if has_monthly else f"⏳ {count} خرید این ماه"
    out = (
        f"⚙️ <b>مدیریت کاربر <code>{user_id}</code></b>\n"
        f"👤 {esc(name)}\n\n"
        f"💳 موجودی فعلی: {balance:,} تومان\n"
        f"📋 وضعیت دسترسی: {'🔴 مسدود (بن)' if banned else '🟢 باز'}\n"
        f"🎁 آفر وفاداری: {status_text}\n"
    )
    if referred_by:
        out += f"🔗 دعوت‌شده توسط: <code>{referred_by}</code>\n"
    if note:
        out += f"📝 یادداشت: {esc(note)}\n"
    out += "\n📦 <b>کانفیگ‌های فعال کاربر:</b>\n"

    keyboard = []
    for cfg_id, cfg_name, exp in configs:
        exp_txt = f" - تا {jalali_from_iso(exp)}" if exp else ""
        out += f"🔹 {esc(cfg_name or '')}{exp_txt} (ID: {cfg_id})\n"
        keyboard.append(
            [
                InlineKeyboardButton(text=f"🗑 حذف #{cfg_id}", callback_data=f"del_cfg_user_{cfg_id}_{user_id}"),
                InlineKeyboardButton(text=f"📅 تاریخ #{cfg_id}", callback_data=f"cfgexp_{cfg_id}_{user_id}"),
            ]
        )
    if not configs:
        out += "هیچ کانفیگ فعالی ندارد.\n"

    b = InlineKeyboardButton
    keyboard.extend(
        [
            [
                b(text="✉️ پیام خصوصی", callback_data=f"pm_{user_id}"),
                b(text="💵 موجودی", callback_data=f"balmenu_{user_id}"),
            ],
            [
                b(text="📝 یادداشت", callback_data=f"note_{user_id}"),
                b(text="📜 تاریخچه", callback_data=f"uhist_{user_id}"),
            ],
            [
                b(
                    text="🔓 آزاد سازی کاربر" if banned else "🚫 مسدود کردن (بن) کاربر",
                    callback_data=f"toggle_ban_{user_id}",
                )
            ],
            [b(text="🔙 بازگشت به لیست کاربران", callback_data="admin_users")],
        ]
    )
    await reply_to(target, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data.startswith("manage_user_"), Perm("users"))
async def admin_user_management_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await _render_user_management(call, int(call.data.split("_")[2]))


@dp.callback_query(F.data.startswith("toggle_ban_"), Perm("users"))
async def toggle_ban_action(call: types.CallbackQuery):
    user_id = int(call.data.split("_")[2])
    with SessionLocal() as s:
        user = get_or_create_user(s, user_id)
        user.is_banned = not bool(user.is_banned)
        new_state = user.is_banned
        s.commit()
    _ban_cache.pop(user_id, None)  # اعمال فوری بن/آزادسازی
    log_action(call.from_user.id, f"{'بن' if new_state else 'آزادسازی'} کاربر {user_id}")
    await call.answer("وضعیت کاربر تغییر یافت.", show_alert=True)
    await _render_user_management(call, user_id)


@dp.callback_query(F.data.startswith("del_cfg_user_"), Perm("users"))
async def delete_config_user_action(call: types.CallbackQuery):
    parts = call.data.split("_")
    cfg_id, target_user_id = int(parts[3]), int(parts[4])
    with SessionLocal() as s:
        cfg = s.get(UserConfig, cfg_id)
        if cfg is not None:
            s.delete(cfg)
            s.commit()
    log_action(call.from_user.id, f"حذف کانفیگ {cfg_id} کاربر {target_user_id}")
    await call.answer("کانفیگ کاربر حذف شد.", show_alert=True)
    try:
        await call.bot.send_message(chat_id=target_user_id, text="⚠️ یکی از سرویس‌های شما توسط ادمین حذف شد.")
    except Exception:  # noqa: BLE001
        pass
    await _render_user_management(call, target_user_id)


@dp.callback_query(F.data.startswith("cfgexp_"), Perm("users"))
async def cfg_expiry_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    _, cfg_id, uid = call.data.split("_")
    await state.set_state(PaymentState.editing_expiry)
    await state.update_data(exp_cfg=int(cfg_id), exp_uid=int(uid))
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"manage_user_{uid}")]]
    )
    await edit_or_send(
        call,
        f"📅 تاریخ پایان سرویس #{cfg_id} را تنظیم کنید:\n\n"
        "• یک عدد = چند روز از امروز، مثلاً <code>30</code>\n"
        "• یا تاریخ شمسی، مثلاً <code>1405/09/15</code>",
        markup,
    )


def parse_expiry_input(raw):
    """عدد (روز از امروز) یا تاریخ شمسی YYYY/MM/DD → date میلادی یا None."""
    t = (raw or "").strip().translate(DIGITS_FA)
    if re.fullmatch(r"\d{1,4}", t):
        days = int(t)
        return today_iran() + timedelta(days=days) if days <= 3650 else None
    m = re.fullmatch(r"(\d{4})/(\d{1,2})/(\d{1,2})", t)
    if m:
        try:
            return jdatetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3))).togregorian()
        except (ValueError, OverflowError):
            return None
    return None


@dp.message(PaymentState.editing_expiry, Perm("users"))
async def cfg_expiry_save(message: types.Message, state: FSMContext):
    new_date = parse_expiry_input(message.text)
    if new_date is None:
        await message.answer("ورودی نامعتبر است. مثال: 30 یا 1405/09/15")
        return
    data = await state.get_data()
    cfg_id, uid = data.get("exp_cfg"), data.get("exp_uid")
    with SessionLocal() as s:
        cfg = s.get(UserConfig, cfg_id)
        if cfg is None:
            await state.clear()
            await message.answer("سرویس یافت نشد.")
            return
        cfg.expires_at = new_date.isoformat()
        cfg.reminded = False
        s.commit()
    await state.clear()
    log_action(message.from_user.id, f"تنظیم تاریخ انقضای کانفیگ {cfg_id} به {new_date.isoformat()}")
    try:
        await message.bot.send_message(
            chat_id=uid, text=f"📅 تاریخ پایان یکی از سرویس‌های شما به {jalali_from_iso(new_date.isoformat())} تغییر کرد."
        )
    except Exception:  # noqa: BLE001
        pass
    await _render_user_management(message, uid)


@dp.callback_query(F.data.startswith("uhist_"), Perm("users"))
async def admin_user_history(call: types.CallbackQuery):
    uid = int(call.data.split("_")[1])
    lines = render_history(uid, 15)
    if not lines:
        await call.answer("این کاربر تراکنشی ندارد.", show_alert=True)
        return
    await call.answer()
    out = f"📜 <b>تاریخچه کاربر <code>{uid}</code>:</b>\n\n" + "\n".join(lines)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data=f"manage_user_{uid}")]]
    )
    await edit_or_send(call, out, markup)


# --- موجودی ---
@dp.callback_query(F.data.startswith("balmenu_"), Perm("balance"))
async def balance_menu(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    uid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        user = s.get(User, uid)
        balance = (user.balance if user else 0) or 0
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="➕ افزایش", callback_data=f"baladd_{uid}"), b(text="➖ کاهش", callback_data=f"balsub_{uid}")],
            [b(text="🎯 تنظیم مبلغ دقیق", callback_data=f"balset_{uid}")],
            [b(text="🔙 بازگشت", callback_data=f"manage_user_{uid}")],
        ]
    )
    await edit_or_send(call, f"💵 موجودی فعلی کاربر <code>{uid}</code>: {balance:,} تومان\nچه کاری انجام شود؟", markup)


@dp.callback_query(F.data.regexp(r"^bal(add|sub|set)_\d+$"), Perm("balance"))
async def balance_mode(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    mode, uid = call.data[:6], int(call.data.split("_")[1])
    mode = mode.rstrip("_")
    await state.set_state(PaymentState.adjusting_balance)
    await state.update_data(target_adjust_user=uid, mode=mode)
    label = {"baladd": "افزایش", "balsub": "کاهش", "balset": "تنظیم دقیق"}[mode]
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"manage_user_{uid}")]]
    )
    await edit_or_send(call, f"💵 مبلغ ({label}) را برای کاربر <code>{uid}</code> به عدد وارد کنید:", markup)


@dp.message(PaymentState.adjusting_balance, Perm("balance"))
async def process_balance_adjustment(message: types.Message, state: FSMContext):
    data = await state.get_data()
    uid, mode = data.get("target_adjust_user"), data.get("mode")
    raw = (message.text or "").replace(",", "").strip()
    if not uid or not raw.isdigit():
        await message.answer("مقدار وارد شده نامعتبر است. لطفاً فقط عدد وارد کنید.")
        return
    amount = int(raw)
    if amount > MAX_BALANCE:
        await message.answer(f"مبلغ بیش از حد مجاز است (حداکثر {MAX_BALANCE:,} تومان).")
        return

    with SessionLocal() as s:
        user = get_or_create_user(s, uid)
        current = user.balance or 0
        if mode == "baladd":
            new_balance = current + amount
            if new_balance > MAX_BALANCE:
                await message.answer(f"موجودی نهایی نباید از {MAX_BALANCE:,} تومان بیشتر شود.")
                return
        elif mode == "balsub":
            if amount > current:
                await message.answer(f"مبلغ بیشتر از موجودی فعلی ({current:,}) است.")
                return
            new_balance = current - amount
        else:
            new_balance = amount
        user.balance = new_balance
        s.commit()

    await state.clear()
    log_action(message.from_user.id, f"تغییر موجودی کاربر {uid}: {current:,} -> {new_balance:,}")
    if not is_main(message.from_user.id):  # هر تغییر موجودی توسط ادمین فرعی به ادمین اصلی اطلاع داده می‌شود
        try:
            await message.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"⚠️ ادمین فرعی <code>{message.from_user.id}</code> موجودی کاربر <code>{uid}</code> را "
                f"از {current:,} به {new_balance:,} تومان تغییر داد.",
            )
        except Exception:  # noqa: BLE001
            pass
    await message.answer(
        f"✅ موجودی کاربر <code>{uid}</code> از {current:,} به {new_balance:,} تغییر یافت.",
        reply_markup=get_admin_main_keyboard(message.from_user.id),
    )
    try:
        await message.bot.send_message(
            chat_id=uid, text=f"💰 موجودی کیف پول شما توسط پشتیبانی تغییر کرد.\nموجودی فعلی: {new_balance:,} تومان"
        )
    except Exception:  # noqa: BLE001
        pass


# --- پیام خصوصی و یادداشت ---
@dp.callback_query(F.data.startswith("pm_"), Perm("users"))
async def private_message_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    uid = int(call.data.split("_")[1])
    await state.set_state(PaymentState.private_message)
    await state.update_data(pm_target=uid)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"manage_user_{uid}")]]
    )
    await edit_or_send(call, f"✉️ پیامی که می‌خواهید از طرف ربات برای کاربر <code>{uid}</code> ارسال شود را بفرستید:", markup)


@dp.message(PaymentState.private_message, Perm("users"))
async def private_message_send(message: types.Message, state: FSMContext):
    data = await state.get_data()
    uid = data.get("pm_target")
    await state.clear()
    try:
        await message.copy_to(chat_id=uid)
        await message.answer("✅ پیام ارسال شد.", reply_markup=get_admin_main_keyboard(message.from_user.id))
        log_action(message.from_user.id, f"پیام خصوصی به کاربر {uid}")
    except Exception as e:  # noqa: BLE001
        await message.answer(f"❌ ارسال ناموفق بود: {err_text(e)}")


@dp.callback_query(F.data.startswith("note_"), Perm("users"))
async def note_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    uid = int(call.data.split("_")[1])
    await state.set_state(PaymentState.editing_note)
    await state.update_data(note_target=uid)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"manage_user_{uid}")]]
    )
    await edit_or_send(call, "📝 یادداشت خصوصی را بفرستید (فقط ادمین می‌بیند). برای پاک کردن، - بفرستید:", markup)


@dp.message(PaymentState.editing_note, Perm("users"))
async def note_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    uid = data.get("note_target")
    value = (message.text or "").strip()
    with SessionLocal() as s:
        user = get_or_create_user(s, uid)
        user.note = "" if value == "-" else value
        s.commit()
    await state.clear()
    await message.answer("✅ یادداشت ذخیره شد.", reply_markup=get_admin_main_keyboard(message.from_user.id))


# ---------------------------------------------------------------------------
# پنل ادمین: مدیریت پلن‌ها
# ---------------------------------------------------------------------------
@dp.callback_query(F.data == "admin_plans", Perm("plans"))
async def admin_plans_list(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    cmap = category_map()
    with SessionLocal() as s:
        order = {c.key: i for i, c in enumerate(s.query(Category).order_by(Category.sort_order, Category.id).all())}
        rows = [(r.id, r.category, r.name, r.price) for r in s.query(Service).order_by(Service.id).all()]
    rows.sort(key=lambda r: (order.get(r[1], 999), r[0]))
    out = "📦 <b>مدیریت پلن‌ها</b>\n\nروی پلن بزنید تا ویرایش، انتقال یا حذف شود:\n"
    keyboard = []
    for sid, cat, name, price in rows:
        keyboard.append(
            [InlineKeyboardButton(text=f"{cat_label(cat, cmap, emoji=True)} | {name} | {price:,}"[:60], callback_data=f"plan_{sid}")]
        )
    if not rows:
        out += "\nهنوز پلنی ثبت نشده است."
    keyboard.append(
        [
            InlineKeyboardButton(text="➕ افزودن پلن", callback_data="plan_add"),
            InlineKeyboardButton(text="📂 مدیریت دسته‌ها", callback_data="acats"),
        ]
    )
    keyboard.append([InlineKeyboardButton(text="✨ تنظیم پلن دلخواه (حجم/روز/قیمت)", callback_data="spage_custom")])
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data.regexp(r"^plan_\d+$"), Perm("plans"))
async def admin_plan_view(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    sid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        svc = s.get(Service, sid)
        if svc is None:
            await edit_or_send(call, "پلن یافت نشد.")
            return
        cat, name, price = svc.category, svc.name, svc.price
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="💰 تغییر قیمت", callback_data=f"planprice_{sid}"), b(text="✏️ تغییر نام", callback_data=f"planname_{sid}")],
            [b(text="📂 انتقال به دسته دیگر", callback_data=f"planmove_{sid}")],
            [b(text="🗑 حذف پلن", callback_data=f"plandel_{sid}")],
            [b(text="🔙 بازگشت", callback_data="admin_plans")],
        ]
    )
    await edit_or_send(
        call,
        f"📦 <b>{esc(name)}</b>\nدسته: {esc(cat_label(cat, emoji=True))}\nقیمت: {price:,} تومان",
        markup,
    )


@dp.callback_query(F.data.regexp(r"^planmove_\d+$"), Perm("plans"))
async def plan_move_start(call: types.CallbackQuery):
    await call.answer()
    sid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        cats = [(c.id, c.label, c.emoji or "") for c in s.query(Category).order_by(Category.sort_order, Category.id).all()]
    b = InlineKeyboardButton
    keyboard = [[b(text=f"{em} {label}".strip(), callback_data=f"planmoveto_{sid}_{cid}")] for cid, label, em in cats]
    keyboard.append([b(text="🔙 بازگشت", callback_data=f"plan_{sid}")])
    await edit_or_send(call, "پلن به کدام دسته منتقل شود؟", InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data.startswith("planmoveto_"), Perm("plans"))
async def plan_move_do(call: types.CallbackQuery, state: FSMContext):
    _, sid_s, cid_s = call.data.split("_")
    with SessionLocal() as s:
        svc, cat = s.get(Service, int(sid_s)), s.get(Category, int(cid_s))
        if svc is None or cat is None:
            await call.answer("پلن یا دسته یافت نشد.", show_alert=True)
            return
        svc.category = cat.key
        name, label = svc.name, cat.label
        s.commit()
    log_action(call.from_user.id, f"انتقال پلن {name} به دسته {label}")
    await admin_plan_view(call, state)


@dp.callback_query(F.data.startswith("planprice_"), Perm("plans"))
async def plan_price_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    sid = int(call.data.split("_")[1])
    await state.set_state(PaymentState.editing_plan_price)
    await state.update_data(plan_id=sid)
    await edit_or_send(call, "قیمت جدید را به تومان و به عدد بفرستید:")


@dp.message(PaymentState.editing_plan_price, Perm("plans"))
async def plan_price_save(message: types.Message, state: FSMContext):
    raw = (message.text or "").replace(",", "").strip()
    if not raw.isdigit() or int(raw) <= 0:
        await message.answer("عدد معتبر بفرستید.")
        return
    data = await state.get_data()
    with SessionLocal() as s:
        svc = s.get(Service, data.get("plan_id"))
        if svc is None:
            await state.clear()
            await message.answer("پلن یافت نشد.")
            return
        old = svc.price
        svc.price = int(raw)
        name = svc.name
        s.commit()
    await state.clear()
    log_action(message.from_user.id, f"قیمت پلن {name}: {old:,} -> {int(raw):,}")
    await message.answer("✅ قیمت به‌روز شد.", reply_markup=get_admin_main_keyboard(message.from_user.id))


@dp.callback_query(F.data.startswith("planname_"), Perm("plans"))
async def plan_name_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    sid = int(call.data.split("_")[1])
    await state.set_state(PaymentState.editing_plan_name)
    await state.update_data(plan_id=sid)
    await edit_or_send(call, "نام جدید پلن را بفرستید:")


@dp.message(PaymentState.editing_plan_name, Perm("plans"))
async def plan_name_save(message: types.Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name or len(name) > 60:
        await message.answer("نام معتبر (حداکثر ۶۰ کاراکتر) بفرستید.")
        return
    data = await state.get_data()
    with SessionLocal() as s:
        svc = s.get(Service, data.get("plan_id"))
        if svc is None:
            await state.clear()
            await message.answer("پلن یافت نشد.")
            return
        svc.name = name
        s.commit()
    await state.clear()
    log_action(message.from_user.id, f"تغییر نام پلن به {name}")
    await message.answer("✅ نام پلن به‌روز شد.", reply_markup=get_admin_main_keyboard(message.from_user.id))


@dp.callback_query(F.data.startswith("plandel_"), Perm("plans"))
async def plan_delete_ask(call: types.CallbackQuery):
    await call.answer()
    sid = int(call.data.split("_")[1])
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="✅ بله، حذف شود", callback_data=f"plandelok_{sid}"), b(text="انصراف", callback_data=f"plan_{sid}")]
        ]
    )
    await edit_or_send(call, "⚠️ مطمئنید این پلن حذف شود؟ (کانفیگ‌های قبلی کاربران حفظ می‌شوند)", markup)


@dp.callback_query(F.data.startswith("plandelok_"), Perm("plans"))
async def plan_delete_do(call: types.CallbackQuery):
    sid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        svc = s.get(Service, sid)
        name = svc.name if svc else str(sid)
        if svc is not None:
            s.delete(svc)
            s.commit()
    log_action(call.from_user.id, f"حذف پلن {name}")
    await admin_plans_list(call, _NoState())


class _NoState:
    """جایگزین سبک برای فراخوانی داخلی admin_plans_list بدون FSM."""

    async def clear(self):
        return None


@dp.callback_query(F.data == "plan_add", Perm("plans"))
async def plan_add_start(call: types.CallbackQuery):
    await call.answer()
    b = InlineKeyboardButton
    with SessionLocal() as s:
        cats = [(c.key, c.label, c.emoji or "") for c in s.query(Category).order_by(Category.sort_order, Category.id).all()]
    keyboard = [[b(text=f"{em} {label}".strip(), callback_data=f"planaddcat_{k}")] for k, label, em in cats]
    keyboard.append([b(text="📂 ساخت دسته جدید", callback_data="acatadd")])
    keyboard.append([b(text="🔙 بازگشت", callback_data="admin_plans")])
    await edit_or_send(call, "دسته‌بندی پلن جدید را انتخاب کنید:", InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data.startswith("planaddcat_"), Perm("plans"))
async def plan_add_cat(call: types.CallbackQuery, state: FSMContext):
    cat = call.data.split("_", 1)[1]
    with SessionLocal() as s:
        exists = s.query(Category).filter(Category.key == cat).first() is not None
    if not exists:
        await call.answer("دسته یافت نشد.", show_alert=True)
        return
    await call.answer()
    await state.set_state(PaymentState.adding_plan)
    await state.update_data(category=cat)
    await edit_or_send(
        call,
        f"پلن جدید برای دسته <b>{esc(cat_label(cat, emoji=True))}</b>\n"
        "نام و قیمت را در دو خط بفرستید:\n\n<code>50 گیگ - ۳۰ روزه\n790000</code>",
    )


@dp.message(PaymentState.adding_plan, Perm("plans"))
async def plan_add_save(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    name, price = None, None
    if "|" in raw:
        n, _, p = raw.rpartition("|")
        name, price = n.strip(), p.replace(",", "").strip()
    else:
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        if len(lines) >= 2:
            name, price = " ".join(lines[:-1]), lines[-1].replace(",", "")
    if not name or not price or not price.isdigit() or int(price) <= 0 or len(name) > 60:
        await message.answer("فرمت نادرست است. نام در یک خط و قیمت (عدد) در خط بعد.")
        return
    data = await state.get_data()
    with SessionLocal() as s:
        s.add(Service(category=data.get("category"), name=name, price=int(price)))
        s.commit()
    await state.clear()
    log_action(message.from_user.id, f"افزودن پلن {name} ({int(price):,})")
    cat_key = data.get("category")
    b = InlineKeyboardButton
    await message.answer(
        f"✅ پلن «{esc(name)}» اضافه شد.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [b(text="➕ پلن دیگر در همین دسته", callback_data=f"planaddcat_{cat_key}")],
                [b(text="📦 لیست پلن‌ها", callback_data="admin_plans")],
            ]
        ),
    )


# ---------------------------------------------------------------------------
# پنل ادمین: مدیریت دسته‌ها
# ---------------------------------------------------------------------------
def parse_category_input(raw):
    """'🔥 ویژه' → ('🔥', 'ویژه')؛ بدون ایموجی → ('', 'ویژه')."""
    raw = (raw or "").strip()
    parts = raw.split(None, 1)
    if len(parts) == 2 and len(parts[0]) <= 4 and not parts[0].isalnum():
        return parts[0], parts[1].strip()
    return "", raw


@dp.callback_query(F.data == "acats", Perm("plans"))
async def admin_cats_list(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    with SessionLocal() as s:
        rows = []
        for c in s.query(Category).order_by(Category.sort_order, Category.id).all():
            n = s.query(func.count(Service.id)).filter(Service.category == c.key).scalar()
            rows.append((c.id, c.label, c.emoji or "", bool(c.is_active), n))
    out = (
        "📂 <b>مدیریت دسته‌ها</b>\n\n"
        "هر دسته یک تب در صفحه خرید کاربر است و پلن‌ها زیر مجموعه آن هستند.\n\n"
    )
    keyboard = []
    for cid, label, emoji, active, n in rows:
        out += f"{'🟢' if active else '⚪️'} {emoji} {esc(label)} — {n} پلن\n"
        keyboard.append([InlineKeyboardButton(text=f"⚙️ {emoji} {label}".strip()[:40], callback_data=f"acat_{cid}")])
    if not rows:
        out += "هنوز دسته‌ای نیست.\n"
    keyboard.append([InlineKeyboardButton(text="➕ دسته جدید", callback_data="acatadd")])
    keyboard.append([InlineKeyboardButton(text="🔙 مدیریت پلن‌ها", callback_data="admin_plans")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


async def _render_cat(call, cid):
    with SessionLocal() as s:
        c = s.get(Category, cid)
        if c is None:
            await reply_to(call, "دسته یافت نشد.", None)
            return
        key, label, emoji, active = c.key, c.label, c.emoji or "", bool(c.is_active)
        n = s.query(func.count(Service.id)).filter(Service.category == key).scalar()
    b = InlineKeyboardButton
    out = (
        f"📂 <b>{esc((emoji + ' ' + label).strip())}</b>\n"
        f"تعداد پلن: {n}\nوضعیت: {'🟢 فعال (برای کاربر نمایش داده می‌شود)' if active else '⚪️ غیرفعال (مخفی از کاربر)'}"
    )
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="➕ افزودن پلن به این دسته", callback_data=f"planaddcat_{key}")],
            [b(text="✏️ تغییر نام", callback_data=f"acatren_{cid}"), b(text="😀 تغییر ایموجی", callback_data=f"acatemo_{cid}")],
            [b(text="⬆️ بالاتر", callback_data=f"acatup_{cid}"), b(text="⬇️ پایین‌تر", callback_data=f"acatdn_{cid}")],
            [b(text="🔴 غیرفعال کن" if active else "🟢 فعال کن", callback_data=f"acattog_{cid}")],
            [b(text="🗑 حذف دسته", callback_data=f"acatdel_{cid}")],
            [b(text="🔙 لیست دسته‌ها", callback_data="acats")],
        ]
    )
    await reply_to(call, out, markup)


@dp.callback_query(F.data.regexp(r"^acat_\d+$"), Perm("plans"))
async def admin_cat_view(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await _render_cat(call, int(call.data.split("_")[1]))


@dp.callback_query(F.data == "acatadd", Perm("plans"))
async def admin_cat_add_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PaymentState.adding_category)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data="acats")]])
    await edit_or_send(
        call,
        "نام دسته جدید را بفرستید. می‌توانید اول اسم یک ایموجی بگذارید:\n\n<code>🔥 ویژه</code>",
        markup,
    )


@dp.message(PaymentState.adding_category, Perm("plans"))
async def admin_cat_add_save(message: types.Message, state: FSMContext):
    emoji, label = parse_category_input(message.text)
    if not label or len(label) > 30:
        await message.answer("نام معتبر (حداکثر ۳۰ کاراکتر) بفرستید.")
        return
    with SessionLocal() as s:
        last = s.query(func.max(Category.sort_order)).scalar() or 0
        c = Category(key=f"tmp{int(now_iran().timestamp() * 1000)}", label=label, emoji=emoji, sort_order=last + 1, is_active=True)
        s.add(c)
        s.flush()
        c.key = f"c{c.id}"
        cid, key = c.id, c.key
        s.commit()
    await state.clear()
    log_action(message.from_user.id, f"ساخت دسته {label}")
    b = InlineKeyboardButton
    await message.answer(
        f"✅ دسته «{esc((emoji + ' ' + label).strip())}» ساخته شد. حالا می‌توانید پلن‌هایش را اضافه کنید:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [b(text="➕ افزودن پلن به این دسته", callback_data=f"planaddcat_{key}")],
                [b(text="📂 تنظیمات دسته", callback_data=f"acat_{cid}")],
                [b(text="📂 لیست دسته‌ها", callback_data="acats")],
            ]
        ),
    )


@dp.callback_query(F.data.regexp(r"^acatren_\d+$"), Perm("plans"))
async def admin_cat_rename_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    cid = int(call.data.split("_")[1])
    await state.set_state(PaymentState.renaming_category)
    await state.update_data(cat_id=cid)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"acat_{cid}")]])
    await edit_or_send(call, "نام جدید دسته را بفرستید (پلن‌های قبلی خراب نمی‌شوند):", markup)


@dp.message(PaymentState.renaming_category, Perm("plans"))
async def admin_cat_rename_save(message: types.Message, state: FSMContext):
    label = (message.text or "").strip()
    if not label or len(label) > 30:
        await message.answer("نام معتبر (حداکثر ۳۰ کاراکتر) بفرستید.")
        return
    data = await state.get_data()
    cid = data.get("cat_id")
    with SessionLocal() as s:
        c = s.get(Category, cid)
        if c is None:
            await state.clear()
            await message.answer("دسته یافت نشد.")
            return
        c.label = label
        s.commit()
    await state.clear()
    log_action(message.from_user.id, f"تغییر نام دسته به {label}")
    await _render_cat(message, cid)


@dp.callback_query(F.data.regexp(r"^acatemo_\d+$"), Perm("plans"))
async def admin_cat_emoji_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    cid = int(call.data.split("_")[1])
    await state.set_state(PaymentState.editing_cat_emoji)
    await state.update_data(cat_id=cid)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"acat_{cid}")]])
    await edit_or_send(call, "یک ایموجی بفرستید. برای حذف، - بفرستید:", markup)


@dp.message(PaymentState.editing_cat_emoji, Perm("plans"))
async def admin_cat_emoji_save(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not raw or len(raw) > 8:
        await message.answer("فقط یک ایموجی بفرستید یا - برای حذف.")
        return
    data = await state.get_data()
    cid = data.get("cat_id")
    with SessionLocal() as s:
        c = s.get(Category, cid)
        if c is None:
            await state.clear()
            await message.answer("دسته یافت نشد.")
            return
        c.emoji = "" if raw == "-" else raw
        s.commit()
    await state.clear()
    await _render_cat(message, cid)


@dp.callback_query(F.data.regexp(r"^acat(up|dn)_\d+$"), Perm("plans"))
async def admin_cat_move(call: types.CallbackQuery):
    up = call.data.startswith("acatup_")
    cid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        cats = s.query(Category).order_by(Category.sort_order, Category.id).all()
        ids = [c.id for c in cats]
        if cid not in ids:
            await call.answer("دسته یافت نشد.", show_alert=True)
            return
        for i, c in enumerate(cats):  # ترتیب‌ها را یکتا و پشت‌سرهم می‌کنیم
            c.sort_order = i
        i = ids.index(cid)
        j = i - 1 if up else i + 1
        if 0 <= j < len(cats):
            cats[i].sort_order, cats[j].sort_order = cats[j].sort_order, cats[i].sort_order
        s.commit()
    await call.answer("ترتیب تغییر کرد.")
    await _render_cat(call, cid)


@dp.callback_query(F.data.regexp(r"^acattog_\d+$"), Perm("plans"))
async def admin_cat_toggle(call: types.CallbackQuery):
    cid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(Category, cid)
        if c is not None:
            c.is_active = not bool(c.is_active)
            s.commit()
    await call.answer()
    await _render_cat(call, cid)


@dp.callback_query(F.data.regexp(r"^acatdel_\d+$"), Perm("plans"))
async def admin_cat_delete_ask(call: types.CallbackQuery):
    await call.answer()
    cid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(Category, cid)
        if c is None:
            await edit_or_send(call, "دسته یافت نشد.")
            return
        label = c.label
        n = s.query(func.count(Service.id)).filter(Service.category == c.key).scalar()
    b = InlineKeyboardButton
    warn = f"⚠️ دسته «{esc(label)}» حذف شود؟"
    if n:
        warn += f"\nاین دسته <b>{n} پلن</b> دارد که همراه آن حذف می‌شوند (کانفیگ‌های قبلی کاربران حفظ می‌شود)."
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[b(text="✅ بله، حذف شود", callback_data=f"acatdelok_{cid}"), b(text="انصراف", callback_data=f"acat_{cid}")]]
    )
    await edit_or_send(call, warn, markup)


@dp.callback_query(F.data.regexp(r"^acatdelok_\d+$"), Perm("plans"))
async def admin_cat_delete_do(call: types.CallbackQuery):
    cid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(Category, cid)
        label = c.label if c else str(cid)
        if c is not None:
            s.query(Service).filter(Service.category == c.key).delete()
            s.delete(c)
            s.commit()
    log_action(call.from_user.id, f"حذف دسته {label}")
    await admin_cats_list(call, _NoState())


# ---------------------------------------------------------------------------
# پنل ادمین: کدهای تخفیف
# ---------------------------------------------------------------------------
@dp.callback_query(F.data == "admin_codes", Perm("codes"))
async def admin_codes_list(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    with SessionLocal() as s:
        rows = [
            (c.id, c.code, c.percent, c.expires_at, c.max_uses, c.used_count, bool(c.is_active))
            for c in s.query(DiscountCode).order_by(DiscountCode.id.desc()).limit(20).all()
        ]
    out = "🏷 <b>کدهای تخفیف</b>\n\n"
    keyboard = []
    for cid, code, pct, exp, mx, used, active in rows:
        exp_txt = jalali_from_iso(exp) if exp else "بدون انقضا"
        mx_txt = str(mx) if mx else "نامحدود"
        out += f"{'🟢' if active else '⚪️'} <code>{esc(code)}</code> | {pct}٪ | {exp_txt} | {used}/{mx_txt}\n"
        keyboard.append([InlineKeyboardButton(text=f"⚙️ {code}", callback_data=f"code_{cid}")])
    if not rows:
        out += "هنوز کدی ساخته نشده است.\n"
    keyboard.append([InlineKeyboardButton(text="➕ ساخت کد جدید", callback_data="code_add")])
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data.regexp(r"^code_\d+$"), Perm("codes"))
async def admin_code_view(call: types.CallbackQuery):
    await call.answer()
    cid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(DiscountCode, cid)
        if c is None:
            await edit_or_send(call, "کد یافت نشد.")
            return
        code, active = c.code, bool(c.is_active)
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="🔴 غیرفعال کن" if active else "🟢 فعال کن", callback_data=f"codetoggle_{cid}")],
            [b(text="🗑 حذف کد", callback_data=f"codedel_{cid}")],
            [b(text="🔙 بازگشت", callback_data="admin_codes")],
        ]
    )
    await edit_or_send(call, f"کد <code>{esc(code)}</code> - وضعیت: {'فعال' if active else 'غیرفعال'}", markup)


@dp.callback_query(F.data.startswith("codetoggle_"), Perm("codes"))
async def admin_code_toggle(call: types.CallbackQuery):
    cid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(DiscountCode, cid)
        if c is not None:
            c.is_active = not bool(c.is_active)
            s.commit()
    await admin_code_view(call)


@dp.callback_query(F.data.startswith("codedel_"), Perm("codes"))
async def admin_code_delete(call: types.CallbackQuery):
    cid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(DiscountCode, cid)
        code = c.code if c else str(cid)
        if c is not None:
            s.query(CodeUse).filter(CodeUse.code_id == cid).delete()
            s.delete(c)
            s.commit()
    log_action(call.from_user.id, f"حذف کد تخفیف {code}")
    await admin_codes_list(call, _NoState())


@dp.callback_query(F.data == "code_add", Perm("codes"))
async def admin_code_add_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PaymentState.creating_code)
    await edit_or_send(
        call,
        "کد جدید را به این شکل بفرستید:\n\n<code>کد درصد [تعداد روز اعتبار] [حداکثر استفاده]</code>\n\n"
        "مثال: <code>SWAN20 20 30 100</code>\n(۰ برای روز یا تعداد یعنی بدون محدودیت)",
    )


@dp.message(PaymentState.creating_code, Perm("codes"))
async def admin_code_add_save(message: types.Message, state: FSMContext):
    parts = (message.text or "").split()
    if len(parts) < 2 or not re.fullmatch(r"[A-Za-z0-9_\-]{3,20}", parts[0]) or not parts[1].isdigit():
        await message.answer("فرمت نادرست است. مثال: SWAN20 20 30 100")
        return
    code, pct = parts[0].upper(), int(parts[1])
    days = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    max_uses = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    if not (1 <= pct <= MAX_DISCOUNT):
        await message.answer(f"درصد باید بین ۱ تا {MAX_DISCOUNT} باشد.")
        return
    with SessionLocal() as s:
        if s.query(DiscountCode).filter(DiscountCode.code == code).first():
            await message.answer("این کد قبلاً ساخته شده است.")
            return
        exp = (today_iran() + timedelta(days=days)).isoformat() if days > 0 else None
        s.add(DiscountCode(code=code, percent=pct, expires_at=exp, max_uses=max_uses, used_count=0, is_active=True))
        s.commit()
    await state.clear()
    log_action(message.from_user.id, f"ساخت کد تخفیف {code} ({pct}٪)")
    await message.answer(f"✅ کد <code>{esc(code)}</code> ساخته شد.", reply_markup=get_admin_main_keyboard(message.from_user.id))


# ---------------------------------------------------------------------------
# پنل ادمین: تیکت‌ها (سمت پشتیبان)
# ---------------------------------------------------------------------------
@dp.callback_query(F.data == "admin_tickets", Staff())
async def admin_tickets_list(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    with SessionLocal() as s:
        rows = [
            (t.id, t.user_id, t.created_at)
            for t in s.query(Ticket).filter(Ticket.status == "open").order_by(Ticket.id.desc()).limit(15).all()
        ]
    out = "🎫 <b>تیکت‌های باز:</b>\n\n"
    keyboard = []
    for tid, uid, created in rows:
        out += f"#{tid} | کاربر {uid} | {created}\n"
        keyboard.append([InlineKeyboardButton(text=f"💬 مشاهده تیکت #{tid}", callback_data=f"ticket_view_{tid}")])
    if not rows:
        out += "تیکت بازی وجود ندارد."
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data.startswith("ticket_view_"), Staff())
async def admin_ticket_view(call: types.CallbackQuery):
    await call.answer()
    tid = int(call.data.split("_")[2])
    with SessionLocal() as s:
        t = s.get(Ticket, tid)
        if t is None:
            await edit_or_send(call, "تیکت یافت نشد.")
            return
        uid, status = t.user_id, t.status
        msgs = [
            (m.sender, m.text, m.photo_id, m.created_at)
            for m in s.query(TicketMessage).filter(TicketMessage.ticket_id == tid).order_by(TicketMessage.id.desc()).limit(8).all()
        ]
    msgs.reverse()
    out = f"🎫 <b>تیکت #{tid}</b> | کاربر <code>{uid}</code> | {'باز' if status == 'open' else 'بسته'}\n\n"
    for sender, body, photo, created in msgs:
        who = "👤 کاربر" if sender == "user" else "🎧 پشتیبان"
        out += f"{who} ({created}):\n{esc(body or '')}{' 📷' if photo else ''}\n\n"
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="💬 پاسخ", callback_data=f"ticket_reply_{tid}"), b(text="✅ بستن", callback_data=f"ticket_close_{tid}")],
            [b(text="🔙 لیست تیکت‌ها", callback_data="admin_tickets")],
        ]
    )
    await edit_or_send(call, out[:4000], markup)


@dp.callback_query(F.data.startswith("ticket_reply_"), Staff())
async def ticket_staff_reply_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    tid = int(call.data.split("_")[2])
    await state.set_state(PaymentState.ticket_staff_reply)
    await state.update_data(ticket_id=tid)
    await call.message.answer(f"✍️ پاسخ خود را برای تیکت #{tid} بنویسید (متن یا عکس):")


@dp.message(PaymentState.ticket_staff_reply, Staff())
async def ticket_staff_reply_send(message: types.Message, state: FSMContext):
    body = message.caption if message.photo else message.text
    if not body and not message.photo:
        await message.answer("لطفاً متن یا عکس بفرستید.")
        return
    data = await state.get_data()
    tid = data.get("ticket_id")
    with SessionLocal() as s:
        t = s.get(Ticket, tid)
        if t is None:
            await state.clear()
            await message.answer("تیکت یافت نشد.")
            return
        uid = t.user_id
        s.add(
            TicketMessage(
                ticket_id=tid, sender="staff", text=body or "",
                photo_id=message.photo[-1].file_id if message.photo else None, created_at=get_current_jalali(),
            )
        )
        s.commit()
    await state.clear()

    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[b(text="↩️ پاسخ به پشتیبانی", callback_data=f"ticket_ureply_{tid}")]]
    )
    try:
        await relay(message.bot, uid, message, f"💬 پاسخ پشتیبانی به تیکت #{tid}:\n\n", markup)
        await message.answer("✅ پاسخ ارسال شد.", reply_markup=get_admin_main_keyboard(message.from_user.id))
        log_action(message.from_user.id, f"پاسخ به تیکت #{tid}")
    except Exception as e:  # noqa: BLE001
        await message.answer(f"❌ ارسال به کاربر ناموفق بود: {err_text(e)}")


@dp.callback_query(F.data.startswith("ticket_close_"), Staff())
async def ticket_close(call: types.CallbackQuery):
    tid = int(call.data.split("_")[2])
    with SessionLocal() as s:
        t = s.get(Ticket, tid)
        if t is None:
            await call.answer("تیکت یافت نشد.", show_alert=True)
            return
        t.status = "closed"
        uid = t.user_id
        s.commit()
    await call.answer("تیکت بسته شد.")
    log_action(call.from_user.id, f"بستن تیکت #{tid}")
    await append_status(call, f"✅ تیکت #{tid} بسته شد.", get_admin_main_keyboard(call.from_user.id))
    try:
        await call.bot.send_message(chat_id=uid, text=f"✅ تیکت #{tid} شما بسته شد. در صورت نیاز تیکت جدید ثبت کنید.")
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# پنل ادمین: آمار، گزارش روزانه، لاگ
# ---------------------------------------------------------------------------
def stats_numbers():
    today = jalali_today().strftime("%Y/%m/%d")
    month = jalali_today().strftime("%Y/%m")
    sale_types = ["buy", "renew"]
    with SessionLocal() as s:
        def revenue(*extra):
            q = s.query(func.coalesce(func.sum(Request.price), 0)).filter(
                Request.status == "answered", Request.req_type.in_(sale_types), *extra
            )
            return int(q.scalar() or 0)

        def sales_count(*extra):
            q = s.query(func.count(Request.id)).filter(
                Request.status == "answered", Request.req_type.in_(sale_types), *extra
            )
            return int(q.scalar() or 0)

        charged_today = int(
            s.query(func.coalesce(func.sum(Request.price), 0))
            .filter(Request.status == "answered", Request.req_type == "charge", Request.answered_at.like(f"{today}%"))
            .scalar() or 0
        )
        return dict(
            today=today, month=month,
            total_users=s.query(func.count(User.user_id)).scalar(),
            new_today=s.query(func.count(User.user_id)).filter(User.joined_at == today_iran().isoformat()).scalar(),
            configs=s.query(func.count(UserConfig.id)).scalar(),
            total_rev=revenue(), month_rev=revenue(Request.answered_at.like(f"{month}%")),
            day_rev=revenue(Request.answered_at.like(f"{today}%")),
            day_sales=sales_count(Request.answered_at.like(f"{today}%")),
            charged_today=charged_today,
            unanswered=s.query(func.count(Request.id)).filter(Request.status == "unanswered").scalar(),
            processed=s.query(func.count(Request.id)).filter(Request.status != "unanswered").scalar(),
            open_tickets=s.query(func.count(Ticket.id)).filter(Ticket.status == "open").scalar(),
            avg_rating=float(s.query(func.avg(Rating.score)).scalar() or 0),
            rating_count=int(s.query(func.count(Rating.id)).scalar() or 0),
        )


@dp.callback_query(F.data == "admin_sales_stats", Perm("stats"))
async def admin_sales_stats_handler(call: types.CallbackQuery):
    await call.answer()
    n = stats_numbers()
    out = (
        "📊 <b>آمار جامع و وضعیت درخواست‌ها:</b>\n\n"
        f"👥 کل کاربران: {n['total_users']} (امروز جدید: {n['new_today']})\n"
        f"📦 کل سرویس‌های فعال: {n['configs']}\n"
        f"💰 درآمد کل: {n['total_rev']:,} تومان\n\n"
        f"📅 <b>ماه جاری ({n['month']}):</b> {n['month_rev']:,} تومان\n"
        f"☀️ <b>امروز ({n['today']}):</b> {n['day_rev']:,} تومان در {n['day_sales']} فروش\n\n"
        f"🔴 در انتظار پاسخ: {n['unanswered']}\n"
        f"🟢 بررسی‌شده: {n['processed']}\n"
        f"🎫 تیکت باز: {n['open_tickets']}\n"
        f"⭐ میانگین امتیاز: {n['avg_rating']:.2f} ({n['rating_count']} نظر)"
    )
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                b(text="📈 نمودار هفتگی", callback_data="sreport_week"),
                b(text="📈 نمودار ماهانه", callback_data="sreport_month"),
            ],
            [b(text="🔙 بازگشت به پنل", callback_data="admin_home")],
        ]
    )
    await edit_or_send(call, out, markup)


SALE_TYPES = ["buy", "renew"]


def _rev_for_prefixes(s, prefixes):
    conds = [Request.answered_at.like(f"{p}%") for p in prefixes]
    total, cnt = (
        s.query(func.coalesce(func.sum(Request.price), 0), func.count(Request.id))
        .filter(Request.status == "answered", Request.req_type.in_(SALE_TYPES), or_(*conds))
        .one()
    )
    return int(total or 0), int(cnt or 0)


def _bar(value, maxv, width=12):
    if maxv <= 0 or value <= 0:
        return "▫️"
    return "█" * max(1, round(value / maxv * width))


def build_sales_report(period):
    today = jalali_today()
    if period == "week":
        title = "۷ روز اخیر"
        buckets = []
        for i in range(6, -1, -1):
            d = jdatetime.date.fromgregorian(date=today_iran() - timedelta(days=i))
            buckets.append((d.strftime("%m/%d"), [d.strftime("%Y/%m/%d")]))
        rank_prefixes = [p for _label, ps in buckets for p in ps]
    else:
        title = "۶ ماه اخیر"
        buckets = []
        idx = today.year * 12 + today.month - 1
        for i in range(5, -1, -1):
            y, m0 = divmod(idx - i, 12)
            buckets.append((f"{y}/{m0 + 1:02d}", [f"{y}/{m0 + 1:02d}"]))
        rank_prefixes = buckets[-1][1]

    with SessionLocal() as s:
        series = []
        for label, ps in buckets:
            total, cnt = _rev_for_prefixes(s, ps)
            series.append((label, total, cnt))
        cond = or_(*[Request.answered_at.like(f"{p}%") for p in rank_prefixes])
        base = (Request.status == "answered", Request.req_type.in_(SALE_TYPES), cond)
        plans = (
            s.query(Request.plan_name, func.count(Request.id), func.coalesce(func.sum(Request.price), 0))
            .filter(*base).group_by(Request.plan_name)
            .order_by(func.sum(Request.price).desc()).limit(5).all()
        )
        custs = (
            s.query(Request.user_id, func.count(Request.id), func.coalesce(func.sum(Request.price), 0))
            .filter(*base).group_by(Request.user_id)
            .order_by(func.sum(Request.price).desc()).limit(5).all()
        )
        names = {}
        if custs:
            for u in s.query(User).filter(User.user_id.in_([c[0] for c in custs])).all():
                names[u.user_id] = user_display(u)

    maxv = max([v for _l, v, _c in series] + [0])
    out = f"📈 <b>نمودار فروش {title}</b>\n\n"
    for label, v, c in series:
        out += f"<code>{label}</code> {_bar(v, maxv)} {v:,} ({c})\n"
    out += f"\n💰 جمع: {sum(v for _l, v, _c in series):,} تومان | 🧾 {sum(c for _l, _v, c in series)} فروش\n"
    scope = "۷ روز اخیر" if period == "week" else "ماه جاری"
    out += f"\n🏆 <b>پرفروش‌ترین پلن‌ها ({scope}):</b>\n"
    for i, (name, c, total) in enumerate(plans, 1):
        out += f"{i}. {esc(name or '')} — {c} فروش | {int(total):,}\n"
    if not plans:
        out += "—\n"
    out += f"\n👑 <b>بهترین مشتری‌ها ({scope}):</b>\n"
    for i, (uid, c, total) in enumerate(custs, 1):
        out += f"{i}. {esc(names.get(uid, str(uid)))} (<code>{uid}</code>) — {c} خرید | {int(total):,}\n"
    if not custs:
        out += "—\n"
    return out


@dp.callback_query(F.data.in_({"sreport_week", "sreport_month"}), Perm("stats"))
async def admin_sales_report(call: types.CallbackQuery):
    await call.answer()
    period = "week" if call.data == "sreport_week" else "month"
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                b(text=("• " if period == "week" else "") + "هفتگی", callback_data="sreport_week"),
                b(text=("• " if period == "month" else "") + "ماهانه", callback_data="sreport_month"),
            ],
            [b(text="🔙 آمار کلی", callback_data="admin_sales_stats")],
        ]
    )
    await edit_or_send(call, build_sales_report(period), markup)


def daily_report_text():
    n = stats_numbers()
    return (
        f"📋 <b>گزارش روزانه {n['today']}</b>\n\n"
        f"💰 فروش امروز: {n['day_sales']} سفارش | {n['day_rev']:,} تومان\n"
        f"💳 شارژ کیف پول امروز: {n['charged_today']:,} تومان\n"
        f"👥 کاربر جدید: {n['new_today']} | کل کاربران: {n['total_users']}\n"
        f"🔴 درخواست در انتظار: {n['unanswered']}\n"
        f"🎫 تیکت باز: {n['open_tickets']}"
    )


@dp.callback_query(F.data == "admin_logs", Perm("stats"))
async def admin_logs_handler(call: types.CallbackQuery):
    await call.answer()
    with SessionLocal() as s:
        rows = [(r.admin_id, r.action, r.created_at) for r in s.query(AdminLog).order_by(AdminLog.id.desc()).limit(20).all()]
    out = "🧾 <b>آخرین فعالیت‌های ادمین‌ها:</b>\n\n"
    for admin_id, action, created in rows:
        out += f"• {created} | <code>{admin_id}</code>\n  {esc(action or '')}\n"
    if not rows:
        out += "هنوز لاگی ثبت نشده است."
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin_home")]]
    )
    await edit_or_send(call, out[:4000], markup)


# ---------------------------------------------------------------------------
# پنل ادمین: پیام همگانی
# ---------------------------------------------------------------------------
BC_SEGMENTS = {
    "all": "👥 همه کاربران",
    "expired": "⌛ سرویس‌شان تمام شده",
    "nobuy": "🆕 بدون هیچ خرید",
    "active": "🟢 دارای سرویس فعال",
    "expiring": "⏰ سرویس‌شان نزدیک انقضاست",
    "lapsed": "😴 بیش از ۳۰ روز خرید نکرده‌اند",
    "new7": "🆕 عضو ۷ روز اخیر",
    "richbal": "💰 موجودی کیف پول بالای ۱۰۰ هزار",
}
DIGITS_FA = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def segment_user_ids(s, seg, promo=True):
    ok = or_(User.is_banned.is_(None), User.is_banned.is_(False))
    not_blocked = or_(User.blocked.is_(None), User.blocked.is_(False))
    q = s.query(User.user_id).filter(ok, not_blocked)
    if promo:
        q = q.filter(or_(User.promo_off.is_(None), User.promo_off.is_(False)))
    base = {r[0] for r in q.all()}
    if seg == "all":
        return sorted(base)
    today_iso = today_iran().isoformat()
    active = {
        r[0]
        for r in s.query(UserConfig.user_id)
        .filter(or_(UserConfig.expires_at.is_(None), UserConfig.expires_at >= today_iso))
        .all()
    }
    if seg == "active":
        return sorted(base & active)
    if seg == "expired":
        expired = {
            r[0] for r in s.query(UserConfig.user_id).filter(UserConfig.expires_at < today_iso).all()
        }
        return sorted((base & expired) - active)
    if seg == "nobuy":
        buyers = {
            r[0]
            for r in s.query(Request.user_id)
            .filter(Request.status == "answered", Request.req_type.in_(SALE_TYPES))
            .all()
        }
        return sorted(base - buyers)
    if seg == "expiring":
        limit = (today_iran() + timedelta(days=max(1, get_int("reminder_days")))).isoformat()
        soon = {
            r[0]
            for r in s.query(UserConfig.user_id)
            .filter(UserConfig.expires_at.isnot(None), UserConfig.expires_at >= today_iso, UserConfig.expires_at <= limit)
            .all()
        }
        return sorted(base & soon)
    if seg == "lapsed":
        cutoff = (now_iran().replace(tzinfo=None) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M")
        ever = {
            r[0]
            for r in s.query(Request.user_id).filter(Request.status == "answered", Request.req_type.in_(SALE_TYPES)).all()
        }
        recent = {
            r[0]
            for r in s.query(Request.user_id)
            .filter(Request.status == "answered", Request.req_type.in_(SALE_TYPES), Request.created_iso >= cutoff)
            .all()
        }
        return sorted((base & ever) - recent)
    if seg == "new7":
        cutoff = (today_iran() - timedelta(days=7)).isoformat()
        fresh = {r[0] for r in s.query(User.user_id).filter(User.joined_at >= cutoff).all()}
        return sorted(base & fresh)
    if seg == "richbal":
        rich = {r[0] for r in s.query(User.user_id).filter(User.balance >= 100000).all()}
        return sorted(base & rich)
    if seg.startswith("cat_"):
        key = seg[4:]
        buyers = {
            r[0]
            for r in s.query(Request.user_id)
            .join(Service, Service.id == Request.service_id)
            .filter(Request.status == "answered", Request.req_type.in_(SALE_TYPES), Service.category == key)
            .all()
        }
        return sorted(base & buyers)
    return []


async def run_broadcast(bot, from_chat, message_id, seg):
    """پیام را با copy_message برای بخش انتخابی می‌فرستد؛ (موفق, بلاک, ناموفق)."""
    with SessionLocal() as s:
        ids = segment_user_ids(s, seg)
    sent = blocked = failed = 0
    for u_id in ids:
        try:
            await bot.copy_message(chat_id=u_id, from_chat_id=from_chat, message_id=message_id)
            sent += 1
        except TelegramForbiddenError:
            blocked += 1
            mark_blocked(u_id)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            try:
                await bot.copy_message(chat_id=u_id, from_chat_id=from_chat, message_id=message_id)
                sent += 1
            except Exception:  # noqa: BLE001
                failed += 1
        except Exception:  # noqa: BLE001
            failed += 1
        await asyncio.sleep(0.05)
    return sent, blocked, failed


async def _broadcast_and_report(bot, report_chat, from_chat, message_id, seg, admin_id, prefix=""):
    sent, blocked, failed = await run_broadcast(bot, from_chat, message_id, seg)
    log_action(admin_id, f"پیام همگانی ({bc_segments().get(seg, seg)}): {sent} موفق")
    try:
        await bot.send_message(
            chat_id=report_chat,
            text=(
                f"{prefix}✅ پیام همگانی ({esc(bc_segments().get(seg, seg))}) ارسال شد!\n\n"
                f"📨 ارسال‌شده به: {sent} کاربر\n🚫 مسدود شده توسط کاربر: {blocked}\n⚠️ ناموفق: {failed}"
            ),
            reply_markup=get_admin_main_keyboard(admin_id),
        )
    except Exception as e:  # noqa: BLE001
        logging.warning("broadcast report failed: %s", e)
    return f"{sent} موفق / {blocked} بلاک / {failed} ناموفق"


def parse_run_time(raw):
    """'HH:MM' (نزدیک‌ترین زمان آینده) یا 'YYYY/MM/DD HH:MM' شمسی. خروجی: datetime ساده به وقت ایران یا None."""
    t = (raw or "").strip().translate(DIGITS_FA)
    now = now_iran().replace(tzinfo=None)
    try:
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", t)
        if m:
            cand = now.replace(hour=int(m.group(1)), minute=int(m.group(2)), second=0, microsecond=0)
            if cand <= now:
                cand += timedelta(days=1)
            return cand
        m = re.fullmatch(r"(\d{4})/(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})", t)
        if m:
            g = jdatetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3))).togregorian()
            cand = datetime(g.year, g.month, g.day, int(m.group(4)), int(m.group(5)))
            return cand if cand > now else None
    except (ValueError, OverflowError):
        return None
    return None


@dp.callback_query(F.data == "admin_broadcast_start", Perm("broadcast"))
async def admin_broadcast_start_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    b = InlineKeyboardButton
    with SessionLocal() as s:
        counts = {k: len(segment_user_ids(s, k)) for k in bc_segments()}
    rows = [[b(text=f"{label} ({counts[k]})", callback_data=f"bcseg_{k}")] for k, label in bc_segments().items()]
    rows.append([b(text="⏰ پیام‌های زمان‌بندی‌شده", callback_data="bc_scheduled")])
    rows.append([b(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(
        call, "📢 <b>پیام همگانی</b>\n\nگروه مخاطب را انتخاب کنید:", InlineKeyboardMarkup(inline_keyboard=rows)
    )


@dp.callback_query(F.data.startswith("bcseg_"), Perm("broadcast"))
async def admin_broadcast_segment(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    seg = call.data.split("_", 1)[1]
    if seg not in bc_segments():
        return
    await state.set_state(PaymentState.broadcasting)
    await state.update_data(bc_seg=seg)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data="admin_broadcast_start")]])
    await edit_or_send(
        call,
        f"📢 گروه: <b>{esc(bc_segments()[seg])}</b>\n\nپیام، متن یا رسانه مورد نظر را بفرستید:",
        markup,
    )


@dp.message(PaymentState.broadcasting, Perm("broadcast"))
async def process_broadcast_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    seg = data.get("bc_seg", "all")
    await state.set_state(None)
    await state.update_data(bc_chat=message.chat.id, bc_msg=message.message_id, bc_seg=seg)
    with SessionLocal() as s:
        count = len(segment_user_ids(s, seg))
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="📤 ارسال همین الان", callback_data="bc_now")],
            [b(text="⏰ زمان‌بندی ارسال", callback_data="bc_sched")],
            [b(text="❌ انصراف", callback_data="admin_broadcast_start")],
        ]
    )
    await message.answer(
        f"پیام دریافت شد. مخاطب: <b>{esc(bc_segments().get(seg, seg))}</b> ({count} نفر)\nچطور ارسال شود؟",
        reply_markup=markup,
    )


@dp.callback_query(F.data == "bc_now", Perm("broadcast"))
async def broadcast_now(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get("bc_msg"):
        await call.answer("درخواست منقضی شده است.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    await edit_or_send(call, "⏳ ارسال پیام همگانی آغاز شد؛ پس از اتمام گزارش می‌آید.")
    spawn(
        _broadcast_and_report(
            call.bot, call.message.chat.id, data["bc_chat"], data["bc_msg"], data.get("bc_seg", "all"),
            call.from_user.id,
        )
    )


@dp.callback_query(F.data == "bc_sched", Perm("broadcast"))
async def broadcast_schedule_start(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get("bc_msg"):
        await call.answer("درخواست منقضی شده است.", show_alert=True)
        return
    await call.answer()
    await state.set_state(PaymentState.broadcast_time)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data="admin_broadcast_start")]])
    await edit_or_send(
        call,
        "⏰ زمان ارسال را بفرستید (به وقت ایران):\n\n"
        "• فقط ساعت، مثلاً <code>18:30</code> (نزدیک‌ترین زمان آینده)\n"
        "• یا تاریخ شمسی کامل، مثلاً <code>1405/08/20 18:30</code>\n\n"
        "⚠️ پیام اصلی را از چت خود پاک نکنید، چون ربات در زمان مقرر از روی آن کپی می‌کند.",
        markup,
    )


@dp.message(PaymentState.broadcast_time, Perm("broadcast"))
async def broadcast_schedule_save(message: types.Message, state: FSMContext):
    when = parse_run_time(message.text or "")
    if when is None:
        await message.answer("زمان نامعتبر یا گذشته است. مثال: 18:30 یا 1405/08/20 18:30")
        return
    data = await state.get_data()
    seg = data.get("bc_seg", "all")
    with SessionLocal() as s:
        s.add(
            ScheduledBroadcast(
                segment=seg, chat_id=data["bc_chat"], message_id=data["bc_msg"],
                run_at=when.strftime("%Y-%m-%dT%H:%M"), status="pending",
                created_by=message.from_user.id, created_at=get_current_jalali(),
            )
        )
        s.commit()
    await state.clear()
    jwhen = jdatetime.datetime.fromgregorian(datetime=when).strftime("%Y/%m/%d %H:%M")
    log_action(message.from_user.id, f"زمان‌بندی پیام همگانی برای {jwhen}")
    await message.answer(
        f"✅ پیام برای <b>{jwhen}</b> زمان‌بندی شد (گروه: {esc(bc_segments().get(seg, seg))}).",
        reply_markup=get_admin_main_keyboard(message.from_user.id),
    )


@dp.callback_query(F.data == "bc_scheduled", Perm("broadcast"))
async def broadcast_scheduled_list(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    with SessionLocal() as s:
        rows = [
            (x.id, x.segment, x.run_at)
            for x in s.query(ScheduledBroadcast)
            .filter(ScheduledBroadcast.status == "pending").order_by(ScheduledBroadcast.run_at).limit(20).all()
        ]
    out = "⏰ <b>پیام‌های زمان‌بندی‌شده:</b>\n\n"
    keyboard = []
    for bid, seg, run_at in rows:
        try:
            jw = jdatetime.datetime.fromgregorian(datetime=datetime.strptime(run_at, "%Y-%m-%dT%H:%M")).strftime(
                "%Y/%m/%d %H:%M"
            )
        except ValueError:
            jw = run_at
        out += f"#{bid} | {jw} | {esc(bc_segments().get(seg, seg))}\n"
        keyboard.append([InlineKeyboardButton(text=f"🗑 لغو #{bid}", callback_data=f"bcdel_{bid}")])
    if not rows:
        out += "موردی وجود ندارد."
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_broadcast_start")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data.startswith("bcdel_"), Perm("broadcast"))
async def broadcast_scheduled_cancel(call: types.CallbackQuery, state: FSMContext):
    bid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        x = s.get(ScheduledBroadcast, bid)
        if x is not None and x.status == "pending":
            x.status = "cancelled"
            s.commit()
    log_action(call.from_user.id, f"لغو پیام زمان‌بندی‌شده #{bid}")
    await broadcast_scheduled_list(call, state)


# ---------------------------------------------------------------------------
# پنل ادمین: تنظیمات ربات (عمومی و داده‌محور)
# ---------------------------------------------------------------------------
def setting_display(key):
    meta, value = SETTINGS[key], get_setting(key)
    t = meta["type"]
    if t == "toggle":
        return "✅ روشن" if value in ("1", "true", "True") else "⬜ خاموش"
    if t in ("photo", "sticker"):
        return "✅ تنظیم شده" if value else "—"
    if not value:
        return "—"
    return esc(value if len(value) <= 40 else value[:40] + "…")


def render_settings_page(page):
    out = f"<b>{SETTING_PAGES[page]}</b>\n\n"
    keyboard = []
    for key, meta in SETTINGS.items():
        if meta["page"] != page:
            continue
        out += f"• {meta['label']}: {setting_display(key)}\n"
        if meta["type"] == "toggle":
            label = ("🔴 خاموش: " if get_bool(key) else "🟢 روشن: ") + meta["label"]
        else:
            label = "✏️ " + meta["label"]
        keyboard.append([InlineKeyboardButton(text=label[:60], callback_data=f"sedit_{key}")])
    if page == "flash":
        out += "\n" + flash_status_text() + "\n"
        keyboard.append([InlineKeyboardButton(text="🔥 شروع تخفیف زمان‌دار جدید", callback_data="flash_start")])
        if flash_remaining() is not None:
            keyboard.append(
                [
                    InlineKeyboardButton(text="📢 اعلان به کاربران", callback_data="flash_announce"),
                    InlineKeyboardButton(text="⛔ پایان تخفیف", callback_data="flash_stop"),
                ]
            )
    if page == "sec":
        out += "\nPIN مدیریتی: " + ("✅ تنظیم شده" if pin_is_set() else "— (بدون PIN، کد تصادفی استفاده می‌شود)") + "\n"
        keyboard.append([InlineKeyboardButton(text="🔐 تنظیم / تغییر / حذف PIN", callback_data="pin_set")])
        keyboard.append([InlineKeyboardButton(text="👮 ادمین‌ها و دسترسی‌ها", callback_data="sadmins")])
    if page == "auto":
        keyboard.append([InlineKeyboardButton(text="📤 ارسال بکاپ به کانال (تست)", callback_data="backup_to_channel")])
    if page == "join":
        out += "\nℹ️ ربات باید در کانال «ادمین» باشد تا بتواند عضویت را بررسی کند.\n"
    if page == "chpost":
        keyboard.append([InlineKeyboardButton(text="📤 ارسال همین الان به کانال", callback_data="chpost_now")])
    if page == "mon":
        keyboard.append(
            [
                InlineKeyboardButton(text="📣 ارسال اعلان قطعی", callback_data="outage_send"),
                InlineKeyboardButton(text="✅ ارسال اعلان وصل شدن", callback_data="recovery_send"),
            ]
        )
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به تنظیمات", callback_data="admin_settings")])
    return out, InlineKeyboardMarkup(inline_keyboard=keyboard)


@dp.callback_query(F.data == "chpost_now", MainAdmin())
async def chpost_now(call: types.CallbackQuery):
    try:
        await post_to_channel(call.bot)
        await call.answer("✅ در کانال ارسال شد.", show_alert=True)
    except Exception as e:  # noqa: BLE001
        await call.answer(f"ناموفق: {e}"[:190], show_alert=True)


@dp.callback_query(F.data.in_({"outage_send", "recovery_send"}), MainAdmin())
async def manual_outage_send(call: types.CallbackQuery):
    down = call.data == "outage_send"
    spawn(broadcast_text(call.bot, outage_message(down)))
    log_action(call.from_user.id, "ارسال دستی اعلان " + ("قطعی" if down else "وصل شدن"))
    await call.answer("ارسال برای همه کاربران آغاز شد.", show_alert=True)


@dp.callback_query(F.data == "admin_settings", MainAdmin())
async def admin_settings_menu(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    keyboard = [[InlineKeyboardButton(text=title, callback_data=f"spage_{page}")] for page, title in SETTING_PAGES.items()]
    keyboard.append([InlineKeyboardButton(text="👮 مدیریت ادمین‌ها", callback_data="sadmins")])
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(call, "⚙️ <b>تنظیمات ربات</b>\nیک بخش را انتخاب کنید:", InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data.startswith("spage_"), MainAdmin())
async def admin_settings_page(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    page = call.data.split("_", 1)[1]
    if page not in SETTING_PAGES:
        return
    out, markup = render_settings_page(page)
    await edit_or_send(call, out, markup)


@dp.callback_query(F.data.startswith("sedit_"), MainAdmin())
async def admin_setting_edit(call: types.CallbackQuery, state: FSMContext):
    key = call.data[len("sedit_"):]
    meta = SETTINGS.get(key)
    if meta is None:
        await call.answer()
        return
    await call.answer()
    if meta["type"] == "toggle":
        new_value = "0" if get_bool(key) else "1"
        set_setting(key, new_value)
        log_action(call.from_user.id, f"تنظیم {key} = {new_value}")
        out, markup = render_settings_page(meta["page"])
        await edit_or_send(call, out, markup)
        return

    hints = {
        "num": f"یک عدد بین {meta.get('min', 0)} تا {meta.get('max', 0)} بفرستید.",
        "text": "متن جدید را بفرستید. برای پاک کردن، - بفرستید.",
        "photo": "یک عکس بفرستید. برای حذف، - بفرستید.",
        "sticker": "یک استیکر بفرستید. برای حذف، - بفرستید.",
    }
    await state.set_state(PaymentState.editing_setting)
    await state.update_data(setting_key=key)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"spage_{meta['page']}")]]
    )
    await edit_or_send(call, f"✏️ <b>{meta['label']}</b>\n\n{hints[meta['type']]}", markup)


@dp.message(PaymentState.editing_setting, MainAdmin())
async def admin_setting_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    key = data.get("setting_key")
    meta = SETTINGS.get(key)
    if meta is None:
        await state.clear()
        return

    t, value = meta["type"], None
    text_in = (message.text or "").strip()
    if t == "num":
        raw = text_in.replace(",", "")
        if not raw.isdigit() or not (meta["min"] <= int(raw) <= meta["max"]):
            await message.answer(f"عدد معتبر بین {meta['min']} تا {meta['max']} بفرستید.")
            return
        value = str(int(raw))
    elif t == "text":
        if not text_in:
            await message.answer("لطفاً متن بفرستید.")
            return
        value = "" if text_in == "-" else text_in
        if key == "charge_amounts" and value:
            nums = [p for p in re.split(r"[,\s،]+", value) if p.isdigit() and int(p) > 0]
            if not nums or len(nums) > 12:
                await message.answer("چند عدد (حداکثر ۱۲ تا) با کاما جدا کنید. مثال: 200000,400000")
                return
            value = ",".join(nums)
        if key == "post_times" and value:
            times = parse_times(value)
            if not times:
                await message.answer("ساعت نامعتبر است. مثال: 10:00,20:00")
                return
            value = ",".join(times)
        if key == "monitor_urls" and value:
            urls = parse_urls(value)
            if not urls:
                await message.answer("آدرس معتبر http/https بفرستید. مثال: https://example.com")
                return
            value = ",".join(urls)
        if key in ("card_number", "card_holder") and not value:
            await message.answer("این مورد نمی‌تواند خالی باشد.")
            return
        if key in ("bonus_tiers", "ref_tiers") and value:
            tiers = parse_tiers(value)
            if not tiers or len(tiers) > 8 or (key == "bonus_tiers" and any(p > 100 for _, p in tiers)):
                await message.answer("فرمت نامعتبر است. مثال: 500000:5,1000000:8 (حداکثر ۸ پله؛ درصد حداکثر ۱۰۰)")
                return
            value = ",".join(f"{a}:{b}" for a, b in tiers)
        if key in ("force_join_channel", "backup_channel") and value:
            if not valid_chat_ref(value):
                await message.answer("آیدی کانال نامعتبر است. مثال: @mychannel یا -1001234567890")
                return
            value = value.strip()
    elif t == "photo":
        if message.photo:
            value = message.photo[-1].file_id
        elif text_in == "-":
            value = ""
        else:
            await message.answer("لطفاً یک عکس بفرستید یا - برای حذف.")
            return
    elif t == "sticker":
        if message.sticker:
            value = message.sticker.file_id
        elif text_in == "-":
            value = ""
        else:
            await message.answer("لطفاً یک استیکر بفرستید یا - برای حذف.")
            return

    set_setting(key, value)
    await state.clear()
    log_action(message.from_user.id, f"ویرایش تنظیم {key}")
    out, markup = render_settings_page(meta["page"])
    await message.answer("✅ ذخیره شد.\n\n" + out, reply_markup=markup)


# --- مدیریت ادمین‌ها ---
async def _render_subadmins(call):
    ids = sub_admins()
    out = (
        "👮 <b>مدیریت ادمین‌ها</b>\n\n"
        "ادمین‌های فرعی همیشه می‌توانند درخواست‌ها (رسید و شارژ) و تیکت‌ها را مدیریت کنند؛ "
        "بخش‌های دیگر را از «⚙️ دسترسی‌ها» برای هر ادمین جداگانه باز کنید. "
        "تنظیمات، بکاپ و دکمه‌ها همیشه فقط برای ادمین اصلی است.\n\n"
        f"ادمین اصلی: <code>{ADMIN_ID}</code>\n"
    )
    keyboard = []
    for i in ids:
        out += f"• <code>{i}</code> — {len(staff_perms(i))} دسترسی اضافه\n"
        keyboard.append(
            [
                InlineKeyboardButton(text=f"⚙️ دسترسی‌ها {i}", callback_data=f"sperm_{i}"),
                InlineKeyboardButton(text=f"🗑 حذف {i}", callback_data=f"sadmindel_{i}"),
            ]
        )
    if not ids:
        out += "• ادمین فرعی ثبت نشده است.\n"
    keyboard.append([InlineKeyboardButton(text="➕ افزودن ادمین", callback_data="sadmin_add")])
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_settings")])
    await reply_to(call, out, InlineKeyboardMarkup(inline_keyboard=keyboard))


@dp.callback_query(F.data == "sadmins", MainAdmin())
async def sadmins_menu(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await _render_subadmins(call)


@dp.callback_query(F.data == "sadmin_add", MainAdmin())
async def sadmin_add_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PaymentState.adding_subadmin)
    await edit_or_send(call, "آیدی عددی تلگرام ادمین جدید را بفرستید (او باید قبلاً ربات را استارت کرده باشد):")


@dp.message(PaymentState.adding_subadmin, MainAdmin())
async def sadmin_add_save(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("آیدی عددی معتبر بفرستید.")
        return
    new_id = int(raw)
    ids = sub_admins()
    if new_id != ADMIN_ID and new_id not in ids:
        ids.append(new_id)
        set_setting("sub_admins", ",".join(str(i) for i in ids))
        log_action(message.from_user.id, f"افزودن ادمین فرعی {new_id}")
    await state.clear()
    await message.answer("✅ ادمین فرعی ثبت شد.", reply_markup=get_admin_main_keyboard(message.from_user.id))


@dp.callback_query(F.data.startswith("sadmindel_"), MainAdmin())
async def sadmin_delete(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    rm = int(call.data.split("_")[1])
    if await two_step_gate(call, state, "sadmindel", rm):
        return
    await _do_sadmin_delete(call, rm)


# ---------------------------------------------------------------------------
# دکمه‌ساز: دکمه‌های شیشه‌ای دلخواه (چندمرحله‌ای) — مدیریت توسط ادمین اصلی
# ---------------------------------------------------------------------------
CB_MAX_DEPTH = 6  # حداکثر تعداد مرحله‌ها (عمق درخت)
CB_MAX_PER_LEVEL = 20  # حداکثر دکمه در هر مرحله
CB_LABEL_MAX = 40
CB_TEXT_MAX = 3500

CB_EDIT_PROMPTS = {
    "label": "✏️ نام جدید دکمه را بفرستید (حداکثر ۴۰ کاراکتر).",
    "text": "📝 متن جدید را بفرستید. برای پاک کردن متن، - بفرستید.",
    "photo": "🖼 عکس جدید را بفرستید. برای حذف عکس، - بفرستید.",
    "url": "🔗 لینک جدید را بفرستید (https://... یا @username).",
}


def normalize_url(raw):
    raw = (raw or "").strip()
    if re.fullmatch(r"@[A-Za-z0-9_]{4,}", raw):
        return f"https://t.me/{raw[1:]}"
    if raw.lower().startswith("t.me/"):
        raw = "https://" + raw
    if re.fullmatch(r"https?://\S+", raw, re.I) and len(raw) <= 500:
        return raw
    return None


def cb_children(s, pid, only_active=False):
    q = s.query(CustomButton).filter(CustomButton.parent_id == pid)
    if only_active:
        q = q.filter(CustomButton.is_active.is_(True))
    return q.order_by(CustomButton.sort_order, CustomButton.id).all()


def cb_cols(pid):
    return 2 if get_setting(f"cbcols_{pid}").strip() == "2" else 1


def cb_depth(s, node_id):
    """عمق دکمه (دکمه‌های منوی اصلی = ۱ ؛ خودِ منوی اصلی = ۰)."""
    depth = 0
    while node_id and depth < 50:
        node = s.get(CustomButton, node_id)
        if node is None:
            break
        depth += 1
        node_id = node.parent_id
    return depth


def cb_descendants(s, node_id):
    """آیدی خود دکمه + همه زیردکمه‌هایش."""
    out, queue = [], [node_id]
    while queue and len(out) < 2000:
        cur = queue.pop()
        out.append(cur)
        queue += [i for (i,) in s.query(CustomButton.id).filter(CustomButton.parent_id == cur).all()]
    return out


def cb_is_visible(s, node):
    """دکمه و همه والدهایش باید فعال باشند."""
    for _ in range(CB_MAX_DEPTH + 2):
        if node is None or not node.is_active:
            return False
        if node.parent_id is None or node.parent_id <= 0:
            return True
        node = s.get(CustomButton, node.parent_id)
    return False


def cb_layout(buttons, cols):
    rows, cur = [], []
    for btn in buttons:
        cur.append(btn)
        if len(cur) == cols:
            rows.append(cur)
            cur = []
    if cur:
        rows.append(cur)
    return rows


def cb_user_rows(pid):
    """ردیف‌های دکمه‌ی فعال یک مرحله برای کاربر."""
    with SessionLocal() as s:
        items = [(c.id, c.kind, c.label or "", c.url or "") for c in cb_children(s, pid, only_active=True)]
    btns = []
    for cid, kind, label, url in items:
        if not label.strip():
            continue
        if kind == "url":
            safe = normalize_url(url)
            if safe:
                btns.append(InlineKeyboardButton(text=label[:64], url=safe))
        else:
            btns.append(InlineKeyboardButton(text=label[:64], callback_data=f"cb_{cid}"))
    return cb_layout(btns, cb_cols(pid))


def cb_main_rows():
    """دکمه‌های دلخواه منوی اصلی؛ هیچ‌وقت نباید منوی اصلی را خراب کند."""
    try:
        return cb_user_rows(0)
    except Exception as e:  # noqa: BLE001
        logging.warning("custom buttons failed: %s", e)
        return []


def cb_user_view(node_id, preview=False):
    with SessionLocal() as s:
        node = s.get(CustomButton, node_id)
        if node is None or node.kind != "msg":
            return None
        if not preview and not cb_is_visible(s, node):
            return None
        label, text_, photo, parent = node.label or "", node.text or "", node.photo_id or "", node.parent_id or 0
    body = esc(text_) if text_.strip() else f"<b>{esc(label)}</b>"
    rows = cb_user_rows(node_id)
    b = InlineKeyboardButton
    if preview:
        rows.append([b(text="🔙 بازگشت به ویرایش", callback_data=f"acbn_{node_id}")])
    else:
        nav = [
            b(text="⬅️ بازگشت", callback_data=f"cb_{parent}" if parent > 0 else SCREEN_HOME_CB.get(parent, "main_menu"))
        ]
        if parent != 0:
            nav.append(b(text="🏠 منوی اصلی", callback_data="main_menu"))
        rows.append(nav)
    return body, photo, InlineKeyboardMarkup(inline_keyboard=rows)


async def present_view(call: types.CallbackQuery, text_, photo, markup):
    """متن (یا عکس+کپشن) را نشان می‌دهد؛ جابه‌جایی بین پیام عکس‌دار و بی‌عکس را هم درست مدیریت می‌کند."""
    msg = call.message
    chat_id = call.from_user.id
    had_photo = bool(getattr(msg, "photo", None))
    if photo:
        try:
            await msg.delete()
        except Exception:  # noqa: BLE001
            pass
        try:
            if len(text_) <= 1000:
                await call.bot.send_photo(chat_id=chat_id, photo=photo, caption=text_, reply_markup=markup)
            else:
                await call.bot.send_photo(chat_id=chat_id, photo=photo)
                await call.bot.send_message(chat_id=chat_id, text=text_, reply_markup=markup)
        except Exception as e:  # noqa: BLE001
            logging.warning("custom button photo failed: %s", e)
            await call.bot.send_message(chat_id=chat_id, text=text_, reply_markup=markup)
        return
    if had_photo:
        try:
            await msg.delete()
        except Exception:  # noqa: BLE001
            pass
        await call.bot.send_message(chat_id=chat_id, text=text_, reply_markup=markup)
        return
    await edit_or_send(call, text_, markup)


@dp.callback_query(F.data.regexp(r"^cb_\d+$"))
async def cb_user_open(call: types.CallbackQuery, state: FSMContext):
    view = cb_user_view(int(call.data[3:]))
    if view is None:
        await call.answer("این گزینه دیگر در دسترس نیست.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    await present_view(call, *view)


# ----- نمای مدیریت (ادمین) -----
def cb_list_view(pid):
    with SessionLocal() as s:
        parent = s.get(CustomButton, pid) if pid else None
        if pid and parent is None:
            return None
        plabel = (parent.label or "") if parent else ""
        depth = cb_depth(s, pid)
        items = []
        for c in cb_children(s, pid):
            n = s.query(func.count(CustomButton.id)).filter(CustomButton.parent_id == c.id).scalar() or 0
            items.append((c.id, c.kind, c.label or "", bool(c.is_active), n))
    if pid == 0:
        out = (
            "🔘 <b>دکمه‌ساز منوی اصلی</b>\n\n"
            "دکمه‌هایی که اینجا بسازید زیر منوی اصلی ربات برای همه کاربران نمایش داده می‌شود.\n\n"
        )
    else:
        out = (
            f"📂 <b>مرحله بعدِ «{esc(plabel)}»</b>\n\n"
            f"این دکمه‌ها وقتی کاربر روی «{esc(plabel)}» بزند نشان داده می‌شوند.\n\n"
        )
    b = InlineKeyboardButton
    keyboard = []
    for cid, kind, label, active, n in items:
        out += f"{'🟢' if active else '⚪️'} {'💬' if kind == 'msg' else '🔗'} {esc(label)}"
        out += f" — {n} زیردکمه\n" if kind == "msg" and n else "\n"
        keyboard.append([b(text=f"⚙️ {label}"[:60], callback_data=f"acbn_{cid}")])
    if not items:
        out += "هنوز دکمه‌ای نیست.\n"
    can_add = len(items) < CB_MAX_PER_LEVEL and depth < CB_MAX_DEPTH
    if can_add:
        keyboard.append([b(text="➕ افزودن دکمه", callback_data=f"acbadd_{pid}")])
    elif len(items) >= CB_MAX_PER_LEVEL:
        out += f"\nℹ️ به سقف {CB_MAX_PER_LEVEL} دکمه در این مرحله رسیده‌اید."
    else:
        out += f"\nℹ️ حداکثر {CB_MAX_DEPTH} مرحله پشت‌سرهم مجاز است."
    if len(items) > 1:
        keyboard.append(
            [b(text=f"↔️ چیدمان: {'۲ ستونه' if cb_cols(pid) == 2 else '۱ ستونه'} (تغییر)", callback_data=f"acbcols_{pid}")]
        )
    if pid == 0:
        keyboard.append([b(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    else:
        keyboard.append([b(text=f"🔙 ویرایش «{plabel}»"[:60], callback_data=f"acbn_{pid}")])
    return out, InlineKeyboardMarkup(inline_keyboard=keyboard)


def cb_node_view(node_id):
    with SessionLocal() as s:
        c = s.get(CustomButton, node_id)
        if c is None:
            return None
        kind, label, text_, has_photo = c.kind, c.label or "", c.text or "", bool(c.photo_id)
        url, active, parent = c.url or "", bool(c.is_active), c.parent_id or 0
        n = s.query(func.count(CustomButton.id)).filter(CustomButton.parent_id == node_id).scalar() or 0
    out = (
        f"🔘 <b>{esc(label)}</b>\n\n"
        f"نوع: {'💬 پیام (با امکان مرحله بعد)' if kind == 'msg' else '🔗 لینک'}\n"
        f"وضعیت: {'🟢 فعال' if active else '⚪️ غیرفعال (مخفی از کاربر)'}\n"
    )
    b = InlineKeyboardButton
    rows = []
    if kind == "msg":
        preview = text_.strip().replace("\n", " ")
        out += f"متن: {esc(preview[:150]) + ('…' if len(preview) > 150 else '') if preview else 'ندارد'}\n"
        out += f"عکس: {'✅ دارد' if has_photo else 'ندارد'}\n"
        out += f"دکمه‌های مرحله بعد: {n}"
        rows.append([b(text=f"📂 دکمه‌های مرحله بعد ({n})", callback_data=f"acbs_{node_id}")])
        rows.append(
            [b(text="📝 متن", callback_data=f"acbed_text_{node_id}"), b(text="🖼 عکس", callback_data=f"acbed_photo_{node_id}")]
        )
    else:
        out += f"لینک: {esc(url)}"
        rows.append([b(text="🔗 تغییر لینک", callback_data=f"acbed_url_{node_id}")])
    rows.append([b(text="✏️ نام دکمه", callback_data=f"acbed_label_{node_id}")])
    if kind == "msg":
        rows.append([b(text="👁 پیش‌نمایش", callback_data=f"acbprev_{node_id}")])
    rows.append(
        [b(text="⬆️ بالاتر", callback_data=f"acbup_{node_id}"), b(text="⬇️ پایین‌تر", callback_data=f"acbdn_{node_id}")]
    )
    rows.append([b(text="🔴 غیرفعال کن" if active else "🟢 فعال کن", callback_data=f"acbtog_{node_id}")])
    rows.append([b(text="🗑 حذف دکمه", callback_data=f"acbdel_{node_id}")])
    rows.append([b(text="🔙 برگشت به لیست", callback_data=f"acbs_{parent}")])
    return out, InlineKeyboardMarkup(inline_keyboard=rows)


async def cb_show_list(target, pid):
    if pid <= 0:  # منوهای اصلی (اصلی/حساب/پشتیبانی) با ویرایشگر دکمه‌های کاربر مدیریت می‌شوند
        await ub_show_screen(target, PID_SCREEN.get(pid, "main"))
        return
    view = cb_list_view(pid)
    if view is None:
        view = (
            "دکمه یافت نشد.",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 دکمه‌ساز", callback_data="acbs_0")]]),
        )
    await reply_to(target, *view)


async def cb_show_node(target, nid):
    view = cb_node_view(nid)
    if view is None:
        view = (
            "دکمه یافت نشد.",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 دکمه‌ساز", callback_data="acbs_0")]]),
        )
    await reply_to(target, *view)


@dp.callback_query(F.data.regexp(r"^acbs_-?\d+$"), MainAdmin())
async def cb_admin_list(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await cb_show_list(call, int(call.data.split("_")[1]))


@dp.callback_query(F.data.regexp(r"^acbn_\d+$"), MainAdmin())
async def cb_admin_node(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await cb_show_node(call, int(call.data.split("_")[1]))


@dp.callback_query(F.data.regexp(r"^acbprev_\d+$"), MainAdmin())
async def cb_admin_preview(call: types.CallbackQuery, state: FSMContext):
    view = cb_user_view(int(call.data.split("_")[1]), preview=True)
    if view is None:
        await call.answer("این دکمه پیام ندارد.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    await present_view(call, *view)


@dp.callback_query(F.data.regexp(r"^acbcols_\d+$"), MainAdmin())
async def cb_admin_cols(call: types.CallbackQuery):
    pid = int(call.data.split("_")[1])
    set_setting(f"cbcols_{pid}", "1" if cb_cols(pid) == 2 else "2")
    await call.answer("چیدمان تغییر کرد.")
    await cb_show_list(call, pid)


# ----- ساخت دکمه‌ی جدید (مرحله‌به‌مرحله) -----
def cb_cancel_markup(pid):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"acbs_{pid}")]])


@dp.callback_query(F.data.regexp(r"^acbadd_-?\d+$"), MainAdmin())
async def cb_add_start(call: types.CallbackQuery, state: FSMContext):
    pid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        if pid > 0 and s.get(CustomButton, pid) is None:
            await call.answer("مرحله یافت نشد.", show_alert=True)
            return
        n = s.query(func.count(CustomButton.id)).filter(CustomButton.parent_id == pid).scalar() or 0
        depth = cb_depth(s, pid)
    if n >= CB_MAX_PER_LEVEL or depth >= CB_MAX_DEPTH:
        await call.answer("به سقف تعداد دکمه یا مرحله رسیده‌اید.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="💬 دکمه پیام (قابل ساخت مرحله بعد)", callback_data=f"acbk_msg_{pid}")],
            [b(text="🔗 دکمه لینک (باز کردن مستقیم)", callback_data=f"acbk_url_{pid}")],
            [b(text="انصراف", callback_data=f"acbs_{pid}")],
        ]
    )
    await edit_or_send(
        call,
        "➕ <b>دکمه جدید</b>\n\nنوع دکمه را انتخاب کنید:\n\n"
        "💬 <b>پیام:</b> با زدن دکمه، متن (و در صورت تمایل عکس) نمایش داده می‌شود و می‌توانید برایش دکمه‌های مرحله بعد بسازید.\n\n"
        "🔗 <b>لینک:</b> با زدن دکمه، لینک (کانال، سایت، آیدی...) باز می‌شود.",
        markup,
    )


@dp.callback_query(F.data.regexp(r"^acbk_(msg|url)_-?\d+$"), MainAdmin())
async def cb_add_kind(call: types.CallbackQuery, state: FSMContext):
    _, kind, pid = call.data.split("_")
    await call.answer()
    await state.set_state(PaymentState.cb_label)
    await state.update_data(cb_pid=int(pid), cb_kind=kind)
    await edit_or_send(
        call,
        "✏️ <b>مرحله ۱:</b> متن روی دکمه را بفرستید (حداکثر ۴۰ کاراکتر، ایموجی مجاز است).\n\n"
        "مثال: <code>📚 آموزش‌ها</code>",
        cb_cancel_markup(int(pid)),
    )


@dp.message(PaymentState.cb_label, MainAdmin())
async def cb_add_label(message: types.Message, state: FSMContext):
    label = (message.text or "").strip()
    if not label or len(label) > CB_LABEL_MAX:
        await message.answer(f"یک متن معتبر (حداکثر {CB_LABEL_MAX} کاراکتر) بفرستید.")
        return
    data = await state.get_data()
    pid, kind = int(data.get("cb_pid", 0)), data.get("cb_kind", "msg")
    await state.update_data(cb_label=label)
    if kind == "url":
        await state.set_state(PaymentState.cb_url)
        await message.answer(
            "🔗 <b>مرحله ۲:</b> لینک را بفرستید.\n\nمثال: <code>https://t.me/channel</code> یا <code>@channel</code>",
            reply_markup=cb_cancel_markup(pid),
        )
    else:
        await state.set_state(PaymentState.cb_text)
        await message.answer(
            "📝 <b>مرحله ۲:</b> متنی را بفرستید که با زدن این دکمه به کاربر نشان داده می‌شود.\n\n"
            "💡 اگر می‌خواهید عکس هم داشته باشد، می‌توانید همین‌جا یک عکس همراه کپشن (متن) بفرستید.",
            reply_markup=cb_cancel_markup(pid),
        )


async def _cb_finish_add(event, state: FSMContext, text_, photo_id, url=""):
    data = await state.get_data()
    pid, kind, label = int(data.get("cb_pid", 0)), data.get("cb_kind", "msg"), data.get("cb_label", "")
    with SessionLocal() as s:
        if pid > 0 and s.get(CustomButton, pid) is None:
            await state.clear()
            await reply_to(event, "مرحله والد پیدا نشد (احتمالاً حذف شده).", None)
            return
        last = s.query(func.max(CustomButton.sort_order)).filter(CustomButton.parent_id == pid).scalar() or 0
        c = CustomButton(
            parent_id=pid, kind=kind, label=label, text=text_ or "", photo_id=photo_id or "",
            url=url or "", sort_order=last + 1, is_active=True,
        )
        s.add(c)
        s.flush()
        cid = c.id
        depth = cb_depth(s, cid)
        s.commit()
    await state.clear()
    log_action(event.from_user.id, f"ساخت دکمه دلخواه «{label}»")
    b = InlineKeyboardButton
    rows = []
    if kind == "msg":
        out = (
            f"✅ دکمه «{esc(label)}» ساخته شد و همین الان برای کاربران فعال است.\n\n"
            "می‌خواهید برای مرحله بعدِ این دکمه هم دکمه بسازید؟ (گزینه‌هایی که بعد از زدن این دکمه نشان داده می‌شوند)"
        )
        if depth < CB_MAX_DEPTH:
            rows.append([b(text="➕ ساخت دکمه برای مرحله بعد", callback_data=f"acbadd_{cid}")])
        rows.append([b(text="👁 پیش‌نمایش", callback_data=f"acbprev_{cid}")])
    else:
        out = f"✅ دکمه لینک «{esc(label)}» ساخته شد و همین الان برای کاربران فعال است."
    rows.append([b(text="➕ دکمه دیگر در همین سطح", callback_data=f"acbadd_{pid}")])
    rows.append([b(text="✅ پایان (لیست دکمه‌ها)", callback_data=f"acbs_{pid}")])
    await reply_to(event, out, InlineKeyboardMarkup(inline_keyboard=rows))


@dp.message(PaymentState.cb_text, MainAdmin())
async def cb_add_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    pid = int(data.get("cb_pid", 0))
    if message.photo:
        text_ = (message.caption or "").strip()
        if len(text_) > CB_TEXT_MAX:
            await message.answer(f"متن خیلی طولانی است (حداکثر {CB_TEXT_MAX} کاراکتر).")
            return
        await _cb_finish_add(message, state, text_, message.photo[-1].file_id)
        return
    text_ = (message.text or "").strip()
    if not text_ or len(text_) > CB_TEXT_MAX:
        await message.answer(f"یک متن معتبر (حداکثر {CB_TEXT_MAX} کاراکتر) یا یک عکس همراه کپشن بفرستید.")
        return
    await state.update_data(cb_text=text_)
    await state.set_state(PaymentState.cb_photo)
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="⏭ بدون عکس، ساخت دکمه", callback_data="acbnophoto")],
            [b(text="انصراف", callback_data=f"acbs_{pid}")],
        ]
    )
    await message.answer("🖼 <b>مرحله ۳ (اختیاری):</b> اگر می‌خواهید عکسی همراه این پیام باشد، همین‌جا بفرستید.", reply_markup=markup)


@dp.callback_query(PaymentState.cb_photo, F.data == "acbnophoto", MainAdmin())
async def cb_add_no_photo(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    await _cb_finish_add(call, state, data.get("cb_text", ""), "")


@dp.message(PaymentState.cb_photo, MainAdmin())
async def cb_add_photo(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if message.photo:
        await _cb_finish_add(message, state, data.get("cb_text", ""), message.photo[-1].file_id)
    elif (message.text or "").strip() == "-":
        await _cb_finish_add(message, state, data.get("cb_text", ""), "")
    else:
        await message.answer("لطفاً یک عکس بفرستید، یا دکمه «بدون عکس» را بزنید.")


@dp.message(PaymentState.cb_url, MainAdmin())
async def cb_add_url(message: types.Message, state: FSMContext):
    url = normalize_url(message.text)
    if not url:
        await message.answer("لینک معتبر بفرستید. مثال: https://t.me/channel یا @channel")
        return
    await _cb_finish_add(message, state, "", "", url=url)


# ----- ویرایش، جابه‌جایی، فعال/غیرفعال، حذف -----
@dp.callback_query(F.data.regexp(r"^acbed_(label|text|photo|url)_\d+$"), MainAdmin())
async def cb_edit_start(call: types.CallbackQuery, state: FSMContext):
    _, field, nid = call.data.split("_")
    await call.answer()
    await state.set_state(PaymentState.cb_edit)
    await state.update_data(cb_id=int(nid), cb_field=field)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"acbn_{nid}")]])
    await edit_or_send(call, CB_EDIT_PROMPTS[field], markup)


@dp.message(PaymentState.cb_edit, MainAdmin())
async def cb_edit_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    nid, field = data.get("cb_id"), data.get("cb_field")
    raw = (message.text or "").strip()
    with SessionLocal() as s:
        c = s.get(CustomButton, nid)
        if c is None:
            await state.clear()
            await message.answer("دکمه یافت نشد.")
            return
        if field == "label":
            if not raw or len(raw) > CB_LABEL_MAX:
                await message.answer(f"یک نام معتبر (حداکثر {CB_LABEL_MAX} کاراکتر) بفرستید.")
                return
            c.label = raw
        elif field == "text":
            if not raw or len(raw) > CB_TEXT_MAX:
                await message.answer(f"متن معتبر (حداکثر {CB_TEXT_MAX} کاراکتر) یا - بفرستید.")
                return
            c.text = "" if raw == "-" else raw
        elif field == "photo":
            if message.photo:
                c.photo_id = message.photo[-1].file_id
            elif raw == "-":
                c.photo_id = ""
            else:
                await message.answer("لطفاً یک عکس بفرستید یا - برای حذف.")
                return
        elif field == "url":
            url = normalize_url(raw)
            if not url:
                await message.answer("لینک معتبر بفرستید. مثال: https://t.me/channel یا @channel")
                return
            c.url = url
        s.commit()
    await state.clear()
    log_action(message.from_user.id, f"ویرایش دکمه دلخواه #{nid} ({field})")
    await message.answer("✅ ذخیره شد.")
    await cb_show_node(message, nid)


@dp.callback_query(F.data.regexp(r"^acb(up|dn)_\d+$"), MainAdmin())
async def cb_move(call: types.CallbackQuery):
    up = call.data.startswith("acbup_")
    nid = int(call.data.split("_")[1])
    root_screen = None
    with SessionLocal() as s:
        c = s.get(CustomButton, nid)
        if c is None:
            await call.answer("دکمه یافت نشد.", show_alert=True)
            return
        if (c.parent_id or 0) <= 0:
            root_screen = PID_SCREEN.get(c.parent_id or 0, "main")
        else:
            sibs = cb_children(s, c.parent_id)
            ids = [x.id for x in sibs]
            for i, x in enumerate(sibs):  # ترتیب‌ها را یکتا و پشت‌سرهم می‌کنیم
                x.sort_order = i
            i = ids.index(nid)
            j = i - 1 if up else i + 1
            if 0 <= j < len(sibs):
                sibs[i].sort_order, sibs[j].sort_order = sibs[j].sort_order, sibs[i].sort_order
            s.commit()
    if root_screen:  # دکمه‌ی منوی اصلی/حساب/پشتیبانی: ترتیب در لیست یکپارچه
        ub_move(root_screen, f"c{nid}", up)
    await call.answer("ترتیب تغییر کرد.")
    await cb_show_node(call, nid)


@dp.callback_query(F.data.regexp(r"^acbtog_\d+$"), MainAdmin())
async def cb_toggle(call: types.CallbackQuery):
    nid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(CustomButton, nid)
        if c is None:
            await call.answer("دکمه یافت نشد.", show_alert=True)
            return
        c.is_active = not bool(c.is_active)
        s.commit()
    await call.answer("انجام شد.")
    await cb_show_node(call, nid)


@dp.callback_query(F.data.regexp(r"^acbdel_\d+$"), MainAdmin())
async def cb_delete_ask(call: types.CallbackQuery):
    nid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(CustomButton, nid)
        if c is None:
            await call.answer("دکمه یافت نشد.", show_alert=True)
            return
        label = c.label or ""
        extra = len(cb_descendants(s, nid)) - 1
    await call.answer()
    out = f"🗑 دکمه «{esc(label)}» حذف شود؟"
    if extra:
        out += f"\n\n⚠️ {extra} زیردکمه (مرحله‌های بعدی) هم همراهش حذف می‌شود."
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="✅ بله، حذف کن", callback_data=f"acbdelok_{nid}")],
            [b(text="انصراف", callback_data=f"acbn_{nid}")],
        ]
    )
    await edit_or_send(call, out, markup)


@dp.callback_query(F.data.regexp(r"^acbdelok_\d+$"), MainAdmin())
async def cb_delete_do(call: types.CallbackQuery):
    nid = int(call.data.split("_")[1])
    with SessionLocal() as s:
        c = s.get(CustomButton, nid)
        if c is None:
            await call.answer("دکمه یافت نشد.", show_alert=True)
            return
        pid, label = c.parent_id or 0, c.label or ""
        ids = cb_descendants(s, nid)
        s.query(CustomButton).filter(CustomButton.id.in_(ids)).delete(synchronize_session=False)
        s.query(KV).filter(KV.key.in_([f"cbcols_{i}" for i in ids])).delete(synchronize_session=False)
        s.commit()
    for i in ids:
        _cache.pop(f"cbcols_{i}", None)
    await call.answer("حذف شد.")
    log_action(call.from_user.id, f"حذف دکمه دلخواه «{label}» ({len(ids)} مورد)")
    await cb_show_list(call, pid)


# ===========================================================================
# قابلیت‌های تکمیلی: مبلغ تصادفی، باشگاه، پاداش شارژ، تخفیف زمان‌دار، عضویت اجباری،
# گزارش هفتگی، سطح دسترسی ادمین‌ها، تأیید دومرحله‌ای، ویرایش دکمه‌های کاربر و ...
# ===========================================================================


# --- متن‌های قابل ویرایش از پنل ---
def fill_text(key, **kw):
    """متن قابل ویرایش را escape می‌کند و جاینشین‌های {name} را با مقدارهای (HTML-امن) پر می‌کند."""
    body = esc((get_setting(key) or "").strip() or DEFAULTS.get(key, ""))
    for k, v in kw.items():
        body = body.replace("{" + k + "}", str(v))
    return body


# --- مبلغ تصادفی پرداخت (کارت‌به‌کارت) ---
_issued_amounts = {}


def _taken_pay_amounts():
    with SessionLocal() as s:
        rows = s.query(Request.pay_amount).filter(Request.status == "unanswered", Request.pay_amount.isnot(None)).all()
    taken = {int(r[0]) for r in rows if r[0]}
    now = time.time()
    for amt, ts in list(_issued_amounts.items()):
        if now - ts > 7200:
            _issued_amounts.pop(amt, None)
        else:
            taken.add(amt)
    return taken


def pay_amount_for(base):
    """مبلغ دقیق واریزی = مبلغ پایه ± یک عدد تصادفی (یکتا بین پرداخت‌های در انتظار)."""
    base = int(base)
    if not get_bool("payrand_on") or base <= 0 or get_int("payrand_max") <= 0:
        return base
    lo = max(1, get_int("payrand_min"))
    hi = max(lo, get_int("payrand_max"))
    taken = _taken_pay_amounts()
    amount = base
    for _ in range(60):
        delta = random.randint(lo, hi)
        if get_bool("payrand_down") and random.random() < 0.5 and base - delta >= 1000:
            amount = base - delta
        else:
            amount = base + delta
        if amount not in taken:
            break
    _issued_amounts[amount] = time.time()
    return amount


async def ensure_pay_amount(state, base, key="pay"):
    """مبلغ تصادفی را یک‌بار می‌سازد و در state نگه می‌دارد تا با رفت‌وبرگشت عوض نشود."""
    data = await state.get_data()
    if data.get(f"{key}_base") == base and data.get(f"{key}_amount"):
        return int(data[f"{key}_amount"])
    amount = pay_amount_for(base)
    await state.update_data(**{f"{key}_base": base, f"{key}_amount": amount})
    return amount


def pay_note(base, amount):
    if amount == base:
        return ""
    return "\n⚠️ لطفاً <b>دقیقاً همین مبلغ</b> را واریز کنید؛ عدد انتهایی برای شناسایی پرداخت شما تنظیم شده است."


# --- پله‌ها (پاداش شارژ و دعوت پله‌ای) ---
def parse_tiers(raw):
    """'500000:5,1000000:8' -> [(500000, 5), (1000000, 8)] مرتب‌شده؛ موارد نامعتبر نادیده گرفته می‌شود."""
    out = []
    for part in re.split(r"[,\s،]+", (raw or "").translate(DIGITS_FA)):
        m = re.fullmatch(r"(\d+):(\d+)", part)
        if m and int(m.group(1)) > 0 and int(m.group(2)) > 0:
            out.append((int(m.group(1)), int(m.group(2))))
    return sorted(set(out))


def charge_bonus_for(amount):
    """(مبلغ پاداش, درصد) برای یک شارژ."""
    if not get_bool("bonus_on"):
        return 0, 0
    pct = 0
    for thr, p in parse_tiers(get_setting("bonus_tiers")):
        if amount >= thr:
            pct = p
    return ((amount * pct) // 100, pct) if pct else (0, 0)


def club_points_for(price):
    per = get_int("club_earn_per")
    if not get_bool("club_on") or per <= 0 or (price or 0) <= 0:
        return 0
    return int(price // per)


def referral_tier_hits(s, ref_id):
    """پله‌های دعوتِ تازه‌رسیده [(تعداد, پاداش)]؛ شمارنده در همان session ثبت می‌شود (هم‌تراکنش با پاداش)."""
    if not get_bool("ref_tiers_on"):
        return []
    count = (
        s.query(func.count(User.user_id)).filter(User.referred_by == ref_id, User.referral_rewarded.is_(True)).scalar()
        or 0
    )
    key = f"reftier_{ref_id}"
    row = s.get(KV, key)
    try:
        claimed = int(row.value) if row is not None and row.value else 0
    except ValueError:
        claimed = 0
    hits = [(n, amt) for n, amt in parse_tiers(get_setting("ref_tiers")) if claimed < n <= count]
    if hits:
        if row is None:
            s.add(KV(key=key, value=str(count)))
        else:
            row.value = str(count)
    return hits


# --- تخفیف زمان‌دار ---
def flash_remaining():
    """زمان باقی‌مانده تخفیف زمان‌دار (timedelta) یا None."""
    if get_int("flash_percent") <= 0:
        return None
    try:
        until = datetime.strptime(get_setting("flash_until").strip(), "%Y-%m-%dT%H:%M")
    except ValueError:
        return None
    left = until - now_iran().replace(tzinfo=None)
    return left if left.total_seconds() > 0 else None


def flash_percent_now():
    return get_int("flash_percent") if flash_remaining() else 0


def fmt_remaining(td):
    mins = int(td.total_seconds() // 60)
    d, rem = divmod(mins, 1440)
    h, m = divmod(rem, 60)
    if d:
        return f"{d} روز و {h} ساعت"
    if h:
        return f"{h} ساعت و {m} دقیقه"
    return f"{max(m, 1)} دقیقه"


def flash_status_text():
    left = flash_remaining()
    if left is None:
        return "وضعیت: ⚪️ تخفیف زمان‌دار فعالی وجود ندارد."
    return f"وضعیت: 🔥 فعال ({get_int('flash_percent')}٪) | زمان باقی‌مانده: {fmt_remaining(left)}"


# --- عضویت اجباری در کانال ---
_join_cache = {}


def chat_ref(raw):
    ch = (raw or "").strip()
    if not ch:
        raise ValueError("کانال تنظیم نشده است.")
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if ch.startswith(prefix):
            ch = "@" + ch[len(prefix):].strip("/")
    if re.fullmatch(r"-?\d+", ch):
        return int(ch)
    return ch if ch.startswith("@") else "@" + ch


def valid_chat_ref(raw):
    try:
        ref = chat_ref(raw)
    except ValueError:
        return False
    return isinstance(ref, int) or bool(re.fullmatch(r"@[A-Za-z0-9_]{4,}", ref))


def join_channel_url():
    try:
        ref = chat_ref(get_setting("force_join_channel"))
    except ValueError:
        return channel_url()
    if isinstance(ref, str):
        return f"https://t.me/{ref[1:]}"
    return channel_url()


async def is_channel_member(bot, user_id, use_cache=True):
    raw = get_setting("force_join_channel").strip()
    if not raw:
        return True
    now = time.time()
    hit = _join_cache.get(user_id)
    if use_cache and hit and now - hit < 300:
        return True
    try:
        member = await bot.get_chat_member(chat_id=chat_ref(raw), user_id=user_id)
        ok = member.status in ("member", "administrator", "creator") or (
            member.status == "restricted" and bool(getattr(member, "is_member", False))
        )
    except Exception as e:  # noqa: BLE001
        logging.warning("force-join check failed (ربات باید ادمین کانال باشد): %s", e)
        return True  # اگر بررسی ممکن نبود، کاربر قفل نشود
    if ok:
        _join_cache[user_id] = now
    return ok


async def send_join_prompt(event):
    b = InlineKeyboardButton
    rows = []
    url = join_channel_url()
    if url:
        rows.append([b(text="📢 عضویت در کانال", url=url)])
    rows.append([b(text="✅ عضو شدم", callback_data="fj_check")])
    text_ = esc(get_setting("force_join_text").strip() or DEFAULTS["force_join_text"])
    markup = InlineKeyboardMarkup(inline_keyboard=rows)
    if isinstance(event, types.CallbackQuery):
        await event.answer("ابتدا در کانال عضو شوید.", show_alert=True)
        try:
            await event.bot.send_message(chat_id=event.from_user.id, text=text_, reply_markup=markup)
        except Exception as e:  # noqa: BLE001
            logging.info("join prompt failed: %s", e)
    else:
        await event.answer(text_, reply_markup=markup)


# --- گزارش خطا به ادمین ---
_err_seen = {}


async def report_error(bot, where, exc):
    if not get_bool("err_report_on") or bot is None:
        return
    sig = f"{type(exc).__name__}:{str(exc)[:80]}:{where}"
    now = time.time()
    if now - _err_seen.get(sig, 0) < 600:  # خطای تکراری را هر ۱۰ دقیقه یک‌بار بفرست
        return
    _err_seen[sig] = now
    if len(_err_seen) > 200:
        _err_seen.clear()
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-1200:]
    text_ = (
        "🚨 <b>خطا در ربات</b>\n"
        f"📍 {esc(str(where))}\n"
        f"<code>{esc(type(exc).__name__)}: {esc(str(exc)[:300])}</code>\n\n"
        f"<pre>{esc(tb)}</pre>"
    )
    try:
        await bot.send_message(chat_id=ADMIN_ID, text=text_)
    except Exception as e:  # noqa: BLE001
        logging.warning("report_error failed: %s", e)


_IGNORED_ERRORS = ("message is not modified", "query is too old", "message to edit not found", "message can't be edited")


@dp.errors()
async def global_error_handler(event, bot: Bot):
    exc = getattr(event, "exception", None)
    logging.error("Unhandled error: %r", exc, exc_info=exc)
    if exc is None or isinstance(exc, (TelegramForbiddenError, TelegramRetryAfter)):
        return True
    if any(m in str(exc).lower() for m in _IGNORED_ERRORS):
        return True
    await report_error(bot, "پردازش پیام کاربر", exc)
    return True


# --- داشبورد ادمین ---
def expiring_soon_count(days=3):
    today = today_iran()
    limit = (today + timedelta(days=days)).isoformat()
    with SessionLocal() as s:
        return int(
            s.query(func.count(UserConfig.id))
            .filter(
                UserConfig.expires_at.isnot(None),
                UserConfig.expires_at >= today.isoformat(),
                UserConfig.expires_at <= limit,
            )
            .scalar()
            or 0
        )


def admin_dashboard_text(user_id):
    head = "⚙️ <b>پنل مدیریت</b>"
    try:
        n = stats_numbers()
        if not is_main(user_id) and not has_perm(user_id, "stats"):
            return (
                f"{head}\n\n🔴 درخواست در انتظار: {n['unanswered']}\n🎫 تیکت باز: {n['open_tickets']}"
            )
        return (
            f"{head} | 📅 {n['today']}\n\n"
            f"💰 فروش امروز: {n['day_sales']} سفارش | {n['day_rev']:,} تومان\n"
            f"💳 شارژ امروز: {n['charged_today']:,} تومان\n"
            f"👥 کاربر جدید امروز: {n['new_today']} (کل: {n['total_users']})\n"
            f"🔴 در انتظار تایید: {n['unanswered']}  |  🎫 تیکت باز: {n['open_tickets']}\n"
            f"⏰ سرویس نزدیک انقضا (۳ روز): {expiring_soon_count(3)}"
        )
    except Exception as e:  # noqa: BLE001
        logging.warning("dashboard failed: %s", e)
        return head


# --- پیام همگانی هدفمند ---
def bc_segments():
    out = dict(BC_SEGMENTS)
    try:
        with SessionLocal() as s:
            for c in s.query(Category).filter(Category.is_active.is_(True)).order_by(Category.sort_order, Category.id).all():
                out[f"cat_{c.key}"] = f"🛍 خریداران دسته «{(c.emoji or '') + ' ' + (c.label or '')}»".replace("  ", " ")
    except Exception as e:  # noqa: BLE001
        logging.warning("bc_segments failed: %s", e)
    return out


def bonus_hint_text():
    if not get_bool("bonus_on"):
        return ""
    tiers = parse_tiers(get_setting("bonus_tiers"))
    if not tiers:
        return ""
    parts = " | ".join(f"از {thr:,} تومان ← {pct}٪" for thr, pct in tiers)
    return f"🎁 <b>پاداش شارژ:</b> {parts}\n\n"


def _bind_referral(user_id, arg):
    """لینک دعوت /start ref_<id>؛ فقط اگر کاربر هنوز معرف نداشته باشد ثبت می‌شود."""
    if not arg.startswith("ref_"):
        return
    try:
        ref_id = int(arg[4:])
    except ValueError:
        return
    if not ref_id or ref_id == user_id:
        return
    with SessionLocal() as s:
        if s.get(User, ref_id) is None:
            return
        me = s.get(User, user_id)
        if me is not None and not me.referred_by:
            me.referred_by = ref_id
            s.commit()


# --- عضویت اجباری: دکمه «عضو شدم» ---
@dp.callback_query(F.data == "fj_check")
async def fj_check_handler(call: types.CallbackQuery, state: FSMContext):
    if not await is_channel_member(call.bot, call.from_user.id, use_cache=False):
        await call.answer("هنوز عضو کانال نشده‌اید.", show_alert=True)
        return
    await call.answer("✅ عضویت تایید شد.")
    await state.clear()
    await edit_or_send(
        call, build_welcome(call.from_user.id, call.from_user.first_name), main_inline_markup(call.from_user.id)
    )
    await call.bot.send_message(
        chat_id=call.from_user.id, text="🔽 منوی پایین هم همیشه در دسترسته.", reply_markup=get_user_keyboard()
    )


# --- باشگاه مشتریان ---
def club_view(user_id):
    cur_month = jalali_today().strftime("%Y/%m")
    with SessionLocal() as s:
        user = s.get(User, user_id)
        points = (user.points if user else 0) or 0
        count = ((user.purchase_count_current_month or 0) if (user and user.last_purchase_month == cur_month) else 0)
        total_cnt, total_spent = (
            s.query(func.count(Request.id), func.coalesce(func.sum(Request.price), 0))
            .filter(Request.user_id == user_id, Request.status == "answered", Request.req_type.in_(SALE_TYPES))
            .one()
        )
    value, per, min_redeem = get_int("club_point_value"), get_int("club_earn_per"), get_int("club_min_redeem")
    tier = tier_for_count(count) if get_bool("loyalty_on") else None
    next_need = next((n for n, _k, _l in reversed(LOYALTY_TIERS) if n > count), None)

    out = "🏆 <b>باشگاه مشتریان Swan VPN</b>\n\n"
    out += f"⭐ امتیاز شما: <b>{points:,}</b>"
    if value > 0:
        out += f" (معادل {points * value:,} تومان اعتبار)"
    out += f"\n🛍 کل خریدها: {total_cnt} | مجموع: {int(total_spent or 0):,} تومان\n📈 خرید این ماه: {count}\n"
    if tier:
        out += f"🎖 سطح فعلی: {tier[1]} ({tier[2]}٪ تخفیف روی خریدهای بعدی)\n"
    else:
        out += "🎖 سطح فعلی: هنوز سطحی ندارید (با ۲ خرید در یک ماه به برنزی می‌رسید)\n"
    if next_need:
        out += f"🎯 تا سطح بعدی: {next_need - count} خرید دیگر\n"
    if per > 0 and value > 0:
        out += f"\n💡 با هر {per:,} تومان خرید، ۱ امتیاز می‌گیرید و هر امتیاز {value:,} تومان اعتبار کیف پول است."
        if min_redeem > 0:
            out += f"\n🔁 حداقل امتیاز برای تبدیل: {min_redeem}"
    b = InlineKeyboardButton
    rows = []
    if value > 0 and points >= max(1, min_redeem):
        rows.append([b(text=f"💳 تبدیل {points:,} امتیاز به {points * value:,} تومان", callback_data="club_redeem")])
    rows.append([b(text="🎁 دعوت دوستان", callback_data="referral_menu")])
    rows.append([b(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")])
    return out, InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "club_menu")
async def club_menu_handler(call: types.CallbackQuery, state: FSMContext):
    if not get_bool("club_on"):
        await call.answer("باشگاه مشتریان فعلاً غیرفعال است.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    out, markup = club_view(call.from_user.id)
    await edit_or_send(call, out, markup)


@dp.callback_query(F.data == "club_redeem")
async def club_redeem_handler(call: types.CallbackQuery):
    value, min_redeem = get_int("club_point_value"), get_int("club_min_redeem")
    with SessionLocal() as s:
        user = get_or_create_user(s, call.from_user.id)
        points = user.points or 0
        if not get_bool("club_on") or value <= 0 or points < max(1, min_redeem):
            await call.answer("امتیاز شما برای تبدیل کافی نیست.", show_alert=True)
            return
        credit = points * value
        user.points = 0
        user.balance = (user.balance or 0) + credit
        s.commit()
    await call.answer(f"✅ {credit:,} تومان به کیف پول شما اضافه شد.", show_alert=True)
    out, markup = club_view(call.from_user.id)
    await edit_or_send(call, out, markup)


# --- صفحه سرویس: QR ---
@dp.callback_query(F.data.regexp(r"^cfg_qr_\d+$"))
async def cfg_qr_handler(call: types.CallbackQuery):
    cfg_id = int(call.data.split("_")[2])
    with SessionLocal() as s:
        cfg = s.get(UserConfig, cfg_id)
        if cfg is None or cfg.user_id != call.from_user.id:
            await call.answer("سرویس یافت نشد.", show_alert=True)
            return
        details = cfg.config_details or ""
    m = LINK_RE.search(details)
    png = make_qr_png(m.group(0)) if m else None
    if not png:
        await call.answer("QR برای این سرویس در دسترس نیست.", show_alert=True)
        return
    await call.answer()
    await call.bot.send_photo(
        chat_id=call.from_user.id,
        photo=BufferedInputFile(png, filename="qr.png"),
        caption="📷 QR کد اتصال (با دوربین یا از داخل برنامه اسکن کنید)",
    )


# --- آموزش اتصال هوشمند (به تفکیک دستگاه) ---
GUIDE_PLATFORMS = {
    "android": "📱 اندروید",
    "ios": "🍎 آیفون (iOS)",
    "windows": "💻 ویندوز",
    "mac": "🖥 مک",
}


@dp.callback_query(F.data == "guide_connect")
async def guide_connect_handler(call: types.CallbackQuery):
    await call.answer()
    b = InlineKeyboardButton
    general = esc(get_setting("connect_guide").strip() or DEFAULTS["connect_guide"])
    out = general + "\n\n👇 <b>برای آموزش کامل‌تر، دستگاه خود را انتخاب کنید:</b>"
    plats = list(GUIDE_PLATFORMS.items())
    rows = [[b(text=lbl, callback_data=f"guide_p_{k}") for k, lbl in plats[i : i + 2]] for i in range(0, len(plats), 2)]
    url = channel_url()
    if url:
        rows.append([b(text="📢 آموزش تصویری در کانال", url=url)])
    rows.append([b(text="🎧 پشتیبانی", callback_data="support_menu")])
    rows.append([b(text="🏠 منوی اصلی", callback_data="main_menu")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=rows))


@dp.callback_query(F.data.regexp(r"^guide_p_(android|ios|windows|mac)$"))
async def guide_platform_handler(call: types.CallbackQuery):
    await call.answer()
    plat = call.data.split("_")[2]
    key = f"guide_{plat}"
    out = esc(get_setting(key).strip() or DEFAULTS[key])
    b = InlineKeyboardButton
    rows = []
    url = channel_url()
    if url:
        rows.append([b(text="📢 آموزش تصویری در کانال", url=url)])
    rows.append([b(text="🎧 پشتیبانی", callback_data="support_menu")])
    rows.append([b(text="⬅️ انتخاب دستگاه دیگر", callback_data="guide_connect")])
    rows.append([b(text="🏠 منوی اصلی", callback_data="main_menu")])
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=rows))


# --- تخفیف زمان‌دار (ادمین) ---
def _flash_back_markup():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data="spage_flash")]])


@dp.callback_query(F.data == "flash_start", MainAdmin())
async def flash_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PaymentState.flash_percent_in)
    await edit_or_send(call, f"🔥 درصد تخفیف زمان‌دار را بفرستید (۱ تا {MAX_DISCOUNT}):", _flash_back_markup())


@dp.message(PaymentState.flash_percent_in, MainAdmin())
async def flash_percent_got(message: types.Message, state: FSMContext):
    raw = (message.text or "").translate(DIGITS_FA).strip()
    if not raw.isdigit() or not (1 <= int(raw) <= MAX_DISCOUNT):
        await message.answer(f"یک عدد بین ۱ تا {MAX_DISCOUNT} بفرستید.")
        return
    await state.update_data(flash_pct=int(raw))
    await state.set_state(PaymentState.flash_hours_in)
    await message.answer("⏳ مدت تخفیف را به ساعت بفرستید (۱ تا ۷۲۰). مثال: 24", reply_markup=_flash_back_markup())


@dp.message(PaymentState.flash_hours_in, MainAdmin())
async def flash_hours_got(message: types.Message, state: FSMContext):
    raw = (message.text or "").translate(DIGITS_FA).strip()
    if not raw.isdigit() or not (1 <= int(raw) <= 720):
        await message.answer("یک عدد بین ۱ تا ۷۲۰ (ساعت) بفرستید.")
        return
    data = await state.get_data()
    pct, hours = int(data.get("flash_pct", 0)), int(raw)
    if pct <= 0:
        await state.clear()
        await message.answer("درخواست منقضی شد؛ دوباره شروع کنید.")
        return
    until = (now_iran().replace(tzinfo=None) + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M")
    set_setting("flash_percent", str(pct))
    set_setting("flash_until", until)
    await state.clear()
    log_action(message.from_user.id, f"شروع تخفیف زمان‌دار {pct}٪ برای {hours} ساعت")
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="📢 اعلان به همه کاربران", callback_data="flash_announce")],
            [b(text="بدون اعلان", callback_data="spage_flash")],
        ]
    )
    await message.answer(
        f"✅ تخفیف زمان‌دار {pct}٪ برای {hours} ساعت فعال شد و روی همه پلن‌ها اعمال می‌شود.\n"
        "می‌خواهید به کاربران اعلام شود؟",
        reply_markup=markup,
    )


@dp.callback_query(F.data == "flash_announce", MainAdmin())
async def flash_announce(call: types.CallbackQuery):
    left = flash_remaining()
    if left is None:
        await call.answer("تخفیف زمان‌دار فعالی وجود ندارد.", show_alert=True)
        return
    text_ = (
        "🔥 <b>تخفیف ویژه زمان‌دار!</b>\n\n"
        f"🎁 {get_int('flash_percent')}٪ تخفیف روی همه پلن‌ها\n"
        f"⏳ فقط {fmt_remaining(left)} فرصت دارید.\n\n"
        f"🛒 همین الان از ربات خرید کنید: @{BOT_USERNAME}"
    )
    spawn(broadcast_text(call.bot, text_))
    log_action(call.from_user.id, "اعلان تخفیف زمان‌دار برای همه کاربران")
    await call.answer("ارسال برای همه کاربران آغاز شد.", show_alert=True)


@dp.callback_query(F.data == "flash_stop", MainAdmin())
async def flash_stop(call: types.CallbackQuery):
    set_setting("flash_percent", "0")
    set_setting("flash_until", "")
    log_action(call.from_user.id, "پایان تخفیف زمان‌دار")
    await call.answer("تخفیف زمان‌دار پایان یافت.")
    out, markup = render_settings_page("flash")
    await edit_or_send(call, out, markup)


# --- تأیید دومرحله‌ای (کد تصادفی یا PIN) برای کارهای حساس ---
_PIN_ITERATIONS = 200_000


def pin_hash(pin):
    """هش PBKDF2 با salt تصادفی (قابل‌حمل؛ به BOT_TOKEN وابسته نیست)."""
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", str(pin).encode(), bytes.fromhex(salt), _PIN_ITERATIONS).hex()
    return f"pbkdf2${_PIN_ITERATIONS}${salt}${digest}"


def pin_matches(pin, stored):
    stored = (stored or "").strip()
    if not stored:
        return False
    if stored.startswith("pbkdf2$"):
        try:
            _, iters, salt, digest = stored.split("$")
            calc = hashlib.pbkdf2_hmac("sha256", str(pin).encode(), bytes.fromhex(salt), int(iters)).hex()
        except ValueError:
            return False
        return hmac.compare_digest(calc, digest)
    # فرمت قدیمی (SHA-256 ساده)؛ بعد از ورود موفق به فرمت جدید ارتقا داده می‌شود.
    legacy = hashlib.sha256(f"{TOKEN}:{pin}".encode()).hexdigest()
    return hmac.compare_digest(legacy, stored)


def pin_is_set():
    return bool(get_setting("admin_pin_hash").strip())


async def two_step_gate(call, state, action, arg=""):
    """اگر تأیید دومرحله‌ای روشن باشد، درخواست کد/PIN می‌کند و True برمی‌گرداند (عملیات منتظر می‌ماند)."""
    if not get_bool("twostep_on"):
        return False
    code = str(secrets.randbelow(90000) + 10000)
    await state.set_state(PaymentState.confirm_code)
    await state.update_data(cf_action=action, cf_arg=str(arg), cf_code=code, cf_tries=0)
    if pin_is_set():
        prompt = "🔐 <b>تأیید دومرحله‌ای</b>\n\nبرای ادامه، <b>PIN مدیریتی</b> خود را بفرستید:"
    else:
        prompt = f"🔐 <b>تأیید دومرحله‌ای</b>\n\nبرای ادامه، این عدد را همین‌جا بفرستید:\n<code>{code}</code>"
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data="cf_cancel")]])
    await edit_or_send(call, prompt, markup)
    return True


@dp.callback_query(F.data == "cf_cancel", MainAdmin())
async def two_step_cancel(call: types.CallbackQuery, state: FSMContext):
    await call.answer("لغو شد.")
    await state.clear()
    await edit_or_send(call, admin_dashboard_text(call.from_user.id), get_admin_main_keyboard(call.from_user.id))


async def _ask_new_pin(target, state):
    await state.set_state(PaymentState.setting_pin)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data="spage_sec")]])
    await reply_to(
        target,
        "🔐 PIN جدید را بفرستید (۴ تا ۱۲ رقم).\nبرای حذف PIN، عبارت - را بفرستید.\n"
        "⚠️ پیام شما بعد از ثبت پاک می‌شود. PIN را فراموش نکنید.",
        markup,
    )


@dp.callback_query(F.data == "pin_set", MainAdmin())
async def pin_set_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    if pin_is_set() and await two_step_gate(call, state, "setpin"):
        return
    await _ask_new_pin(call, state)


@dp.message(PaymentState.setting_pin, MainAdmin())
async def pin_set_save(message: types.Message, state: FSMContext):
    raw = (message.text or "").translate(DIGITS_FA).strip()
    try:
        await message.delete()
    except Exception:  # noqa: BLE001
        pass
    if raw == "-":
        set_setting("admin_pin_hash", "")
        note = "🔓 PIN حذف شد؛ از این به بعد کد تصادفی استفاده می‌شود."
    elif raw.isdigit() and 4 <= len(raw) <= 12:
        set_setting("admin_pin_hash", pin_hash(raw))
        note = "✅ PIN ثبت شد."
    else:
        await message.answer("PIN باید فقط عدد و بین ۴ تا ۱۲ رقم باشد.")
        return
    await state.clear()
    log_action(message.from_user.id, "تغییر PIN مدیریتی")
    out, markup = render_settings_page("sec")
    await message.answer(f"{note}\n\n{out}", reply_markup=markup)


async def _two_step_dispatch(message, state, action, arg):
    if action == "restore":
        await _run_restore(message.bot, message.chat.id, message.from_user.id, state)
    elif action == "sadmindel":
        await state.clear()
        await _do_sadmin_delete(message, int(arg))
    elif action == "setpin":
        await state.clear()
        await _ask_new_pin(message, state)
    else:
        await state.clear()


@dp.message(PaymentState.confirm_code, MainAdmin())
async def two_step_check(message: types.Message, state: FSMContext):
    data = await state.get_data()
    entered = (message.text or "").translate(DIGITS_FA).strip()
    if pin_is_set():
        stored_pin = get_setting("admin_pin_hash").strip()
        ok = pin_matches(entered, stored_pin)
        if ok and not stored_pin.startswith("pbkdf2$"):
            set_setting("admin_pin_hash", pin_hash(entered))  # ارتقای هش قدیمی
        try:
            await message.delete()  # PIN در چت نماند
        except Exception:  # noqa: BLE001
            pass
    else:
        ok = bool(entered) and entered == str(data.get("cf_code", ""))
    if not ok:
        tries = int(data.get("cf_tries", 0)) + 1
        if tries >= 3:
            await state.clear()
            log_action(message.from_user.id, "تأیید دومرحله‌ای: سه بار اشتباه")
            await message.answer("❌ سه بار اشتباه وارد شد؛ عملیات لغو شد.", reply_markup=get_admin_main_keyboard(message.from_user.id))
            return
        await state.update_data(cf_tries=tries)
        await message.answer(f"❌ نادرست است. ({3 - tries} تلاش باقی‌مانده)")
        return
    await _two_step_dispatch(message, state, data.get("cf_action"), data.get("cf_arg", ""))


# --- سطح دسترسی ادمین‌های فرعی ---
def _perm_view(uid):
    perms = staff_perms(uid)
    out = (
        f"⚙️ <b>دسترسی‌های ادمین</b> <code>{uid}</code>\n\n"
        "پیش‌فرض: همه ادمین‌ها رسیدها و تیکت‌ها را مدیریت می‌کنند. بخش‌های زیر را می‌توانید جداگانه باز کنید:\n"
    )
    b = InlineKeyboardButton
    rows = []
    for p, label in ADMIN_PERMS.items():
        on = p in perms
        out += f"{'✅' if on else '⬜'} {label}\n"
        rows.append([b(text=("🔴 بستن: " if on else "🟢 باز کردن: ") + label, callback_data=f"spermt_{uid}_{p}")])
    rows.append([b(text="🔙 بازگشت", callback_data="sadmins")])
    return out, InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data.regexp(r"^sperm_\d+$"), MainAdmin())
async def sperm_view(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    uid = int(call.data.split("_")[1])
    if uid not in sub_admins():
        await call.answer("این ادمین یافت نشد.", show_alert=True)
        return
    out, markup = _perm_view(uid)
    await edit_or_send(call, out, markup)


@dp.callback_query(F.data.regexp(r"^spermt_\d+_[a-z]+$"), MainAdmin())
async def sperm_toggle(call: types.CallbackQuery):
    _, uid_s, perm = call.data.split("_")
    uid = int(uid_s)
    if uid not in sub_admins() or perm not in ADMIN_PERMS:
        await call.answer("نامعتبر.", show_alert=True)
        return
    perms = staff_perms(uid)
    perms.symmetric_difference_update({perm})
    set_setting(f"perm_{uid}", ",".join(p for p in ADMIN_PERMS if p in perms))
    log_action(call.from_user.id, f"تغییر دسترسی {perm} برای ادمین {uid}")
    await call.answer("انجام شد.")
    out, markup = _perm_view(uid)
    await edit_or_send(call, out, markup)


# ---------------------------------------------------------------------------
# ویرایش همه دکمه‌های شیشه‌ای منوهای کاربر (منوی اصلی، حساب کاربری، پشتیبانی)
# هر دکمه‌ی داخلی: تغییر نام / مخفی‌سازی / جابه‌جایی؛ دکمه‌های دلخواه: افزودن، ویرایش و حذف
# ---------------------------------------------------------------------------
SCREENS = {
    "main": dict(pid=0, title="🏠 منوی اصلی", home_cb="main_menu"),
    "account": dict(pid=-1, title="👤 حساب کاربری", home_cb="back_to_account"),
    "support": dict(pid=-2, title="🎧 پشتیبانی", home_cb="support_menu"),
    "svc": dict(pid=-3, title="📦 صفحه‌ی هر سرویس", home_cb="my_services_callback"),
    "deliv": dict(pid=-4, title="🎉 دکمه‌های پیام تحویل سرویس", home_cb="main_menu"),
}
PID_SCREEN = {v["pid"]: k for k, v in SCREENS.items()}
SCREEN_HOME_CB = {v["pid"]: v["home_cb"] for v in SCREENS.values()}

UB_ITEMS = {
    "main": [
        dict(key="buy", label=BTN_BUY, cb="main_buy", wide=True),
        dict(key="trial", label="🎁 سرویس تست رایگان", cb="trial_request", wide=True, cond="trial"),
        dict(key="my", label=BTN_MY, cb="my_services_callback"),
        dict(key="wallet", label=BTN_WALLET, cb="wallet_menu"),
        dict(key="account", label=BTN_ACCOUNT, cb="back_to_account"),
        dict(key="support", label=BTN_SUPPORT, cb="support_menu"),
        dict(key="referral", label="🎁 دعوت دوستان", cb="referral_menu", wide=True),
        dict(key="club", label="🏆 باشگاه مشتریان", cb="club_menu", wide=True, cond="club"),
        dict(key="channel", label="📢 کانال ما", url=True, wide=True, tail=True, cond="channel"),
    ],
    "account": [
        dict(key="history", label="📜 تاریخچه تراکنش‌ها", cb="tx_history", wide=True),
        dict(key="club", label="🏆 باشگاه مشتریان", cb="club_menu", wide=True, cond="club"),
        dict(key="referral", label="🎁 دعوت دوستان", cb="referral_menu", wide=True),
        dict(key="home", label="🏠 بازگشت به منوی اصلی", cb="main_menu", wide=True, tail=True),
    ],
    "support": [
        dict(key="ticket", label="🎫 ارسال تیکت", cb="ticket_new", wide=True),
        dict(key="faq", label="❓ سوالات متداول", cb="faq_show", wide=True),
        dict(key="guide", label="📲 آموزش اتصال", cb="guide_connect", wide=True),
        dict(key="home", label="🏠 بازگشت به منوی اصلی", cb="main_menu", wide=True, tail=True),
    ],
}
UB_SCREEN_RE = "(?:main|account|support|svc|deliv)"
UB_ITEMS["svc"] = [
    dict(key="copy", label="🔗 کپی لینک", cb="cfg_copy_{id}"),
    dict(key="renew", label="🔄 تمدید", cb="renew_{id}", cond="sid"),
    dict(key="guide", label="📲 آموزش", cb="guide_connect"),
    dict(key="rebuy", label="➕ همین پلن رو دوباره بگیر", cb="rebuy_{id}", wide=True, cond="sid"),
    dict(key="qr", label="📷 QR کد", cb="cfg_qr_{id}", cond="link", hide_default=True),
    dict(key="support", label="🎧 پشتیبانی", cb="support_menu", hide_default=True),
    dict(key="back", label="⬅️ بازگشت به لیست سرویس‌ها", cb="my_services_callback", wide=True, tail=True),
    dict(key="home", label="🏠 بازگشت به منوی اصلی", cb="main_menu", wide=True, tail=True),
]
UB_ITEMS["deliv"] = [
    dict(key="guide", label="📲 آموزش اتصال", cb="guide_connect", wide=True),
    dict(key="my", label="📦 سرویس‌های من", cb="my_services_callback", wide=True),
    dict(key="home", label="🏠 منوی اصلی", cb="main_menu", wide=True, tail=True),
]


def ub_item(screen, key):
    return next((i for i in UB_ITEMS.get(screen, []) if i["key"] == key), None)


def ub_label(screen, item):
    return get_setting(f"ub_label_{screen}_{item['key']}").strip() or item["label"]


def ub_hidden(screen, key):
    v = get_setting(f"ub_hide_{screen}_{key}")
    if v in ("0", "1"):
        return v == "1"
    it = ub_item(screen, key)
    return bool(it and it.get("hide_default"))


def ub_tokens(screen):
    """ترتیب نهایی: کلید دکمه‌های داخلی و c<id> برای دکمه‌های دلخواه."""
    pid = SCREENS[screen]["pid"]
    with SessionLocal() as s:
        custom_ids = [c.id for c in cb_children(s, pid)]
    items = UB_ITEMS[screen]
    tail = {i["key"] for i in items if i.get("tail")}
    default = (
        [i["key"] for i in items if not i.get("tail")]
        + [f"c{cid}" for cid in custom_ids]
        + [i["key"] for i in items if i.get("tail")]
    )
    valid = set(default)
    result = list(dict.fromkeys(t for t in get_setting(f"uorder_{screen}").split(",") if t in valid))
    for t in default:
        if t in result:
            continue
        if t in tail:
            result.append(t)
        else:
            idx = next((i for i, x in enumerate(result) if x in tail), len(result))
            result.insert(idx, t)
    return result


def ub_move(screen, token, up):
    order = ub_tokens(screen)
    if token not in order:
        return
    i = order.index(token)
    j = i - 1 if up else i + 1
    if 0 <= j < len(order):
        order[i], order[j] = order[j], order[i]
    set_setting(f"uorder_{screen}", ",".join(order))


def ub_build_rows(screen, user_id=None, ctx=None):
    """ردیف‌های دکمه‌ی شیشه‌ای یک منو برای کاربر (با احتساب تغییر نام، مخفی‌سازی، ترتیب و دکمه‌های دلخواه)."""
    ctx = ctx or {}
    mode = get_setting(f"ucols_{screen}").strip()
    items = {i["key"]: i for i in UB_ITEMS[screen]}
    with SessionLocal() as s:
        customs = {
            f"c{c.id}": (c.kind, c.label or "", c.url or "")
            for c in cb_children(s, SCREENS[screen]["pid"], only_active=True)
        }
    entries = []  # (تمام‌عرض؟, دکمه)
    for tok in ub_tokens(screen):
        if tok in items:
            it = items[tok]
            if ub_hidden(screen, tok):
                continue
            cond = it.get("cond")
            if cond == "trial" and not trial_available(user_id):
                continue
            if cond == "club" and not get_bool("club_on"):
                continue
            if cond in GROWTH_CONDS and not get_bool(GROWTH_CONDS[cond]):
                continue
            if cond == "sid" and not ctx.get("sid"):
                continue
            if cond == "link" and not ctx.get("has_link"):
                continue
            label = ub_label(screen, it)[:64]
            if it.get("url"):
                url = channel_url()
                if not url:
                    continue
                btn = InlineKeyboardButton(text=label, url=url)
            else:
                btn = InlineKeyboardButton(text=label, callback_data=it["cb"].replace("{id}", str(ctx.get("id", ""))))
            entries.append((bool(it.get("wide")), btn))
        elif tok in customs:
            kind, label, url = customs[tok]
            if not label.strip():
                continue
            if kind == "url":
                safe = normalize_url(url)
                if not safe:
                    continue
                btn = InlineKeyboardButton(text=label[:64], url=safe)
            else:
                btn = InlineKeyboardButton(text=label[:64], callback_data=f"cb_{tok[1:]}")
            entries.append((True, btn))
    per = 3 if (mode == "3" or (mode == "" and screen == "svc")) else 2
    rows, pending = [], []
    for wide, btn in entries:
        if mode == "1" or (wide and mode not in ("2", "3")):
            if pending:
                rows.append(pending)
                pending = []
            rows.append([btn])
        else:
            pending.append(btn)
            if len(pending) == per:
                rows.append(pending)
                pending = []
    if pending:
        rows.append(pending)
    return rows


# ----- نمای ادمین -----
UB_MODE_LABELS = {"": "خودکار (پیش‌فرض)", "1": "۱ ستونه", "2": "۲ ستونه", "3": "۳ ستونه"}


def ub_screen_view(screen):
    pid = SCREENS[screen]["pid"]
    items = {i["key"]: i for i in UB_ITEMS[screen]}
    with SessionLocal() as s:
        customs = {f"c{c.id}": (c.id, c.label or "", bool(c.is_active)) for c in cb_children(s, pid)}
    b = InlineKeyboardButton
    out = (
        f"🎛 <b>دکمه‌های {SCREENS[screen]['title']}</b>\n\n"
        "روی هر دکمه بزنید تا ویرایشش کنید. با ⬆️⬇️ ترتیب را عوض کنید.\n"
        "👁 نمایش داده می‌شود | 🙈 مخفی | 🔘 دکمه‌ی ساخته‌شده توسط شما\n"
    )
    rows = []
    for tok in ub_tokens(screen):
        if tok in items:
            it = items[tok]
            icon = "🙈" if ub_hidden(screen, tok) else "👁"
            open_btn = b(text=f"{icon} {ub_label(screen, it)}"[:50], callback_data=f"ubi_{screen}_{tok}")
        elif tok in customs:
            cid, label, active = customs[tok]
            icon = "🔘" if active else "⚪️"
            open_btn = b(text=f"{icon} {label}"[:50], callback_data=f"acbn_{cid}")
        else:
            continue
        rows.append(
            [
                open_btn,
                b(text="⬆️", callback_data=f"ubmv_up_{screen}_{tok}"),
                b(text="⬇️", callback_data=f"ubmv_dn_{screen}_{tok}"),
            ]
        )
    mode = get_setting(f"ucols_{screen}").strip()
    if mode not in UB_MODE_LABELS:
        mode = ""
    rows.append([b(text="➕ افزودن دکمه جدید", callback_data=f"acbadd_{pid}")])
    rows.append([b(text=f"↔️ چیدمان: {UB_MODE_LABELS[mode]} (تغییر)", callback_data=f"ubcols_{screen}")])
    rows.append([b(text="♻️ بازنشانی نام‌ها، ترتیب و نمایش", callback_data=f"ubreset_{screen}")])
    rows.append([b(text="🔙 بازگشت", callback_data="ubh")])
    return out, InlineKeyboardMarkup(inline_keyboard=rows)


async def ub_show_screen(target, screen):
    await reply_to(target, *ub_screen_view(screen))


def ub_item_view(screen, key):
    it = ub_item(screen, key)
    if it is None:
        return None
    hidden = ub_hidden(screen, key)
    out = (
        f"🔘 <b>{esc(ub_label(screen, it))}</b>\n\n"
        f"نام پیش‌فرض: {esc(it['label'])}\n"
        f"وضعیت: {'🙈 مخفی' if hidden else '👁 نمایش داده می‌شود'}"
    )
    if it.get("cond") == "trial":
        out += "\nℹ️ این دکمه فقط وقتی «تست رایگان» روشن و برای کاربر در دسترس باشد دیده می‌شود."
    if it.get("cond") == "club":
        out += "\nℹ️ این دکمه فقط وقتی «باشگاه مشتریان» روشن باشد دیده می‌شود."
    if it.get("cond") == "sid":
        out += "\nℹ️ این دکمه فقط برای سرویس‌هایی که از یک پلن ثابت خریداری شده‌اند دیده می‌شود."
    if it.get("cond") == "link":
        out += "\nℹ️ این دکمه فقط وقتی کانفیگ شامل لینک باشد دیده می‌شود."
    if it.get("url"):
        out += "\nℹ️ این دکمه فقط وقتی «لینک کانال» تنظیم شده باشد دیده می‌شود."
    b = InlineKeyboardButton
    rows = [
        [b(text="✏️ تغییر نام", callback_data=f"ubren_{screen}_{key}")],
        [b(text="👁 نمایش دادن" if hidden else "🙈 مخفی کردن", callback_data=f"ubtg_{screen}_{key}")],
        [b(text="↩️ برگرداندن نام پیش‌فرض", callback_data=f"ubrl_{screen}_{key}")],
        [b(text="🔙 بازگشت", callback_data=f"ubs_{screen}")],
    ]
    return out, InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "ubh", MainAdmin())
async def ub_home(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    b = InlineKeyboardButton
    rows = [[b(text=v["title"], callback_data=f"ubs_{k}")] for k, v in SCREENS.items()]
    rows.append([b(text="⌨️ کیبورد پایین صفحه (همیشه در دسترس)", callback_data="kbh")])
    rows.append([b(text="🔙 بازگشت به پنل", callback_data="admin_home")])
    await edit_or_send(
        call,
        "🎛 <b>دکمه‌های محیط کاربر</b>\n\n"
        "یکی از منوهای ربات را انتخاب کنید تا دکمه‌هایش را تغییر نام بدهید، مخفی کنید، جابه‌جا کنید "
        "یا دکمه‌ی جدید (پیام چندمرحله‌ای یا لینک) اضافه کنید.\n\n"
        "ℹ️ کیبورد پایین صفحه جداگانه (دکمه‌ی ⌨️ بالا) و پلن‌ها از «مدیریت پلن‌ها» ویرایش می‌شوند.",
        InlineKeyboardMarkup(inline_keyboard=rows),
    )


@dp.callback_query(F.data.regexp(rf"^ubs_{UB_SCREEN_RE}$"), MainAdmin())
async def ub_screen(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await ub_show_screen(call, call.data.split("_")[1])


@dp.callback_query(F.data.regexp(rf"^ubi_{UB_SCREEN_RE}_[a-z]+$"), MainAdmin())
async def ub_item_open(call: types.CallbackQuery, state: FSMContext):
    _, screen, key = call.data.split("_")
    view = ub_item_view(screen, key)
    if view is None:
        await call.answer("یافت نشد.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    await edit_or_send(call, *view)


@dp.callback_query(F.data.regexp(rf"^ubtg_{UB_SCREEN_RE}_[a-z]+$"), MainAdmin())
async def ub_item_toggle(call: types.CallbackQuery):
    _, screen, key = call.data.split("_")
    if ub_item(screen, key) is None:
        await call.answer("یافت نشد.", show_alert=True)
        return
    set_setting(f"ub_hide_{screen}_{key}", "0" if ub_hidden(screen, key) else "1")
    log_action(call.from_user.id, f"تغییر نمایش دکمه {screen}/{key}")
    await call.answer("انجام شد.")
    await edit_or_send(call, *ub_item_view(screen, key))


@dp.callback_query(F.data.regexp(rf"^ubrl_{UB_SCREEN_RE}_[a-z]+$"), MainAdmin())
async def ub_item_reset_label(call: types.CallbackQuery):
    _, screen, key = call.data.split("_")
    if ub_item(screen, key) is None:
        await call.answer("یافت نشد.", show_alert=True)
        return
    set_setting(f"ub_label_{screen}_{key}", "")
    await call.answer("نام پیش‌فرض برگشت.")
    await edit_or_send(call, *ub_item_view(screen, key))


@dp.callback_query(F.data.regexp(rf"^ubren_{UB_SCREEN_RE}_[a-z]+$"), MainAdmin())
async def ub_rename_start(call: types.CallbackQuery, state: FSMContext):
    _, screen, key = call.data.split("_")
    if ub_item(screen, key) is None:
        await call.answer("یافت نشد.", show_alert=True)
        return
    await call.answer()
    await state.set_state(PaymentState.ub_rename)
    await state.update_data(ub_screen=screen, ub_key=key)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"ubi_{screen}_{key}")]]
    )
    await edit_or_send(call, f"✏️ نام جدید دکمه را بفرستید (حداکثر {CB_LABEL_MAX} کاراکتر، ایموجی مجاز است).", markup)


@dp.message(PaymentState.ub_rename, MainAdmin())
async def ub_rename_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    screen, key = data.get("ub_screen"), data.get("ub_key")
    raw = (message.text or "").strip()
    if screen not in SCREENS or ub_item(screen, key) is None:
        await state.clear()
        await message.answer("درخواست منقضی شد.")
        return
    if not raw or len(raw) > CB_LABEL_MAX:
        await message.answer(f"یک نام معتبر (حداکثر {CB_LABEL_MAX} کاراکتر) بفرستید.")
        return
    set_setting(f"ub_label_{screen}_{key}", raw)
    await state.clear()
    log_action(message.from_user.id, f"تغییر نام دکمه {screen}/{key}")
    await message.answer("✅ ذخیره شد.")
    await ub_show_screen(message, screen)


@dp.callback_query(F.data.regexp(rf"^ubmv_(up|dn)_{UB_SCREEN_RE}_(c\d+|[a-z]+)$"), MainAdmin())
async def ub_move_handler(call: types.CallbackQuery):
    _, direction, screen, token = call.data.split("_")
    ub_move(screen, token, direction == "up")
    await call.answer("ترتیب تغییر کرد.")
    await ub_show_screen(call, screen)


@dp.callback_query(F.data.regexp(rf"^ubcols_{UB_SCREEN_RE}$"), MainAdmin())
async def ub_cols_handler(call: types.CallbackQuery):
    screen = call.data.split("_")[1]
    cycle = {"": "1", "1": "2", "2": "3", "3": ""}
    cur = get_setting(f"ucols_{screen}").strip()
    set_setting(f"ucols_{screen}", cycle.get(cur, ""))
    await call.answer("چیدمان تغییر کرد.")
    await ub_show_screen(call, screen)


@dp.callback_query(F.data.regexp(rf"^ubreset_{UB_SCREEN_RE}$"), MainAdmin())
async def ub_reset_handler(call: types.CallbackQuery):
    screen = call.data.split("_")[1]
    for it in UB_ITEMS[screen]:
        set_setting(f"ub_label_{screen}_{it['key']}", "")
        set_setting(f"ub_hide_{screen}_{it['key']}", "")
    set_setting(f"uorder_{screen}", "")
    set_setting(f"ucols_{screen}", "")
    log_action(call.from_user.id, f"بازنشانی دکمه‌های {screen}")
    await call.answer("بازنشانی شد (دکمه‌های ساخته‌شده توسط شما حذف نشدند).", show_alert=True)
    await ub_show_screen(call, screen)


# --- گزارش هفتگی و پاکسازی خودکار ---
def _jalali_prefixes(start, days):
    return [jdatetime.date.fromgregorian(date=start + timedelta(days=i)).strftime("%Y/%m/%d") for i in range(days)]


def _week_stats(s, start):
    prefixes = _jalali_prefixes(start, 7)
    rev, cnt = _rev_for_prefixes(s, prefixes)
    answered = [Request.answered_at.like(f"{p}%") for p in prefixes]
    charged = int(
        s.query(func.coalesce(func.sum(Request.price), 0))
        .filter(Request.status == "answered", Request.req_type == "charge", or_(*answered))
        .scalar()
        or 0
    )
    start_iso, end_iso = start.isoformat(), (start + timedelta(days=7)).isoformat()
    new_users = (
        s.query(func.count(User.user_id)).filter(User.joined_at >= start_iso, User.joined_at < end_iso).scalar() or 0
    )
    tickets = s.query(func.count(Ticket.id)).filter(or_(*[Ticket.created_at.like(f"{p}%") for p in prefixes])).scalar() or 0
    avg = float(s.query(func.avg(Rating.score)).filter(or_(*[Rating.created_at.like(f"{p}%") for p in prefixes])).scalar() or 0)
    return dict(rev=rev, cnt=cnt, charged=charged, new_users=int(new_users), tickets=int(tickets), avg=avg)


def _delta(a, b):
    if b == 0:
        return "—" if a == 0 else "🆕"
    pct = int((a - b) * 100 / b)
    return f"{'▲' if pct > 0 else ('▼' if pct < 0 else '▬')} {abs(pct)}٪"


def weekly_report_text():
    today = today_iran()
    with SessionLocal() as s:
        cur = _week_stats(s, today - timedelta(days=7))
        prev = _week_stats(s, today - timedelta(days=14))
    a = jdatetime.date.fromgregorian(date=today - timedelta(days=7)).strftime("%Y/%m/%d")
    z = jdatetime.date.fromgregorian(date=today - timedelta(days=1)).strftime("%Y/%m/%d")
    out = f"📆 <b>گزارش هفتگی</b> ({a} تا {z})\n<i>مقایسه با هفته قبل</i>\n\n"
    out += f"💰 فروش: {cur['rev']:,} تومان ({cur['cnt']} سفارش) {_delta(cur['rev'], prev['rev'])}\n"
    out += f"💳 شارژ کیف پول: {cur['charged']:,} تومان {_delta(cur['charged'], prev['charged'])}\n"
    out += f"👥 کاربر جدید: {cur['new_users']} {_delta(cur['new_users'], prev['new_users'])}\n"
    out += f"🎫 تیکت جدید: {cur['tickets']} {_delta(cur['tickets'], prev['tickets'])}\n"
    if cur["avg"] > 0:
        out += f"⭐ میانگین امتیاز: {cur['avg']:.2f}\n"
    out += f"\n🔴 درخواست در انتظار: {stats_numbers()['unanswered']}"
    return out


async def job_weekly_report(bot):
    if not get_bool("weekly_report_on") or jalali_today().weekday() != 0:  # شنبه
        return
    await bot.send_message(chat_id=ADMIN_ID, text=weekly_report_text())


async def job_cleanup(bot):
    """حذف لاگ‌ها، پیام‌های زمان‌بندی‌شده‌ی تمام‌شده و تیکت‌های بسته‌ی قدیمی تا دیتابیس سبک بماند."""
    if not get_bool("cleanup_on"):
        return
    keep = max(7, get_int("log_keep_days"))
    cutoff = today_iran() - timedelta(days=keep)
    cutoff_j = jdatetime.date.fromgregorian(date=cutoff).strftime("%Y/%m/%d")
    removed = {}
    with SessionLocal() as s:
        removed["logs"] = s.query(AdminLog).filter(AdminLog.created_at < cutoff_j).delete(synchronize_session=False)
        removed["broadcasts"] = (
            s.query(ScheduledBroadcast)
            .filter(ScheduledBroadcast.status.in_(["sent", "cancelled"]), ScheduledBroadcast.run_at < cutoff.isoformat())
            .delete(synchronize_session=False)
        )
        old = [t.id for t in s.query(Ticket).filter(Ticket.status != "open", Ticket.created_at < cutoff_j).all()]
        for i in range(0, len(old), 400):
            chunk = old[i : i + 400]
            s.query(TicketMessage).filter(TicketMessage.ticket_id.in_(chunk)).delete(synchronize_session=False)
            s.query(Ticket).filter(Ticket.id.in_(chunk)).delete(synchronize_session=False)
        removed["tickets"] = len(old)
        s.commit()
    logging.info("cleanup done: %s", removed)


# --- بکاپ: بازیابی با تأیید دومرحله‌ای و ارسال به کانال خصوصی ---
async def _run_restore(bot, chat_id, admin_id, state):
    data = await state.get_data()
    file_id = data.get("restore_file_id")
    if not file_id:
        await state.clear()
        await bot.send_message(chat_id=chat_id, text="درخواست منقضی شده است.")
        return
    try:
        buf = io.BytesIO()
        await bot.download(file_id, destination=buf)
        payload = parse_backup(buf.getvalue())
        await asyncio.to_thread(apply_backup, payload)
        refresh_menu_texts()
    except Exception as e:  # noqa: BLE001
        logging.exception("restore failed: %s", e)  # خطای کامل فقط در لاگ سرور
        await state.clear()
        await bot.send_message(
            chat_id=chat_id, text=f"❌ بازیابی ناموفق بود (اطلاعات قبلی دست‌نخورده ماند): {err_text(e)}"
        )
        return
    await state.clear()
    log_action(admin_id, "بازیابی دیتابیس از بکاپ")
    await bot.send_message(
        chat_id=chat_id, text="✅ بازیابی با موفقیت انجام شد.", reply_markup=get_admin_main_keyboard(admin_id)
    )


@dp.callback_query(F.data == "backup_to_channel", MainAdmin())
async def backup_to_channel(call: types.CallbackQuery):
    chan = get_setting("backup_channel").strip()
    if not chan:
        await call.answer(
            "ابتدا آیدی کانال بکاپ را در تنظیمات ← یادآوری، گزارش و نظرسنجی تنظیم کنید.", show_alert=True
        )
        return
    await call.answer("در حال ارسال...")
    try:
        await send_backup(call.bot, chat_ref(chan), f"🧰 بکاپ دستی - {get_current_jalali()}")
        log_action(call.from_user.id, "ارسال بکاپ به کانال")
        await call.message.answer("✅ بکاپ به کانال ارسال شد.")
    except Exception as e:  # noqa: BLE001
        await call.message.answer(f"❌ ارسال ناموفق بود: {err_text(e)}\nمطمئن شوید ربات در کانال «ادمین» است.")


async def _do_sadmin_delete(target, rm):
    ids = [i for i in sub_admins() if i != rm]
    set_setting("sub_admins", ",".join(str(i) for i in ids))
    set_setting(f"perm_{rm}", "")
    log_action(target.from_user.id, f"حذف ادمین فرعی {rm}")
    await _render_subadmins(target)


# ---------------------------------------------------------------------------
# بکاپ و بازیابی
# ---------------------------------------------------------------------------
def build_backup_bytes():
    payload = {"version": 1, "created_at": get_current_jalali(), "tables": {}}
    with engine.connect() as conn:
        for table in Base.metadata.sorted_tables:
            rows = conn.execute(table.select()).mappings().all()
            payload["tables"][table.name] = [dict(r) for r in rows]
    return json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")


def parse_backup(raw_bytes):
    payload = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("tables"), dict):
        raise ValueError("ساختار فایل بکاپ معتبر نیست.")
    names = {t.name for t in Base.metadata.sorted_tables}
    tables = payload["tables"]
    if not set(tables.keys()) <= names or not isinstance(tables.get("users"), list):
        raise ValueError("این فایل بکاپ ربات Swan VPN نیست.")
    for rows in tables.values():
        if not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
            raise ValueError("ساختار جدول‌ها در فایل بکاپ معتبر نیست.")
    return payload


def apply_backup(payload):
    tables = payload["tables"]
    ordered = list(Base.metadata.sorted_tables)
    with engine.begin() as conn:
        for t in reversed(ordered):
            conn.execute(t.delete())
        for t in ordered:
            rows = tables.get(t.name, [])
            cols = list(t.c.keys())
            clean = [{c: row.get(c) for c in cols} for row in rows]
            if clean:
                conn.execute(t.insert(), clean)
        if engine.dialect.name == "postgresql":
            for t in ordered:
                if "id" in t.c:
                    try:
                        with conn.begin_nested():
                            conn.execute(
                                text(
                                    f"SELECT setval(pg_get_serial_sequence('{t.name}', 'id'), "
                                    f"COALESCE((SELECT MAX(id) FROM {t.name}), 1), "
                                    f"(SELECT MAX(id) FROM {t.name}) IS NOT NULL)"
                                )
                            )
                    except Exception as e:  # noqa: BLE001
                        logging.warning("sequence reset skipped for %s: %s", t.name, e)
    with SessionLocal() as s:
        ensure_categories(s)
    _cache.clear()
    _ban_cache.clear()
    _user_seen.clear()


async def send_backup(bot, chat_id, caption, data=None):
    data = data if data is not None else await asyncio.to_thread(build_backup_bytes)
    name = f"swan_backup_{now_iran().strftime('%Y%m%d_%H%M')}.json"
    await bot.send_document(chat_id=chat_id, document=BufferedInputFile(data, filename=name), caption=caption)


@dp.callback_query(F.data == "admin_backup", MainAdmin())
async def admin_backup_menu(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="📤 دریافت بکاپ همین الان", callback_data="backup_now")],
            [b(text="📤 ارسال بکاپ به کانال خصوصی", callback_data="backup_to_channel")],
            [b(text="📥 بازیابی از فایل بکاپ", callback_data="backup_restore")],
            [b(text="🔙 بازگشت به پنل", callback_data="admin_home")],
        ]
    )
    await edit_or_send(
        call,
        "🧰 <b>بکاپ دیتابیس</b>\n\nبکاپ شامل کاربران، موجودی‌ها، سرویس‌ها، درخواست‌ها و تنظیمات است. "
        "فایل را جای امنی نگه دارید.",
        markup,
    )


@dp.callback_query(F.data == "backup_now", MainAdmin())
async def backup_now(call: types.CallbackQuery):
    await call.answer("در حال ساخت بکاپ...")
    try:
        await send_backup(call.bot, call.message.chat.id, f"🧰 بکاپ دستی - {get_current_jalali()}")
        log_action(call.from_user.id, "دریافت بکاپ دستی")
    except Exception as e:  # noqa: BLE001
        await call.message.answer(f"❌ ساخت بکاپ ناموفق بود: {err_text(e)}")


@dp.callback_query(F.data == "backup_restore", MainAdmin())
async def backup_restore_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PaymentState.restore_wait_file)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data="admin_backup")]])
    await edit_or_send(
        call,
        "📥 فایل بکاپ (.json) را همین‌جا بفرستید.\n"
        "⚠️ با بازیابی، اطلاعات فعلی دیتابیس با محتوای فایل جایگزین می‌شود.",
        markup,
    )


@dp.message(PaymentState.restore_wait_file, MainAdmin(), F.document)
async def backup_restore_file(message: types.Message, state: FSMContext):
    buf = io.BytesIO()
    try:
        await message.bot.download(message.document.file_id, destination=buf)
        payload = parse_backup(buf.getvalue())
    except Exception as e:  # noqa: BLE001
        await message.answer(f"❌ فایل نامعتبر است: {err_text(e)}")
        return

    summary = "\n".join(f"• {name}: {len(rows)}" for name, rows in payload["tables"].items())
    await state.update_data(restore_file_id=message.document.file_id)
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="✅ بله، بازیابی کن", callback_data="restore_confirm")],
            [b(text="انصراف", callback_data="admin_backup")],
        ]
    )
    await message.answer(
        f"فایل بکاپ ({esc(str(payload.get('created_at', '')))}) خوانده شد:\n{summary}\n\n"
        "⚠️ مطمئنید که اطلاعات فعلی با این بکاپ جایگزین شود؟",
        reply_markup=markup,
    )


@dp.message(PaymentState.restore_wait_file, MainAdmin())
async def backup_restore_not_file(message: types.Message):
    await message.answer("لطفاً فایل بکاپ را به صورت فایل (document) بفرستید.")


@dp.callback_query(F.data == "restore_confirm", MainAdmin())
async def backup_restore_confirm(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get("restore_file_id"):
        await call.answer("درخواست منقضی شده است.", show_alert=True)
        return
    await call.answer()
    if await two_step_gate(call, state, "restore"):
        return
    await _run_restore(call.bot, call.message.chat.id, call.from_user.id, state)


# ---------------------------------------------------------------------------
# کارهای زمان‌بندی‌شده: یادآوری تمدید، گزارش روزانه، بکاپ شبانه
# ---------------------------------------------------------------------------
async def job_reminders_orig(bot):
    days = get_int("reminder_days")
    todo = []
    with SessionLocal() as s:
        rows = (
            s.query(UserConfig)
            .filter(UserConfig.expires_at.isnot(None), or_(UserConfig.reminded.is_(None), UserConfig.reminded.is_(False)))
            .all()
        )
        for c in rows:
            try:
                left = (date.fromisoformat(c.expires_at) - today_iran()).days
            except (TypeError, ValueError):
                continue
            if left <= days:
                todo.append((c.id, c.user_id, c.config_name, left, c.service_id))
                c.reminded = True
        s.commit()

    for cfg_id, uid, name, left, sid in todo:
        if left >= 0:
            msg = f"⏰ سرویس «{esc(name or '')}» {left} روز دیگه تموم می‌شه. برای تمدید روی دکمه زیر بزن 👇"
        else:
            msg = f"⚠️ سرویس «{esc(name or '')}» منقضی شده. برای تمدید روی دکمه زیر بزن 👇"
        markup = None
        if sid:
            markup = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔄 تمدید سرویس", callback_data=f"renew_{cfg_id}")]]
            )
        try:
            await bot.send_message(chat_id=uid, text=msg, reply_markup=markup)
        except Exception as e:  # noqa: BLE001
            logging.info("reminder to %s failed: %s", uid, e)
        await asyncio.sleep(0.05)


async def job_report(bot):
    if get_bool("report_on"):
        await bot.send_message(chat_id=ADMIN_ID, text=daily_report_text())


async def job_backup(bot):
    if not get_bool("backup_on"):
        return
    data = await asyncio.to_thread(build_backup_bytes)
    caption = f"🧰 بکاپ شبانه خودکار - {get_current_jalali()}"
    await send_backup(bot, ADMIN_ID, caption, data)
    chan = get_setting("backup_channel").strip()
    if chan:
        try:
            await send_backup(bot, chat_ref(chan), caption, data)
        except Exception as e:  # noqa: BLE001
            logging.warning("backup to channel failed: %s", e)
            try:
                await bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"⚠️ ارسال بکاپ به کانال ناموفق بود: {err_text(e)}\nمطمئن شوید ربات در کانال «ادمین» است.",
                )
            except Exception:  # noqa: BLE001
                pass


# ---------------------------------------------------------------------------
# مانیتور سرور و اعلان قطعی
# ---------------------------------------------------------------------------
_monitor = {"fails": {}, "down": False}


def parse_urls(raw):
    out = []
    for part in re.split(r"[,\s،]+", raw or ""):
        part = part.strip()
        if re.match(r"^https?://\S+$", part) and part not in out:
            out.append(part)
    return out


async def check_url(session, url):
    try:
        async with session.get(url, allow_redirects=True) as resp:
            return resp.status < 500
    except Exception:  # noqa: BLE001
        return False


async def send_message_retry(bot, chat_id, text, **kwargs):
    """ارسال پیام؛ با محدودیت نرخ تلگرام یک‌بار بعد از صبر دوباره تلاش می‌کند (قبلاً پیام گم می‌شد)."""
    try:
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)


async def broadcast_text(bot, text_):
    with SessionLocal() as s:
        ids = segment_user_ids(s, "all", promo=False)
    for uid in ids:
        try:
            await send_message_retry(bot, uid, text_)
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(0.05)


def outage_message(down):
    key = "outage_text" if down else "recovery_text"
    return esc(get_setting(key).strip() or DEFAULTS[key])


async def announce_outage(bot, down, bad_urls=None):
    if down:
        detail = "\n".join(f"• {esc(u)}" for u in (bad_urls or []))
        await notify_staff(bot, f"🔴 <b>مانیتور: قطعی تشخیص داده شد</b>\n{detail}")
    else:
        await notify_staff(bot, "🟢 <b>مانیتور: سرورها دوباره در دسترس‌اند.</b>")
    if get_bool("monitor_notify_users"):
        spawn(broadcast_text(bot, outage_message(down)))


async def monitor_loop(bot):
    async with ClientSession(timeout=ClientTimeout(total=10)) as session:
        while True:
            await asyncio.sleep(60)
            try:
                if not get_bool("monitor_on"):
                    continue
                urls = parse_urls(get_setting("monitor_urls"))
                if not urls:
                    continue
                results = await asyncio.gather(*(check_url(session, u) for u in urls))
                for u, ok in zip(urls, results):
                    _monitor["fails"][u] = 0 if ok else _monitor["fails"].get(u, 0) + 1
                threshold = max(1, get_int("monitor_fails"))
                bad = [u for u in urls if _monitor["fails"].get(u, 0) >= threshold]
                if bad and not _monitor["down"]:
                    _monitor["down"] = True
                    await announce_outage(bot, True, bad)
                elif not bad and _monitor["down"]:
                    _monitor["down"] = False
                    await announce_outage(bot, False)
            except Exception as e:  # noqa: BLE001
                logging.error("monitor error: %s", e)


# ---------------------------------------------------------------------------
# پست خودکار لیست تعرفه در کانال
# ---------------------------------------------------------------------------
def parse_times(raw):
    out = []
    for part in re.split(r"[,\s،]+", (raw or "").translate(DIGITS_FA)):
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", part)
        if m and int(m.group(1)) < 24 and int(m.group(2)) < 60:
            t = f"{int(m.group(1)):02d}:{m.group(2)}"
            if t not in out:
                out.append(t)
    return out


def channel_post_text():
    general = get_int("discount_percent")
    with SessionLocal() as s:
        cats = [
            (c.key, c.label, c.emoji or "")
            for c in s.query(Category).filter(Category.is_active.is_(True)).order_by(Category.sort_order, Category.id).all()
        ]
        by_cat = {}
        for r in s.query(Service).order_by(Service.id).all():
            by_cat.setdefault(r.category, []).append((r.name, r.price))
    title = get_setting("post_title").strip() or "📊 لیست تعرفه Swan VPN"
    out = f"<b>{esc(title)}</b>\n"
    if general > 0:
        out += f"\n🎁 تخفیف ویژه {general}٪ فعال است!\n"
    for key, label, emoji in cats:
        plans = by_cat.get(key, [])
        if not plans:
            continue
        out += f"\n📂 <b>{esc((emoji + ' ' + label).strip())}:</b>\n"
        for name, price in plans:
            out += price_line(name, price, price_with_percent(price, general))
    out += f"\n🤖 برای خرید: @{BOT_USERNAME}"
    return out


def channel_chat_id():
    ch = get_setting("post_channel").strip()
    if not ch:
        raise ValueError("کانال تنظیم نشده است.")
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if ch.startswith(prefix):
            ch = "@" + ch[len(prefix):].strip("/")
    if re.fullmatch(r"-?\d+", ch):
        return int(ch)
    return ch if ch.startswith("@") else "@" + ch


async def post_to_channel(bot):
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🛒 شروع خرید", url=f"https://t.me/{BOT_USERNAME}")]]
    )
    chat_id = channel_chat_id()
    text_ = channel_post_text()
    photo = get_setting("post_photo").strip()
    if not photo:
        await bot.send_message(chat_id=chat_id, text=text_, reply_markup=markup)
        return
    # کپشن تلگرام حداکثر ۱۰۲۴ کاراکتر است؛ اگر لیست بلندتر بود عکس با عنوان و لیست در پیام بعدی می‌آید
    if len(text_) <= 1024:
        await bot.send_photo(chat_id=chat_id, photo=photo, caption=text_, reply_markup=markup)
    else:
        title = get_setting("post_title").strip() or "📊 لیست تعرفه Swan VPN"
        await bot.send_photo(chat_id=chat_id, photo=photo, caption=f"<b>{esc(title)}</b>")
        await bot.send_message(chat_id=chat_id, text=text_, reply_markup=markup)


async def job_channel_posts(bot):
    if not get_bool("post_on") or not get_setting("post_channel").strip():
        return
    now = now_iran()
    today = now.date().isoformat()
    for t in parse_times(get_setting("post_times")):
        h, m = (int(x) for x in t.split(":"))
        diff = (now.hour * 60 + now.minute) - (h * 60 + m)
        if 0 <= diff <= 10 and get_setting(f"last_post_{t}") != today:
            set_setting(f"last_post_{t}", today)
            try:
                await post_to_channel(bot)
            except Exception as e:  # noqa: BLE001
                logging.error("channel post failed: %s", e)
                try:
                    await bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"❌ پست خودکار کانال ناموفق بود: {err_text(e)}\n"
                        "مطمئن شوید ربات در کانال ادمین است و اجازه ارسال پیام دارد.",
                    )
                except Exception:  # noqa: BLE001
                    pass


# ---------------------------------------------------------------------------
# اجرای پیام‌های همگانی زمان‌بندی‌شده
# ---------------------------------------------------------------------------
async def _run_scheduled_broadcast(bot, bid, seg, chat_id, msg_id, admin_id):
    result = await _broadcast_and_report(
        bot, admin_id, chat_id, msg_id, seg, admin_id, prefix=f"⏰ (زمان‌بندی #{bid}) "
    )
    with SessionLocal() as s:
        x = s.get(ScheduledBroadcast, bid)
        if x is not None:
            x.status = "sent"
            x.result = result
            s.commit()


async def job_scheduled_broadcasts(bot):
    now_s = now_iran().strftime("%Y-%m-%dT%H:%M")
    due = []
    with SessionLocal() as s:
        for x in s.query(ScheduledBroadcast).filter(
            ScheduledBroadcast.status == "pending", ScheduledBroadcast.run_at <= now_s
        ).all():
            x.status = "sending"
            due.append((x.id, x.segment, x.chat_id, x.message_id, x.created_by))
        s.commit()
    for bid, seg, chat_id, msg_id, admin_id in due:
        spawn(_run_scheduled_broadcast(bot, bid, seg, chat_id, msg_id, admin_id))


async def job_pending_reminder(bot):
    """هر N دقیقه (حداکثر ۵ بار) برای سفارش‌های بررسی‌نشده به ادمین یادآوری می‌فرستد."""
    if not get_bool("admin_remind_on"):
        return
    mins = max(5, get_int("admin_remind_minutes"))
    cutoff = (now_iran().replace(tzinfo=None) - timedelta(minutes=mins)).strftime("%Y-%m-%dT%H:%M")
    items = []
    with SessionLocal() as s:
        rows = (
            s.query(Request)
            .filter(
                Request.status == "unanswered",
                Request.created_iso.isnot(None),
                Request.created_iso <= cutoff,
                or_(Request.last_remind_iso.is_(None), Request.last_remind_iso <= cutoff),
                or_(Request.remind_count.is_(None), Request.remind_count < 5),
            )
            .order_by(Request.id)
            .limit(15)
            .all()
        )
        for r in rows:
            items.append((r.id, r.req_type, r.plan_name, r.user_id))
            r.last_remind_iso = iso_now_minute()
            r.remind_count = (r.remind_count or 0) + 1
        s.commit()
    if not items:
        return
    out = f"⏰ <b>{len(items)} سفارش بیش از {mins} دقیقه است بررسی نشده:</b>\n\n"
    for rid, rtype, plan, uid in items:
        out += f"#{rid} | {TYPE_LABELS.get(rtype, rtype or '')} | {esc(plan or '')} | <code>{uid}</code>\n"
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔴 مشاهده درخواست‌ها", callback_data="admin_unanswered")]]
    )
    await notify_staff(bot, out, markup)


# ---------------------------------------------------------------------------
# مصرف و وضعیت سرویس از ساب‌لینک (هدر استاندارد subscription-userinfo)
# ---------------------------------------------------------------------------
SUB_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)
USAGE_TTL = 300  # کش ۵ دقیقه‌ای
_usage_cache = {}


def find_sub_url(text_):
    m = SUB_URL_RE.search(text_ or "")
    return m.group(0).rstrip(".,;)،") if m else None


def parse_userinfo(raw):
    vals = {}
    for part in (raw or "").split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            try:
                vals[k.strip().lower()] = int(float(v.strip()))
            except ValueError:
                pass
    return vals


async def _url_is_public(url):
    """فقط آدرس‌های http(s) که به IP عمومی می‌رسند مجازند (جلوگیری از SSRF به شبکه داخلی/متادیتای کلود)."""
    try:
        parts = urlsplit(url)
        host = parts.hostname
        port = parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError:
        return False
    if parts.scheme not in ("http", "https") or not host:
        return False
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError:
        return False
    if not infos:
        return False
    for info in infos:
        try:
            if not ipaddress.ip_address(info[4][0].split("%")[0]).is_global:
                return False
        except ValueError:
            return False
    return True


def _apply_usage_response(res, body, info):
    if not body.strip() and not info:
        res["error"] = "پاسخ خالی"
        return
    res["ok"] = True
    if info:
        v = parse_userinfo(info)
        res.update(
            has_info=True,
            used=v.get("upload", 0) + v.get("download", 0),
            total=v.get("total", 0),
            expire=v.get("expire", 0),
        )


async def fetch_usage(url, force=False, timeout=12):
    """{ok, error, has_info, used, total, expire} را از ساب‌لینک می‌خواند (با کش ۵ دقیقه‌ای)."""
    now = time.time()
    hit = _usage_cache.get(url)
    if hit and not force and now - hit[0] < USAGE_TTL:
        return hit[1]
    res = {"ok": False, "error": "", "has_info": False, "used": 0, "total": 0, "expire": 0}
    try:
        async with ClientSession(timeout=ClientTimeout(total=timeout)) as session:
            current = url
            for _hop in range(4):  # ریدایرکت‌ها دستی دنبال می‌شوند تا هر مقصد بررسی شود
                if not await _url_is_public(current):
                    res["error"] = "blocked"
                    break
                async with session.get(current, headers={"User-Agent": "v2rayN/6.0"}, allow_redirects=False) as resp:
                    location = resp.headers.get("Location")
                    if resp.status in (301, 302, 303, 307, 308) and location:
                        current = urljoin(current, location)
                        continue
                    if resp.status != 200:
                        res["error"] = f"HTTP {resp.status}"
                    else:
                        body = await resp.content.read(65536)
                        _apply_usage_response(res, body, resp.headers.get("subscription-userinfo"))
                    break
            else:
                res["error"] = "too many redirects"
    except asyncio.TimeoutError:
        res["error"] = "timeout"
    except Exception as e:  # noqa: BLE001
        res["error"] = type(e).__name__
    if len(_usage_cache) > 2000:
        _usage_cache.clear()
    _usage_cache[url] = (now, res)
    return res


def fmt_bytes(n):
    n = float(max(0, n))
    if n >= 1024**3:
        return f"{n / 1024**3:.2f} GB"
    return f"{n / 1024**2:.0f} MB"


def usage_is_finished(u):
    return bool(u and u.get("ok") and u.get("has_info") and u.get("total", 0) > 0 and u["used"] >= u["total"])


def usage_lines(u):
    if not u or not u.get("ok") or not u.get("has_info"):
        return ""
    used, total = u["used"], u["total"]
    if total <= 0:
        return f"📊 مصرف: {fmt_bytes(used)} (حجم نامحدود ♾)\n"
    pct = min(100, int(used * 100 / total))
    filled = round(pct / 10)
    out = f"📊 مصرف: {fmt_bytes(used)} از {fmt_bytes(total)} ({pct}٪)\n{'▰' * filled}{'▱' * (10 - filled)}\n"
    if used >= total:
        out += "🔴 حجمت تموم شده؛ برای ادامه تمدید کن.\n"
    else:
        out += f"📥 باقی‌مانده: {fmt_bytes(total - used)}\n"
    return out


def sync_real_expiry(cfg_id, u):
    """تاریخ انقضای واقعی پنل را در دیتابیس ثبت می‌کند و تاریخ ISO نهایی را برمی‌گرداند."""
    ts = (u or {}).get("expire") or 0
    if ts <= 0:
        return None
    try:
        real = datetime.fromtimestamp(ts, iran_timezone).date()
    except (OverflowError, OSError, ValueError):
        return None
    with SessionLocal() as s:
        cfg = s.get(UserConfig, cfg_id)
        if cfg is None:
            return None
        try:
            old = date.fromisoformat(cfg.expires_at) if cfg.expires_at else None
        except ValueError:
            old = None
        if old == real:
            return real.isoformat()
        hold = get_setting(f"renewhold_{cfg_id}")
        if hold and old and real < old:  # تمدید تازه در ربات تایید شده و پنل هنوز به‌روز نشده
            try:
                if today_iran() <= date.fromisoformat(hold):
                    return None
            except ValueError:
                pass
        cfg.expires_at = real.isoformat()
        if (real - today_iran()).days > get_int("reminder_days"):
            cfg.reminded = False
        s.commit()
    return real.isoformat()


def test_report(r):
    if not r["ok"]:
        return f"❌ <b>لینک مشکل دارد</b>\nخطا: {esc(r.get('error') or '?')}"
    out = "✅ <b>لینک سالم است</b>\n"
    if r["has_info"]:
        out += usage_lines(r)
        if r["expire"] > 0:
            try:
                d = datetime.fromtimestamp(r["expire"], iran_timezone).date()
                out += f"📅 انقضا: {jalali_from_iso(d.isoformat())} ({(d - today_iran()).days} روز)\n"
            except (OverflowError, OSError, ValueError):
                pass
        else:
            out += "📅 انقضا: بدون تاریخ\n"
        if usage_is_finished(r):
            out += "⚠️ حجم این لینک تموم شده.\n"
    else:
        out += "ℹ️ لینک پاسخ می‌دهد ولی هدر مصرف (subscription-userinfo) ندارد.\n"
    return out


@dp.message(Command("testsub"), Staff())
async def testsub_cmd(message: types.Message, command: CommandObject):
    url = find_sub_url(command.args or "")
    if not url:
        await message.answer("استفاده: <code>/testsub لینک_ساب</code>")
        return
    await message.answer("🔎 دارم لینک رو تست می‌کنم...")
    r = await fetch_usage(url, force=True)
    await message.answer(test_report(r))


_usage_job_running = False


async def job_usage_alerts(bot):
    """هر ۳۰ دقیقه مصرف همه‌ی ساب‌لینک‌ها را می‌خواند؛ هشدار بدون تکرار برای کاربر و گزارش برای ادمین."""
    global _usage_job_running
    if _usage_job_running or not get_bool("usage_on"):
        return
    try:
        if int(time.time()) - int(get_setting("usage_last_run") or 0) < 1800:
            return
    except ValueError:
        pass
    _usage_job_running = True
    try:
        set_setting("usage_last_run", str(int(time.time())))
        with SessionLocal() as s:
            rows = [
                (c.id, c.user_id, c.config_name, c.config_details, c.service_id)
                for c in s.query(UserConfig).all()
            ]
        targets = []
        for cid, uid, name, details, sid in rows:
            url = find_sub_url(details)
            if url:
                targets.append((cid, uid, name, sid, url))
        targets = targets[:400]
        if not targets:
            return
        sem = asyncio.Semaphore(5)

        async def one(t):
            async with sem:
                return t, await fetch_usage(t[4], force=True)

        results = await asyncio.gather(*(one(t) for t in targets))
        warn_pct = get_int("usage_warn_percent") or 80
        alerts_on = get_bool("usage_alerts_on")
        report_on = get_bool("usage_admin_report_on")
        sync_on = get_bool("usage_sync_expiry_on")
        broken, finished = [], []
        for (cid, uid, name, sid, _url), u in results:
            prev = {x for x in get_setting(f"ualert_{cid}").split(",") if x}
            flags = set()
            if not u["ok"]:
                n = get_int(f"ufail_{cid}") + 1
                set_setting(f"ufail_{cid}", str(n))
                flags |= prev & {"w", "f"}
                if n >= 2:
                    flags.add("x")
            else:
                if get_int(f"ufail_{cid}"):
                    set_setting(f"ufail_{cid}", "0")
                if sync_on and u["has_info"]:
                    sync_real_expiry(cid, u)
                if u["has_info"] and u["total"] > 0:
                    pct = u["used"] * 100 / u["total"]
                    if pct >= 100:
                        flags |= {"w", "f"}
                    elif pct >= warn_pct:
                        flags.add("w")
            new = flags - prev
            if flags != prev:
                set_setting(f"ualert_{cid}", ",".join(sorted(flags)))
            if report_on:
                if "x" in new:
                    broken.append(f"❌ #{cid} | {esc(name or '')} | <code>{uid}</code> | {esc(u.get('error') or '?')}")
                if "f" in new:
                    finished.append(f"🔴 #{cid} | {esc(name or '')} | <code>{uid}</code>")
            if alerts_on and ("f" in new or "w" in new):
                if "f" in new:
                    msg = f"🔴 حجم سرویس «{esc(name or '')}» تموم شد. برای ادامه تمدیدش کن 👇"
                else:
                    pct_i = int(u["used"] * 100 / u["total"])
                    msg = (
                        f"⚠️ {pct_i}٪ حجم سرویس «{esc(name or '')}» مصرف شده "
                        f"({fmt_bytes(u['total'] - u['used'])} باقی مونده). قبل از تموم شدن تمدیدش کن 👇"
                    )
                rows_kb = []
                if sid:
                    rows_kb.append([InlineKeyboardButton(text="🔄 تمدید سرویس", callback_data=f"renew_{cid}")])
                rows_kb.append([InlineKeyboardButton(text="📦 سرویس‌های من", callback_data="my_services_callback")])
                try:
                    await bot.send_message(chat_id=uid, text=msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows_kb))
                except Exception as e:  # noqa: BLE001
                    logging.info("usage alert to %s failed: %s", uid, e)
                await asyncio.sleep(0.05)
        if broken or finished:
            out = "🚨 <b>گزارش لینک سرویس‌ها</b>\n"
            if broken:
                out += "\n<b>لینک خراب/بی‌پاسخ:</b>\n" + "\n".join(broken[:25]) + "\n"
            if finished:
                out += "\n<b>حجم تموم‌شده:</b>\n" + "\n".join(finished[:25]) + "\n"
            await notify_staff(bot, out)
    except Exception as e:  # noqa: BLE001
        logging.error("usage alerts failed: %s", e)
    finally:
        _usage_job_running = False


# ---------------------------------------------------------------------------
# تحویل کانفیگ: تست لینک و تصمیم ادمین (ارسال یا عدم ارسال لینک خراب)
# ---------------------------------------------------------------------------
async def _finish_config_delivery(bot, chat_id, admin_id, state, config_text, note=""):
    data = await state.get_data()
    req_id = data.get("req_id")
    target_user_id = data.get("target_user_id")
    plan_name = data.get("plan_name")
    service_id = data.get("service_id")
    start_date = today_iran()

    with SessionLocal() as s:
        r = s.get(Request, req_id) if req_id else None
        if r is None or r.status != "unanswered":
            await state.clear()
            await bot.send_message(chat_id=chat_id, text="این درخواست قبلاً بررسی شده است.")
            return
        is_trial = r.req_type == "trial"
        sale_req_type = r.req_type
        days = max(1, get_int("trial_days")) if is_trial else (r.custom_days or 30)
        end_date = start_date + timedelta(days=days)
        r.status = "answered"
        r.answered_at = get_current_jalali()
        s.add(
            UserConfig(
                user_id=target_user_id, config_name=plan_name, config_details=config_text,
                service_id=service_id, expires_at=end_date.isoformat(), reminded=False,
            )
        )
        monthly_notif, reward = (None, None) if is_trial else register_purchase(s, target_user_id, r.price or 0)
        s.commit()

    await state.clear()
    log_action(admin_id, f"ارسال کانفیگ برای درخواست #{req_id}")
    await bot.send_message(
        chat_id=chat_id,
        text=f"✅ کانفیگ با موفقیت برای کاربر {target_user_id} ارسال و ثبت شد.{note}",
        reply_markup=get_admin_main_keyboard(admin_id),
    )

    try:
        config_html = f"<code>{esc(config_text)}</code>"
        template = (get_setting("t_config_ready") or "").strip() or DEFAULTS["t_config_ready"]
        notify = fill_text(
            "t_config_ready", plan=esc(plan_name or ""), start=jalali_from_iso(start_date.isoformat()),
            end=jalali_from_iso(end_date.isoformat()), days=days, config=config_html,
        )
        if "{config}" not in template:
            notify += "\n\n" + config_html
        extra_text = get_setting("config_extra_text").strip()
        if extra_text:
            notify += "\n\n" + esc(extra_text)
        await bot.send_message(chat_id=target_user_id, text=notify, reply_markup=delivery_markup())
        await send_sticker(bot, target_user_id, "sticker_config")
        await send_config_qr(bot, target_user_id, config_text)
    except Exception as e:  # noqa: BLE001
        logging.error("Error notifying user about config delivery: %s", e)
    await post_purchase_notifications(bot, target_user_id, monthly_notif, reward)
    if not is_trial:
        spawn(post_sale_feed(bot, plan_name, sale_req_type))
    if not is_trial:
        await send_rating_request(bot, target_user_id, req_id)


@dp.message(PaymentState.waiting_for_admin_config, Staff())
async def process_admin_config(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("لطفاً کانفیگ را به صورت متن ارسال کنید.")
        return
    text_ = message.text
    note = ""
    sub = find_sub_url(text_)
    if sub and get_bool("usage_on") and get_bool("testlink_on"):
        await message.answer("🔎 دارم لینک رو تست می‌کنم...")
        r = await fetch_usage(sub, force=True)
        bad = (not r["ok"]) or usage_is_finished(r)
        if bad:
            await state.update_data(pending_cfg=text_)
            b = InlineKeyboardButton
            markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [b(text="✅ بازم برای کاربر بفرست", callback_data="cfgsend_force")],
                    [b(text="✏️ لینک جدید می‌فرستم", callback_data="cfgsend_retry")],
                    [b(text="⛔ فعلاً نفرست (سفارش در انتظار می‌مونه)", callback_data="cfgsend_cancel")],
                ]
            )
            await message.answer(test_report(r) + "\nچی کار کنم؟", reply_markup=markup)
            return
        note = "\n🔎 تست لینک: سالم ✅"
    await _finish_config_delivery(message.bot, message.chat.id, message.from_user.id, state, text_, note)


@dp.callback_query(PaymentState.waiting_for_admin_config, F.data == "cfgsend_force", Staff())
async def cfgsend_force(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text_ = data.get("pending_cfg")
    if not text_:
        await call.answer("درخواست منقضی شده؛ لینک رو دوباره بفرست.", show_alert=True)
        return
    await call.answer()
    await _finish_config_delivery(
        call.bot, call.message.chat.id, call.from_user.id, state, text_, "\n⚠️ (با وجود مشکل در تست لینک ارسال شد)"
    )


@dp.callback_query(PaymentState.waiting_for_admin_config, F.data == "cfgsend_retry", Staff())
async def cfgsend_retry(call: types.CallbackQuery):
    await call.answer()
    await edit_or_send(call, "✏️ لینک/کانفیگ جدید رو بفرست:")


@dp.callback_query(PaymentState.waiting_for_admin_config, F.data == "cfgsend_cancel", Staff())
async def cfgsend_cancel(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await edit_or_send(
        call,
        "⛔ ارسال انجام نشد. سفارش هنوز در «پاسخ داده نشده» هست و هر وقت لینک آماده شد دوباره تاییدش کن.",
        get_admin_main_keyboard(call.from_user.id),
    )


# ---------------------------------------------------------------------------
# خرید مجدد، پرفروش و راهنمای انتخاب پلن
# ---------------------------------------------------------------------------
_best_cache = (0.0, None)


def bestseller_id():
    """پلنی که بیشترین سفارش تاییدشده (خرید/تمدید) را دارد؛ ۱۰ دقیقه کش می‌شود."""
    global _best_cache
    if not get_bool("bestseller_on"):
        return None
    if time.time() - _best_cache[0] < 600:
        return _best_cache[1]
    best = None
    try:
        with SessionLocal() as s:
            rows = (
                s.query(Request.service_id, func.count(Request.id))
                .filter(
                    Request.service_id.isnot(None), Request.status == "answered",
                    Request.req_type.in_(("buy", "renew")),
                )
                .group_by(Request.service_id)
                .all()
            )
            alive = {x for (x,) in s.query(Service.id).all()}
        rows = [(sid, c) for sid, c in rows if sid in alive]
        if rows:
            sid, cnt = max(rows, key=lambda r: (r[1], -r[0]))
            if cnt >= max(1, get_int("bestseller_min")):
                best = sid
    except Exception as e:  # noqa: BLE001
        logging.warning("bestseller failed: %s", e)
    _best_cache = (time.time(), best)
    return best


@dp.callback_query(F.data.startswith("rebuy_"))
async def rebuy_handler(call: types.CallbackQuery, state: FSMContext):
    try:
        cfg_id = int(call.data.split("_", 1)[1])
    except ValueError:
        return
    with SessionLocal() as s:
        cfg = s.get(UserConfig, cfg_id)
        if cfg is None or cfg.user_id != call.from_user.id:
            await call.answer("سرویس یافت نشد.", show_alert=True)
            return
        service_id = cfg.service_id
        exists = bool(service_id and s.get(Service, service_id))
    if not service_id:
        await call.answer("این سرویس از «پلن دلخواه» بوده؛ از بخش خرید دوباره بسازش.", show_alert=True)
        return
    if not exists:
        await call.answer("این پلن دیگه موجود نیست؛ از «خرید سرویس» یه پلن جدید انتخاب کن.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    await state.set_state(PaymentState.waiting_for_receipt)
    await state.update_data(service_id=service_id, renew_cfg_id=None, code=None)
    await send_checkout(call, state, call.from_user.id)


@dp.callback_query(F.data.startswith("cfg_copy_"))
async def cfg_copy_handler(call: types.CallbackQuery):
    try:
        cfg_id = int(call.data.split("_")[2])
    except (ValueError, IndexError):
        return
    with SessionLocal() as s:
        cfg = s.get(UserConfig, cfg_id)
        if cfg is None or cfg.user_id != call.from_user.id:
            await call.answer("سرویس یافت نشد.", show_alert=True)
            return
        details = cfg.config_details or ""
    link = find_sub_url(details) or details
    await call.answer()
    await call.bot.send_message(
        chat_id=call.from_user.id,
        text="🔗 روی لینک بزن تا کپی بشه 👇\n\n<code>" + esc(link[:3500]) + "</code>",
    )


PK_USAGE = {
    "1": ("🪶 کم — چت و تلگرام", 15),
    "2": ("🌐 متوسط — وب، اینستاگرام، یوتیوب", 40),
    "3": ("🎬 زیاد — فیلم، دانلود، آپدیت", 90),
}
PK_PURPOSE = {"t": "💬 بیشتر تلگرام و چت", "v": "🎬 فیلم و شبکه‌های اجتماعی", "g": "🎮 بازی آنلاین"}
PK_PEOPLE = {"1": ("👤 فقط خودم", 1.0), "2": ("👥 ۲ نفر", 1.7), "3": ("👨‍👩‍👧 ۳ نفر یا بیشتر", 2.4)}


def _plan_gb(name):
    m = re.search(r"(\d+)\s*گیگ", (name or "").translate(DIGITS_FA))
    return int(m.group(1)) if m else None


def pick_plans(u, p, n):
    """(پلن اصلی, پلن جایگزین) را بر اساس حجم موردنیاز، نوع استفاده و تعداد نفرات پیشنهاد می‌دهد."""
    need = PK_USAGE[u][1] * PK_PEOPLE[n][1]
    with SessionLocal() as s:
        active = {c.key for c in s.query(Category).filter(Category.is_active.is_(True)).all()}
        svcs = [(x.id, x.name, x.price, x.category) for x in s.query(Service).all() if x.category in active]
    if p == "g":
        pool = [x for x in svcs if x[3] == "gaming"] or svcs
    else:
        pool = [x for x in svcs if x[3] != "gaming"] or svcs
    if n in ("2", "3"):
        kws = ("نامحدود",) if n == "3" else ("نامحدود", "کاربر")
        multi = [x for x in pool if any(k in (x[1] or "") for k in kws)]
        if multi:  # تعداد نفرات مهم‌تر از حجم است؛ اگر پلن چندکاربره هست فقط از همان‌ها پیشنهاد بده
            pool = multi
    sized = [(x, _plan_gb(x[1])) for x in pool if _plan_gb(x[1])]
    if not sized:
        pool = sorted(pool, key=lambda x: x[2] or 0)
        return (pool[0] if pool else None), None
    enough = sorted([t for t in sized if t[1] >= need], key=lambda t: (t[0][2] or 0, t[1]))
    if enough:
        main = enough[0][0]
        alt = enough[1][0] if len(enough) > 1 else None
    else:
        sized.sort(key=lambda t: (-t[1], t[0][2] or 0))
        main = sized[0][0]
        alt = None
    return main, alt


def _pk_markup(rows):
    b = InlineKeyboardButton
    rows = list(rows)
    rows.append([b(text="⬅️ برگشت به خرید", callback_data="main_buy")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "pk_start")
async def pk_start(call: types.CallbackQuery):
    await call.answer()
    b = InlineKeyboardButton
    rows = [[b(text=lbl, callback_data=f"pk1_{k}")] for k, (lbl, _g) in PK_USAGE.items()]
    await edit_or_send(
        call, "🤔 <b>راهنمای انتخاب</b> | سوال ۱ از ۳\n\nمعمولاً چقدر از اینترنت استفاده می‌کنی؟", _pk_markup(rows)
    )


@dp.callback_query(F.data.regexp(r"^pk1_[123]$"))
async def pk_q2(call: types.CallbackQuery):
    await call.answer()
    u = call.data.split("_")[1]
    b = InlineKeyboardButton
    rows = [[b(text=lbl, callback_data=f"pk2_{u}_{k}")] for k, lbl in PK_PURPOSE.items()]
    await edit_or_send(call, "🤔 <b>راهنمای انتخاب</b> | سوال ۲ از ۳\n\nبیشتر برای چی می‌خوایش؟", _pk_markup(rows))


@dp.callback_query(F.data.regexp(r"^pk2_[123]_[tvg]$"))
async def pk_q3(call: types.CallbackQuery):
    await call.answer()
    _, u, p = call.data.split("_")
    b = InlineKeyboardButton
    rows = [[b(text=lbl, callback_data=f"pk3_{u}_{p}_{k}")] for k, (lbl, _f) in PK_PEOPLE.items()]
    await edit_or_send(call, "🤔 <b>راهنمای انتخاب</b> | سوال ۳ از ۳\n\nچند نفر ازش استفاده می‌کنن؟", _pk_markup(rows))


@dp.callback_query(F.data.regexp(r"^pk3_[123]_[tvg]_[123]$"))
async def pk_result(call: types.CallbackQuery):
    await call.answer()
    _, u, p, n = call.data.split("_")
    main, alt = pick_plans(u, p, n)
    b = InlineKeyboardButton
    if main is None:
        await edit_or_send(call, "فعلاً پلنی برای پیشنهاد ندارم؛ از لیست خرید انتخاب کن.", _pk_markup([]))
        return
    total, _parts = discount_breakdown(call.from_user.id)
    out = (
        "✅ <b>پیشنهاد من برای تو:</b>\n"
        f"({PK_USAGE[u][0].split(' —')[0]} | {PK_PURPOSE[p]} | {PK_PEOPLE[n][0]})\n\n"
    )
    rows = []
    for tag, x in (("🥇", main), ("🥈 گزینه‌ی دیگه", alt)):
        if x is None:
            continue
        sid, name, price, cat = x
        final = price_with_percent(price, total)
        price_txt = (
            f"<s>{price:,}</s> {ARROW} <b>{final:,}</b> تومان" if final != price else f"{price:,} تومان"
        )
        out += f"{tag} <b>{esc(cat_label(cat))} - {esc(name)}</b>\n💰 {price_txt}\n\n"
        rows.append([b(text=f"🛒 {name} — {final:,} تومان", callback_data=f"buy_{sid}")])
    rows.append([b(text="🔁 دوباره جواب می‌دم", callback_data="pk_start")])
    await edit_or_send(call, out, _pk_markup(rows))


# ---------------------------------------------------------------------------
# ویرایش دکمه‌های کیبورد پایین صفحه (فقط ادمین اصلی)
# ---------------------------------------------------------------------------
def kb_screen_view():
    b = InlineKeyboardButton
    out = (
        "⌨️ <b>کیبورد پایین صفحه</b>\n\n"
        "این دکمه‌ها همیشه زیر چت کاربر هستند. روی هر دکمه بزن تا نامش را عوض کنی یا مخفی‌اش کنی؛ "
        "با ⬆️⬇️ ترتیب را تغییر بده.\n👁 نمایش | 🙈 مخفی\n\n"
        "ℹ️ تغییر برای هر کاربر با /start یا با دکمه «اعمال فوری برای همه» فعال می‌شود."
    )
    rows = []
    for k in kb_tokens():
        it = kb_find(k)
        icon = "👁" if kb_visible(it) else "🙈"
        rows.append(
            [
                b(text=f"{icon} {kb_label(it)}"[:50], callback_data=f"kbi_{k}"),
                b(text="⬆️", callback_data=f"kbmv_up_{k}"),
                b(text="⬇️", callback_data=f"kbmv_dn_{k}"),
            ]
        )
    rows.append([b(text=f"↔️ تعداد ستون: {kb_cols()} (تغییر)", callback_data="kbcols")])
    rows.append([b(text="📤 اعمال فوری برای همه کاربران", callback_data="kbpush")])
    rows.append([b(text="♻️ بازنشانی کیبورد", callback_data="kbreset")])
    rows.append([b(text="🔙 بازگشت", callback_data="ubh")])
    return out, InlineKeyboardMarkup(inline_keyboard=rows)


def kb_item_view(key):
    it = kb_find(key)
    if it is None:
        return None
    vis = kb_visible(it)
    out = (
        f"🔘 <b>{esc(kb_label(it))}</b>\n\nنام پیش‌فرض: {esc(it['label'])}\n"
        f"وضعیت: {'👁 نمایش داده می‌شود' if vis else '🙈 مخفی'}"
    )
    b = InlineKeyboardButton
    rows = [
        [b(text="✏️ تغییر نام", callback_data=f"kbren_{key}")],
        [b(text="🙈 مخفی کردن" if vis else "👁 نمایش دادن", callback_data=f"kbtg_{key}")],
        [b(text="↩️ برگرداندن نام پیش‌فرض", callback_data=f"kbrl_{key}")],
        [b(text="🔙 بازگشت", callback_data="kbh")],
    ]
    return out, InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "kbh", MainAdmin())
async def kb_home(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await edit_or_send(call, *kb_screen_view())


@dp.callback_query(F.data.regexp(r"^kbi_[a-z]+$"), MainAdmin())
async def kb_item_open(call: types.CallbackQuery, state: FSMContext):
    view = kb_item_view(call.data.split("_")[1])
    if view is None:
        await call.answer("یافت نشد.", show_alert=True)
        return
    await call.answer()
    await state.clear()
    await edit_or_send(call, *view)


@dp.callback_query(F.data.regexp(r"^kbtg_[a-z]+$"), MainAdmin())
async def kb_item_toggle(call: types.CallbackQuery):
    key = call.data.split("_")[1]
    it = kb_find(key)
    if it is None:
        await call.answer("یافت نشد.", show_alert=True)
        return
    if kb_visible(it) and sum(1 for i in KB_ITEMS if kb_visible(i)) <= 1:
        await call.answer("حداقل یک دکمه باید در کیبورد بماند.", show_alert=True)
        return
    set_setting(f"kb_show_{key}", "0" if kb_visible(it) else "1")
    log_action(call.from_user.id, f"تغییر نمایش دکمه کیبورد {key}")
    await call.answer("انجام شد.")
    await edit_or_send(call, *kb_item_view(key))


@dp.callback_query(F.data.regexp(r"^kbrl_[a-z]+$"), MainAdmin())
async def kb_item_reset_label(call: types.CallbackQuery):
    key = call.data.split("_")[1]
    if kb_find(key) is None:
        await call.answer("یافت نشد.", show_alert=True)
        return
    set_setting(f"kb_label_{key}", "")
    refresh_menu_texts()
    await call.answer("نام پیش‌فرض برگشت.")
    await edit_or_send(call, *kb_item_view(key))


@dp.callback_query(F.data.regexp(r"^kbren_[a-z]+$"), MainAdmin())
async def kb_rename_start(call: types.CallbackQuery, state: FSMContext):
    key = call.data.split("_")[1]
    if kb_find(key) is None:
        await call.answer("یافت نشد.", show_alert=True)
        return
    await call.answer()
    await state.set_state(PaymentState.kb_rename)
    await state.update_data(kb_key=key)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="انصراف", callback_data=f"kbi_{key}")]])
    await edit_or_send(call, "✏️ نام جدید دکمه را بفرست (حداکثر ۲۵ کاراکتر، ایموجی مجاز است).", markup)


@dp.message(PaymentState.kb_rename, MainAdmin())
async def kb_rename_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    key = data.get("kb_key")
    raw = (message.text or "").strip()
    if kb_find(key) is None:
        await state.clear()
        await message.answer("درخواست منقضی شد.")
        return
    if not raw or len(raw) > 25:
        await message.answer("یک نام معتبر (حداکثر ۲۵ کاراکتر) بفرست.")
        return
    if any(i["key"] != key and raw in (kb_label(i), i["label"]) for i in KB_ITEMS):
        await message.answer("این نام برای دکمه‌ی دیگری استفاده شده؛ یک نام دیگر بفرست.")
        return
    set_setting(f"kb_label_{key}", raw)
    refresh_menu_texts()
    await state.clear()
    log_action(message.from_user.id, f"تغییر نام دکمه کیبورد {key}")
    await message.answer("✅ ذخیره شد.")
    await reply_to(message, *kb_screen_view())


@dp.callback_query(F.data.regexp(r"^kbmv_(up|dn)_[a-z]+$"), MainAdmin())
async def kb_move(call: types.CallbackQuery):
    _, direction, key = call.data.split("_")
    order = kb_tokens()
    if key in order:
        i = order.index(key)
        j = i - 1 if direction == "up" else i + 1
        if 0 <= j < len(order):
            order[i], order[j] = order[j], order[i]
        set_setting("uorder_kbd", ",".join(order))
    await call.answer("ترتیب تغییر کرد.")
    await edit_or_send(call, *kb_screen_view())


@dp.callback_query(F.data == "kbcols", MainAdmin())
async def kb_cols_toggle(call: types.CallbackQuery):
    set_setting("kb_cols", str(kb_cols() % 3 + 1))
    await call.answer("چیدمان تغییر کرد.")
    await edit_or_send(call, *kb_screen_view())


@dp.callback_query(F.data == "kbreset", MainAdmin())
async def kb_reset(call: types.CallbackQuery):
    for it in KB_ITEMS:
        set_setting(f"kb_label_{it['key']}", "")
        set_setting(f"kb_show_{it['key']}", "")
    set_setting("uorder_kbd", "")
    set_setting("kb_cols", "")
    refresh_menu_texts()
    log_action(call.from_user.id, "بازنشانی کیبورد پایین")
    await call.answer("بازنشانی شد.", show_alert=True)
    await edit_or_send(call, *kb_screen_view())


@dp.callback_query(F.data == "kbpush", MainAdmin())
async def kb_push_ask(call: types.CallbackQuery):
    await call.answer()
    b = InlineKeyboardButton
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [b(text="✅ بله، برای همه بفرست", callback_data="kbpushgo")],
            [b(text="انصراف", callback_data="kbh")],
        ]
    )
    await edit_or_send(
        call, "یک پیام کوتاه «منوی پایین به‌روز شد» با کیبورد جدید برای همه کاربران ارسال می‌شود. ادامه بدم؟", markup
    )


async def _push_keyboard(bot, admin_chat):
    with SessionLocal() as s:
        ids = segment_user_ids(s, "all")
    ok = 0
    for uid in ids:
        try:
            await send_message_retry(bot, uid, "🔄 منوی پایین به‌روز شد 👇", reply_markup=build_reply_keyboard())
            ok += 1
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(0.05)
    try:
        await bot.send_message(chat_id=admin_chat, text=f"✅ کیبورد جدید برای {ok} نفر از {len(ids)} نفر ارسال شد.")
    except Exception:  # noqa: BLE001
        pass


@dp.callback_query(F.data == "kbpushgo", MainAdmin())
async def kb_push_go(call: types.CallbackQuery):
    await call.answer("ارسال شروع شد.", show_alert=True)
    log_action(call.from_user.id, "اعمال فوری کیبورد پایین برای همه")
    spawn(_push_keyboard(call.bot, call.message.chat.id))
    await edit_or_send(call, *kb_screen_view())



# ===========================================================================
# 🚀 بستهٔ رشد و فروش (v12)
# همهٔ قابلیت‌ها به‌صورت پیش‌فرض خاموش‌اند و از «⚙️ تنظیمات ربات ← بخش‌های 🚀» روشن می‌شوند.
# ===========================================================================
from aiogram.types import InlineQueryResultArticle, InputTextMessageContent  # noqa: E402

TS_FMT = "%Y-%m-%dT%H:%M"


class GrowthLog(Base):
    """لاگ عمومی رویدادهای رشد؛ برای جلوگیری از ارسال تکراری و ثبت چک‌این/دلیل ریزش/هدیه."""

    __tablename__ = "growth_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    kind = Column(String)
    ref = Column(String)
    created_iso = Column(String)


class CartEvent(Base):
    """سبد خرید رهاشده: کاربری که به مرحلهٔ پرداخت رسید."""

    __tablename__ = "cart_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    label = Column(String)
    price = Column(Integer, default=0)
    created_iso = Column(String)
    stage = Column(Integer, default=0)  # 0 جدید | 1 یادآوری اول | 2 تمام


class GiftCode(Base):
    __tablename__ = "gift_codes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True)
    amount = Column(Integer, default=0)
    max_uses = Column(Integer, default=1)
    used_count = Column(Integer, default=0)
    expires_at = Column(String)
    is_active = Column(Boolean, default=True)


class GiftUse(Base):
    __tablename__ = "gift_uses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    gift_id = Column(Integer)
    user_id = Column(BigInteger)


class GrowthState(StatesGroup):
    waiting_gift = State()


DEFAULTS.update(
    {
        # --- یادآوری و بازگشت مشتری ---
        "remind_multi_on": "0",
        "cart_on": "0",
        "cart_minutes": "45",
        "cart_code_percent": "10",
        "cart_text": "🛒 سفارشت نیمه‌کاره موند!\n\n📦 {plan}\n💰 {price} تومان\n\nاگه تو پرداخت مشکلی داشتی یا سوالی داری همینجا بپرس 🙌",
        "winback_on": "0",
        "winback_days": "3",
        "winback_percent": "15",
        "winback_text": "👋 دلمون برات تنگ شده! سرویست چند روزه تموم شده.\nبرای برگشتت یه کد تخفیف {percent}٪ مخصوص خودت ساختیم:",
        "churn_ask_on": "0",
        "drip_on": "0",
        "drip_t1": "سلام {name} 👋\nهنوز سرویسی نگرفتی؟ تعرفه‌ها رو ببین؛ خرید اولت تخفیف ویژه داره 🎁",
        "drip_t2": "{name} جان، سرویس‌های ما با پشتیبانی سریع و تحویل فوری آماده‌ست 🚀\nاگه سوالی داری از بخش پشتیبانی بپرس.",
        "drip_t3": "⭐ تا الان ده‌ها نفر از مشتری‌هامون راضی بودن. تو هم امتحان کن؛ پلن‌های اقتصادی از همین‌جا شروع می‌شه 👇",
        "comp_on": "0",
        "comp_minutes": "60",
        "comp_amount": "10000",
        # --- رشد و دعوت ---
        "ref_invitee_reward": "0",
        "checkin_on": "0",
        "checkin_points": "1",
        "checkin_streak_bonus": "10",
        "transfer_on": "0",
        "transfer_min": "20000",
        "transfer_fee_percent": "0",
        "toprefs_on": "0",
        "promo_toggle_on": "0",
        "anniv_on": "0",
        "anniv_percent": "20",
        "pergb_on": "0",
        "cost_per_gb": "0",
        # --- چند کارت و پیامک بانکی ---
        "cards_rotate_on": "0",
        "cards_extra": "",
        "sms_on": "0",
        "sms_secret": "",
        "sms_window": "180",
        # --- فید فروش در کانال ---
        "salefeed_on": "0",
        "salefeed_gap": "30",
        "salefeed_text": "🔥 یک نفر همین الان پلن {plan} رو خرید ✅",
    }
)

SETTING_PAGES.update(
    {
        "grow_a": "🚀 یادآوری و بازگشت مشتری",
        "grow_b": "🚀 دعوت، امتیاز و هدیه",
        "grow_c": "🚀 چند کارت و پیامک بانکی",
    }
)
SETTINGS.update(
    {
        "remind_multi_on": dict(page="grow_a", type="toggle", label="یادآوری چندمرحله‌ای تمدید (۳ روز، ۱ روز، انقضا، ۲ روز بعد)"),
        "cart_on": dict(page="grow_a", type="toggle", label="یادآوری سبد رهاشده"),
        "cart_minutes": dict(page="grow_a", type="num", label="یادآوری اول بعد از (دقیقه)", min=5, max=1440),
        "cart_code_percent": dict(page="grow_a", type="num", label="کد تخفیف یادآوری دوم (٪؛ ۰ = بدون کد)", min=0, max=50),
        "cart_text": dict(page="grow_a", type="text", label="متن یادآوری سبد ({plan} {price})"),
        "winback_on": dict(page="grow_a", type="toggle", label="کد تخفیف بازگشت برای منقضی‌شده‌ها"),
        "winback_days": dict(page="grow_a", type="num", label="چند روز بعد از انقضا", min=1, max=60),
        "winback_percent": dict(page="grow_a", type="num", label="درصد تخفیف بازگشت", min=0, max=50),
        "winback_text": dict(page="grow_a", type="text", label="متن پیام بازگشت ({percent})"),
        "churn_ask_on": dict(page="grow_a", type="toggle", label="پرسیدن دلیل نرفتن (دکمه‌های نظرسنجی)"),
        "drip_on": dict(page="grow_a", type="toggle", label="پیام‌های پیگیری کاربران بدون خرید (روز ۱، ۳، ۷)"),
        "drip_t1": dict(page="grow_a", type="text", label="متن روز ۱ ({name})"),
        "drip_t2": dict(page="grow_a", type="text", label="متن روز ۳ ({name})"),
        "drip_t3": dict(page="grow_a", type="text", label="متن روز ۷ ({name})"),
        "comp_on": dict(page="grow_a", type="toggle", label="جبران خودکار تأخیر تحویل (اعتبار)"),
        "comp_minutes": dict(page="grow_a", type="num", label="اگر تحویل بیش از این دقیقه طول کشید", min=10, max=1440),
        "comp_amount": dict(page="grow_a", type="num", label="مبلغ جبران (تومان)", min=0, max=1000000),
        "ref_invitee_reward": dict(page="grow_b", type="num", label="هدیهٔ دعوت‌شده بعد از اولین خرید (تومان؛ دعوت دوطرفه)", min=0, max=10000000),
        "checkin_on": dict(page="grow_b", type="toggle", label="چک‌این روزانه (امتیاز باشگاه)"),
        "checkin_points": dict(page="grow_b", type="num", label="امتیاز هر چک‌این", min=1, max=1000),
        "checkin_streak_bonus": dict(page="grow_b", type="num", label="امتیاز ویژه هر ۷ روز پشت‌سرهم", min=0, max=10000),
        "transfer_on": dict(page="grow_b", type="toggle", label="انتقال اعتبار بین کاربران (/transfer)"),
        "transfer_min": dict(page="grow_b", type="num", label="حداقل مبلغ انتقال (تومان)", min=1000, max=100000000),
        "transfer_fee_percent": dict(page="grow_b", type="num", label="کارمزد انتقال (٪)", min=0, max=20),
        "toprefs_on": dict(page="grow_b", type="toggle", label="جدول برترین دعوت‌کننده‌ها"),
        "promo_toggle_on": dict(page="grow_b", type="toggle", label="دکمهٔ قطع/وصل تبلیغات برای کاربر"),
        "anniv_on": dict(page="grow_b", type="toggle", label="هدیهٔ سالگرد عضویت"),
        "anniv_percent": dict(page="grow_b", type="num", label="درصد تخفیف سالگرد", min=1, max=50),
        "pergb_on": dict(page="grow_b", type="toggle", label="نمایش قیمت هر گیگ کنار پلن‌ها"),
        "cost_per_gb": dict(page="grow_b", type="num", label="هزینهٔ هر گیگ برای شما (گزارش /profit)", min=0, max=10000000),
        "cards_rotate_on": dict(page="grow_c", type="toggle", label="چرخش چند کارت"),
        "cards_extra": dict(page="grow_c", type="text", label="کارت‌های اضافه (هر خط: شماره|نام صاحب کارت)"),
        "sms_on": dict(page="grow_c", type="toggle", label="تشخیص واریز با پیامک بانکی (وب‌هوک)"),
        "sms_secret": dict(page="grow_c", type="text", label="رمز آدرس وب‌هوک (فقط حروف و عدد انگلیسی)"),
        "sms_window": dict(page="grow_c", type="num", label="بازهٔ تطبیق پیامک با سفارش (دقیقه)", min=10, max=1440),
        "salefeed_on": dict(page="chpost", type="toggle", label="اعلام خودکار فروش در کانال (ناشناس)"),
        "salefeed_gap": dict(page="chpost", type="num", label="حداقل فاصلهٔ دو اعلام فروش (دقیقه)", min=0, max=1440),
        "salefeed_text": dict(page="chpost", type="text", label="متن اعلام فروش ({plan})"),
    }
)

GROWTH_CONDS = {"checkin": "checkin_on", "toprefs": "toprefs_on", "promo": "promo_toggle_on"}
UB_ITEMS["main"].append(dict(key="checkin", label="📅 چک‌این روزانه", cb="checkin", cond="checkin"))
UB_ITEMS["main"].append(dict(key="gift", label="🎁 کد هدیه", cb="gift_start", hide_default=True))
UB_ITEMS["account"].append(dict(key="toprefs", label="🏅 برترین دعوت‌کننده‌ها", cb="toprefs", wide=True, cond="toprefs"))
UB_ITEMS["account"].append(dict(key="promo", label="🔔 قطع/وصل تبلیغات", cb="promo_toggle", wide=True, cond="promo"))

_INVITEE_NOTICE = {}  # user_id -> مبلغ هدیهٔ دعوت‌شده (برای اعلان بعد از خرید)
_sms_seen = {}  # مبلغ -> زمان دیدن در پیامک بانکی
GROWTH_BOT = None
_growth_last = 0.0
_growth_busy = False
_card_assign = {}


def set_growth_bot(bot):
    global GROWTH_BOT
    GROWTH_BOT = bot


# ---------------------------------------------------------------------------
# ابزارهای مشترک
# ---------------------------------------------------------------------------
def glog_has(s, uid, kind, ref=""):
    return (
        s.query(GrowthLog.id)
        .filter(GrowthLog.user_id == uid, GrowthLog.kind == kind, GrowthLog.ref == str(ref))
        .first()
        is not None
    )


def glog_add(s, uid, kind, ref=""):
    s.add(GrowthLog(user_id=uid, kind=kind, ref=str(ref), created_iso=iso_now_minute()))


def _num(raw):
    t = (raw or "").translate(DIGITS_FA).replace(",", "").replace("،", "").strip()
    return int(t) if t.isdigit() else None


def mark_blocked(uid):
    try:
        with SessionLocal() as s:
            u = s.get(User, uid)
            if u is not None and not u.blocked:
                u.blocked = True
                s.commit()
    except Exception as e:  # noqa: BLE001
        logging.warning("mark_blocked failed: %s", e)


async def gsend(bot, uid, text_, markup=None):
    """ارسال امن؛ اگر کاربر ربات را بلاک کرده باشد علامت می‌خورد."""
    try:
        await bot.send_message(chat_id=uid, text=text_, reply_markup=markup)
        return True
    except TelegramForbiddenError:
        mark_blocked(uid)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        try:
            await bot.send_message(chat_id=uid, text=text_, reply_markup=markup)
            return True
        except Exception:  # noqa: BLE001
            pass
    except Exception as e:  # noqa: BLE001
        logging.info("gsend to %s failed: %s", uid, e)
    return False


def make_personal_code(s, percent, days, prefix):
    """کد تخفیف یک‌بارمصرف و تصادفی می‌سازد."""
    percent = max(1, min(MAX_DISCOUNT, int(percent)))
    code = ""
    for _ in range(8):
        code = f"{prefix}{secrets.token_hex(3).upper()}"
        if not s.query(DiscountCode.id).filter(DiscountCode.code == code).first():
            break
    s.add(
        DiscountCode(
            code=code, percent=percent, max_uses=1, used_count=0, is_active=True,
            expires_at=(today_iran() + timedelta(days=days)).isoformat(),
        )
    )
    return code


def _buy_markup(extra_rows=None):
    rows = [[InlineKeyboardButton(text="🛒 مشاهده تعرفه و خرید", callback_data="main_buy")]]
    rows.extend(extra_rows or [])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------------------------------------------------------------------
# ۱) چرخش چند کارت
# ---------------------------------------------------------------------------
def _cards():
    cards = [(get_setting("card_number").strip(), get_setting("card_holder").strip())]
    if get_bool("cards_rotate_on"):
        for line in get_setting("cards_extra").splitlines():
            if "|" in line:
                num, holder = (x.strip() for x in line.split("|", 1))
                if num:
                    cards.append((num, holder))
    return cards


def pick_card(user_id=None):
    """(شماره کارت, نام صاحب). هر کاربر تا ۶ ساعت یک کارت ثابت می‌بیند؛ کاربر جدید نوبتی کارت بعدی را می‌گیرد."""
    cards = _cards()
    if len(cards) == 1:
        return cards[0]
    now = time.time()
    hit = _card_assign.get(user_id)
    if hit and now - hit[1] < 6 * 3600 and hit[0] < len(cards):
        return cards[hit[0]]
    try:
        counter = int(get_setting("card_rr") or 0)
    except ValueError:
        counter = 0
    idx = counter % len(cards)
    set_setting("card_rr", counter + 1)
    if len(_card_assign) > 5000:
        _card_assign.clear()
    _card_assign[user_id] = (idx, now)
    return cards[idx]


# ---------------------------------------------------------------------------
# ۲) پیامک بانکی (وب‌هوک)
# ---------------------------------------------------------------------------
def _extract_amounts(text_):
    t = (text_ or "").translate(DIGITS_FA)
    nums = set()
    for m in re.finditer(r"\d[\d,٬،]*", t):
        raw = re.sub(r"[,٬،]", "", m.group(0))
        if raw.isdigit() and 4 <= len(raw) <= 12:
            n = int(raw)
            nums.add(n)
            if n % 10 == 0:
                nums.add(n // 10)  # پیامک بانک‌ها معمولاً ریال است
    return nums


def sms_badge(pay_amount):
    ts = _sms_seen.get(int(pay_amount)) if pay_amount else None
    if ts and time.time() - ts < 6 * 3600:
        return "\n✅ <b>پیامک بانکی: واریز همین مبلغ دیده شد</b>"
    return ""


def sms_match(text_):
    """لیست سفارش‌های در انتظارِ هم‌مبلغ با پیامک. (id, user_id, amount)"""
    nums = _extract_amounts(text_)
    for n in nums:
        _sms_seen[n] = time.time()
    if len(_sms_seen) > 2000:
        _sms_seen.clear()
    window = max(10, get_int("sms_window") or 180)
    cutoff = (now_iran().replace(tzinfo=None) - timedelta(minutes=window)).strftime(TS_FMT)
    out = []
    with SessionLocal() as s:
        rows = (
            s.query(Request)
            .filter(Request.status == "unanswered", Request.pay_amount.isnot(None), Request.created_iso >= cutoff)
            .all()
        )
        for r in rows:
            if r.pay_amount in nums and not glog_has(s, r.user_id, "sms", r.id):
                out.append((r.id, r.user_id, r.pay_amount))
        for rid, uid, _amt in out:
            glog_add(s, uid, "sms", rid)
        s.commit()
    return out


async def sms_webhook(request):
    secret = get_setting("sms_secret").strip()
    given = request.match_info.get("secret", "")
    if not get_bool("sms_on") or not secret or not hmac.compare_digest(given.encode(), secret.encode()):
        return web.Response(status=404)
    try:
        raw = await request.text()
    except Exception:  # noqa: BLE001
        return web.Response(status=400)
    body = raw
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            body = " ".join(str(data.get(k, "")) for k in ("text", "message", "body", "sms", "content")) or raw
    except ValueError:
        if "=" in raw and "&" in raw:
            from urllib.parse import parse_qs

            qs = parse_qs(raw)
            body = " ".join(" ".join(v) for v in qs.values())
    if "برداشت" in body and "واریز" not in body:
        return web.json_response({"ok": True, "matched": 0})
    if "واریز" not in body and "deposit" not in body.lower() and "+" not in body:
        return web.json_response({"ok": True, "matched": 0})
    hits = sms_match(body)
    bot = GROWTH_BOT
    if bot is not None:
        for rid, uid, amount in hits:
            try:
                await notify_staff(
                    bot,
                    f"💚 <b>پیامک بانکی:</b> واریز {amount:,} تومان با سفارش #{rid} مطابقت دارد.",
                    admin_request_markup(rid),
                )
                await gsend(bot, uid, "✅ واریزت دیده شد و سفارشت در صف تحویل قرار گرفت. ممنون 🙏")
            except Exception as e:  # noqa: BLE001
                logging.warning("sms notify failed: %s", e)
    return web.json_response({"ok": True, "matched": len(hits)})


# ---------------------------------------------------------------------------
# ۳) سبد رهاشده
# ---------------------------------------------------------------------------
def record_cart(user_id, label, price):
    try:
        with SessionLocal() as s:
            s.query(CartEvent).filter(CartEvent.user_id == user_id, CartEvent.stage < 2).delete()
            s.add(CartEvent(user_id=user_id, label=label, price=int(price or 0), created_iso=iso_now_minute(), stage=0))
            s.commit()
    except Exception as e:  # noqa: BLE001
        logging.warning("record_cart failed: %s", e)


async def job_cart(bot):
    if not get_bool("cart_on"):
        return
    mins = max(5, get_int("cart_minutes"))
    pct = get_int("cart_code_percent")
    now = now_iran().replace(tzinfo=None)
    c1 = (now - timedelta(minutes=mins)).strftime(TS_FMT)
    c2 = (now - timedelta(hours=24)).strftime(TS_FMT)
    c_old = (now - timedelta(days=3)).strftime(TS_FMT)
    todo = []
    with SessionLocal() as s:
        for c in s.query(CartEvent).filter(CartEvent.stage < 2).limit(200).all():
            bought = (
                s.query(Request.id).filter(Request.user_id == c.user_id, Request.created_iso >= c.created_iso).first()
            )
            if bought or c.created_iso <= c_old:
                c.stage = 2
                continue
            if c.stage == 0 and c.created_iso <= c1:
                c.stage = 1
                todo.append((c.user_id, c.label, c.price, ""))
            elif c.stage == 1 and c.created_iso <= c2:
                c.stage = 2
                if pct > 0:
                    todo.append((c.user_id, c.label, c.price, make_personal_code(s, pct, 2, "CART")))
        s.commit()
    for uid, label, price, code in todo:
        text_ = (get_setting("cart_text") or DEFAULTS["cart_text"]).replace("{plan}", esc(label or "")).replace(
            "{price}", f"{int(price or 0):,}"
        )
        if code:
            text_ += f"\n\n🎁 یه کد تخفیف {pct}٪ مخصوص خودت: <code>{code}</code> (۲ روز اعتبار)"
        await gsend(bot, uid, text_, _buy_markup([[InlineKeyboardButton(text="🎧 پشتیبانی", callback_data="support_menu")]]))
        await asyncio.sleep(0.05)


# ---------------------------------------------------------------------------
# ۴) جبران تأخیر تحویل
# ---------------------------------------------------------------------------
async def job_comp(bot):
    if not get_bool("comp_on"):
        return
    amount = get_int("comp_amount")
    if amount <= 0:
        return
    mins = max(10, get_int("comp_minutes"))
    cutoff = (now_iran().replace(tzinfo=None) - timedelta(minutes=mins)).strftime(TS_FMT)
    todo = []
    with SessionLocal() as s:
        rows = (
            s.query(Request)
            .filter(
                Request.status == "unanswered", Request.req_type.in_(SALE_TYPES), Request.price > 0,
                Request.created_iso.isnot(None), Request.created_iso <= cutoff,
            )
            .limit(30)
            .all()
        )
        for r in rows:
            if glog_has(s, r.user_id, "comp", r.id):
                continue
            day_key = f"{r.user_id}:{today_iran().isoformat()}"
            if glog_has(s, r.user_id, "compday", day_key):
                continue
            u = s.get(User, r.user_id)
            if u is None:
                continue
            u.balance = min(MAX_BALANCE, (u.balance or 0) + amount)
            glog_add(s, r.user_id, "comp", r.id)
            glog_add(s, r.user_id, "compday", day_key)
            todo.append((r.user_id, r.id))
        s.commit()
    for uid, rid in todo:
        await gsend(
            bot, uid,
            f"🙏 بابت تأخیر در تحویل سفارش #{rid} ببخشید؛ {amount:,} تومان اعتبار هدیه به کیف پولت اضافه شد.",
        )


async def job_growth_tick(bot):
    """هر ~۵ دقیقه: سبد رهاشده و جبران تأخیر."""
    global _growth_last, _growth_busy
    now = time.time()
    if _growth_busy or now - _growth_last < 280:
        return
    _growth_busy = True
    _growth_last = now
    try:
        for fn in (job_cart, job_comp):
            try:
                await fn(bot)
            except Exception as e:  # noqa: BLE001
                logging.error("growth %s failed: %s", fn.__name__, e)
    finally:
        _growth_busy = False


# ---------------------------------------------------------------------------
# ۵) یادآوری چندمرحله‌ای تمدید
# ---------------------------------------------------------------------------
def _remind_stage_for(left, days):
    if left <= -2:
        return 4
    if left <= 0:
        return 3
    if left <= 1:
        return 2
    if left <= days:
        return 1
    return 0


async def job_reminders(bot):
    if not get_bool("remind_multi_on"):
        return await job_reminders_orig(bot)
    days = max(2, get_int("reminder_days"))
    todo = []
    with SessionLocal() as s:
        rows = s.query(UserConfig).filter(UserConfig.expires_at.isnot(None)).all()
        for c in rows:
            try:
                left = (date.fromisoformat(c.expires_at) - today_iran()).days
            except (TypeError, ValueError):
                continue
            cur = c.remind_stage or 0
            target = _remind_stage_for(left, days)
            if target == 0:
                if cur:
                    c.remind_stage = 0  # تمدید شده
                    c.reminded = False
                continue
            if left < -3:
                if cur < 4:
                    c.remind_stage = 4
                continue
            if target > cur:
                c.remind_stage = target
                c.reminded = True
                todo.append((c.id, c.user_id, c.config_name, left, c.service_id, target))
        s.commit()
    for cfg_id, uid, name, left, sid, stage in todo:
        n = esc(name or "")
        if stage == 1:
            msg = f"⏰ سرویس «{n}» {left} روز دیگه تموم می‌شه. برای تمدید روی دکمه زیر بزن 👇"
        elif stage == 2:
            msg = f"⚠️ فقط ۱ روز تا پایان سرویس «{n}» مونده! قبل از قطع شدن تمدید کن 👇"
        elif stage == 3:
            msg = f"❌ سرویس «{n}» تموم شد. همین الان تمدید کن تا وصل بمونی 👇"
        else:
            msg = f"🙏 سرویس «{n}» ۲ روزه که منقضی شده. هنوز می‌تونی تمدیدش کنی 👇"
        markup = None
        if sid:
            markup = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔄 تمدید سرویس", callback_data=f"renew_{cfg_id}")]]
            )
        await gsend(bot, uid, msg, markup)
        await asyncio.sleep(0.05)


# ---------------------------------------------------------------------------
# ۶) بازگشت مشتری، دلیل ریزش، پیگیری کاربران بدون خرید، هدیهٔ سالگرد
# ---------------------------------------------------------------------------
CHURN_REASONS = {"price": "💰 گرون بود", "speed": "🐢 کند بود", "cut": "🔌 قطع و وصل می‌شد", "other": "🤷 دلیل دیگه"}


async def _winback(bot):
    if not get_bool("winback_on"):
        return
    days = max(1, get_int("winback_days"))
    pct = get_int("winback_percent")
    today = today_iran()
    limit = (today - timedelta(days=days)).isoformat()
    oldest = (today - timedelta(days=days + 30)).isoformat()
    todo = []
    with SessionLocal() as s:
        rows = (
            s.query(UserConfig.user_id, func.max(UserConfig.expires_at))
            .filter(UserConfig.expires_at.isnot(None))
            .group_by(UserConfig.user_id)
            .all()
        )
        for uid, mx in rows:
            if not mx or mx >= limit or mx < oldest or glog_has(s, uid, "winback", mx):
                continue
            u = s.get(User, uid)
            if u is None or u.is_banned or u.blocked:
                continue
            code = make_personal_code(s, pct, 3, "BACK") if pct > 0 else ""
            glog_add(s, uid, "winback", mx)
            todo.append((uid, code))
        s.commit()
    for uid, code in todo:
        text_ = (get_setting("winback_text") or DEFAULTS["winback_text"]).replace("{percent}", str(pct))
        if code:
            text_ += f"\n\n🎁 <code>{code}</code> (۳ روز اعتبار، یک‌بار مصرف)"
        extra = []
        if get_bool("churn_ask_on"):
            extra.append(
                [InlineKeyboardButton(text=lbl, callback_data=f"churn_{k}") for k, lbl in list(CHURN_REASONS.items())[:2]]
            )
            extra.append(
                [InlineKeyboardButton(text=lbl, callback_data=f"churn_{k}") for k, lbl in list(CHURN_REASONS.items())[2:]]
            )
            text_ += "\n\n🙋 اگه تمدید نمی‌کنی، بگو چرا؟ کمکمون می‌کنه بهتر بشیم:"
        await gsend(bot, uid, text_, _buy_markup(extra))
        await asyncio.sleep(0.05)


@dp.callback_query(F.data.startswith("churn_"))
async def churn_answer(call: types.CallbackQuery):
    key = call.data.split("_", 1)[1]
    if key not in CHURN_REASONS:
        await call.answer()
        return
    with SessionLocal() as s:
        if not glog_has(s, call.from_user.id, "churn", key):
            glog_add(s, call.from_user.id, "churn", key)
            s.commit()
    await call.answer("ممنون که گفتی 🙏", show_alert=True)


async def _drip(bot):
    if not get_bool("drip_on"):
        return
    today = today_iran()
    first = (today - timedelta(days=9)).isoformat()
    todo = []
    with SessionLocal() as s:
        buyers = {
            r[0]
            for r in s.query(Request.user_id).filter(Request.req_type.in_(SALE_TYPES + ["trial"])).all()
        }
        users = s.query(User).filter(User.joined_at >= first).all()
        for u in users:
            if u.user_id in buyers or u.is_banned or u.blocked:
                continue
            try:
                age = (today - date.fromisoformat(u.joined_at)).days
            except (TypeError, ValueError):
                continue
            stage = 3 if age >= 7 else 2 if age >= 3 else 1 if age >= 1 else 0
            if not stage or glog_has(s, u.user_id, "drip", stage):
                continue
            for st in range(1, stage + 1):
                if not glog_has(s, u.user_id, "drip", st):
                    glog_add(s, u.user_id, "drip", st)
            todo.append((u.user_id, u.first_name or "دوست عزیز", stage))
        s.commit()
    for uid, name, stage in todo:
        text_ = (get_setting(f"drip_t{stage}") or DEFAULTS[f"drip_t{stage}"]).replace("{name}", esc(name))
        await gsend(bot, uid, text_, _buy_markup())
        await asyncio.sleep(0.05)


async def _anniversary(bot):
    if not get_bool("anniv_on"):
        return
    pct = get_int("anniv_percent")
    today = today_iran()
    todo = []
    with SessionLocal() as s:
        for u in s.query(User).filter(User.joined_at.isnot(None)).all():
            try:
                j = date.fromisoformat(u.joined_at)
            except (TypeError, ValueError):
                continue
            if (j.month, j.day) != (today.month, today.day) or j.year >= today.year:
                continue
            if u.is_banned or u.blocked or glog_has(s, u.user_id, "anniv", today.year):
                continue
            glog_add(s, u.user_id, "anniv", today.year)
            todo.append((u.user_id, today.year - j.year, make_personal_code(s, pct, 7, "YEAR")))
        s.commit()
    for uid, years, code in todo:
        await gsend(
            bot, uid,
            f"🎂 امروز {years} ساله که همراه ما هستی! ممنون 💙\n🎁 کد تخفیف {pct}٪ سالگرد: <code>{code}</code> (۷ روز اعتبار)",
            _buy_markup(),
        )
        await asyncio.sleep(0.05)


async def job_growth_daily(bot):
    for fn in (_winback, _drip, _anniversary):
        try:
            await fn(bot)
        except Exception as e:  # noqa: BLE001
            logging.error("growth daily %s failed: %s", fn.__name__, e)


# ---------------------------------------------------------------------------
# فید فروش ناشناس در کانال
# ---------------------------------------------------------------------------
_salefeed_last = 0.0


async def post_sale_feed(bot, plan_name, req_type):
    """بعد از تحویل یک خرید جدید، بدون نام کاربر در کانال اعلام می‌کند."""
    global _salefeed_last
    if req_type != "buy" or not get_bool("salefeed_on"):
        return
    try:
        chat_id = channel_chat_id()
    except ValueError:
        return
    now = time.time()
    if now - _salefeed_last < max(0, get_int("salefeed_gap")) * 60:
        return
    _salefeed_last = now
    text_ = (get_setting("salefeed_text") or DEFAULTS["salefeed_text"]).replace("{plan}", esc(plan_name or ""))
    markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🛒 خرید از ربات", url=f"https://t.me/{BOT_USERNAME}")]]
    )
    try:
        await bot.send_message(chat_id=chat_id, text=text_, reply_markup=markup)
    except Exception as e:  # noqa: BLE001
        logging.warning("sale feed failed: %s", e)


# ---------------------------------------------------------------------------
# ۷) ردیابی منبع (/start src_xxx)
# ---------------------------------------------------------------------------
def bind_source(user_id, arg):
    src = re.sub(r"[^A-Za-z0-9_\-]", "", (arg or "")[4:])[:30]
    if not src:
        return
    try:
        with SessionLocal() as s:
            u = s.get(User, user_id)
            if u is not None and not u.src:
                u.src = src
                s.commit()
    except Exception as e:  # noqa: BLE001
        logging.warning("bind_source failed: %s", e)


# ---------------------------------------------------------------------------
# ۸) نمایش قیمت هر گیگ
# ---------------------------------------------------------------------------
def per_gb_note(name, final):
    if not get_bool("pergb_on"):
        return ""
    gb = _plan_gb(name)
    if not gb or not final:
        return ""
    return f" (≈{int(final / gb):,}/گیگ)"


# ---------------------------------------------------------------------------
# ۹) چک‌این روزانه، جدول دعوت، قطع/وصل تبلیغات
# ---------------------------------------------------------------------------
@dp.callback_query(F.data == "checkin")
async def checkin_handler(call: types.CallbackQuery):
    if not get_bool("checkin_on"):
        await call.answer("چک‌این فعلاً غیرفعاله.", show_alert=True)
        return
    uid = call.from_user.id
    today = today_iran()
    with SessionLocal() as s:
        if glog_has(s, uid, "checkin", today.isoformat()):
            await call.answer("امروز چک‌این کردی ✅ فردا دوباره بیا!", show_alert=True)
            return
        dates = {
            r[0]
            for r in s.query(GrowthLog.ref).filter(GrowthLog.user_id == uid, GrowthLog.kind == "checkin").all()
        }
        streak, d = 1, today - timedelta(days=1)
        while d.isoformat() in dates:
            streak += 1
            d -= timedelta(days=1)
        pts = max(1, get_int("checkin_points"))
        bonus = get_int("checkin_streak_bonus") if streak % 7 == 0 else 0
        u = get_or_create_user(s, uid)
        u.points = (u.points or 0) + pts + bonus
        glog_add(s, uid, "checkin", today.isoformat())
        total = u.points
        s.commit()
    msg = f"📅 چک‌این ثبت شد! +{pts} امتیاز\n🔥 زنجیرهٔ {streak} روزه\n🏆 مجموع امتیاز: {total}"
    if bonus:
        msg += f"\n🎉 جایزهٔ ۷ روز پشت‌سرهم: +{bonus} امتیاز"
    await call.answer(msg[:190], show_alert=True)


@dp.callback_query(F.data == "toprefs")
async def toprefs_handler(call: types.CallbackQuery):
    if not get_bool("toprefs_on"):
        await call.answer("فعلاً غیرفعاله.", show_alert=True)
        return
    await call.answer()
    uid = call.from_user.id
    with SessionLocal() as s:
        rows = (
            s.query(User.referred_by, func.count(User.user_id))
            .filter(User.referred_by.isnot(None), User.referral_rewarded.is_(True))
            .group_by(User.referred_by)
            .order_by(func.count(User.user_id).desc())
            .limit(10)
            .all()
        )
        names = {}
        for r in rows:
            ru = s.get(User, r[0])
            names[r[0]] = (ru.first_name if ru is not None and ru.first_name else "کاربر")
        mine = (
            s.query(func.count(User.user_id)).filter(User.referred_by == uid, User.referral_rewarded.is_(True)).scalar()
            or 0
        )
    medals = ["🥇", "🥈", "🥉"]
    out = "🏅 <b>برترین دعوت‌کننده‌ها</b>\n\n"
    if not rows:
        out += "هنوز کسی در جدول نیست؛ اولیش تو باش! 😉\n"
    for i, (rid, cnt) in enumerate(rows):
        nm = esc(names[rid][:2]) + "***"
        out += f"{medals[i] if i < 3 else str(i + 1) + '.'} {nm} — {cnt} دعوت موفق{' (شما)' if rid == uid else ''}\n"
    out += f"\n👤 دعوت‌های موفق شما: {mine}"
    await edit_or_send(call, out, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎁 دعوت دوستان", callback_data="referral_menu")], [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]]))


@dp.callback_query(F.data == "promo_toggle")
async def promo_toggle_handler(call: types.CallbackQuery):
    if not get_bool("promo_toggle_on"):
        await call.answer("فعلاً غیرفعاله.", show_alert=True)
        return
    with SessionLocal() as s:
        u = get_or_create_user(s, call.from_user.id)
        u.promo_off = not bool(u.promo_off)
        off = u.promo_off
        s.commit()
    await call.answer(
        "🔕 تبلیغات خاموش شد؛ فقط اعلان‌های مهم سرویس می‌رسه." if off else "🔔 تبلیغات و تخفیف‌ها دوباره فعال شد.",
        show_alert=True,
    )


# ---------------------------------------------------------------------------
# ۱۰) کد هدیه و انتقال اعتبار
# ---------------------------------------------------------------------------
def redeem_gift(uid, code):
    code = (code or "").strip().upper()
    if not code:
        return 0, "کدی وارد نشده."
    with SessionLocal() as s:
        g = s.query(GiftCode).filter(GiftCode.code == code).with_for_update().first()
        if g is None or not g.is_active:
            return 0, "این کد معتبر نیست."
        if g.expires_at:
            try:
                if date.fromisoformat(g.expires_at) < today_iran():
                    return 0, "این کد منقضی شده."
            except ValueError:
                pass
        if (g.max_uses or 0) > 0 and (g.used_count or 0) >= g.max_uses:
            return 0, "ظرفیت این کد تموم شده."
        if s.query(GiftUse.id).filter(GiftUse.gift_id == g.id, GiftUse.user_id == uid).first():
            return 0, "قبلاً از این کد استفاده کردی."
        u = get_or_create_user(s, uid)
        u.balance = min(MAX_BALANCE, (u.balance or 0) + (g.amount or 0))
        g.used_count = (g.used_count or 0) + 1
        s.add(GiftUse(gift_id=g.id, user_id=uid))
        amount = g.amount or 0
        s.commit()
    return amount, ""


@dp.callback_query(F.data == "gift_start")
async def gift_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(GrowthState.waiting_gift)
    await edit_or_send(call, "🎁 کد هدیه‌ت رو بفرست:", back_home_markup())


@dp.message(GrowthState.waiting_gift, F.text)
async def gift_entered(message: types.Message, state: FSMContext):
    amount, err = await asyncio.to_thread(redeem_gift, message.from_user.id, message.text)
    if err:
        await message.answer(f"❌ {err}\nدوباره بفرست یا /start بزن.")
        return
    await state.clear()
    await message.answer(f"🎉 {amount:,} تومان به کیف پولت اضافه شد!", reply_markup=back_home_markup())


@dp.message(Command("redeem"))
async def redeem_cmd(message: types.Message, command: CommandObject):
    if not command.args:
        await message.answer("استفاده: <code>/redeem کد</code>")
        return
    amount, err = await asyncio.to_thread(redeem_gift, message.from_user.id, command.args)
    await message.answer(f"🎉 {amount:,} تومان به کیف پولت اضافه شد!" if not err else f"❌ {err}")


@dp.message(Command("gift"), MainAdmin())
async def gift_admin_cmd(message: types.Message, command: CommandObject):
    parts = (command.args or "").split()
    amount = _num(parts[0]) if parts else None
    if not amount:
        await message.answer("استفاده: <code>/gift مبلغ [تعداد کد] [روز اعتبار] [تعداد استفاده هر کد]</code>\nمثال: <code>/gift 50000 10 30 1</code>")
        return
    count = min(50, _num(parts[1]) or 1) if len(parts) > 1 else 1
    days = _num(parts[2]) if len(parts) > 2 else 30
    uses = max(1, _num(parts[3])) if len(parts) > 3 and _num(parts[3]) else 1
    codes = []
    with SessionLocal() as s:
        for _ in range(count):
            code = "GIFT" + secrets.token_hex(3).upper()
            s.add(
                GiftCode(
                    code=code, amount=min(amount, MAX_BALANCE), max_uses=uses, used_count=0, is_active=True,
                    expires_at=(today_iran() + timedelta(days=days or 30)).isoformat(),
                )
            )
            codes.append(code)
        s.commit()
    log_action(message.from_user.id, f"ساخت {count} کد هدیه {amount:,} تومانی")
    await message.answer(
        f"✅ {count} کد هدیه {amount:,} تومانی ({days or 30} روز اعتبار، {uses} بار مصرف):\n\n"
        + "\n".join(f"<code>{c}</code>" for c in codes)
    )


@dp.message(Command("transfer"))
async def transfer_cmd(message: types.Message, command: CommandObject):
    if not get_bool("transfer_on"):
        await message.answer("انتقال اعتبار فعلاً غیرفعاله.")
        return
    parts = (command.args or "").split()
    to_id = _num(parts[0]) if len(parts) == 2 else None
    amount = _num(parts[1]) if len(parts) == 2 else None
    minimum = get_int("transfer_min")
    if not to_id or not amount:
        await message.answer(
            f"استفاده: <code>/transfer آیدی_عددی_گیرنده مبلغ</code>\nحداقل مبلغ: {minimum:,} تومان\n"
            "آیدی عددی رو گیرنده از «حساب کاربری» می‌بینه."
        )
        return
    uid = message.from_user.id
    fee = amount * get_int("transfer_fee_percent") // 100
    if amount < minimum or to_id == uid:
        await message.answer(f"مبلغ باید حداقل {minimum:,} تومان باشه و گیرنده خودت نباشه.")
        return
    async with user_lock(uid):
        with SessionLocal() as s:
            me, to = s.get(User, uid), s.get(User, to_id)
            if to is None or to.is_banned:
                await message.answer("گیرنده پیدا نشد (باید قبلاً ربات رو استارت کرده باشه).")
                return
            if (me.balance or 0) < amount + fee:
                await message.answer(f"موجودی کافی نیست. لازم: {amount + fee:,} تومان (کارمزد {fee:,})")
                return
            if (to.balance or 0) + amount > MAX_BALANCE:
                await message.answer("سقف موجودی گیرنده پر می‌شه.")
                return
            me.balance = (me.balance or 0) - amount - fee
            to.balance = (to.balance or 0) + amount
            glog_add(s, uid, "transfer", f"{to_id}:{amount}")
            s.commit()
    await message.answer(f"✅ {amount:,} تومان به <code>{to_id}</code> منتقل شد.")
    await gsend(message.bot, to_id, f"🎁 {amount:,} تومان اعتبار از طرف یکی از دوستانت به کیف پولت اضافه شد!")


# ---------------------------------------------------------------------------
# ۱۱) حالت اینلاین: @ربات در هر چت
# ---------------------------------------------------------------------------
@dp.inline_query()
async def inline_invite(q: types.InlineQuery):
    uid = q.from_user.id
    if not BOT_USERNAME:
        await q.answer([], cache_time=5)
        return
    link = f"https://t.me/{BOT_USERNAME}"
    if get_bool("referral_on"):
        link += f"?start=ref_{uid}"
    text_ = f"🚀 سرویس پرسرعت و پایدار با پشتیبانی سریع\n🎁 با این لینک وارد شو و خرید اولت رو با تخفیف بگیر:\n{link}"
    res = InlineQueryResultArticle(
        id=f"inv{uid}", title="📨 ارسال لینک دعوت", description="لینک اختصاصی دعوت رو برای این چت بفرست",
        input_message_content=InputTextMessageContent(message_text=text_),
    )
    await q.answer([res], cache_time=30, is_personal=True)


# ---------------------------------------------------------------------------
# ۱۲) گزارش‌های ادمین: /funnel /sources /profit /pricenotice /growth
# ---------------------------------------------------------------------------
@dp.message(Command("funnel"), MainAdmin())
async def funnel_cmd(message: types.Message):
    cutoff = (today_iran() - timedelta(days=30)).isoformat()
    with SessionLocal() as s:
        total = s.query(func.count(User.user_id)).scalar() or 0
        new30 = s.query(func.count(User.user_id)).filter(User.joined_at >= cutoff).scalar() or 0
        blocked = s.query(func.count(User.user_id)).filter(User.blocked.is_(True)).scalar() or 0
        trial = s.query(func.count(User.user_id)).filter(User.trial_used.is_(True)).scalar() or 0
        ordered = (
            s.query(func.count(func.distinct(Request.user_id))).filter(Request.req_type.in_(SALE_TYPES)).scalar() or 0
        )
        paid_rows = (
            s.query(Request.user_id, func.count(Request.id))
            .filter(Request.status == "answered", Request.req_type.in_(SALE_TYPES))
            .group_by(Request.user_id)
            .all()
        )
        carts = s.query(func.count(CartEvent.id)).scalar() or 0
        churn = dict(
            s.query(GrowthLog.ref, func.count(GrowthLog.id)).filter(GrowthLog.kind == "churn").group_by(GrowthLog.ref).all()
        )
    buyers = len(paid_rows)
    repeat = sum(1 for _u, c in paid_rows if c >= 2)

    def pct(a, b):
        return f"{(a * 100 / b):.1f}٪" if b else "—"

    out = (
        "📊 <b>قیف فروش</b>\n\n"
        f"👥 کل کاربران: {total:,} (۳۰ روز اخیر: {new30:,})\n"
        f"🚫 بلاک‌کننده‌ها: {blocked:,}\n"
        f"🎁 تست رایگان گرفته: {trial:,}\n"
        f"🛒 سفارش ثبت کرده: {ordered:,} ({pct(ordered, total)} از کل)\n"
        f"✅ خریدار واقعی: {buyers:,} ({pct(buyers, total)} از کل | {pct(buyers, ordered)} از سفارش‌دهنده‌ها)\n"
        f"🔁 مشتری تکراری: {repeat:,} ({pct(repeat, buyers)} از خریداران)\n"
        f"🛍 سبد ثبت‌شده در پرداخت: {carts:,}\n"
    )
    if churn:
        out += "\n🙋 <b>دلیل نرفتن/تمدید نکردن:</b>\n" + "\n".join(
            f"• {CHURN_REASONS.get(k, k)}: {v}" for k, v in churn.items()
        )
    await message.answer(out)


@dp.message(Command("sources"), MainAdmin())
async def sources_cmd(message: types.Message):
    with SessionLocal() as s:
        users = s.query(User.user_id, User.src).filter(User.src.isnot(None), User.src != "").all()
        rev = {
            r[0]: (r[1], r[2] or 0)
            for r in s.query(Request.user_id, func.count(Request.id), func.sum(Request.price))
            .filter(Request.status == "answered", Request.req_type.in_(SALE_TYPES))
            .group_by(Request.user_id)
            .all()
        }
    agg = {}
    for uid, src in users:
        a = agg.setdefault(src, [0, 0, 0])
        a[0] += 1
        if uid in rev:
            a[1] += 1
            a[2] += int(rev[uid][1])
    if not agg:
        link = f"https://t.me/{BOT_USERNAME}?start=src_" if BOT_USERNAME else "https://t.me/ربات?start=src_"
        await message.answer(f"هنوز منبعی ثبت نشده.\nلینک رهگیری بساز: <code>{link}نام_کانال</code>")
        return
    out = "🔗 <b>منابع جذب کاربر</b>\n\n"
    for src, (n, b, r) in sorted(agg.items(), key=lambda kv: -kv[1][2]):
        out += f"• <code>{esc(src)}</code>: {n} کاربر | {b} خریدار | {r:,} تومان\n"
    out += f"\nساخت لینک: <code>https://t.me/{BOT_USERNAME}?start=src_نام</code>"
    await message.answer(out)


@dp.message(Command("profit"), MainAdmin())
async def profit_cmd(message: types.Message, command: CommandObject):
    cost = get_int("cost_per_gb")
    if cost <= 0:
        await message.answer("اول «هزینهٔ هر گیگ» رو در تنظیمات ← 🚀 دعوت، امتیاز و هدیه وارد کن.")
        return
    days = _num(command.args) if command.args else None
    with SessionLocal() as s:
        q = s.query(Request.plan_name, func.count(Request.id), func.sum(Request.price)).filter(
            Request.status == "answered", Request.req_type.in_(SALE_TYPES)
        )
        if days:
            q = q.filter(Request.created_iso >= (now_iran().replace(tzinfo=None) - timedelta(days=days)).strftime(TS_FMT))
        rows = q.group_by(Request.plan_name).all()
    lines, tot_rev, tot_cost = [], 0, 0
    for name, cnt, rev in rows:
        rev = int(rev or 0)
        gb = _plan_gb(name)
        c = gb * cost * cnt if gb else 0
        tot_rev += rev
        tot_cost += c
        lines.append((rev - c, name, cnt, rev, c if gb else None))
    lines.sort(reverse=True)
    out = f"💹 <b>سود تخمینی{f' ({days} روز اخیر)' if days else ''}</b> | هزینه هر گیگ {cost:,}\n\n"
    for margin, name, cnt, rev, c in lines[:15]:
        out += (
            f"• {esc(name or '')}: {cnt} فروش | درآمد {rev:,} | "
            + (f"سود {margin:,} ({margin * 100 // rev if rev else 0}٪)" if c is not None else "هزینه نامشخص")
            + "\n"
        )
    out += f"\n💰 درآمد کل: {tot_rev:,}\n📉 هزینه تخمینی: {tot_cost:,}\n✅ سود تخمینی: {tot_rev - tot_cost:,}"
    await message.answer(out)


@dp.message(Command("pricenotice"), MainAdmin())
async def pricenotice_cmd(message: types.Message, command: CommandObject):
    days = _num(command.args) if command.args else None
    if not days:
        await message.answer("استفاده: <code>/pricenotice تعداد_روز</code>\nمثال: <code>/pricenotice 3</code> ← «قیمت‌ها تا ۳ روز دیگه بالا می‌ره»")
        return
    until = jalali_from_iso((today_iran() + timedelta(days=days)).isoformat())
    text_ = f"📢 <b>اطلاعیه افزایش قیمت</b>\n\nبه‌خاطر تغییر هزینه‌ها، قیمت سرویس‌ها از تاریخ {until} افزایش پیدا می‌کنه.\nتا اون موقع با قیمت‌های فعلی بخر یا تمدید کن 👇"

    async def _go():
        with SessionLocal() as s:
            ids = segment_user_ids(s, "all")
        for uid in ids:
            await gsend(message.bot, uid, text_, _buy_markup())
            await asyncio.sleep(0.05)

    spawn(_go())
    log_action(message.from_user.id, f"ارسال اطلاعیه افزایش قیمت ({days} روز)")
    await message.answer("✅ ارسال برای کاربران شروع شد.")


@dp.message(Command("growth"), MainAdmin())
async def growth_help(message: types.Message):
    await message.answer(
        "🚀 <b>دستورهای رشد (ادمین اصلی)</b>\n\n"
        "/funnel — قیف فروش و دلیل ریزش\n"
        "/sources — آمار لینک‌های رهگیری (<code>?start=src_نام</code>)\n"
        "/profit [روز] — سود تخمینی هر پلن\n"
        "/gift مبلغ [تعداد] [روز] [دفعات] — ساخت کد هدیه اعتباری\n"
        "/pricenotice روز — اطلاعیه افزایش قیمت برای همه\n\n"
        "تنظیمات: ⚙️ تنظیمات ربات ← بخش‌های «🚀»\n"
        "کاربران: /redeem کد ، /transfer آیدی مبلغ"
    )


JOBS = [
    ("backup", 3, 0, job_backup),
    ("cleanup", 4, 0, job_cleanup),
    ("weekly", 9, 0, job_weekly_report),
    ("reminders", 10, 0, job_reminders),
    ("growthd", 11, 0, job_growth_daily),
    ("report", 23, 30, job_report),
]


async def run_jobs(bot):
    now = now_iran()
    today = now.date().isoformat()
    for name, hour, minute, func_ in JOBS:
        if (now.hour, now.minute) < (hour, minute):
            continue
        if get_setting(f"last_{name}") == today:
            continue
        set_setting(f"last_{name}", today)
        try:
            await func_(bot)
        except Exception as e:  # noqa: BLE001
            logging.error("job %s failed: %s", name, e)
            await report_error(bot, f"زمان‌بندی: {name}", e)


async def scheduler(bot):
    while True:
        try:
            await run_jobs(bot)
            await job_scheduled_broadcasts(bot)
            await job_channel_posts(bot)
            await job_pending_reminder(bot)
            spawn(job_usage_alerts(bot))
            spawn(job_growth_tick(bot))
        except Exception as e:  # noqa: BLE001
            logging.error("scheduler error: %s", e)
            await report_error(bot, "زمان‌بند اصلی", e)
        await asyncio.sleep(60)


async def keepalive():
    """جلوگیری از خوابیدن سرویس رایگان Render با پینگ آدرس عمومی خودش."""
    url = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    if not url:
        return
    async with ClientSession(timeout=ClientTimeout(total=20)) as session:
        while True:
            await asyncio.sleep(600)
            try:
                async with session.get(url) as resp:
                    await resp.read()
            except Exception as e:  # noqa: BLE001
                logging.info("keepalive ping failed: %s", e)


# ---------------------------------------------------------------------------
# وب‌سرور سلامت و اجرای ربات
# ---------------------------------------------------------------------------
async def health_check(request):
    return web.Response(text="Bot is running", status=200)


async def update_bot_commands(bot):
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="منوی اصلی"),
            BotCommand(command="admin", description="پنل ادمین"),
        ]
    )


async def run_web_server(app, port):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


async def with_retry(fn, *args, tries=8, **kw):
    """تماس با تلگرام را در برابر timeout/قطعی موقت دوباره امتحان می‌کند."""
    for i in range(tries):
        try:
            return await fn(*args, **kw)
        except Exception as e:  # noqa: BLE001
            logging.warning("telegram call failed (%s/%s): %s", i + 1, tries, e)
            if i == tries - 1:
                raise
            await asyncio.sleep(min(30, 3 * (i + 1)))


async def main():
    global BOT_USERNAME
    init_db()
    refresh_menu_texts()
    try:
        logging.info("DB round-trip: %.0f ms (زیر ۳۰ عالی، بالای ۱۰۰ یعنی دیتابیس دور است)", await asyncio.to_thread(measure_db_latency))
    except Exception as e:  # noqa: BLE001
        logging.warning("DB latency check failed: %s", e)
    if not database_url:
        logging.warning(
            "DATABASE_URL تنظیم نشده؛ از SQLite محلی استفاده می‌شود. روی هاست‌های بدون دیسک پایدار "
            "(مثل Render رایگان) با هر دیپلوی اطلاعات پاک می‌شود."
        )
    from aiogram.client.session.aiohttp import AiohttpSession

    bot = Bot(
        token=TOKEN,
        session=AiohttpSession(timeout=60),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    # وب‌سرور اول بالا میاد تا Render پورت را ببیند، حتی اگر تلگرام کند باشد
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_post("/sms/{secret}", sms_webhook)
    set_growth_bot(bot)
    port = int(os.environ.get("PORT", 8080))
    await run_web_server(app, port)

    me = await with_retry(bot.get_me)
    BOT_USERNAME = me.username or ""
    await with_retry(bot.delete_webhook, drop_pending_updates=True)
    try:
        await update_bot_commands(bot)
    except Exception as e:  # noqa: BLE001
        logging.warning("update_bot_commands failed: %s", e)

    background = [
        asyncio.create_task(scheduler(bot)),
        asyncio.create_task(keepalive()),
        asyncio.create_task(monitor_loop(bot)),
        asyncio.create_task(loop_lag_monitor()),
    ]
    logging.info("BUILD %s", BUILD_TAG)
    logging.info("Bot @%s is running; web server on port %s", BOT_USERNAME, port)
    try:
        # اگر polling به‌خاطر خطای شبکه/Conflict افتاد، کل برنامه کرش نکند
        while True:
            try:
                await dp.start_polling(bot, handle_signals=False)
                break
            except Exception as e:  # noqa: BLE001
                logging.error("polling crashed, restarting in 5s: %s", e)
                await asyncio.sleep(5)
    finally:
        for task in background:
            task.cancel()
        try:
            await bot.session.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

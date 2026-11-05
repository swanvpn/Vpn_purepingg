import asyncio
import logging
import sqlite3
import os
import os

# خواندن آدرس دیتابیس از متغیرهای محیطی Render
database_url = os.environ.get("DATABASE_URL")

# نکته مهم: برخی کتابخانه‌ها نیازمند پیشوند postgresql:// هستند در حالی که رندر ممکن است postgres:// بدهد
if database_url and database_url.startswith("postgres://"):
  database_url = database_url.replace("postgres://", "postgresql://", 1)

# نمونه اتصال با SQLAlchemy:
# engine = create_engine(database_url)

from datetime import datetime, timedelta
import jdatetime
from aiohttp import web

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReactionTypeEmoji, BotCommand, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

TOKEN = "9007376692:AAGJGPvs7dJP-USYmdqo7kNAIO4CS7I32HA"
CARD_NUMBER = "6219861493813108"
CARD_HOLDER = "باقرزاده"
ADMIN_ID = 98695770

class PaymentState(StatesGroup):
    waiting_for_receipt = State()
    replying_to_user = State()
    waiting_for_charge_receipt = State()
    broadcasting = State()
    adjusting_balance = State()
    waiting_for_admin_config = State()

def get_current_jalali():
    now_gregorian = datetime.now()
    shamsi_date = jdatetime.date.fromgregorian(date=now_gregorian.date()).strftime('%Y/%m/%d')
    shamsi_time = now_gregorian.strftime('%H:%M:%S')
    return f"{shamsi_date} - {shamsi_time}"

def get_jalali_date(gregorian_date):
    return jdatetime.date.fromgregorian(date=gregorian_date).strftime('%Y/%m/%d')

def init_db():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY, 
                        balance INTEGER DEFAULT 0,
                        is_banned INTEGER DEFAULT 0)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS services (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        category TEXT, 
                        name TEXT, 
                        price INTEGER)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_configs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        user_id INTEGER, 
                        config_name TEXT, 
                        config_details TEXT)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS requests (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        req_type TEXT,
                        plan_name TEXT,
                        status TEXT DEFAULT 'unanswered',
                        created_at TEXT,
                        answered_at TEXT,
                        receipt_file_id TEXT)''')

    cursor.execute("PRAGMA table_info(users)")
    user_columns = [info[1] for info in cursor.fetchall()]
    if 'is_banned' not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0")

    cursor.execute("PRAGMA table_info(user_configs)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'config_name' not in columns:
        cursor.execute("ALTER TABLE user_configs ADD COLUMN config_name TEXT")
        
    cursor.execute("PRAGMA table_info(requests)")
    columns_req = [info[1] for info in cursor.fetchall()]
    if 'receipt_file_id' not in columns_req:
        cursor.execute("ALTER TABLE requests ADD COLUMN receipt_file_id TEXT")
        
    cursor.execute('SELECT COUNT(*) FROM services')
    if cursor.fetchone()[0] == 0:
        plans = [
            ('economic', '۱00 گیگ - ۲ کاربر', 298000),
            ('economic', '۱00 گیگ - نامحدود', 398000),
            ('vip', '10 گیگ', 190000),
            ('vip', '20 گیگ', 320000),
            ('vip', '30 گیگ', 490000),
            ('vip', '50 گیگ', 790000),
            ('vip', '100 گیگ', 1330000),
            ('gaming', '10 گیگ', 220000),
            ('gaming', '20 گیگ', 360000),
            ('gaming', '30 گیگ', 530000),
            ('gaming', '50 گیگ', 850000)
        ]
        cursor.executemany('INSERT INTO services (category, name, price) VALUES (?, ?, ?)', plans)
    conn.commit()
    conn.close()

def get_category_services(category):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT name, price FROM services WHERE category = ?', (category,))
    services = cursor.fetchall()
    conn.close()
    return services

async def update_bot_commands(bot: Bot):
    user_commands = [
        BotCommand(command="start", description="شروع به کار با ربات")
    ]
    admin_commands = [
        BotCommand(command="start", description="شروع به کار با ربات"),
        BotCommand(command="admin", description="پنل مدیریت")
    ]
    await bot.set_my_commands(user_commands)
    try:
        await bot.set_my_commands(admin_commands, scope=types.BotCommandScopeChat(chat_id=ADMIN_ID))
    except Exception as e:
        logging.error(f"Failed to set admin commands: {e}")

def get_main_keyboard():
    btn_buy = KeyboardButton(text="🛒 خرید سرویس")
    btn_services = KeyboardButton(text="📦 سرویس‌های من")
    btn_tariffs = KeyboardButton(text="📈 تعرفه سرویس‌ها")
    btn_wallet = KeyboardButton(text="💳 شارژ کیف پول")
    btn_account = KeyboardButton(text="👤 حساب کاربری")
    btn_support = KeyboardButton(text="🎧 پشتیبانی")
    
    markup = ReplyKeyboardMarkup(
        keyboard=[
            [btn_buy, btn_services],
            [btn_tariffs, btn_wallet],
            [btn_account, btn_support]
        ],
        resize_keyboard=True,
        persistent=True,
        input_field_placeholder="منوی اصلی ربات"
    )
    return markup

def get_admin_main_keyboard():
    btn_unanswered = InlineKeyboardButton(text="🔴 پاسخ داده نشده", callback_data="admin_unanswered")
    btn_answered = InlineKeyboardButton(text="🟢 پاسخ داده شده", callback_data="admin_answered")
    btn_users = InlineKeyboardButton(text="👥 مدیریت کاربران", callback_data="admin_users_list")
    btn_stats = InlineKeyboardButton(text="📈 آمار کلی فروش", callback_data="admin_sales_stats")
    btn_broadcast = InlineKeyboardButton(text="📢 ارسال پیام همگانی", callback_data="admin_broadcast_start")
    btn_home = InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn_unanswered, btn_answered],
        [btn_users],
        [btn_stats],
        [btn_broadcast],
        [btn_home]
    ])

async def check_ban(message: types.Message):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT is_banned FROM users WHERE user_id = ?', (message.from_user.id,))
    row = cursor.fetchone()
    conn.close()
    return row and row[0] == 1
async def show_main_menu(message: types.Message):
    if await check_ban(message):
        return
    user_id = message.from_user.id
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO users (user_id, balance, is_banned) VALUES (?, 0, 0)', (user_id,))
    conn.commit()
    conn.close()
    
    markup = get_main_keyboard()
    await message.answer("سلام! به پنل Swan VPN خوش آمدید.\n/start", reply_markup=markup)

async def main_menu_handler_call(call: types.CallbackQuery, state: FSMContext):
    if await check_ban(call.message):
        return
    await state.clear()
    markup = get_main_keyboard()
    await call.message.answer("سلام! به پنل Swan VPN خوش آمدید.\n/start", reply_markup=markup)
    try:
        await call.message.delete()
    except Exception:
        pass

async def tariffs_handler(message: types.Message):
    if await check_ban(message):
        return
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT category, name, price FROM services ORDER BY category')
    rows = cursor.fetchall()
    conn.close()
    
    current_cat = ""
    text = "📊 **لیست تعرفه و قیمت سرویس‌ها:**\n\n"
    for cat, name, price in rows:
        if cat != current_cat:
            current_cat = cat
            text += f"\n📂 **دسته‌بندی {cat.upper()}:**\n"
        text += f"🔹 {name}: {price:,} تومان\n"
        
    btn_back = InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_back]])
    await message.answer(text, parse_mode="Markdown", reply_markup=markup)

async def message_buy_handler(message: types.Message, state: FSMContext):
    if await check_ban(message):
        return
    await state.clear()
    btn_econ = InlineKeyboardButton(text="🌱 سرویس اقتصادی", callback_data="cat_economic")
    btn_vip = InlineKeyboardButton(text="💎 سرویس وی‌آی‌پی", callback_data="cat_vip")
    btn_gaming = InlineKeyboardButton(text="🎮 سرویس گیمینگ", callback_data="cat_gaming")
    btn_back_main = InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_econ], [btn_vip], [btn_gaming], [btn_back_main]])
    await message.answer("لطفاً دسته‌بندی مورد نظر را انتخاب کنید:", reply_markup=markup)

async def main_buy_handler(call: types.CallbackQuery, state: FSMContext):
    if await check_ban(call.message):
        return
    await state.clear()
    btn_econ = InlineKeyboardButton(text="🌱 سرویس اقتصادی", callback_data="cat_economic")
    btn_vip = InlineKeyboardButton(text="💎 سرویس وی‌آی‌پی", callback_data="cat_vip")
    btn_gaming = InlineKeyboardButton(text="🎮 سرویس گیمینگ", callback_data="cat_gaming")
    btn_back_main = InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_econ], [btn_vip], [btn_gaming], [btn_back_main]])
    try:
        await call.message.edit_text("لطفاً دسته‌بندی مورد نظر را انتخاب کنید:", reply_markup=markup)
    except Exception:
        await call.message.answer("لطفاً دسته‌بندی مورد نظر را انتخاب کنید:", reply_markup=markup)

async def category_handler(call: types.CallbackQuery):
    if await check_ban(call.message):
        return
    try:
        await call.bot.set_message_reaction(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reaction=[ReactionTypeEmoji(emoji="👍")]
        )
    except Exception:
        pass
    category = call.data.split("_")[1]
    services = get_category_services(category)
    keyboard = []
    text = f"لیست پلن‌های {category.upper()}:\n\n"
    for name, price in services:
        text += f"🔹 {name}: {price:,} تومان\n"
        keyboard.append([InlineKeyboardButton(text=f"خرید {name}", callback_data=f"buy_{name}")])
    btn_back = InlineKeyboardButton(text="⬅️ برگشت به منوی خرید", callback_data="main_buy")
    keyboard.append([btn_back])
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await call.message.edit_text(text, reply_markup=markup)

async def buy_plan_handler(call: types.CallbackQuery, state: FSMContext):
    if await check_ban(call.message):
        return
    plan_name = call.data.replace("buy_", "")
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT price FROM services WHERE name = ?', (plan_name,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        await call.message.answer("خطایی در یافتن سرویس رخ داد.")
        return
        
    price = row[0]
    await state.set_state(PaymentState.waiting_for_receipt)
    await state.update_data(plan_name=plan_name, plan_price=price)
    
    text = (f"🛒 سفارش شما: {plan_name}\n"
            f"💰 مبلغ قابل پرداخت: {price:,} تومان\n\n"
            f"شما می‌توانید از طریق کارت یا موجودی کیف پول پرداخت کنید.\n\n"
            f"💳 شماره کارت جهت واریز:\n`{CARD_NUMBER}`\n"
            f"به نام: {CARD_HOLDER}\n\n"
            f"پس از واریز فیش را ارسال کنید یا در صورت داشتن موجودی کافی از دکمه زیر استفاده کنید.")
            
    btn_confirm = InlineKeyboardButton(text="✅ ارسال رسید واریز", callback_data="confirm_payment")
    btn_wallet = InlineKeyboardButton(text="💳 پرداخت از کیف پول", callback_data="pay_with_wallet")
    btn_back = InlineKeyboardButton(text="⬅️ انصراف و بازگشت", callback_data="main_buy")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_confirm], [btn_wallet], [btn_back]])
    await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")

async def pay_with_wallet_handler(call: types.CallbackQuery, state: FSMContext):
    if await check_ban(call.message):
        return
    user_id = call.from_user.id
    data = await state.get_data()
    plan_name = data.get("plan_name", "سرویس")
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT price FROM services WHERE name = ?', (plan_name,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        await call.answer("خطایی در یافتن قیمت سرویس رخ داد.", show_alert=True)
        return
    
    price = row[0]
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_balance = cursor.fetchone()
    balance = user_balance[0] if user_balance else 0
    
    if balance < price:
        conn.close()
        await call.answer(f"موجودی شما ({balance:,} تومان) برای پرداخت این سرویس کافی نیست.", show_alert=True)
        return
        
    cursor.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (price, user_id))
    created_at = get_current_jalali()
    
    cursor.execute('''INSERT INTO requests (user_id, req_type, plan_name, status, created_at, receipt_file_id) 
                      VALUES (?, 'buy', ?, 'unanswered', ?, 'None')''',
                   (user_id, plan_name, created_at))
    req_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    await state.clear()
    await call.message.edit_text("✅ پرداخت با موفقیت از کیف پول انجام شد. لطفاً منتظر بررسی ادمین باشید. 🙏")
    
    try:
        user_info = f"@{call.from_user.username}" if call.from_user.username else f"아یدی: {call.from_user.id}"
        caption = (f"🚨 درخواست جدید با کیف پول (کد #{req_id})\n"
                   f"👤 کاربر: {user_info} ({user_id})\n"
                   f"📦 پلن: {plan_name}\n"
                   f"💰 مبلغ: {price:,} تومان\n"
                   f"⏱ زمان ثبت: {created_at}")
        btn_confirm = InlineKeyboardButton(text="✅ تایید و ارسال کانفیگ", callback_data=f"admin_confirm_{req_id}")
        btn_reject = InlineKeyboardButton(text="❌ رد درخواست", callback_data=f"admin_reject_{req_id}")
        btn_admin = InlineKeyboardButton(text="⚙️ پنل مدیریت", callback_data="admin_home")
        markup = InlineKeyboardMarkup(inline_keyboard=[[btn_confirm, btn_reject], [btn_admin]])
        await call.bot.send_message(chat_id=ADMIN_ID, text=caption, reply_markup=markup)
    except Exception as e:
        print(f"Error sending wallet purchase to admin: {e}")

async def confirm_payment_handler(call: types.CallbackQuery):
    if await check_ban(call.message):
        return
    await call.message.edit_text("لطفاً اسکرین‌شات یا عکس رسید واریزی خود را در چت بفرستید تا بررسی شود.")

async def receipt_handler(message: types.Message, state: FSMContext):
    if await check_ban(message):
        return
    data = await state.get_data()
    plan_name = data.get("plan_name", "سرویس نامشخص")
    created_at = get_current_jalali()
    user_id = message.from_user.id
    photo_id = message.photo[-1].file_id
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO requests (user_id, req_type, plan_name, status, created_at, receipt_file_id) 
                      VALUES (?, 'buy', ?, 'unanswered', ?, ?)''',
                   (user_id, plan_name, created_at, photo_id))
    req_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    await state.clear()
    await message.answer(f"رسید شما در تاریخ {created_at} دریافت شد و به زودی بررسی می‌شود. با تشکر از خرید شما! 🙏")
    
    try:
        user_info = f"@{message.from_user.username}" if message.from_user.username else f"아یدی: {message.from_user.id}"
        caption = (f"🚨 رسید جدید (کد #{req_id})\n"
                   f"👤 کاربر: {user_info} ({user_id})\n"
                   f"📦 پلن: {plan_name}\n"
                   f"⏱ زمان ارسال: {created_at}")
            
        btn_confirm = InlineKeyboardButton(text="✅ تایید و ارسال کانفیگ", callback_data=f"admin_confirm_{req_id}")
        btn_reject = InlineKeyboardButton(text="❌ رد درخواست", callback_data=f"admin_reject_{req_id}")

        btn_admin = InlineKeyboardButton(text="⚙️ پنل مدیریت", callback_data="admin_home")
        markup = InlineKeyboardMarkup(inline_keyboard=[[btn_confirm, btn_reject], [btn_admin]])
        await message.bot.send_photo(chat_id=ADMIN_ID, photo=photo_id, caption=caption, reply_markup=markup)
    except Exception as e:
        print(f"Error sending receipt to admin: {e}")

async def wallet_menu_handler(message: types.Message):
    if await check_ban(message):
        return
    user_id = message.from_user.id
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    balance = row[0] if row else 0
    conn.close()

    text = (f"💳 **شارژ کیف پول Swan VPN**\n\n"
            f"💰 موجودی فعلی: {balance:,} تومان\n\n"
            f"لطفاً مبلغ مورد نظر خود را انتخاب کنید:")

    buttons = [
        [InlineKeyboardButton(text="200,000 تومان", callback_data="charge_200000"),
         InlineKeyboardButton(text="400,000 تومان", callback_data="charge_400000")],
        [InlineKeyboardButton(text="600,000 تومان", callback_data="charge_600000"),
         InlineKeyboardButton(text="800,000 تومان", callback_data="charge_800000")],
        [InlineKeyboardButton(text="1,000,000 تومان", callback_data="charge_1000000"),
         InlineKeyboardButton(text="1,500,000 تومان", callback_data="charge_1500000")],
        [InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")]
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer(text, reply_markup=markup, parse_mode="Markdown")

async def select_charge_amount_handler(call: types.CallbackQuery, state: FSMContext):
    if await check_ban(call.message):
        return
    amount = int(call.data.replace("charge_", ""))
    await state.update_data(charge_amount=amount)
    
    text = (f"💳 **درخواست شارژ کیف پول**\n"
            f"💰 مبلغ انتخاب شده: {amount:,} تومان\n\n"
            f"لطفاً مبلغ را به شماره کارت زیر واریز نمایید:\n"
            f"💳 `{CARD_NUMBER}`\n"
            f"به نام: {CARD_HOLDER}\n\n"
            f"پس از واریز، روی دکمه زیر کلیک کرده و رسید خود را ارسال کنید.")
            
    btn_send_receipt = InlineKeyboardButton(text="📤 ارسال رسید واریز", callback_data="send_charge_receipt")
    btn_back = InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_send_receipt], [btn_back]])
    
    await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")

async def prompt_charge_receipt_handler(call: types.CallbackQuery, state: FSMContext):
    if await check_ban(call.message):
        return
    await state.set_state(PaymentState.waiting_for_charge_receipt)
    await call.message.edit_text("لطفاً اسکرین‌شات یا عکس رسید واریزی خود را در چت ارسال کنید:")

async def process_charge_receipt_handler(message: types.Message, state: FSMContext):
    if await check_ban(message):
        return
    data = await state.get_data()
    amount = data.get("charge_amount", 0)
    created_at = get_current_jalali()
    user_id = message.from_user.id
    photo_id = message.photo[-1].file_id
    
    await state.clear()
    await message.answer("🙏 رسید شما دریافت شد و در انتظار بررسی و تایید ادمین است. از صبر شما سپاسگزاریم.")
    
    try:
        user_info = f"@{message.from_user.username}" if message.from_user.username else f"아یدی: {message.from_user.id}"
        caption = (f"🚨 **درخواست جدید شارژ کیف پول**\n"
                   f"👤 کاربر: {user_info} ({user_id})\n"
                   f"💰 مبلغ: {amount:,} تومان\n"
                   f"⏱ زمان ارسال: {created_at}")
                   
        btn_confirm = InlineKeyboardButton(text="✅ تایید و شارژ موجودی", callback_data=f"confirm_charge_{user_id}_{amount}")
        btn_reject = InlineKeyboardButton(text="❌ رد واریز", callback_data=f"reject_charge_{user_id}")
        btn_admin = InlineKeyboardButton(text="⚙️ پنل مدیریت", callback_data="admin_home")
        markup = InlineKeyboardMarkup(inline_keyboard=[[btn_confirm, btn_reject], [btn_admin]])
        
        await message.bot.send_photo(chat_id=ADMIN_ID, photo=photo_id, caption=caption, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        print(f"Error forwarding charge receipt to admin: {e}")

async def confirm_charge_action(call: types.CallbackQuery):
    parts = call.data.split("_")
    user_id = int(parts[2])
    amount = int(parts[3])
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO users (user_id, balance, is_banned) VALUES (?, 0, 0)', (user_id,))
    cursor.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
    conn.commit()
    conn.close()
    
    admin_markup = get_admin_main_keyboard()
    await call.message.edit_caption(caption=f"{call.message.caption}\n\n✅ وضعیت: واریز تایید شد و حساب کاربر شارژ گردید.", reply_markup=admin_markup)
    try:
        await call.bot.send_message(chat_id=user_id, text=f"🎉 تبریک!\n\nمبلغ {amount:,} تومان با موفقیت به موجودی کیف پول شما اضافه شد.")
    except Exception as e:
        print(f"Error notifying user about wallet top-up: {e}")
async def reject_charge_action(call: types.CallbackQuery):
    admin_markup = get_admin_main_keyboard()
    await call.message.edit_caption(caption=f"{call.message.caption}\n\n رد شد.", reply_markup=admin_markup)

async def my_services_handler(message: types.Message):
    if await check_ban(message):
        return
    user_id = message.from_user.id
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT id, config_name FROM user_configs WHERE user_id = ?', (user_id,))
    configs = cursor.fetchall()
    conn.close()
    
    if not configs:
        await message.answer("❌ شما در حال حاضر سرویس فعالی ندارید.")
        return
        
    keyword_buttons = [InlineKeyboardButton(text=f"🌐 {name}", callback_data=f"view_cfg_{cfg_id}") for cfg_id, name in configs]
    compact_keyboard = [keyword_buttons[i:i+2] for i in range(0, len(keyword_buttons), 2)]
    compact_keyboard.append([InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")])
    markup = InlineKeyboardMarkup(inline_keyboard=compact_keyboard)
    await message.answer("📦 **سرویس‌های فعال شما (برای مشاهده جزئیات انتخاب کنید):**", reply_markup=markup, parse_mode="Markdown")

async def view_config_detail_handler(call: types.CallbackQuery):
    if await check_ban(call.message):
        return
    cfg_id = call.data.split("_")[2]
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT config_name, config_details FROM user_configs WHERE id = ?', (cfg_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        await call.answer("سرویس یافت نشد.", show_alert=True)
        return
        
    name, details = row
    text = f"📋 **جزئیات سرویس {name}:**\n\n`{details}`"
    keyboard = [
        [InlineKeyboardButton(text="⬅️ بازگشت به لیست سرویس‌ها", callback_data="my_services_callback")],
        [InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")]
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")

async def my_services_callback_handler(call: types.CallbackQuery):
    if await check_ban(call.message):
        return
    user_id = call.from_user.id
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT id, config_name FROM user_configs WHERE user_id = ?', (user_id,))
    configs = cursor.fetchall()
    conn.close()
    
    if not configs:
        await call.message.edit_text("❌ سرویس فعالی ندارید.")
        return
        
    keyword_buttons = [InlineKeyboardButton(text=f"🌐 {name}", callback_data=f"view_cfg_{cfg_id}") for cfg_id, name in configs]
    compact_keyboard = [keyword_buttons[i:i+2] for i in range(0, len(keyword_buttons), 2)]
    compact_keyboard.append([InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")])
    markup = InlineKeyboardMarkup(inline_keyboard=compact_keyboard)
    await message.answer("📦 **سرویس‌های فعال شما (برای مشاهده جزئیات انتخاب کنید):**", reply_markup=markup, parse_mode="Markdown")

async def account_handler(message: types.Message):
    if await check_ban(message):
        return
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (message.from_user.id,))
    row = cursor.fetchone()
    balance = row[0] if row else 0
    
    cursor.execute('SELECT COUNT(*) FROM user_configs WHERE user_id = ?', (message.from_user.id,))
    active_count = cursor.fetchone()[0]
    conn.close()
    
    text = (f"👤 **حساب کاربری**\n\n"
            f"🆔 آیدی عددی: `{message.from_user.id}`\n"
            f"💳 موجودی کیف پول: {balance:,} تومان\n"
            f"🟢 تعداد سرویس‌های فعال: {active_count}")
            
    keyboard = [
        [InlineKeyboardButton(text="📜 تاریخچه تراکنش‌ها", callback_data="tx_history")],
        [InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")]
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await message.answer(text, reply_markup=markup, parse_mode="Markdown")

async def transaction_history_handler(call: types.CallbackQuery):
    if await check_ban(call.message):
        return
    user_id = call.from_user.id
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT id, plan_name, status, created_at FROM requests WHERE user_id = ? ORDER BY id DESC', (user_id,))
    reqs = cursor.fetchall()
    conn.close()
    
    if not reqs:
        await call.answer("هیچ فاکتور یا درخواستی ثبت نشده است.", show_alert=True)
        return
        
    text = "📜 **تاریخچه تراکنش‌ها و درخواست‌های شما:**\n\n"
    for req_id, plan, status, date in reqs:
        status_emoji = "✅" if status == "answered" else ("❌" if status == "rejected" else "⏳")
        text += f"🔹 فاکتور #{req_id} | {plan} | وضعیت: {status_emoji}\n⏱ تاریخ: {date}\n-----------------------------------\n"
        
    btn_back = InlineKeyboardButton(text="⬅️ بازگشت به حساب کاربری", callback_data="back_to_account")
    btn_home = InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_back], [btn_home]])
    await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")

async def back_to_account_handler(call: types.CallbackQuery):
    if await check_ban(call.message):
        return
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (call.from_user.id,))
    row = cursor.fetchone()
    balance = row[0] if row else 0
    cursor.execute('SELECT COUNT(*) FROM user_configs WHERE user_id = ?', (call.from_user.id,))
    active_count = cursor.fetchone()[0]
    conn.close()
    
    text = (f"👤 **حساب کاربری**\n\n"
            f"🆔 آیدی عددی: `{call.from_user.id}`\n"
            f"💳 موجودی کیف پول: {balance:,} تومان\n"
            f"🟢 تعداد سرویس‌های فعال: {active_count}")
            
    keyboard = [
        [InlineKeyboardButton(text="📜 تاریخچه تراکنش‌ها", callback_data="tx_history")],
        [InlineKeyboardButton(text="🏠 بازگشت به منوی اصلی", callback_data="main_menu")]
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")

async def support_handler(message: types.Message):
    if await check_ban(message):
        return
    await message.answer("🎧 جهت پشتیبانی و سوالات به آیدی @swanvpnnn مراجعه کنید.")
async def admin_panel_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    markup = get_admin_main_keyboard()
    await message.answer("⚙️ **پنل مدیریت ادمین:**", reply_markup=markup)

async def admin_home_callback(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    markup = get_admin_main_keyboard()
    try:
        await call.message.edit_text("⚙️ **پنل مدیریت ادمین:**", reply_markup=markup)
    except Exception:
        await call.message.answer("⚙️ **پنل مدیریت ادمین:**", reply_markup=markup)

async def admin_unanswered_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, user_id, plan_name, created_at FROM requests WHERE status = 'unanswered'")
    reqs = cursor.fetchall()
    conn.close()
    
    if not reqs:
        await call.answer("درخواست پاسخ‌داده‌نشده‌ای وجود ندارد.", show_alert=True)
        return
        
    text = "🔴 **درخواست‌های در انتظار پاسخ:**\n\n"
    keyboard = []
    for req_id, u_id, plan, date in reqs:
        text += f"🆔 کد درخواست: #{req_id} | کاربر: {u_id}\n📦 پلن: {plan} | تاریخ: {date}\n-----------------------------------\n"
        keyboard.append([InlineKeyboardButton(text=f"💬 بررسی جزئیات درخواست #{req_id}", callback_data=f"admin_detail_{req_id}")])
    
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل ادمین", callback_data="admin_home")])
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    try:
        await call.message.edit_text(text, reply_markup=markup, disable_web_page_preview=True)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            await call.answer("محتوا تغییری نکرده است.", show_alert=False)
        else:
            raise e

async def admin_answered_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, user_id, plan_name, created_at, answered_at FROM requests WHERE status != 'unanswered' ORDER BY id DESC LIMIT 10")
    reqs = cursor.fetchall()
    conn.close()
    
    if not reqs:
        await call.answer("درخواست پاسخ‌داده‌شده‌ای یافت نشد.", show_alert=True)
        return
        
    text = "🟢 **آخرین درخواست‌های بررسی‌شده:**\n\n"
    for req_id, u_id, plan, created, answered in reqs:
        text += f"🆔 کد: #{req_id} | کاربر: {u_id}\n📦 پلن: {plan}\n⏱ ثبت: {created}\n✅ پاسخ/وضعیت: {answered}\n-----------------------------------\n"

    keyword_buttons = [InlineKeyboardButton(text=f"💬 #{req_id}", callback_data=f"admin_detail_{req_id}") for req_id, _, _, _, _ in reqs]
    compact_keyboard = [keyword_buttons[i:i+3] for i in range(0, len(keyword_buttons), 3)]
    compact_keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل ادمین", callback_data="admin_home")])
    
    markup = InlineKeyboardMarkup(inline_keyboard=compact_keyboard)
    
    try:
        await call.message.edit_text(text, reply_markup=markup, disable_web_page_preview=True)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            await call.answer("محتوا تغییری نکرده است.", show_alert=False)
        else:
            raise e

async def admin_detail_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    req_id = int(call.data.split("_")[2])
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, req_type, plan_name, status, created_at, answered_at, receipt_file_id FROM requests WHERE id = ?', (req_id,))
    req = cursor.fetchone()
    conn.close()
    
    if not req:
        await call.answer("درخواست یافت نشد.", show_alert=True)
        return
        
    u_id, r_type, plan, status, created, answered, receipt_id = req
    status_text = "پاسخ داده شده 🟢" if status == "answered" else ("رد شده ❌" if status == "rejected" else "در انتظار بررسی 🔴")
    
    text = (f"📋 **جزئیات کامل درخواست #{req_id}**\n\n"
            f"👤 آیدی کاربر: `{u_id}`\n"
            f"📦 نوع و پلن: {r_type.upper()} - {plan}\n"
            f"📌 وضعیت: {status_text}\n"
            f"⏱ زمان ثبت: {created}\n")
    if answered:
        text += f"✅ وضعیت/پاسخ: {answered}\n"
        
    btn_back_list = InlineKeyboardButton(text="⬅️ بازگشت به لیست", callback_data="admin_unanswered" if status == "unanswered" else "admin_answered")
    btn_home = InlineKeyboardButton(text="🏠 پنل اصلی مدیریت", callback_data="admin_home")
    keyboard = [[btn_back_list], [btn_home]]
    
    if status == "unanswered":
        btn_confirm = InlineKeyboardButton(text="✅ تایید و ارسال کانفیگ", callback_data=f"admin_confirm_{req_id}")
        btn_reject = InlineKeyboardButton(text="❌ رد درخواست", callback_data=f"admin_reject_{req_id}")
        keyboard.insert(0, [btn_confirm, btn_reject])
        
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    try:
        if receipt_id and receipt_id != "None":
            try:
                await call.message.delete()
            except:
                pass
            await call.bot.send_photo(chat_id=call.message.chat.id, photo=receipt_id, caption=text, reply_markup=markup, parse_mode="Markdown")
        else:
            await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing request details: {e}")

async def admin_confirm_action(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    req_id = int(call.data.split("_")[2])
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, plan_name FROM requests WHERE id = ?', (req_id,))
    req = cursor.fetchone()
    conn.close()
    
    if not req:
        await call.answer("درخواست یافت نشد.", show_alert=True)
        return
        
    target_user_id, plan_name = req
    now_jalali = get_current_jalali()
    
    cursor = sqlite3.connect("bot_data.db").cursor()
    cursor.execute("UPDATE requests SET status = 'answered', answered_at = ? WHERE id = ?", (now_jalali, req_id))
    cursor.connection.commit()
    cursor.connection.close()
    
    await state.update_data(target_user_id=target_user_id, plan_name=plan_name, req_id=req_id)
    await state.set_state(PaymentState.waiting_for_admin_config)
    
    admin_markup = get_admin_main_keyboard()
    try:
        await call.message.edit_caption(caption=f"{call.message.caption}\n\n✅ وضعیت: درخواست تایید شد. لطفاً کانفیگ را ارسال کنید.", reply_markup=admin_markup)
    except:
        await call.message.answer("درخواست تایید شد. لطفاً متن کانفیگ یا ساب‌لینک را برای ارسال به کاربر وارد کنید:")

async def admin_reject_action(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    req_id = int(call.data.split("_")[2])
    now_jalali = get_current_jalali()
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE requests SET status = 'rejected', answered_at = ? WHERE id = ?", (now_jalali, req_id))
    cursor.execute("SELECT user_id FROM requests WHERE id = ?", (req_id,))
    target_user = cursor.fetchone()
    conn.commit()
    conn.close()
    
    admin_markup = get_admin_main_keyboard()
    try:
        await call.message.edit_caption(caption=f"{call.message.caption}\n\n شد.", reply_markup=admin_markup)
    except:
        await call.message.answer("درخواست رد شد و از لیست ادمین حذف گردید.")
        
    if target_user:
        try:
            await call.bot.send_message(chat_id=target_user[0], text="⚠️ متأسفانه پرداخت/درخواست شما توسط ادمین تایید نشد.")
        except:
            pass

async def process_admin_config(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    target_user_id = data.get("target_user_id")
    plan_name = data.get("plan_name")
    req_id = data.get("req_id")
    
    start_date_gregorian = datetime.now()
    end_date_gregorian = start_date_gregorian + timedelta(days=30)
    
    start_jalali = get_jalali_date(start_date_gregorian)
    end_jalali = get_jalali_date(end_date_gregorian)
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO user_configs (user_id, config_name, config_details) VALUES (?, ?, ?)",
                   (target_user_id, plan_name, message.text))
    conn.commit()
    conn.close()
    
    await state.clear()
    admin_markup = get_admin_main_keyboard()
    await message.answer(f"✅ کانفیگ با موفقیت برای کاربر {target_user_id} ارسال و ثبت شد.", reply_markup=admin_markup)
    
    try:
        notify_text = (f"🎉 **سفارش شما تایید و آماده شد!**\n\n"
                       f"📦 سرویس: {plan_name}\n"
                       f"📅 تاریخ شروع: {start_jalali}\n"
                       f"📅 تاریخ پایان: {end_jalali} (۳۰ روزه)\n\n"
                       f"📋 جزئیات/کانفیگ:\n\n`{message.text}`\n\n"
                       f"طبق ساب‌لینک خود را طبق آموزش داخل کانال وارد برنامه V2Box کنید.\n"
                       f"با تشکر از خرید شما. 🙏")
        await message.bot.send_message(chat_id=target_user_id, text=notify_text, parse_mode="Markdown")
    except Exception as e:
        print(f"Error notifying user about config delivery: {e}")
async def admin_users_list_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, balance, is_banned FROM users")
    users = cursor.fetchall()
    conn.close()
    
    text = f"👥 **لیست کاربران ربات (مجموعاً {len(users)} نفر):**\n\nID | موجودی | وضعیت\n"
    keyboard = []
    
    for u_id, bal, banned in users[:50]:
        status_icon = "🛑 بن شده" if banned else "🟢 فعال"
        text += f"`{u_id}` | {bal:,} | {status_icon}\n"
        keyboard.append([InlineKeyboardButton(text=f"👤 مدیریت کاربر {u_id}", callback_data=f"manage_user_{u_id}")])
        
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل ادمین", callback_data="admin_home")])
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing users list: {e}")

async def admin_user_management_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    user_id = int(call.data.split("_")[2])
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT balance, is_banned FROM users WHERE user_id = ?', (user_id,))
    user_row = cursor.fetchone()
    balance = user_row[0] if user_row else 0
    is_banned = user_row[1] if user_row else 0
    
    cursor.execute('SELECT id, config_name FROM user_configs WHERE user_id = ?', (user_id,))
    configs = cursor.fetchall()
    conn.close()
    
    text = f"⚙️ **مدیریت کاربر `{user_id}`**\n\n💳 موجودی فعلی: {balance:,} تومان\n📋 وضعیت دسترسی: {'🔴 مسدود (بن)' if is_banned else '🟢 باز'}\n\n📦 **کانفیگ‌های فعال کاربر:**\n"
    cfg_keyboard = []
    for cfg_id, cfg_name in configs:
        text += f"🔹 {cfg_name} (ID: {cfg_id})\n"
        cfg_keyboard.append([InlineKeyboardButton(text=f"🗑 حذف کانفیگ #{cfg_id}", callback_data=f"del_cfg_user_{cfg_id}_{user_id}")])
    
    if not configs:
        text += "هیچ کانفیگ فعالی ندارد.\n"
        
    btn_adjust_balance = InlineKeyboardButton(text="💵 تغییر موجودی کیف پول", callback_data=f"adjust_bal_menu_{user_id}")
    btn_toggle_ban = InlineKeyboardButton(text="🔓 آزاد سازی کاربر" if is_banned else "🚫 مسدود کردن (بن) کاربر", callback_data=f"toggle_ban_{user_id}_{is_banned}")
    btn_back = InlineKeyboardButton(text="🔙 بازگشت به لیست کاربران", callback_data="admin_users_list")
    
    cfg_keyboard.extend([[btn_adjust_balance], [btn_toggle_ban], [btn_back]])
    markup = InlineKeyboardMarkup(inline_keyboard=cfg_keyboard)
    
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        print(f"Error in user management menu: {e}")

async def toggle_ban_action(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    parts = call.data.split("_")
    user_id = int(parts[2])
    current_ban = int(parts[3])
    new_ban = 1 if current_ban == 0 else 0
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET is_banned = ? WHERE user_id = ?', (new_ban, user_id))
    conn.commit()
    conn.close()
    
    await call.answer("وضعیت کاربر تغییر یافت.", show_alert=True)
    call.data = f"manage_user_{user_id}"
    await admin_user_management_handler(call)

async def delete_config_user_action(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    parts = call.data.split("_")
    cfg_id = int(parts[3])
    target_user_id = int(parts[4])
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('DELETE FROM user_configs WHERE id = ?', (cfg_id,))
    conn.commit()
    conn.close()
    
    await call.answer("کانفیگ کاربر حذف شد.", show_alert=True)
    try:
        await call.bot.send_message(chat_id=target_user_id, text="⚠️ یکی از سرویس‌های شما توسط ادمین حذف شد.")
    except:
        pass
    call.data = f"manage_user_{target_user_id}"
    await admin_user_management_handler(call)

async def adjust_balance_menu_handler(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    user_id = int(call.data.split("_")[3])
    await state.update_data(target_adjust_user=user_id)
    await state.set_state(PaymentState.adjusting_balance)
    
    text = f"💵 لطفاً مبلغ جدید موجودی را برای کاربر `{user_id}` به عدد وارد کنید (مثلا 500000):"
    btn_cancel = InlineKeyboardButton(text="انصراف", callback_data=f"manage_user_{user_id}")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_cancel]])
    await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")

async def process_balance_adjustment(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    target_user_id = data.get("target_adjust_user")
    if not target_user_id or not message.text.isdigit():
        await message.answer("مقدار وارد شده نامعتبر است. لطفاً فقط عدد وارد کنید.")
        return
        
    new_balance = int(message.text)
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO users (user_id, balance, is_banned) VALUES (?, ?, COALESCE((SELECT is_banned FROM users WHERE user_id = ?), 0))', (target_user_id, new_balance, target_user_id))
    conn.commit()
    conn.close()
    
    await state.clear()
    admin_markup = get_admin_main_keyboard()
    await message.answer(f"✅ موجودی کاربر `{target_user_id}` با موفقیت به {new_balance:,} تغییر یافت.", reply_markup=admin_markup, parse_mode="Markdown")

async def admin_sales_stats_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM user_configs")
    config_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT plan_name FROM requests WHERE status = 'answered'")
    answered_requests = cursor.fetchall()
    
    plan_prices = {}
    cursor.execute("SELECT name, price FROM services")
    for name, price in cursor.fetchall():
        plan_prices[name] = price
        
    total_revenue = sum(plan_prices.get(p[0], 0) for p in answered_requests)
    
    today_jalali = jdatetime.date.today().strftime('%Y/%m/%d')
    cursor.execute("SELECT plan_name FROM requests WHERE status = 'answered' AND answered_at LIKE ?", (f"{today_jalali}%",))
    today_requests = cursor.fetchall()
    daily_revenue = sum(plan_prices.get(p[0], 0) for p in today_requests)
    
    current_year_month = jdatetime.date.today().strftime('%Y/%m')
    cursor.execute("SELECT plan_name FROM requests WHERE status = 'answered' AND answered_at LIKE ?", (f"{current_year_month}%",))
    monthly_requests = cursor.fetchall()
    monthly_revenue = sum(plan_prices.get(p[0], 0) for p in monthly_requests)
    
    cursor.execute("SELECT COUNT(*) FROM requests WHERE status = 'unanswered'")
    unanswered_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM requests WHERE status != 'unanswered'")
    processed_count = cursor.fetchone()[0]
    
    conn.close()
    
    text = (f"📊 **آمار جامع و وضعیت درخواست‌ها:**\n\n"
            f"👥 کل کاربران عضو شده: {total_users}\n"
            f"📦 کل سرویس‌های فعال: {config_count}\n"
            f"💰 درآمد کل Swan VPN: {total_revenue:,} تومان\n\n"
            f"📅 **آمار ماه جاری ({current_year_month}):**\n"
            f"💵 درآمد ماهانه: {monthly_revenue:,} تومان\n\n"
            f"☀️ **آمار امروز ({today_jalali}):**\n"
            f"🪙 درآمد روزانه: {daily_revenue:,} تومان\n\n"
            f"📝 **وضعیت درخواست‌های خرید:**\n"
            f"🔴 در انتظار پاسخ: {unanswered_count}\n"
            f"🟢 پاسخ داده شده/بررسی شده: {processed_count}")
            
    btn_back = InlineKeyboardButton(text="🔙 بازگشت به پنل ادمین", callback_data="admin_home")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_back]])
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            await call.answer("محتوا تغییری نکرده است.", show_alert=False)
        else:
            raise e

async def admin_broadcast_start_handler(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await state.set_state(PaymentState.broadcasting)
    text = "📢 لطفاً پیام، متن یا رسانه مورد نظر خود را برای ارسال همگانی به تمام کاربران ربات بفرستید:"
    btn_cancel = InlineKeyboardButton(text="انصراف", callback_data="admin_home")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_cancel]])
    await call.message.edit_text(text, reply_markup=markup)

async def process_broadcast_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()
    conn.close()
    
    sent_count = 0
    blocked_count = 0
    
    admin_markup = get_admin_main_keyboard()
    await message.answer("⏳ عملیات ارسال پیام همگانی آغاز شد... لطفاً منتظر بمانید.")

    for u in users:
        u_id = u[0]
        try:
            if message.text:
                await message.bot.send_message(chat_id=u_id, text=message.text)
            elif message.photo:
                await message.bot.send_photo(chat_id=u_id, photo=message.photo[-1].file_id, caption=message.caption or "")
            elif message.video:
                await message.bot.send_video(chat_id=u_id, video=message.video.file_id, caption=message.caption or "")
            sent_count += 1
        except TelegramForbiddenError:
            blocked_count += 1
        except Exception:
            pass
        await asyncio.sleep(0.05)
        
    await message.answer(f"✅ پیام همگانی با موفقیت ارسال شد!\n\n📨 ارسال‌شده به: {sent_count} کاربر\n🚫 مسدود شده توسط کاربر: {blocked_count}", reply_markup=admin_markup)

async def health_check(request):
    return web.Response(text="Bot is running", status=200)

async def run_web_server(app, port):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

async def main():
    init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    
    await update_bot_commands(bot)
    
    dp.message.register(show_main_menu, Command(commands=["start"]))
    dp.message.register(admin_panel_cmd, Command(commands=["admin"]))
    dp.message.register(message_buy_handler, F.text == "🛒 خرید سرویس")
    dp.message.register(my_services_handler, F.text == "📦 سرویس‌های من")
    dp.message.register(tariffs_handler, F.text == "📈 تعرفه سرویس‌ها")
    dp.message.register(wallet_menu_handler, F.text == "💳 شارژ کیف پول")
    dp.message.register(account_handler, F.text == "👤 حساب کاربری")
    dp.message.register(support_handler, F.text == "🎧 پشتیبانی")
    
    dp.callback_query.register(main_menu_handler_call, F.data == "main_menu")
    dp.callback_query.register(main_buy_handler, F.data == "main_buy")
    dp.callback_query.register(category_handler, F.data.startswith("cat_"))
    dp.callback_query.register(buy_plan_handler, F.data.startswith("buy_"))
    dp.callback_query.register(confirm_payment_handler, F.data == "confirm_payment")
    dp.callback_query.register(pay_with_wallet_handler, F.data == "pay_with_wallet")
    dp.message.register(receipt_handler, PaymentState.waiting_for_receipt, F.photo)
    
    dp.callback_query.register(wallet_menu_handler, F.data == "wallet_menu")
    dp.callback_query.register(select_charge_amount_handler, F.data.startswith("charge_"))
    dp.callback_query.register(prompt_charge_receipt_handler, F.data == "send_charge_receipt")
    dp.message.register(process_charge_receipt_handler, PaymentState.waiting_for_charge_receipt, F.photo)
    dp.callback_query.register(confirm_charge_action, F.data.startswith("confirm_charge_"))
    dp.callback_query.register(reject_charge_action, F.data.startswith("reject_charge_"))
    
    dp.callback_query.register(view_config_detail_handler, F.data.startswith("view_cfg_"))
    dp.callback_query.register(my_services_callback_handler, F.data == "my_services_callback")
    dp.callback_query.register(transaction_history_handler, F.data == "tx_history")
    dp.callback_query.register(back_to_account_handler, F.data == "back_to_account")
    
    dp.callback_query.register(admin_home_callback, F.data == "admin_home")
    dp.callback_query.register(admin_unanswered_handler, F.data == "admin_unanswered")
    dp.callback_query.register(admin_answered_handler, F.data == "admin_answered")
    dp.callback_query.register(admin_detail_handler, F.data.startswith("admin_detail_"))
    dp.callback_query.register(admin_confirm_action, F.data.startswith("admin_confirm_"))
    dp.callback_query.register(admin_reject_action, F.data.startswith("admin_reject_"))
    dp.message.register(process_admin_config, PaymentState.waiting_for_admin_config)
    
    dp.callback_query.register(admin_users_list_handler, F.data == "admin_users_list")
    dp.callback_query.register(admin_user_management_handler, F.data.startswith("manage_user_"))
    dp.callback_query.register(toggle_ban_action, F.data.startswith("toggle_ban_"))
    dp.callback_query.register(delete_config_user_action, F.data.startswith("del_cfg_user_"))
    dp.callback_query.register(adjust_balance_menu_handler, F.data.startswith("adjust_bal_menu_"))
    dp.message.register(process_balance_adjustment, PaymentState.adjusting_balance)
    
    dp.callback_query.register(admin_sales_stats_handler, F.data == "admin_sales_stats")
    dp.callback_query.register(admin_broadcast_start_handler, F.data == "admin_broadcast_start")
    dp.message.register(process_broadcast_handler, PaymentState.broadcasting)
    
    app = web.Application()
    app.router.add_get('/', health_check)
    port = int(os.environ.get("PORT", 8080))
    await run_web_server(app, port)
    
    print(f"Bot is running and web server started on port {port}...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
async def admin_users_list_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, balance, is_banned FROM users")
    users = cursor.fetchall()
    conn.close()
    
    text = f"👥 **لیست کاربران ربات (مجموعاً {len(users)} نفر):**\n\nID | موجودی | وضعیت\n"
    keyboard = []
    
    for u_id, bal, banned in users[:50]:
        status_icon = "🛑 بن شده" if banned else "🟢 فعال"
        text += f"`{u_id}` | {bal:,} | {status_icon}\n"
        keyboard.append([InlineKeyboardButton(text=f"👤 مدیریت کاربر {u_id}", callback_data=f"manage_user_{u_id}")])
        
    keyboard.append([InlineKeyboardButton(text="🔙 بازگشت به پنل ادمین", callback_data="admin_home")])
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing users list: {e}")

async def admin_user_management_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    user_id = int(call.data.split("_")[2])
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('SELECT balance, is_banned FROM users WHERE user_id = ?', (user_id,))
    user_row = cursor.fetchone()
    balance = user_row[0] if user_row else 0
    is_banned = user_row[1] if user_row else 0
    
    cursor.execute('SELECT id, config_name FROM user_configs WHERE user_id = ?', (user_id,))
    configs = cursor.fetchall()
    conn.close()
    
    text = f"⚙️ **مدیریت کاربر `{user_id}`**\n\n💳 موجودی فعلی: {balance:,} تومان\n📋 وضعیت دسترسی: {'🔴 مسدود (بن)' if is_banned else '🟢 باز'}\n\n📦 **کانفیگ‌های فعال کاربر:**\n"
    cfg_keyboard = []
    for cfg_id, cfg_name in configs:
        text += f"🔹 {cfg_name} (ID: {cfg_id})\n"
        cfg_keyboard.append([InlineKeyboardButton(text=f"🗑 حذف کانفیگ #{cfg_id}", callback_data=f"del_cfg_user_{cfg_id}_{user_id}")])
    
    if not configs:
        text += "هیچ کانفیگ فعالی ندارد.\n"
        
    btn_adjust_balance = InlineKeyboardButton(text="💵 تغییر موجودی کیف پول", callback_data=f"adjust_bal_menu_{user_id}")
    btn_toggle_ban = InlineKeyboardButton(text="🔓 آزاد سازی کاربر" if is_banned else "🚫 مسدود کردن (بن) کاربر", callback_data=f"toggle_ban_{user_id}_{is_banned}")
    btn_back = InlineKeyboardButton(text="🔙 بازگشت به لیست کاربران", callback_data="admin_users_list")
    
    cfg_keyboard.extend([[btn_adjust_balance], [btn_toggle_ban], [btn_back]])
    markup = InlineKeyboardMarkup(inline_keyboard=cfg_keyboard)
    
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        print(f"Error in user management menu: {e}")

async def toggle_ban_action(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    parts = call.data.split("_")
    user_id = int(parts[2])
    current_ban = int(parts[3])
    new_ban = 1 if current_ban == 0 else 0
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET is_banned = ? WHERE user_id = ?', (new_ban, user_id))
    conn.commit()
    conn.close()
    
    await call.answer("وضعیت کاربر تغییر یافت.", show_alert=True)
    call.data = f"manage_user_{user_id}"
    await admin_user_management_handler(call)

async def delete_config_user_action(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    parts = call.data.split("_")
    cfg_id = int(parts[3])
    target_user_id = int(parts[4])
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('DELETE FROM user_configs WHERE id = ?', (cfg_id,))
    conn.commit()
    conn.close()
    
    await call.answer("کانفیگ کاربر حذف شد.", show_alert=True)
    try:
        await call.bot.send_message(chat_id=target_user_id, text="⚠️ یکی از سرویس‌های شما توسط ادمین حذف شد.")
    except:
        pass
    call.data = f"manage_user_{target_user_id}"
    await admin_user_management_handler(call)

async def adjust_balance_menu_handler(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    user_id = int(call.data.split("_")[3])
    await state.update_data(target_adjust_user=user_id)
    await state.set_state(PaymentState.adjusting_balance)
    
    text = f"💵 لطفاً مبلغ جدید موجودی را برای کاربر `{user_id}` به عدد وارد کنید (مثلا 500000):"
    btn_cancel = InlineKeyboardButton(text="انصراف", callback_data=f"manage_user_{user_id}")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_cancel]])
    await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")

async def process_balance_adjustment(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    target_user_id = data.get("target_adjust_user")
    if not target_user_id or not message.text.isdigit():
        await message.answer("مقدار وارد شده نامعتبر است. لطفاً فقط عدد وارد کنید.")
        return
        
    new_balance = int(message.text)
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO users (user_id, balance, is_banned) VALUES (?, ?, COALESCE((SELECT is_banned FROM users WHERE user_id = ?), 0))', (target_user_id, new_balance, target_user_id))
    conn.commit()
    conn.close()
    
    await state.clear()
    admin_markup = get_admin_main_keyboard()
    await message.answer(f"✅ موجودی کاربر `{target_user_id}` با موفقیت به {new_balance:,} تغییر یافت.", reply_markup=admin_markup, parse_mode="Markdown")
async def admin_sales_stats_handler(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM user_configs")
    config_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT plan_name FROM requests WHERE status = 'answered'")
    answered_requests = cursor.fetchall()
    
    plan_prices = {}
    cursor.execute("SELECT name, price FROM services")
    for name, price in cursor.fetchall():
        plan_prices[name] = price
        
    total_revenue = sum(plan_prices.get(p[0], 0) for p in answered_requests)
    
    today_jalali = jdatetime.date.today().strftime('%Y/%m/%d')
    cursor.execute("SELECT plan_name FROM requests WHERE status = 'answered' AND answered_at LIKE ?", (f"{today_jalali}%",))
    today_requests = cursor.fetchall()
    daily_revenue = sum(plan_prices.get(p[0], 0) for p in today_requests)
    
    current_year_month = jdatetime.date.today().strftime('%Y/%m')
    cursor.execute("SELECT plan_name FROM requests WHERE status = 'answered' AND answered_at LIKE ?", (f"{current_year_month}%",))
    monthly_requests = cursor.fetchall()
    monthly_revenue = sum(plan_prices.get(p[0], 0) for p in monthly_requests)
    
    cursor.execute("SELECT COUNT(*) FROM requests WHERE status = 'unanswered'")
    unanswered_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM requests WHERE status != 'unanswered'")
    processed_count = cursor.fetchone()[0]
    
    conn.close()
    
    text = (f"📊 **آمار جامع و وضعیت درخواست‌ها:**\n\n"
            f"👥 کل کاربران عضو شده: {total_users}\n"
            f"📦 کل سرویس‌های فعال: {config_count}\n"
            f"💰 درآمد کل Swan VPN: {total_revenue:,} تومان\n\n"
            f"📅 **آمار ماه جاری ({current_year_month}):**\n"
            f"💵 درآمد ماهانه: {monthly_revenue:,} تومان\n\n"
            f"☀️ **آمار امروز ({today_jalali}):**\n"
            f"🪙 درآمد روزانه: {daily_revenue:,} تومان\n\n"
            f"📝 **وضعیت درخواست‌های خرید:**\n"
            f"🔴 در انتظار پاسخ: {unanswered_count}\n"
            f"🟢 پاسخ داده شده/بررسی شده: {processed_count}")
            
    btn_back = InlineKeyboardButton(text="🔙 بازگشت به پنل ادمین", callback_data="admin_home")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_back]])
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            await call.answer("محتوا تغییری نکرده است.", show_alert=False)
        else:
            raise e

async def admin_broadcast_start_handler(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await state.set_state(PaymentState.broadcasting)
    text = "📢 لطفاً پیام، متن یا رسانه مورد نظر خود را برای ارسال همگانی به تمام کاربران ربات بفرستید:"
    btn_cancel = InlineKeyboardButton(text="انصراف", callback_data="admin_home")
    markup = InlineKeyboardMarkup(inline_keyboard=[[btn_cancel]])
    await call.message.edit_text(text, reply_markup=markup)

async def process_broadcast_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()
    conn.close()
    
    sent_count = 0
    blocked_count = 0
    
    admin_markup = get_admin_main_keyboard()
    await message.answer("⏳ عملیات ارسال پیام همگانی آغاز شد... لطفاً منتظر بمانید.")

    for u in users:
        u_id = u[0]
        try:
            if message.text:
                await message.bot.send_message(chat_id=u_id, text=message.text)
            elif message.photo:
                await message.bot.send_photo(chat_id=u_id, photo=message.photo[-1].file_id, caption=message.caption or "")
            elif message.video:
                await message.bot.send_video(chat_id=u_id, video=message.video.file_id, caption=message.caption or "")
            sent_count += 1
        except TelegramForbiddenError:
            blocked_count += 1
        except Exception:
            pass
        await asyncio.sleep(0.05)
        
    await message.answer(f"✅ پیام همگانی با موفقیت ارسال شد!\n\n📨 ارسال‌شده به: {sent_count} کاربر\n🚫 مسدود شده توسط کاربر: {blocked_count}", reply_markup=admin_markup)

async def health_check(request):
    return web.Response(text="Bot is running", status=200)

async def run_web_server(app, port):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

async def main():
    init_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    
    await update_bot_commands(bot)
    
    dp.message.register(show_main_menu, Command(commands=["start"]))
    dp.message.register(admin_panel_cmd, Command(commands=["admin"]))
    dp.message.register(message_buy_handler, F.text == "🛒 خرید سرویس")
    dp.message.register(my_services_handler, F.text == "📦 سرویس‌های من")
    dp.message.register(tariffs_handler, F.text == "📈 تعرفه سرویس‌ها")
    dp.message.register(wallet_menu_handler, F.text == "💳 شارژ کیف پول")
    dp.message.register(account_handler, F.text == "👤 حساب کاربری")
    dp.message.register(support_handler, F.text == "🎧 پشتیبانی")
    
    dp.callback_query.register(main_menu_handler_call, F.data == "main_menu")
    dp.callback_query.register(main_buy_handler, F.data == "main_buy")
    dp.callback_query.register(category_handler, F.data.startswith("cat_"))
    dp.callback_query.register(buy_plan_handler, F.data.startswith("buy_"))
    dp.callback_query.register(confirm_payment_handler, F.data == "confirm_payment")
    dp.callback_query.register(pay_with_wallet_handler, F.data == "pay_with_wallet")
    dp.message.register(receipt_handler, PaymentState.waiting_for_receipt, F.photo)
    
    dp.callback_query.register(wallet_menu_handler, F.data == "wallet_menu")
    dp.callback_query.register(select_charge_amount_handler, F.data.startswith("charge_"))
    dp.callback_query.register(prompt_charge_receipt_handler, F.data == "send_charge_receipt")
    dp.message.register(process_charge_receipt_handler, PaymentState.waiting_for_charge_receipt, F.photo)
    dp.callback_query.register(confirm_charge_action, F.data.startswith("confirm_charge_"))
    dp.callback_query.register(reject_charge_action, F.data.startswith("reject_charge_"))
    
    dp.callback_query.register(view_config_detail_handler, F.data.startswith("view_cfg_"))
    dp.callback_query.register(my_services_callback_handler, F.data == "my_services_callback")
    dp.callback_query.register(transaction_history_handler, F.data == "tx_history")
    dp.callback_query.register(back_to_account_handler, F.data == "back_to_account")
    
    dp.callback_query.register(admin_home_callback, F.data == "admin_home")
    dp.callback_query.register(admin_unanswered_handler, F.data == "admin_unanswered")
    dp.callback_query.register(admin_answered_handler, F.data == "admin_answered")
    dp.callback_query.register(admin_detail_handler, F.data.startswith("admin_detail_"))
    dp.callback_query.register(admin_confirm_action, F.data.startswith("admin_confirm_"))
    dp.callback_query.register(admin_reject_action, F.data.startswith("admin_reject_"))
    dp.message.register(process_admin_config, PaymentState.waiting_for_admin_config)
    
    dp.callback_query.register(admin_users_list_handler, F.data == "admin_users_list")
    dp.callback_query.register(admin_user_management_handler, F.data.startswith("manage_user_"))
    dp.callback_query.register(toggle_ban_action, F.data.startswith("toggle_ban_"))
    dp.callback_query.register(delete_config_user_action, F.data.startswith("del_cfg_user_"))
    dp.callback_query.register(adjust_balance_menu_handler, F.data.startswith("adjust_bal_menu_"))
    dp.message.register(process_balance_adjustment, PaymentState.adjusting_balance)
    
    dp.callback_query.register(admin_sales_stats_handler, F.data == "admin_sales_stats")
    dp.callback_query.register(admin_broadcast_start_handler, F.data == "admin_broadcast_start")
    dp.message.register(process_broadcast_handler, PaymentState.broadcasting)
    
    app = web.Application()
    app.router.add_get('/', health_check)
    port = int(os.environ.get("PORT", 8080))
    await run_web_server(app, port)
    
    print(f"Bot is running and web server started on port {port}...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

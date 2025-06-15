import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from telebot import TeleBot, types
from datetime import datetime, timedelta
import json
import re
import time

load_dotenv()

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
# This is a specific channel ID. Ensure the bot has necessary permissions in this channel.
CHANNEL_ID = -1002510470267 # Example channel ID, replace with your actual channel ID if needed
DATABASE_URL = os.getenv('DATABASE_URL')

bot = TeleBot(TOKEN)
logging.basicConfig(level=logging.INFO)

# Dictionary to store temporary user data for multi-step conversations
user_states = {}

# List of allowed admin chat IDs (IMPORTANT: replace with actual admin IDs in production)
ALLOWED_ADMINS = [int(admin_id) for admin_id in os.getenv('TELEGRAM_ADMIN_IDS', '').split(',') if admin_id.strip()]
if not ALLOWED_ADMINS:
    logging.warning("No admin IDs specified in TELEGRAM_ADMIN_IDS environment variable. Admin panel might be inaccessible or insecure.")
    # Fallback for local testing if no env var is set, but should be removed in production
    # ALLOWED_ADMINS = [YOUR_TEST_ADMIN_CHAT_ID] # Uncomment and set your chat ID for local testing without env var

# Expanded list of Ukrainian cities, including many towns from Kyiv Oblast with their associated hashtags
UKRAINIAN_CITIES = {
    'київ': '#Київ',
    'харків': '#Харків',
    'одеса': '#Одеса',
    'дніпро': '#Дніпро',
    'донецьк': '#Донецьк',
    'запоріжжя': '#Запоріжжя',
    'львів': '#Львів',
    'кривий_ріг': '#КривийРіг',
    'миколаїв': '#Миколаїв',
    'маріуполь': '#Маріуполь',
    # Cities and towns of Kyiv Oblast (Київська область)
    'біла_церква': '#БілаЦерква',
    'бровари': '#Бровари',
    'бориспіль': '#Бориспіль',
    'ірпінь': '#Ірпінь',
    'буча': '#Буча',
    'фастів': '#Фастів',
    'обухів': '#Обухів',
    'вишневе': '#Вишневе',
    'переяслав': '#Переяслав',
    'васильків': '#Васильків',
    'вишгород': '#Вишгород',
    'славутич': '#Славутич',
    'яготин': '#Яготин',
    'боярка': '#Боярка',
    'тараща': '#Тараща',
    'українка': '#Українка',
    'сквира': '#Сквира',
    'кагарлик': '#Кагарлик',
    'тетіїв': '#Тетіїв',
    'березань': '#Березань',
    'ржащів': '#Ржищів',
    'чорнобиль': '#Чорнобиль', # Although an exclusion zone, still a known settlement
    'прип\'ять': '#Припять' # Similar to Chernobyl, for completeness
}

def get_db_connection():
    """Establishes and returns a database connection."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn

def init_db():
    """Initializes the database by creating necessary tables and populating initial data."""
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            # Table for logging invite attempts
            cur.execute("""
                CREATE TABLE IF NOT EXISTS invite_logs (
                    id SERIAL PRIMARY KEY,
                    user_chat_id BIGINT NOT NULL,
                    status VARCHAR(10) NOT NULL,
                    error_message TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Table for storing metadata related to invites (e.g., last invite time)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS invite_meta (
                    id SERIAL PRIMARY KEY,
                    last_invite_time TIMESTAMP
                );
            """)

            # Table for storing user information
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id BIGINT PRIMARY KEY,
                    username VARCHAR(100),
                    first_name VARCHAR(100),
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    notifications BOOLEAN DEFAULT TRUE,
                    city VARCHAR(50)
                );
            """)

            # Tables for managing target channels and groups (for general usage, e.g., finding relevant ones)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS target_channels (
                    id SERIAL PRIMARY KEY,
                    channel_name VARCHAR(200) NOT NULL,
                    channel_link VARCHAR(500),
                    channel_type VARCHAR(20) DEFAULT 'channel',
                    description TEXT,
                    city VARCHAR(50),
                    added_by BIGINT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (added_by) REFERENCES users(chat_id)
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS target_groups (
                    id SERIAL PRIMARY KEY,
                    group_name VARCHAR(200) NOT NULL,
                    group_link VARCHAR(500),
                    description TEXT,
                    city VARCHAR(50),
                    added_by BIGINT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (added_by) REFERENCES users(chat_id)
                );
            """)

            # NEW: Table for storing specific channels/groups where bot will post comments/invites
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_target_locations (
                    id SERIAL PRIMARY KEY,
                    location_name VARCHAR(255) NOT NULL,
                    location_id BIGINT UNIQUE NOT NULL, -- Telegram chat_id of the channel/group
                    location_type VARCHAR(10) NOT NULL, -- 'channel' or 'group'
                    comment_message_id INTEGER, -- Foreign key to specific_messages (optional)
                    invite_link TEXT, -- Persistent invite link for the location
                    is_active BOOLEAN DEFAULT TRUE,
                    added_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # NEW: Table for storing specific messages/comments the bot will use
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_comment_templates (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    message_text TEXT NOT NULL,
                    subscription_link TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Table for storing broadcast message templates
            cur.execute("""
                CREATE TABLE IF NOT EXISTS broadcast_templates (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE,
                    title VARCHAR(200),
                    message TEXT NOT NULL,
                    buttons_config TEXT,
                    target_cities TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Table for storing user ratings of broadcast messages
            cur.execute("""
                CREATE TABLE IF NOT EXISTS broadcast_ratings (
                    id SERIAL PRIMARY KEY,
                    user_chat_id BIGINT,
                    template_id INTEGER,
                    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
                    feedback TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (user_chat_id, template_id),
                    FOREIGN KEY (user_chat_id) REFERENCES users(chat_id),
                    FOREIGN KEY (template_id) REFERENCES broadcast_templates(id)
                );
            """)

            # Table for storing city hashtags
            cur.execute("""
                CREATE TABLE IF NOT EXISTS city_hashtags (
                    id SERIAL PRIMARY KEY,
                    city_name VARCHAR(50) UNIQUE NOT NULL,
                    hashtag VARCHAR(50) NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE
                );
            """)

            # Add Ukrainian cities and their hashtags to the city_hashtags table
            # This loop will now insert the expanded list of cities/towns
            for city, hashtag in UKRAINIAN_CITIES.items():
                cur.execute("""
                    INSERT INTO city_hashtags (city_name, hashtag)
                    VALUES (%s, %s) ON CONFLICT (city_name) DO NOTHING;
                """, (city, hashtag))

            # Check and add basic invite_meta data if it doesn't exist
            cur.execute("SELECT COUNT(*) FROM invite_meta;")
            count = cur.fetchone()['count']
            if count == 0:
                cur.execute("INSERT INTO invite_meta (last_invite_time) VALUES (NULL);")
    conn.close()

# ============ KEYBOARDS ============

def get_main_menu():
    """Returns the main menu inline keyboard."""
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("📝 Реєстрація", callback_data="register"),
        types.InlineKeyboardButton("🔗 Отримати інвайт", callback_data="get_invite")
    )
    keyboard.add(
        types.InlineKeyboardButton("📺 Додати канал", callback_data="add_channel"),
        types.InlineKeyboardButton("👥 Додати групу", callback_data="add_group")
    )
    keyboard.add(
        types.InlineKeyboardButton("🏙️ Мої міста", callback_data="my_cities"),
        types.InlineKeyboardButton("📊 Статистика", callback_data="stats")
    )
    keyboard.add(
        types.InlineKeyboardButton("⚙️ Налаштування", callback_data="settings"),
        types.InlineKeyboardButton("❓ Допомога", callback_data="help")
    )
    return keyboard

def get_cities_keyboard():
    """Returns an inline keyboard with Ukrainian cities for selection."""
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    cities_list = sorted(list(UKRAINIAN_CITIES.keys())) # Sort for consistent display

    for i in range(0, len(cities_list), 2):
        row_buttons = []
        for j in range(2):
            if i + j < len(cities_list):
                city = cities_list[i + j]
                city_display = city.replace('_', ' ').title()
                row_buttons.append(
                    types.InlineKeyboardButton(city_display, callback_data=f"select_city_{city}")
                )
        if row_buttons:
            keyboard.row(*row_buttons)

    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="main_menu"))
    return keyboard

def get_channel_management_menu():
    """Returns the channel management menu inline keyboard."""
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("📺 Мої канали", callback_data="my_channels"),
        types.InlineKeyboardButton("👥 Мої групи", callback_data="my_groups")
    )
    keyboard.add(
        types.InlineKeyboardButton("🏙️ За містами", callback_data="channels_by_city"),
        types.InlineKeyboardButton("📈 Статистика", callback_data="channels_stats")
    )
    keyboard.add(
        types.InlineKeyboardButton("🔙 Назад", callback_data="main_menu")
    )
    return keyboard

def get_rating_keyboard(template_id):
    """Returns an inline keyboard for rating a broadcast message."""
    keyboard = types.InlineKeyboardMarkup(row_width=5)
    rating_buttons = []
    for i in range(1, 6):
        rating_buttons.append(
            types.InlineKeyboardButton(f"{i}⭐", callback_data=f"rate_{template_id}_{i}")
        )
    keyboard.row(*rating_buttons)
    keyboard.add(types.InlineKeyboardButton("Пропустити", callback_data="skip_rating"))
    return keyboard

def get_admin_menu():
    """Returns the admin panel inline keyboard."""
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("📤 Розсилка", callback_data="admin_broadcast"),
        types.InlineKeyboardButton("👥 Користувачі", callback_data="admin_users")
    )
    keyboard.add(
        types.InlineKeyboardButton("📺 Канали", callback_data="admin_channels"),
        types.InlineKeyboardButton("🏙️ Міста", callback_data="admin_cities")
    )
    keyboard.add(
        types.InlineKeyboardButton("📈 Рейтинги", callback_data="admin_ratings"),
        types.InlineKeyboardButton("⚙️ Бот активність", callback_data="admin_bot_activity") # NEW
    )
    keyboard.add(
        types.InlineKeyboardButton("🔙 Головне меню", callback_data="main_menu")
    )
    return keyboard

def get_admin_broadcast_menu():
    """Returns the admin broadcast management menu."""
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("➕ Створити розсилку", callback_data="admin_broadcast_create_start"),
        types.InlineKeyboardButton("📄 Список розсилок", callback_data="admin_broadcast_list")
    )
    keyboard.add(
        types.InlineKeyboardButton("✉️ Надіслати розсилку", callback_data="admin_broadcast_send_select"),
        types.InlineKeyboardButton("✏️ Редагувати розсилку", callback_data="admin_broadcast_edit_select")
    )
    keyboard.add(
        types.InlineKeyboardButton("🗑️ Видалити розсилку", callback_data="admin_broadcast_delete_select"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="admin_menu")
    )
    return keyboard

def get_admin_edit_delete_broadcast_keyboard(template_id):
    """Returns a keyboard for editing/deleting a specific broadcast."""
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("✏️ Редагувати", callback_data=f"admin_broadcast_edit_{template_id}"),
        types.InlineKeyboardButton("🗑️ Видалити", callback_data=f"admin_broadcast_delete_{template_id}")
    )
    keyboard.add(types.InlineKeyboardButton("🔙 До списку", callback_data="admin_broadcast_list"))
    return keyboard

def get_user_channel_group_management_keyboard(item_id, item_type):
    """Returns a keyboard for deleting a specific channel or group."""
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("🗑️ Видалити", callback_data=f"delete_{item_type}_{item_id}"),
        types.InlineKeyboardButton("🔙 Назад", callback_data=f"my_{item_type}s") # Return to list
    )
    return keyboard

def get_user_settings_menu(notifications_enabled):
    """Returns the user settings menu."""
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    status_text = "✅ Увімкнені" if notifications_enabled else "❌ Вимкнені"
    keyboard.add(
        types.InlineKeyboardButton(f"Сповіщення: {status_text}", callback_data="toggle_notifications")
    )
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="main_menu"))
    return keyboard

# NEW: Bot activity management keyboards
def get_admin_bot_activity_menu():
    """Returns the admin menu for bot activity (commenting/inviting)."""
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        types.InlineKeyboardButton("➕ Додати цільове місце", callback_data="admin_add_bot_target_location_start"),
        types.InlineKeyboardButton("📄 Мої цільові місця", callback_data="admin_list_bot_target_locations"),
        types.InlineKeyboardButton("➕ Створити повідомлення", callback_data="admin_create_comment_template_start"),
        types.InlineKeyboardButton("✉️ Мої повідомлення", callback_data="admin_list_comment_templates"),
        types.InlineKeyboardButton("🚀 Запустити активність", callback_data="admin_run_bot_activity_start"),
        types.InlineKeyboardButton("📈 Статистика активності", callback_data="admin_bot_activity_stats"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="admin_menu")
    )
    return keyboard

def get_bot_target_location_manage_keyboard(location_id):
    """Keyboard to manage a specific bot target location."""
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("✏️ Редагувати", callback_data=f"admin_edit_bot_target_location_{location_id}"),
        types.InlineKeyboardButton("🗑️ Видалити", callback_data=f"admin_delete_bot_target_location_confirm_{location_id}")
    )
    keyboard.add(types.InlineKeyboardButton("🔙 До списку", callback_data="admin_list_bot_target_locations"))
    return keyboard

def get_bot_comment_template_manage_keyboard(template_id):
    """Keyboard to manage a specific bot comment template."""
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("✏️ Редагувати", callback_data=f"admin_edit_comment_template_{template_id}"),
        types.InlineKeyboardButton("🗑️ Видалити", callback_data=f"admin_delete_comment_template_confirm_{template_id}")
    )
    keyboard.add(types.InlineKeyboardButton("🔙 До списку", callback_data="admin_list_comment_templates"))
    return keyboard

# ============ MAIN COMMANDS ============

@bot.message_handler(commands=['start'])
def start_message(message):
    """Handles the /start command, welcoming the user and showing the main menu."""
    user_info = message.from_user
    welcome_text = f"Привіт, {user_info.first_name}! 👋\n\n" \
                   "Я бот для роботи з каналами та групами України.\n" \
                   "Можу допомогти:\n" \
                   "• Додавати канали та групи по містах\n" \
                   "• Розсилати запрошення сегментовано\n" \
                   "• Знаходити цільову аудиторію з хештегами\n\n" \
                   "Оберіть дію з меню:"

    bot.send_message(message.chat.id, welcome_text, reply_markup=get_main_menu())

@bot.message_handler(commands=['admin'])
def admin_panel(message):
    """Handles the /admin command, showing the admin panel if the user is authorized."""
    admin_chat_id = message.chat.id
    if admin_chat_id not in ALLOWED_ADMINS:
        bot.send_message(admin_chat_id, "❌ У вас немає прав доступу до адмін-панелі.")
        return

    bot.send_message(admin_chat_id, "🔧 Панель адміністратора", reply_markup=get_admin_menu())

# ============ CALLBACK HANDLERS ============

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    """Handles all inline keyboard callback queries."""
    chat_id = call.message.chat.id

    # ALWAYS answer the callback query immediately to avoid "query too old" errors
    # This prevents the button from showing "loading" indefinitely
    bot.answer_callback_query(call.id)

    try:
        if call.data == "main_menu":
            bot.edit_message_text("Головне меню:", chat_id, call.message.message_id,
                                  reply_markup=get_main_menu())

        elif call.data == "register":
            handle_registration_start(call)

        elif call.data == "get_invite":
            send_invite_link(chat_id)

        elif call.data == "add_channel":
            handle_add_channel_start(call)

        elif call.data == "add_group":
            handle_add_group_start(call)

        elif call.data == "my_cities":
            show_cities_selection(call)

        elif call.data.startswith("select_city_"):
            handle_city_selection(call)

        elif call.data.startswith("rate_"):
            handle_rating(call)

        elif call.data.startswith("admin_"):
            handle_admin_actions(call)

        elif call.data == "my_channels":
            show_my_channels(call)

        elif call.data == "my_groups":
            show_my_groups(call)

        elif call.data.startswith("delete_channel_"):
            delete_user_channel(call)

        elif call.data.startswith("delete_group_"):
            delete_user_group(call)

        elif call.data == "settings":
            user_settings(call)

        elif call.data == "toggle_notifications":
            toggle_notifications(call)

        elif call.data == "channels_by_city":
            bot.send_message(chat_id, "Функція 'Канали за містами' ще не реалізована для звичайних користувачів, але ви можете переглянути свої додані канали та групи.")

        elif call.data == "channels_stats":
            bot.send_message(chat_id, "Функція 'Статистика каналів' доступна тільки адміністраторам.")

        elif call.data == "stats":
            show_overall_stats(call)

        elif call.data == "help":
            bot.send_message(chat_id, "Допомога ще не реалізована. Зверніться до адміністратора.")

        elif call.data == "skip_rating":
            bot.edit_message_text("Добре, ви пропустили оцінку.", chat_id, call.message.message_id,
                                  reply_markup=get_main_menu())
            return

    except Exception as e:
        logging.error(f"Помилка в callback_handler: {e}")
        bot.send_message(chat_id, "Сталася помилка під час обробки вашого запиту. Спробуйте ще раз або зверніться до адміністратора.")


# ============ REGISTRATION WITH CITY SELECTION ============

def handle_registration_start(call):
    """Starts the registration process by prompting the user to select a city."""
    chat_id = call.message.chat.id

    text = "📝 Реєстрація\n\n" \
           "Спочатку оберіть ваше місто для таргетованих розсилок:"

    bot.edit_message_text(text, chat_id, call.message.message_id,
                          reply_markup=get_cities_keyboard())

def show_cities_selection(call):
    """Displays the city selection keyboard."""
    text = "🏙️ Оберіть місто для налаштування таргетованих розсилок:"
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                          reply_markup=get_cities_keyboard())

def handle_city_selection(call):
    """Handles the user's city selection during registration or city update."""
    chat_id = call.message.chat.id
    city_key = call.data.replace("select_city_", "")
    city_name = city_key.replace('_', ' ').title()
    hashtag = UKRAINIAN_CITIES.get(city_key, f"#{city_name}")

    user_info = call.from_user

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (chat_id, username, first_name, city)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chat_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    city = EXCLUDED.city;
                """, (chat_id, user_info.username, user_info.first_name, city_key))

        conn.close()

        bot.edit_message_text(
            f"✅ Вітаємо в {city_name}! {hashtag}\n\n"
            f"Тепер ви будете отримувати таргетовані розсилки для вашого міста.\n"
            f"Ви також можете додавати канали та групи для {city_name}.",
            chat_id, call.message.message_id,
            reply_markup=get_main_menu()
        )

    except Exception as e:
        logging.error(f"Помилка при реєстрації користувача: {e}")
        bot.send_message(chat_id, "Сталася помилка при реєстрації. Спробуйте ще раз.")

# ============ ADDING CHANNELS / GROUPS ============

def handle_add_channel_start(call):
    """Starts the process of adding a new channel."""
    chat_id = call.message.chat.id

    bot.edit_message_text(
        "📺 Додавання каналу\n\n"
        "Введіть назву каналу (без @):",
        chat_id, call.message.message_id
    )

    if chat_id not in user_states:
        user_states[chat_id] = {}
    user_states[chat_id]['waiting_for'] = 'channel_name'

def handle_add_group_start(call):
    """Starts the process of adding a new group."""
    chat_id = call.message.chat.id

    bot.edit_message_text(
        "👥 Додавання групи\n\n"
        "Введіть назву групи (без @):",
        chat_id, call.message.message_id
    )

    if chat_id not in user_states:
        user_states[chat_id] = {}
    user_states[chat_id]['waiting_for'] = 'group_name'

@bot.message_handler(func=lambda message: message.chat.id in user_states and 'waiting_for' in user_states[message.chat.id])
def handle_user_input(message):
    """Handles user input during multi-step processes like adding channels/groups."""
    chat_id = message.chat.id
    input_type = user_states[chat_id]['waiting_for']
    user_input = message.text.strip()

    # Admin broadcast input handler
    if input_type.startswith('admin_broadcast_'):
        handle_admin_broadcast_input(message, user_input, input_type)
        return
    # NEW: Admin bot activity input handler
    elif input_type.startswith('admin_bot_target_location_') or input_type.startswith('admin_comment_template_'):
        handle_admin_bot_activity_input(message, user_input, input_type)
        return

    if input_type == 'channel_name':
        handle_channel_name_input(message, user_input)
    elif input_type == 'group_name':
        handle_group_name_input(message, user_input)
    elif input_type == 'channel_link':
        complete_channel_addition(message, user_input)
    elif input_type == 'group_link':
        complete_group_addition(message, user_input)
    else:
        # Clear state if an unexpected input type is encountered
        if chat_id in user_states:
            del user_states[chat_id]
        bot.send_message(chat_id, "Неочікуване введення. Будь ласка, спробуйте знову з головного меню.", reply_markup=get_main_menu())


def handle_channel_name_input(message, channel_name):
    """Processes the channel name input from the user."""
    chat_id = message.chat.id

    # Clean the name by removing special characters
    clean_name = re.sub(r'[^a-zA-Z0-9_а-яА-ЯіІїЇєЄ]', '', channel_name)

    if not clean_name:
        bot.send_message(chat_id, "❌ Некоректна назва каналу. Спробуйте ще раз:")
        return

    user_states[chat_id]['channel_name'] = clean_name
    user_states[chat_id]['waiting_for'] = 'channel_link'

    bot.send_message(
        chat_id,
        f"📺 Канал: @{clean_name}\n\n"
        "Тепер введіть посилання на канал (https://t.me/...):"
    )

def handle_group_name_input(message, group_name):
    """Processes the group name input from the user."""
    chat_id = message.chat.id

    # Clean the name by removing special characters
    clean_name = re.sub(r'[^a-zA-Z0-9_а-яА-ЯіІїЇєЄ]', '', group_name)

    if not clean_name:
        bot.send_message(chat_id, "❌ Некоректна назва групи. Спробуйте ще раз:")
        return

    user_states[chat_id]['group_name'] = clean_name
    user_states[chat_id]['waiting_for'] = 'group_link'

    bot.send_message(
        chat_id,
        f"👥 Група: @{clean_name}\n\n"
        "Тепер введіть посилання на групу (https://t.me/...):"
    )

def complete_channel_addition(message, channel_link):
    """Completes the channel addition process, saving data to the database."""
    chat_id = message.chat.id

    if not channel_link.startswith('https://t.me/'):
        bot.send_message(chat_id, "❌ Посилання має починатися з https://t.me/\nСпробуйте ще раз:")
        return

    channel_name = user_states[chat_id].get('channel_name')
    if not channel_name:
        bot.send_message(chat_id, "Назва каналу не знайдена. Будь ласка, почніть знову.", reply_markup=get_main_menu())
        del user_states[chat_id]
        return

    # Get the user's city
    user_city = get_user_city(chat_id)

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO target_channels (channel_name, channel_link, city, added_by)
                    VALUES (%s, %s, %s, %s);
                """, (channel_name, channel_link, user_city, chat_id))

        conn.close()

        # Clear the user's state
        del user_states[chat_id]

        city_hashtag = UKRAINIAN_CITIES.get(user_city, f"#{user_city.replace('_', ' ').title()}")

        bot.send_message(
            chat_id,
            f"✅ Канал успішно додано!\n\n"
            f"📺 @{channel_name}\n"
            f"🏙️ Місто: {user_city.replace('_', ' ').title()} {city_hashtag}\n"
            f"🔗 {channel_link}",
            reply_markup=get_main_menu()
        )

    except Exception as e:
        logging.error(f"Помилка при додаванні каналу: {e}")
        bot.send_message(chat_id, "❌ Сталася помилка при додаванні каналу.")
        if chat_id in user_states:
            del user_states[chat_id]

def complete_group_addition(message, group_link):
    """Completes the group addition process, saving data to the database."""
    chat_id = message.chat.id

    if not group_link.startswith('https://t.me/'):
        bot.send_message(chat_id, "❌ Посилання має починатися з https://t.me/\nСпробуйте ще раз:")
        return

    group_name = user_states[chat_id].get('group_name')
    if not group_name:
        bot.send_message(chat_id, "Назва групи не знайдена. Будь ласка, почніть знову.", reply_markup=get_main_menu())
        del user_states[chat_id]
        return

    # Get the user's city
    user_city = get_user_city(chat_id)

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO target_groups (group_name, group_link, city, added_by)
                    VALUES (%s, %s, %s, %s);
                """, (group_name, group_link, user_city, chat_id))

        conn.close()

        # Clear the user's state
        del user_states[chat_id]

        city_hashtag = UKRAINIAN_CITIES.get(user_city, f"#{user_city.replace('_', ' ').title()}")

        bot.send_message(
            chat_id,
            f"✅ Група успішно додана!\n\n"
            f"👥 @{group_name}\n"
            f"🏙️ Місто: {user_city.replace('_', ' ').title()} {city_hashtag}\n"
            f"🔗 {group_link}",
            reply_markup=get_main_menu()
        )

    except Exception as e:
        logging.error(f"Помилка при додаванні групи: {e}")
        bot.send_message(chat_id, "❌ Сталася помилка при додаванні групи.")
        if chat_id in user_states:
            del user_states[chat_id]

# ============ RATING SYSTEM ============

def handle_rating(call):
    """Handles user rating of a broadcast message."""
    chat_id = call.message.chat.id
    parts = call.data.split('_')
    template_id = int(parts[1])
    rating = int(parts[2])

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO broadcast_ratings (user_chat_id, template_id, rating)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_chat_id, template_id) DO UPDATE SET rating = EXCLUDED.rating;
                """, (chat_id, template_id, rating))

        conn.close()

        bot.edit_message_text(
            f"✅ Дякуємо за оцінку: {rating}⭐\n\n"
            "Ваша думка допоможе нам покращити якість розсилок!",
            chat_id, call.message.message_id,
            reply_markup=get_main_menu()
        )

    except Exception as e:
        logging.error(f"Помилка при збереженні рейтингу: {e}")
        bot.send_message(chat_id, "Помилка при збереженні оцінки.")

# ============ SEGMENTED BROADCAST ============

def send_broadcast_by_city(message_text, target_cities=None, template_id=None, is_test=False, chat_id_for_test=None):
    """
    Sends a broadcast message to users, optionally filtered by city,
    and includes a rating button if a template_id is provided.
    If is_test is True, sends only to chat_id_for_test.
    """
    if is_test and chat_id_for_test:
        users = [{'chat_id': chat_id_for_test, 'city': 'тестове'}] # Mock city for test
    else:
        conn = get_db_connection()
        users = []
        try:
            with conn:
                with conn.cursor() as cur:
                    if target_cities:
                        # Ensure target_cities is a tuple or list for IN clause
                        target_cities_tuple = tuple(c.strip().lower() for c in target_cities if c.strip())
                        if target_cities_tuple:
                            placeholders = ','.join(['%s'] * len(target_cities_tuple))
                            cur.execute(f"""
                                SELECT chat_id, city FROM users
                                WHERE is_active = TRUE AND notifications = TRUE
                                AND city IN ({placeholders});
                            """, target_cities_tuple)
                        else:
                            # If target_cities is provided but empty after stripping, send to no one.
                            users = []
                    else:
                        cur.execute("""
                            SELECT chat_id, city FROM users
                            WHERE is_active = TRUE AND notifications = TRUE;
                        """)
                    users = cur.fetchall()
        except Exception as e:
            logging.error(f"Помилка при отриманні користувачів для розсилки: {e}")
        finally:
            if conn:
                conn.close()

    success_count = 0
    for user in users:
        try:
            chat_id = user['chat_id']
            user_city = user['city'] if 'city' in user else 'не вказано' # Handle potential missing city

            # Use generic hashtag for test messages or if city is not found
            city_hashtag = UKRAINIAN_CITIES.get(user_city, f"#{user_city.replace('_', ' ').title()}")

            # Adding city hashtag to the message
            full_message = f"{message_text}\n\n🏙️ {city_hashtag}"

            # Add rating button if template_id is provided and not a test broadcast
            keyboard = None
            if template_id and not is_test:
                keyboard = get_rating_keyboard(template_id)

            bot.send_message(chat_id, full_message, reply_markup=keyboard)
            success_count += 1

        except Exception as e:
            logging.error(f"Помилка відправки повідомлення {chat_id}: {e}")

    return success_count

# ============ HELPER FUNCTIONS ============

def get_user_city(chat_id):
    """Retrieves the city associated with a user's chat ID."""
    conn = get_db_connection()
    result = None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT city FROM users WHERE chat_id = %s;", (chat_id,))
                result = cur.fetchone()
    except Exception as e:
        logging.error(f"Error fetching user city for {chat_id}: {e}")
    finally:
        if conn:
            conn.close()
    return result['city'] if result and result['city'] else 'київ' # Default to 'київ' if city is None

def get_user_notifications_status(chat_id):
    """Retrieves the notification status for a user."""
    conn = get_db_connection()
    status = True # Default to enabled if not found
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT notifications FROM users WHERE chat_id = %s;", (chat_id,))
                result = cur.fetchone()
                if result:
                    status = result['notifications']
    except Exception as e:
        logging.error(f"Error fetching user notification status for {chat_id}: {e}")
    finally:
        if conn:
            conn.close()
    return status

def update_user_notifications_status(chat_id, status):
    """Updates the notification status for a user."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users SET notifications = %s WHERE chat_id = %s;
                """, (status, chat_id))
    except Exception as e:
        logging.error(f"Error updating user notification status for {chat_id}: {e}")
    finally:
        if conn:
            conn.close()

def get_channels_by_user(chat_id):
    """Retrieves active channels added by a specific user."""
    conn = get_db_connection()
    channels = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, channel_name, channel_link, city FROM target_channels
                    WHERE added_by = %s AND is_active = TRUE
                    ORDER BY created_at DESC;
                """, (chat_id,))
                channels = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching channels by user {chat_id}: {e}")
    finally:
        if conn:
            conn.close()
    return channels

def get_groups_by_user(chat_id):
    """Retrieves active groups added by a specific user."""
    conn = get_db_connection()
    groups = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, group_name, group_link, city FROM target_groups
                    WHERE added_by = %s AND is_active = TRUE
                    ORDER BY created_at DESC;
                """, (chat_id,))
                groups = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching groups by user {chat_id}: {e}")
    finally:
        if conn:
            conn.close()
    return groups

def delete_channel_by_id(channel_id, user_id):
    """Deletes a channel if it was added by the specified user."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM target_channels WHERE id = %s AND added_by = %s;
                """, (channel_id, user_id))
                return cur.rowcount > 0 # Return True if a row was deleted
    except Exception as e:
        logging.error(f"Error deleting channel {channel_id} by user {user_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_group_by_id(group_id, user_id):
    """Deletes a group if it was added by the specified user."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM target_groups WHERE id = %s AND added_by = %s;
                """, (group_id, user_id))
                return cur.rowcount > 0 # Return True if a row was deleted
    except Exception as e:
        logging.error(f"Error deleting group {group_id} by user {user_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_broadcast_templates():
    """Retrieves all broadcast templates."""
    conn = get_db_connection()
    templates = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, title, message, target_cities FROM broadcast_templates ORDER BY created_at DESC;")
                templates = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching broadcast templates: {e}")
    finally:
        if conn:
            conn.close()
    return templates

def get_broadcast_template(template_id):
    """Retrieves a single broadcast template by ID."""
    conn = get_db_connection()
    template = None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, title, message, target_cities FROM broadcast_templates WHERE id = %s;", (template_id,))
                template = cur.fetchone()
    except Exception as e:
        logging.error(f"Error fetching broadcast template {template_id}: {e}")
    finally:
        if conn:
            conn.close()
    return template

def add_broadcast_template(name, title, message, target_cities):
    """Adds a new broadcast template to the database."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO broadcast_templates (name, title, message, target_cities)
                    VALUES (%s, %s, %s, %s);
                """, (name, title, message, target_cities))
                return True
    except Exception as e:
        logging.error(f"Error adding broadcast template: {e}")
        return False
    finally:
        if conn:
            conn.close()

def update_broadcast_template(template_id, name, title, message, target_cities):
    """Updates an existing broadcast template."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE broadcast_templates
                    SET name = %s, title = %s, message = %s, target_cities = %s
                    WHERE id = %s;
                """, (name, title, message, target_cities, template_id))
                return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error updating broadcast template {template_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_broadcast_template_db(template_id):
    """Deletes a broadcast template and its associated ratings."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Delete associated ratings first due to foreign key constraint
                cur.execute("DELETE FROM broadcast_ratings WHERE template_id = %s;", (template_id,))
                cur.execute("DELETE FROM broadcast_templates WHERE id = %s;", (template_id,))
                return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error deleting broadcast template {template_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def send_invite_link(chat_id):
    """Sends instructions on how to get an invite link to the user."""
    # To get an invite link for a private channel (CHANNEL_ID),
    # an admin must first generate an invite link in Telegram,
    # then you can use that link here.
    # Example: https://t.me/+AbCdEfGhIjKlMnOp
    invite_text = (
        "🔗 Щоб отримати посилання-запрошення, будь ласка, зверніться до адміністратора каналу. "
        "Або, якщо ви адміністратор, ви можете створити посилання-запрошення вручну в налаштуваннях каналу "
        f"(Channel ID: {CHANNEL_ID}) і поділитися ним тут."
    )
    bot.send_message(chat_id, invite_text)


# ============ ADMIN FUNCTIONS ============

def handle_admin_actions(call):
    """Routes admin actions based on callback data."""
    chat_id = call.message.chat.id

    # Check if the user is an authorized admin
    if chat_id not in ALLOWED_ADMINS:
        bot.send_message(chat_id, "❌ У вас немає прав доступу до цієї функції.")
        return

    action = call.data.replace("admin_", "")

    if action == "broadcast":
        handle_admin_broadcast_menu(call)
    elif action == "broadcast_create_start":
        admin_create_broadcast_start(call)
    elif action == "broadcast_list":
        admin_list_broadcasts(call)
    elif action == "broadcast_send_select":
        admin_send_broadcast_select_template(call)
    elif action.startswith("broadcast_send_"):
        template_id = int(action.split('_')[2])
        admin_confirm_send_broadcast(call, template_id)
    elif action == "broadcast_edit_select":
        admin_edit_broadcast_select_template(call)
    elif action.startswith("broadcast_edit_"):
        template_id = int(action.split('_')[2])
        admin_edit_broadcast_start(call, template_id)
    elif action == "broadcast_delete_select":
        admin_delete_broadcast_select_template(call)
    elif action.startswith("broadcast_delete_confirm_"):
        template_id = int(action.split('_')[3])
        admin_delete_broadcast(call, template_id)
    elif action.startswith("broadcast_test_"):
        template_id = int(action.split('_')[2])
        admin_send_test_broadcast(call, template_id)
    elif action == "users":
        show_users_stats_by_city(call)
    elif action == "channels":
        show_channels_stats(call)
    elif action == "ratings":
        show_ratings_stats(call)
    elif action == "cities":
        show_city_hashtags(call)
    # NEW Admin Bot Activity actions
    elif action == "bot_activity":
        handle_admin_bot_activity_menu(call)
    elif action == "add_bot_target_location_start":
        admin_add_bot_target_location_start(call)
    elif action == "list_bot_target_locations":
        admin_list_bot_target_locations(call)
    elif action.startswith("edit_bot_target_location_"):
        location_id = int(action.split('_')[4])
        admin_edit_bot_target_location_start(call, location_id)
    elif action.startswith("delete_bot_target_location_confirm_"):
        location_id = int(action.split('_')[4])
        admin_delete_bot_target_location(call, location_id)
    elif action == "create_comment_template_start":
        admin_create_comment_template_start(call)
    elif action == "list_comment_templates":
        admin_list_comment_templates(call)
    elif action.startswith("edit_comment_template_"):
        template_id = int(action.split('_')[3])
        admin_edit_comment_template_start(call, template_id)
    elif action.startswith("delete_comment_template_confirm_"):
        template_id = int(action.split('_')[3])
        admin_delete_comment_template(call, template_id)
    elif action == "run_bot_activity_start":
        admin_run_bot_activity_select_target(call)
    elif action.startswith("run_bot_activity_execute_"):
        location_id = int(action.split('_')[4])
        template_id = int(action.split('_')[5])
        admin_execute_bot_activity(call, location_id, template_id)
    elif action == "bot_activity_stats":
        show_bot_activity_stats(call)
    elif action == "settings":
        bot.send_message(chat_id, "Налаштування адмін-панелі ще не реалізовані.")
    elif action == "menu":
        bot.edit_message_text("🔧 Панель адміністратора", chat_id, call.message.message_id, reply_markup=get_admin_menu())

def handle_admin_broadcast_menu(call):
    """Admin menu for broadcast management."""
    bot.edit_message_text(
        "📤 Меню управління розсилками:",
        call.message.chat.id, call.message.message_id,
        reply_markup=get_admin_broadcast_menu()
    )

def admin_create_broadcast_start(call):
    """Starts the process of creating a new broadcast template."""
    chat_id = call.message.chat.id
    user_states[chat_id] = {'waiting_for': 'admin_broadcast_create_name'}
    bot.edit_message_text(
        "➕ Створення нової розсилки.\n\n"
        "Введіть унікальну *назву* для розсилки (для внутрішнього використання, наприклад, 'Акція_Весна_2025'):",
        chat_id, call.message.message_id, parse_mode='Markdown'
    )

def admin_list_broadcasts(call):
    """Displays a list of all broadcast templates."""
    chat_id = call.message.chat.id
    templates = get_broadcast_templates()
    if not templates:
        bot.edit_message_text("📄 Немає збережених розсилок.", chat_id, call.message.message_id, reply_markup=get_admin_broadcast_menu())
        return

    message_text = "📄 Ваші розсилки:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for tpl in templates:
        message_text += f"ID: `{tpl['id']}`\n" \
                        f"Назва: *{tpl['name']}*\n" \
                        f"Заголовок: _{tpl['title']}_\n" \
                        f"Цільові міста: {tpl['target_cities'] if tpl['target_cities'] else 'Всі'}\n\n"
        keyboard.add(types.InlineKeyboardButton(f"✉️ Надіслати: {tpl['name']}", callback_data=f"admin_broadcast_send_{tpl['id']}"))
        keyboard.add(types.InlineKeyboardButton(f"✏️/🗑️ Керувати: {tpl['name']}", callback_data=f"admin_broadcast_manage_{tpl['id']}"))


    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_broadcast"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id,
                          reply_markup=keyboard, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_broadcast_manage_"))
def admin_manage_broadcast_details(call):
    """Shows options to edit/delete a specific broadcast from the list."""
    template_id = int(call.data.replace("admin_broadcast_manage_", ""))
    template = get_broadcast_template(template_id)
    if not template:
        bot.send_message(call.message.chat.id, "Розсилку не знайдено.")
        admin_list_broadcasts(call)
        return

    message_text = f"Керування розсилкою *{template['name']}* (ID: `{template['id']}`)\n\n" \
                   f"Заголовок: _{template['title']}_\n" \
                   f"Повідомлення:\n_{template['message'][:100]}..._\n" \
                   f"Цільові міста: {template['target_cities'] if template['target_cities'] else 'Всі'}"
    
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("✏️ Редагувати", callback_data=f"admin_broadcast_edit_{template_id}"),
        types.InlineKeyboardButton("🗑️ Видалити", callback_data=f"admin_broadcast_delete_confirm_{template_id}")
    )
    keyboard.add(
        types.InlineKeyboardButton("✉️ Надіслати", callback_data=f"admin_broadcast_send_{template_id}"),
        types.InlineKeyboardButton("🧪 Тестова розсилка", callback_data=f"admin_broadcast_test_{template_id}")
    )
    keyboard.add(types.InlineKeyboardButton("🔙 До списку", callback_data="admin_broadcast_list"))

    bot.edit_message_text(message_text, call.message.chat.id, call.message.message_id,
                          reply_markup=keyboard, parse_mode='Markdown')


def admin_send_broadcast_select_template(call):
    """Lists templates for sending a broadcast."""
    chat_id = call.message.chat.id
    templates = get_broadcast_templates()
    if not templates:
        bot.edit_message_text("✉️ Немає розсилок для відправки. Спочатку створіть нову.", chat_id, call.message.message_id, reply_markup=get_admin_broadcast_menu())
        return

    message_text = "✉️ Оберіть розсилку для надсилання:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for tpl in templates:
        keyboard.add(types.InlineKeyboardButton(f"{tpl['name']} ({tpl['id']})", callback_data=f"admin_broadcast_send_{tpl['id']}"))
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_broadcast"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id, reply_markup=keyboard)

def admin_confirm_send_broadcast(call, template_id):
    """Confirms sending a broadcast."""
    chat_id = call.message.chat.id
    template = get_broadcast_template(template_id)
    if not template:
        bot.send_message(chat_id, "Розсилку не знайдено.")
        admin_send_broadcast_select_template(call)
        return

    message_text = f"Ви збираєтеся надіслати розсилку:\n\n" \
                   f"Назва: *{template['name']}*\n" \
                   f"Заголовок: _{template['title']}_\n" \
                   f"Повідомлення:\n_{template['message'][:100]}..._\n" \
                   f"Цільові міста: {template['target_cities'] if template['target_cities'] else 'Всі'}\n\n" \
                   "Ви впевнені?"

    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("✅ Так, надіслати", callback_data=f"admin_broadcast_execute_send_{template_id}"),
        types.InlineKeyboardButton("❌ Скасувати", callback_data="admin_broadcast")
    )
    bot.edit_message_text(message_text, chat_id, call.message.message_id, reply_markup=keyboard, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_broadcast_execute_send_"))
def admin_execute_send_broadcast(call):
    """Executes sending of the broadcast."""
    chat_id = call.message.chat.id
    template_id = int(call.data.replace("admin_broadcast_execute_send_", ""))
    template = get_broadcast_template(template_id)

    if not template:
        bot.send_message(chat_id, "Розсилку не знайдено.")
        admin_send_broadcast_select_template(call)
        return

    bot.edit_message_text(f"✉️ Починаю надсилання розсилки '{template['name']}'...", chat_id, call.message.message_id)

    target_cities = template['target_cities']
    if target_cities:
        # Convert comma-separated string to a list of cities
        target_cities_list = [city.strip().lower() for city in target_cities.split(',') if city.strip()]
    else:
        target_cities_list = None # Send to all if no cities specified

    sent_count = send_broadcast_by_city(template['message'], target_cities=target_cities_list, template_id=template['id'])
    bot.send_message(chat_id, f"✅ Розсилку '{template['name']}' надіслано *{sent_count}* користувачам.", parse_mode='Markdown', reply_markup=get_admin_broadcast_menu())


def admin_send_test_broadcast(call, template_id):
    """Sends a test broadcast to the admin."""
    chat_id = call.message.chat.id
    template = get_broadcast_template(template_id)
    if not template:
        bot.send_message(chat_id, "Розсилку не знайдено.")
        admin_list_broadcasts(call)
        return

    bot.send_message(chat_id, "🧪 Надсилаю тестову розсилку...")
    sent_count = send_broadcast_by_city(
        f"TEST: {template['message']}",
        is_test=True,
        chat_id_for_test=chat_id,
        template_id=template['id'] # Still include template_id for rating test
    )
    bot.send_message(chat_id, f"Тестова розсилка надіслана. Кількість: {sent_count}", reply_markup=get_admin_broadcast_menu())


def admin_edit_broadcast_select_template(call):
    """Lists templates for editing."""
    chat_id = call.message.chat.id
    templates = get_broadcast_templates()
    if not templates:
        bot.edit_message_text("✏️ Немає розсилок для редагування.", chat_id, call.message.message_id, reply_markup=get_admin_broadcast_menu())
        return

    message_text = "✏️ Оберіть розсилку для редагування:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for tpl in templates:
        keyboard.add(types.InlineKeyboardButton(f"{tpl['name']} (ID: {tpl['id']})", callback_data=f"admin_broadcast_edit_{tpl['id']}"))
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_broadcast"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id, reply_markup=keyboard)


def admin_edit_broadcast_start(call, template_id):
    """Starts the process of editing an existing broadcast template."""
    chat_id = call.message.chat.id
    template = get_broadcast_template(template_id)
    if not template:
        bot.send_message(chat_id, "Розсилку не знайдено.")
        admin_edit_broadcast_select_template(call)
        return

    user_states[chat_id] = {
        'waiting_for': 'admin_broadcast_edit_name',
        'template_id': template_id,
        'original_data': template.copy() # Store original data for step-by-step update
    }
    bot.edit_message_text(
        f"✏️ Редагування розсилки *{template['name']}* (ID: `{template_id}`).\n\n"
        f"Введіть нову *назву* (поточна: '{template['name']}'):",
        chat_id, call.message.message_id, parse_mode='Markdown'
    )


def admin_delete_broadcast_select_template(call):
    """Lists templates for deletion."""
    chat_id = call.message.chat.id
    templates = get_broadcast_templates()
    if not templates:
        bot.edit_message_text("🗑️ Немає розсилок для видалення.", chat_id, call.message.message_id, reply_markup=get_admin_broadcast_menu())
        return

    message_text = "🗑️ Оберіть розсилку для видалення:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for tpl in templates:
        keyboard.add(types.InlineKeyboardButton(f"{tpl['name']} (ID: {tpl['id']})", callback_data=f"admin_broadcast_delete_confirm_{tpl['id']}"))
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_broadcast"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id, reply_markup=keyboard)


def admin_delete_broadcast(call, template_id):
    """Deletes a broadcast template after confirmation."""
    chat_id = call.message.chat.id
    template = get_broadcast_template(template_id)
    if not template:
        bot.send_message(chat_id, "Розсилку не знайдено.")
        admin_delete_broadcast_select_template(call)
        return

    success = delete_broadcast_template_db(template_id)
    if success:
        bot.edit_message_text(f"✅ Розсилку '{template['name']}' (ID: `{template_id}`) успішно видалено.", chat_id, call.message.message_id, parse_mode='Markdown', reply_markup=get_admin_broadcast_menu())
    else:
        bot.edit_message_text(f"❌ Помилка при видаленні розсилки '{template['name']}' (ID: `{template_id}`).", chat_id, call.message.message_id, parse_mode='Markdown', reply_markup=get_admin_broadcast_menu())


def handle_admin_broadcast_input(message, user_input, input_type):
    """Handles multi-step input for admin broadcast creation/editing."""
    chat_id = message.chat.id
    state = user_states.get(chat_id)

    if not state or not input_type.startswith('admin_broadcast_'):
        bot.send_message(chat_id, "Неочікуване введення. Будь ласка, почніть знову з адмін-панелі.", reply_markup=get_admin_menu())
        if chat_id in user_states:
            del user_states[chat_id]
        return

    action_type = state['waiting_for']
    template_id = state.get('template_id')
    current_data = state.get('current_data', {})
    original_data = state.get('original_data', {}) # For editing, to pre-fill

    if action_type == 'admin_broadcast_create_name' or action_type == 'admin_broadcast_edit_name':
        current_data['name'] = user_input
        user_states[chat_id]['waiting_for'] = 'admin_broadcast_create_title' if template_id is None else 'admin_broadcast_edit_title'
        user_states[chat_id]['current_data'] = current_data
        bot.send_message(
            chat_id,
            f"Введіть *заголовок* розсилки (поточний: '{original_data.get('title', '') if template_id else ''}'):",
            parse_mode='Markdown'
        )
    elif action_type == 'admin_broadcast_create_title' or action_type == 'admin_broadcast_edit_title':
        current_data['title'] = user_input
        user_states[chat_id]['waiting_for'] = 'admin_broadcast_create_message' if template_id is None else 'admin_broadcast_edit_message'
        user_states[chat_id]['current_data'] = current_data
        bot.send_message(
            chat_id,
            f"Введіть *текст повідомлення* розсилки (поточний: '{original_data.get('message', '') if template_id else ''}'):",
            parse_mode='Markdown'
        )
    elif action_type == 'admin_broadcast_create_message' or action_type == 'admin_broadcast_edit_message':
        current_data['message'] = user_input
        user_states[chat_id]['waiting_for'] = 'admin_broadcast_create_cities' if template_id is None else 'admin_broadcast_edit_cities'
        user_states[chat_id]['current_data'] = current_data
        bot.send_message(
            chat_id,
            f"Введіть *цільові міста* через кому (наприклад, 'київ, харків', або залиште порожнім для всіх міст). Поточні: '{original_data.get('target_cities', '') if template_id else ''}':",
            parse_mode='Markdown'
        )
    elif action_type == 'admin_broadcast_create_cities' or action_type == 'admin_broadcast_edit_cities':
        current_data['target_cities'] = user_input if user_input else None # Store None if empty
        name = current_data.get('name')
        title = current_data.get('title')
        message_text = current_data.get('message')
        target_cities = current_data.get('target_cities')

        if template_id is None: # Create new broadcast
            success = add_broadcast_template(name, title, message_text, target_cities)
            if success:
                bot.send_message(chat_id, "✅ Розсилку успішно створено!", reply_markup=get_admin_broadcast_menu())
            else:
                bot.send_message(chat_id, "❌ Помилка при створенні розсилки. Можливо, назва вже існує.", reply_markup=get_admin_broadcast_menu())
        else: # Edit existing broadcast
            success = update_broadcast_template(template_id, name, title, message_text, target_cities)
            if success:
                bot.send_message(chat_id, "✅ Розсилку успішно оновлено!", reply_markup=get_admin_broadcast_menu())
            else:
                bot.send_message(chat_id, "❌ Помилка при оновленні розсилки.", reply_markup=get_admin_broadcast_menu())

        if chat_id in user_states:
            del user_states[chat_id]
    else:
        bot.send_message(chat_id, "Неочікуваний стан введення для адмін-розсилки.", reply_markup=get_admin_broadcast_menu())
        if chat_id in user_states:
            del user_states[chat_id]


def show_users_stats_by_city(call):
    """Displays user statistics categorized by city."""
    conn = get_db_connection()
    city_stats = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        city,
                        COUNT(*) as user_count
                    FROM users
                    WHERE is_active = TRUE
                    GROUP BY city
                    ORDER BY user_count DESC;
                """)
                city_stats = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching user stats by city: {e}")
    finally:
        if conn:
            conn.close()

    stats_text = "👥 Статистика користувачів по містах:\n\n"
    total_users = 0

    for stat in city_stats:
        city_name = stat['city'].replace('_', ' ').title() if stat['city'] else 'Не вказано'
        city_hashtag = UKRAINIAN_CITIES.get(stat['city'], '')
        user_count = stat['user_count']
        total_users += user_count

        stats_text += f"🏙️ {city_name} {city_hashtag}: {user_count} користувачів\n"

    stats_text += f"\n📊 Загалом: {total_users} користувачів"

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_menu"))

    bot.edit_message_text(
        stats_text,
        call.message.chat.id, call.message.message_id,
        reply_markup=keyboard
    )

def show_channels_stats(call):
    """Displays statistics about added channels and groups."""
    conn = get_db_connection()
    channel_counts = None
    group_counts = None
    try:
        with conn:
            with conn.cursor() as cur:
                # Get channel counts by city
                cur.execute("""
                    SELECT
                        city,
                        COUNT(*) as count
                    FROM target_channels
                    WHERE is_active = TRUE
                    GROUP BY city
                    ORDER BY count DESC;
                """)
                channel_counts = cur.fetchall()

                # Get group counts by city
                cur.execute("""
                    SELECT
                        city,
                        COUNT(*) as count
                    FROM target_groups
                    WHERE is_active = TRUE
                    GROUP BY city
                    ORDER BY count DESC;
                """)
                group_counts = cur.fetchall()

    except Exception as e:
        logging.error(f"Error fetching channels/groups stats: {e}")
    finally:
        if conn:
            conn.close()

    stats_text = "📊 Статистика каналів та груп:\n\n"

    total_channels = sum(c['count'] for c in channel_counts) if channel_counts else 0
    total_groups = sum(g['count'] for g in group_counts) if group_counts else 0

    stats_text += "📺 Канали по містах:\n"
    if channel_counts:
        for stat in channel_counts:
            city_name = stat['city'].replace('_', ' ').title() if stat['city'] else 'Не вказано'
            stats_text += f"  {city_name}: {stat['count']} каналів\n"
    else:
        stats_text += "  Немає доданих каналів.\n"
    stats_text += f"Всього каналів: {total_channels}\n\n"

    stats_text += "👥 Групи по містах:\n"
    if group_counts:
        for stat in group_counts:
            city_name = stat['city'].replace('_', ' ').title() if stat['city'] else 'Не вказано'
            stats_text += f"  {city_name}: {stat['count']} груп\n"
    else:
        stats_text += "  Немає доданих груп.\n"
    stats_text += f"Всього груп: {total_groups}\n"


    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_menu"))

    bot.edit_message_text(
        stats_text,
        call.message.chat.id, call.message.message_id,
        reply_markup=keyboard
    )


def show_ratings_stats(call):
    """Displays statistics of broadcast ratings."""
    conn = get_db_connection()
    rating_stats = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        bt.name,
                        COUNT(br.rating) as total_ratings,
                        AVG(br.rating) as avg_rating,
                        COUNT(CASE WHEN br.rating >= 4 THEN 1 END) as positive_ratings
                    FROM broadcast_templates bt
                    LEFT JOIN broadcast_ratings br ON bt.id = br.template_id
                    GROUP BY bt.id, bt.name
                    ORDER BY avg_rating DESC NULLS LAST;
                """)
                rating_stats = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching rating stats: {e}")
    finally:
        if conn:
            conn.close()

    stats_text = "⭐ Рейтинги розсилок:\n\n"

    if not rating_stats:
        stats_text += "Немає даних про рейтинги розсилок."
    else:
        for stat in rating_stats:
            name = stat['name']
            total = stat['total_ratings'] or 0
            avg_rating = round(stat['avg_rating'], 1) if stat['avg_rating'] is not None else "N/A"
            positive = stat['positive_ratings'] or 0

            stats_text += f"📝 {name}\n"
            stats_text += f"    📊 Оцінок: {total}\n"
            stats_text += f"    ⭐ Середній рейтинг: {avg_rating}/5\n"
            stats_text += f"    👍 Позитивних: {positive}\n\n"

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_menu"))

    bot.edit_message_text(
        stats_text,
        call.message.chat.id, call.message.message_id,
        reply_markup=keyboard
    )

def show_overall_stats(call):
    """Displays overall statistics, combining user and channel/group stats for now."""
    conn = get_db_connection()
    total_users = 0
    total_active_users = 0
    total_channels = 0
    total_groups = 0

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users;")
                total_users = cur.fetchone()['count']

                cur.execute("SELECT COUNT(*) FROM users WHERE is_active = TRUE;")
                total_active_users = cur.fetchone()['count']

                cur.execute("SELECT COUNT(*) FROM target_channels WHERE is_active = TRUE;")
                total_channels = cur.fetchone()['count']

                cur.execute("SELECT COUNT(*) FROM target_groups WHERE is_active = TRUE;")
                total_groups = cur.fetchone()['count']

    except Exception as e:
        logging.error(f"Error fetching overall stats: {e}")
    finally:
        if conn:
            conn.close()

    stats_text = "📊 Загальна статистика:\n\n" \
                 f"👥 Користувачі: {total_users} (активні: {total_active_users})\n" \
                 f"📺 Каналів додано: {total_channels}\n" \
                 f"👥 Груп додано: {total_groups}\n\n" \
                 "Для детальнішої статистики дивіться відповідні розділи в адмін-панелі."

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="main_menu"))
    bot.edit_message_text(stats_text, call.message.chat.id, call.message.message_id, reply_markup=keyboard)


def show_city_hashtags(call):
    """Admin function to show city hashtags."""
    hashtags = sorted(UKRAINIAN_CITIES.items()) # Get sorted items from the dictionary
    stats_text = "🏙️ Хештеги міст:\n\n"

    if not hashtags:
        stats_text += "Немає визначених хештегів міст."
    else:
        for city, hashtag in hashtags:
            stats_text += f"*{city.replace('_', ' ').title()}*: `{hashtag}`\n"

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_menu"))

    bot.edit_message_text(
        stats_text,
        call.message.chat.id, call.message.message_id,
        reply_markup=keyboard, parse_mode='Markdown'
    )


# ============ USER CHANNELS/GROUPS MANAGEMENT ============

def show_my_channels(call):
    """Displays channels added by the current user."""
    chat_id = call.message.chat.id
    channels = get_channels_by_user(chat_id)
    if not channels:
        bot.edit_message_text("📺 Ви ще не додали жодного каналу.", chat_id, call.message.message_id, reply_markup=get_channel_management_menu())
        return

    message_text = "📺 Ваші додані канали:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for channel in channels:
        city_display = channel['city'].replace('_', ' ').title()
        message_text += f"*{channel['channel_name']}*\n" \
                        f"Посилання: {channel['channel_link']}\n" \
                        f"Місто: {city_display}\n" \
                        f"ID: `{channel['id']}`\n\n"
        keyboard.add(types.InlineKeyboardButton(f"🗑️ Видалити {channel['channel_name']}", callback_data=f"delete_channel_{channel['id']}"))

    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="main_menu"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id, reply_markup=keyboard, parse_mode='Markdown', disable_web_page_preview=True)


def delete_user_channel(call):
    """Deletes a channel added by the user."""
    chat_id = call.message.chat.id
    channel_id = int(call.data.replace("delete_channel_", ""))

    success = delete_channel_by_id(channel_id, chat_id)
    if success:
        bot.send_message(chat_id, "Канал успішно видалено.")
    else:
        bot.send_message(chat_id, "Не вдалося видалити канал. Можливо, він не ваш або вже видалений.")
    show_my_channels(call) # Refresh the list


def show_my_groups(call):
    """Displays groups added by the current user."""
    chat_id = call.message.chat.id
    groups = get_groups_by_user(chat_id)
    if not groups:
        bot.edit_message_text("👥 Ви ще не додали жодної групи.", chat_id, call.message.message_id, reply_markup=get_channel_management_menu())
        return

    message_text = "👥 Ваші додані групи:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for group in groups:
        city_display = group['city'].replace('_', ' ').title()
        message_text += f"*{group['group_name']}*\n" \
                        f"Посилання: {group['group_link']}\n" \
                        f"Місто: {city_display}\n" \
                        f"ID: `{group['id']}`\n\n"
        keyboard.add(types.InlineKeyboardButton(f"🗑️ Видалити {group['group_name']}", callback_data=f"delete_group_{group['id']}"))

    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="main_menu"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id, reply_markup=keyboard, parse_mode='Markdown', disable_web_page_preview=True)


def delete_user_group(call):
    """Deletes a group added by the user."""
    chat_id = call.message.chat.id
    group_id = int(call.data.replace("delete_group_", ""))

    success = delete_group_by_id(group_id, chat_id)
    if success:
        bot.send_message(chat_id, "Групу успішно видалено.")
    else:
        bot.send_message(chat_id, "Не вдалося видалити групу. Можливо, вона не ваша або вже видалена.")
    show_my_groups(call) # Refresh the list

# ============ USER SETTINGS ============

def user_settings(call):
    """Displays user settings menu."""
    chat_id = call.message.chat.id
    notifications_enabled = get_user_notifications_status(chat_id)
    bot.edit_message_text(
        "⚙️ Ваші налаштування:\n\n"
        "Тут ви можете керувати параметрами сповіщень.",
        chat_id, call.message.message_id,
        reply_markup=get_user_settings_menu(notifications_enabled)
    )

def toggle_notifications(call):
    """Toggles user's notification preference."""
    chat_id = call.message.chat.id
    current_status = get_user_notifications_status(chat_id)
    new_status = not current_status
    update_user_notifications_status(chat_id, new_status)

    if new_status:
        bot.send_message(chat_id, "Сповіщення увімкнено.")
    else:
        bot.send_message(chat_id, "Сповіщення вимкнено.")

    user_settings(call) # Refresh settings menu

# ============ NEW BOT ACTIVITY FUNCTIONS (ADMIN ONLY) ============

def handle_admin_bot_activity_menu(call):
    """Admin menu for bot commenting/inviting activity."""
    bot.edit_message_text(
        "⚙️ Меню управління активністю бота (коментування/запрошення):",
        call.message.chat.id, call.message.message_id,
        reply_markup=get_admin_bot_activity_menu()
    )

# --- Bot Target Locations ---

def admin_add_bot_target_location_start(call):
    """Starts adding a new bot target location."""
    chat_id = call.message.chat.id
    user_states[chat_id] = {'waiting_for': 'admin_bot_target_location_name'}
    bot.edit_message_text(
        "➕ Додавання нового цільового місця для бота.\n\n"
        "Введіть *назву* каналу/групи (для ідентифікації, наприклад, 'Група Київ Продаж'):",
        chat_id, call.message.message_id, parse_mode='Markdown'
    )

def admin_list_bot_target_locations(call):
    """Displays a list of all bot target locations."""
    chat_id = call.message.chat.id
    locations = get_bot_target_locations()
    if not locations:
        bot.edit_message_text("📄 Немає доданих цільових місць для бота.", chat_id, call.message.message_id, reply_markup=get_admin_bot_activity_menu())
        return

    message_text = "📄 Цільові місця для активності бота:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for loc in locations:
        message_text += f"ID: `{loc['id']}`\n" \
                        f"Назва: *{loc['location_name']}*\n" \
                        f"ID чату: `{loc['location_id']}`\n" \
                        f"Тип: {loc['location_type'].capitalize()}\n" \
                        f"Посилання: {loc['invite_link'] if loc['invite_link'] else 'Немає'}\n\n"
        keyboard.add(types.InlineKeyboardButton(f"⚙️ Керувати: {loc['location_name']}", callback_data=f"admin_edit_bot_target_location_{loc['id']}"))

    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_bot_activity"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id,
                          reply_markup=keyboard, parse_mode='Markdown', disable_web_page_preview=True)

def admin_edit_bot_target_location_start(call, location_id):
    """Starts editing an existing bot target location."""
    chat_id = call.message.chat.id
    location = get_bot_target_location(location_id)
    if not location:
        bot.send_message(chat_id, "Цільове місце не знайдено.")
        admin_list_bot_target_locations(call)
        return

    user_states[chat_id] = {
        'waiting_for': 'admin_bot_target_location_edit_name',
        'location_id': location_id,
        'original_data': location.copy()
    }
    bot.edit_message_text(
        f"✏️ Редагування цільового місця *{location['location_name']}* (ID: `{location_id}`).\n\n"
        f"Введіть нову *назву* (поточна: '{location['location_name']}'):",
        chat_id, call.message.message_id, parse_mode='Markdown'
    )

def admin_delete_bot_target_location(call, location_id):
    """Deletes a bot target location after confirmation."""
    chat_id = call.message.chat.id
    location = get_bot_target_location(location_id)
    if not location:
        bot.send_message(chat_id, "Цільове місце не знайдено.")
        admin_list_bot_target_locations(call)
        return

    success = delete_bot_target_location_db(location_id)
    if success:
        bot.edit_message_text(f"✅ Цільове місце '{location['location_name']}' (ID: `{location_id}`) успішно видалено.", chat_id, call.message.message_id, parse_mode='Markdown', reply_markup=get_admin_bot_activity_menu())
    else:
        bot.edit_message_text(f"❌ Помилка при видаленні цільового місця '{location['location_name']}' (ID: `{location_id}`).", chat_id, call.message.message_id, parse_mode='Markdown', reply_markup=get_admin_bot_activity_menu())

# --- Comment Templates ---

def admin_create_comment_template_start(call):
    """Starts creating a new bot comment template."""
    chat_id = call.message.chat.id
    user_states[chat_id] = {'waiting_for': 'admin_comment_template_create_name'}
    bot.edit_message_text(
        "➕ Створення нового повідомлення для коментування.\n\n"
        "Введіть унікальну *назву* для шаблону (для внутрішнього використання, наприклад, 'Запрошення_Канал1'):",
        chat_id, call.message.message_id, parse_mode='Markdown'
    )

def admin_list_comment_templates(call):
    """Displays a list of all bot comment templates."""
    chat_id = call.message.chat.id
    templates = get_bot_comment_templates()
    if not templates:
        bot.edit_message_text("📄 Немає збережених повідомлень для коментування.", chat_id, call.message.message_id, reply_markup=get_admin_bot_activity_menu())
        return

    message_text = "📄 Ваші повідомлення для коментування:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for tpl in templates:
        message_text += f"ID: `{tpl['id']}`\n" \
                        f"Назва: *{tpl['name']}*\n" \
                        f"Текст:\n_{tpl['message_text'][:100]}..._\n" \
                        f"Посилання: {tpl['subscription_link'] if tpl['subscription_link'] else 'Немає'}\n\n"
        keyboard.add(types.InlineKeyboardButton(f"⚙️ Керувати: {tpl['name']}", callback_data=f"admin_edit_comment_template_{tpl['id']}"))

    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_bot_activity"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id,
                          reply_markup=keyboard, parse_mode='Markdown', disable_web_page_preview=True)

def admin_edit_comment_template_start(call, template_id):
    """Starts editing an existing bot comment template."""
    chat_id = call.message.chat.id
    template = get_bot_comment_template(template_id)
    if not template:
        bot.send_message(chat_id, "Повідомлення не знайдено.")
        admin_list_comment_templates(call)
        return

    user_states[chat_id] = {
        'waiting_for': 'admin_comment_template_edit_name',
        'template_id': template_id,
        'original_data': template.copy()
    }
    bot.edit_message_text(
        f"✏️ Редагування повідомлення *{template['name']}* (ID: `{template_id}`).\n\n"
        f"Введіть нову *назву* (поточна: '{template['name']}'):",
        chat_id, call.message.message_id, parse_mode='Markdown'
    )

def admin_delete_comment_template(call, template_id):
    """Deletes a bot comment template after confirmation."""
    chat_id = call.message.chat.id
    template = get_bot_comment_template(template_id)
    if not template:
        bot.send_message(chat_id, "Повідомлення не знайдено.")
        admin_list_comment_templates(call)
        return

    success = delete_bot_comment_template_db(template_id)
    if success:
        bot.edit_message_text(f"✅ Повідомлення '{template['name']}' (ID: `{template_id}`) успішно видалено.", chat_id, call.message.message_id, parse_mode='Markdown', reply_markup=get_admin_bot_activity_menu())
    else:
        bot.edit_message_text(f"❌ Помилка при видаленні повідомлення '{template['name']}' (ID: `{template_id}`).", chat_id, call.message.message_id, parse_mode='Markdown', reply_markup=get_admin_bot_activity_menu())


# --- Bot Activity Execution ---

def admin_run_bot_activity_select_target(call):
    """Admin selects a target location and a message template to run bot activity."""
    chat_id = call.message.chat.id
    locations = get_bot_target_locations()
    templates = get_bot_comment_templates()

    if not locations or not templates:
        msg = "Для запуску активності потрібно:\n"
        if not locations: msg += "  - Додати хоча б одне цільове місце (канал/групу).\n"
        if not templates: msg += "  - Створити хоча б одне повідомлення для коментування.\n"
        bot.edit_message_text(msg, chat_id, call.message.message_id, reply_markup=get_admin_bot_activity_menu())
        return

    message_text = "🚀 Оберіть цільове місце та повідомлення для активності бота:\n\n"
    keyboard = types.InlineKeyboardMarkup(row_width=1)

    for loc in locations:
        for tpl in templates:
            button_text = f"👉 {loc['location_name']} ({loc['location_type'].capitalize()}) з повідомленням '{tpl['name']}'"
            callback_data = f"admin_run_bot_activity_execute_{loc['id']}_{tpl['id']}"
            keyboard.add(types.InlineKeyboardButton(button_text, callback_data=callback_data))
    
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_bot_activity"))
    bot.edit_message_text(message_text, chat_id, call.message.message_id, reply_markup=keyboard)


def admin_execute_bot_activity(call, location_id, template_id):
    """Executes the bot activity (commenting/inviting) in the selected location with the selected message."""
    chat_id = call.message.chat.id
    
    location = get_bot_target_location(location_id)
    template = get_bot_comment_template(template_id)

    if not location or not template:
        bot.send_message(chat_id, "Цільове місце або повідомлення не знайдено.")
        admin_run_bot_activity_select_target(call)
        return

    bot.edit_message_text(
        f"🚀 Запускаю активність в *{location['location_name']}* з повідомленням *'{template['name']}'*...\n\n"
        f"Тип: {location['location_type'].capitalize()}\n"
        f"Текст повідомлення:\n_{template['message_text'][:100]}..._\n"
        f"Посилання для підписки: {template['subscription_link'] or 'Немає'}",
        chat_id, call.message.message_id, parse_mode='Markdown'
    )

    full_message = f"{template['message_text']}\n\n{template['subscription_link']}" if template['subscription_link'] else template['message_text']

    try:
        # Simulate sending a message to the target channel/group
        # In a real scenario, this would use bot.send_message(chat_id=location['location_id'], text=full_message)
        # or bot.send_message(chat_id=location['location_id'], text=full_message, reply_to_message_id=...) for comments
        # For inviting, it's more complex (e.g., bot.add_chat_members or using invite links)

        # For demonstration, we'll just log and confirm
        logging.info(f"Спроба відправки повідомлення у {location['location_name']} ({location['location_id']}): '{full_message}'")
        
        # This is a placeholder for actual Telegram API calls.
        # You need to ensure your bot has the necessary permissions (admin, can post messages, etc.)
        # If it's a channel, the bot sends a message. If it's a group, the bot sends a message.
        # For "inviting people", direct adding is problematic. It's usually about sharing invite link.

        # Example for sending message:
        bot.send_message(location['location_id'], full_message, disable_web_page_preview=True)
        # If posting a comment to a channel, it usually involves replying to a channel post.
        # This requires getting the message ID of the channel post.
        # bot.send_message(chat_id=channel_id, text=comment_text, reply_to_message_id=channel_post_id)

        # Update bot_target_locations with the last comment message ID if applicable
        # (This is more complex and depends on whether you're posting a new message or replying)
        
        bot.send_message(chat_id, f"✅ Активність успішно запущено у *{location['location_name']}*! Повідомлення відправлено.", parse_mode='Markdown', reply_markup=get_admin_bot_activity_menu())

    except Exception as e:
        logging.error(f"Помилка при запуску активності в {location['location_name']}: {e}")
        bot.send_message(chat_id, f"❌ Помилка при запуску активності у *{location['location_name']}*: {e}. Переконайтеся, що бот є адміністратором у цій групі/каналі та має необхідні дозволи.", parse_mode='Markdown', reply_markup=get_admin_bot_activity_menu())
    
# --- Statistics ---
def show_bot_activity_stats(call):
    """Displays statistics related to bot's commenting/inviting activity."""
    conn = get_db_connection()
    location_stats = []
    template_stats = []
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Stats for target locations
                cur.execute("""
                    SELECT
                        location_name,
                        location_type,
                        COUNT(*) as total_entries
                    FROM bot_target_locations
                    GROUP BY location_name, location_type
                    ORDER BY total_entries DESC;
                """)
                location_stats = cur.fetchall()

                # Stats for comment templates
                cur.execute("""
                    SELECT
                        name,
                        COUNT(*) as total_uses -- Placeholder, needs actual logging of sends
                    FROM bot_comment_templates
                    GROUP BY name
                    ORDER BY name;
                """)
                template_stats = cur.fetchall()

    except Exception as e:
        logging.error(f"Error fetching bot activity stats: {e}")
    finally:
        if conn:
            conn.close()

    stats_text = "📈 Статистика активності бота:\n\n"

    stats_text += "📍 Цільові місця:\n"
    if location_stats:
        for stat in location_stats:
            stats_text += f"  *{stat['location_name']}* ({stat['location_type'].capitalize()}): {stat['total_entries']} запис(ів)\n"
    else:
        stats_text += "  Немає доданих цільових місць.\n"
    
    stats_text += "\n✉️ Шаблони повідомлень:\n"
    if template_stats:
        for stat in template_stats:
            stats_text += f"  *{stat['name']}*: {stat['total_uses']} використання(ь) (потрібно додати логування)\n"
    else:
        stats_text += "  Немає створених шаблонів повідомлень.\n"

    stats_text += "\n*Примітка*: Детальна статистика використання (скільки разів відправлено повідомлення, успішність) вимагає додаткового логування."

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_bot_activity"))

    bot.edit_message_text(
        stats_text,
        call.message.chat.id, call.message.message_id,
        reply_markup=keyboard, parse_mode='Markdown'
    )


def handle_admin_bot_activity_input(message, user_input, input_type):
    """Handles multi-step input for admin bot activity creation/editing."""
    chat_id = message.chat.id
    state = user_states.get(chat_id)

    if not state:
        bot.send_message(chat_id, "Неочікуване введення. Будь ласка, почніть знову з адмін-панелі.", reply_markup=get_admin_menu())
        if chat_id in user_states:
            del user_states[chat_id]
        return

    # --- Bot Target Location Input ---
    if input_type == 'admin_bot_target_location_name' or input_type == 'admin_bot_target_location_edit_name':
        state['current_data'] = state.get('current_data', {})
        state['current_data']['location_name'] = user_input
        state['waiting_for'] = 'admin_bot_target_location_id' if 'location_id' not in state else 'admin_bot_target_location_edit_id'
        
        prompt_text = "Введіть *ID чату* (каналу/групи). Це *числове ID*, яке можна отримати, наприклад, через @getidsbot. " \
                      "Для каналів це зазвичай від'ємне число (наприклад, -1001234567890).\n"
        if 'original_data' in state and state['original_data'].get('location_id'):
            prompt_text += f"(Поточний ID: `{state['original_data']['location_id']}`)"
        bot.send_message(chat_id, prompt_text, parse_mode='Markdown')

    elif input_type == 'admin_bot_target_location_id' or input_type == 'admin_bot_target_location_edit_id':
        try:
            location_id = int(user_input)
            state['current_data']['location_id'] = location_id
            state['waiting_for'] = 'admin_bot_target_location_type' if 'location_id' not in state else 'admin_bot_target_location_edit_type'

            prompt_text = "Введіть *тип місця* ('channel' для каналу або 'group' для групи):\n"
            if 'original_data' in state and state['original_data'].get('location_type'):
                prompt_text += f"(Поточний тип: '{state['original_data']['location_type']}')"
            bot.send_message(chat_id, prompt_text, parse_mode='Markdown')
        except ValueError:
            bot.send_message(chat_id, "❌ Некоректний ID чату. Будь ласка, введіть числове ID.")

    elif input_type == 'admin_bot_target_location_type' or input_type == 'admin_bot_target_location_edit_type':
        location_type = user_input.lower()
        if location_type not in ['channel', 'group']:
            bot.send_message(chat_id, "❌ Некоректний тип. Введіть 'channel' або 'group'.")
            return
        state['current_data']['location_type'] = location_type
        state['waiting_for'] = 'admin_bot_target_location_invite_link' if 'location_id' not in state else 'admin_bot_target_location_edit_invite_link'

        prompt_text = "Введіть *посилання-запрошення* (необов'язково, для груп/каналів). Залиште порожнім, якщо немає або не потрібно.\n"
        if 'original_data' in state and state['original_data'].get('invite_link'):
            prompt_text += f"(Поточне посилання: `{state['original_data']['invite_link']}`)"
        bot.send_message(chat_id, prompt_text, parse_mode='Markdown', disable_web_page_preview=True)

    elif input_type == 'admin_bot_target_location_invite_link' or input_type == 'admin_bot_target_location_edit_invite_link':
        state['current_data']['invite_link'] = user_input if user_input else None # Store None if empty

        name = state['current_data'].get('location_name')
        location_id = state['current_data'].get('location_id')
        location_type = state['current_data'].get('location_type')
        invite_link = state['current_data'].get('invite_link')
        
        db_location_id = state.get('location_id') # This is the ID in our DB for editing

        if db_location_id is None: # Create new
            success = add_bot_target_location(name, location_id, location_type, invite_link, chat_id)
            if success:
                bot.send_message(chat_id, "✅ Цільове місце успішно додано!", reply_markup=get_admin_bot_activity_menu())
            else:
                bot.send_message(chat_id, "❌ Помилка при додаванні цільового місця. Можливо, такий Chat ID вже існує.", reply_markup=get_admin_bot_activity_menu())
        else: # Edit existing
            success = update_bot_target_location(db_location_id, name, location_id, location_type, invite_link)
            if success:
                bot.send_message(chat_id, "✅ Цільове місце успішно оновлено!", reply_markup=get_admin_bot_activity_menu())
            else:
                bot.send_message(chat_id, "❌ Помилка при оновленні цільового місця.", reply_markup=get_admin_bot_activity_menu())
        
        del user_states[chat_id]

    # --- Comment Template Input ---
    elif input_type == 'admin_comment_template_create_name' or input_type == 'admin_comment_template_edit_name':
        state['current_data'] = state.get('current_data', {})
        state['current_data']['name'] = user_input
        state['waiting_for'] = 'admin_comment_template_message_text' if 'template_id' not in state else 'admin_comment_template_edit_message_text'
        
        prompt_text = "Введіть *текст повідомлення* для коментування:\n"
        if 'original_data' in state and state['original_data'].get('message_text'):
            prompt_text += f"(Поточний текст: '{state['original_data']['message_text']}')"
        bot.send_message(chat_id, prompt_text, parse_mode='Markdown')

    elif input_type == 'admin_comment_template_message_text' or input_type == 'admin_comment_template_edit_message_text':
        state['current_data']['message_text'] = user_input
        state['waiting_for'] = 'admin_comment_template_subscription_link' if 'template_id' not in state else 'admin_comment_template_edit_subscription_link'

        prompt_text = "Введіть *посилання для підписки* (URL). Залиште порожнім, якщо не потрібно.\n"
        if 'original_data' in state and state['original_data'].get('subscription_link'):
            prompt_text += f"(Поточне посилання: `{state['original_data']['subscription_link']}`)"
        bot.send_message(chat_id, prompt_text, parse_mode='Markdown', disable_web_page_preview=True)

    elif input_type == 'admin_comment_template_subscription_link' or input_type == 'admin_comment_template_edit_subscription_link':
        state['current_data']['subscription_link'] = user_input if user_input else None

        name = state['current_data'].get('name')
        message_text = state['current_data'].get('message_text')
        subscription_link = state['current_data'].get('subscription_link')

        template_id = state.get('template_id') # This is the ID in our DB for editing

        if template_id is None: # Create new
            success = add_bot_comment_template(name, message_text, subscription_link)
            if success:
                bot.send_message(chat_id, "✅ Повідомлення для коментування успішно створено!", reply_markup=get_admin_bot_activity_menu())
            else:
                bot.send_message(chat_id, "❌ Помилка при створенні повідомлення. Можливо, назва вже існує.", reply_markup=get_admin_bot_activity_menu())
        else: # Edit existing
            success = update_bot_comment_template(template_id, name, message_text, subscription_link)
            if success:
                bot.send_message(chat_id, "✅ Повідомлення для коментування успішно оновлено!", reply_markup=get_admin_bot_activity_menu())
            else:
                bot.send_message(chat_id, "❌ Помилка при оновленні повідомлення.", reply_markup=get_admin_bot_activity_menu())
        
        del user_states[chat_id]

    else:
        bot.send_message(chat_id, "Неочікуваний стан введення.", reply_markup=get_admin_bot_activity_menu())
        if chat_id in user_states:
            del user_states[chat_id]

# --- Database Operations for Bot Activity ---

def add_bot_target_location(location_name, location_id, location_type, invite_link, added_by):
    """Adds a new bot target location to the database."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_target_locations (location_name, location_id, location_type, invite_link, added_by)
                    VALUES (%s, %s, %s, %s, %s);
                """, (location_name, location_id, location_type, invite_link, added_by))
                return True
    except Exception as e:
        logging.error(f"Error adding bot target location: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_bot_target_locations():
    """Retrieves all bot target locations."""
    conn = get_db_connection()
    locations = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, location_name, location_id, location_type, invite_link FROM bot_target_locations ORDER BY created_at DESC;")
                locations = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching bot target locations: {e}")
    finally:
        if conn:
            conn.close()
    return locations

def get_bot_target_location(location_id):
    """Retrieves a single bot target location by ID."""
    conn = get_db_connection()
    location = None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, location_name, location_id, location_type, invite_link FROM bot_target_locations WHERE id = %s;", (location_id,))
                location = cur.fetchone()
    except Exception as e:
        logging.error(f"Error fetching bot target location {location_id}: {e}")
    finally:
        if conn:
            conn.close()
    return location

def update_bot_target_location(db_id, location_name, location_id, location_type, invite_link):
    """Updates an existing bot target location."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE bot_target_locations
                    SET location_name = %s, location_id = %s, location_type = %s, invite_link = %s
                    WHERE id = %s;
                """, (location_name, location_id, location_type, invite_link, db_id))
                return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error updating bot target location {db_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_bot_target_location_db(location_id):
    """Deletes a bot target location."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bot_target_locations WHERE id = %s;", (location_id,))
                return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error deleting bot target location {location_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def add_bot_comment_template(name, message_text, subscription_link):
    """Adds a new bot comment template to the database."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_comment_templates (name, message_text, subscription_link)
                    VALUES (%s, %s, %s);
                """, (name, message_text, subscription_link))
                return True
    except Exception as e:
        logging.error(f"Error adding bot comment template: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_bot_comment_templates():
    """Retrieves all bot comment templates."""
    conn = get_db_connection()
    templates = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, message_text, subscription_link FROM bot_comment_templates ORDER BY created_at DESC;")
                templates = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching bot comment templates: {e}")
    finally:
        if conn:
            conn.close()
    return templates

def get_bot_comment_template(template_id):
    """Retrieves a single bot comment template by ID."""
    conn = get_db_connection()
    template = None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, message_text, subscription_link FROM bot_comment_templates WHERE id = %s;", (template_id,))
                template = cur.fetchone()
    except Exception as e:
        logging.error(f"Error fetching bot comment template {template_id}: {e}")
    finally:
        if conn:
            conn.close()
    return template

def update_bot_comment_template(template_id, name, message_text, subscription_link):
    """Updates an existing bot comment template."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE bot_comment_templates
                    SET name = %s, message_text = %s, subscription_link = %s
                    WHERE id = %s;
                """, (name, message_text, subscription_link, template_id))
                return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error updating bot comment template {template_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_bot_comment_template_db(template_id):
    """Deletes a bot comment template."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bot_comment_templates WHERE id = %s;", (template_id,))
                return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error deleting bot comment template {template_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

# ============ MAIN FUNCTION ============

if __name__ == '__main__':
    # Initialize the database and create tables if they don't exist
    init_db()
    logging.info("База даних ініціалізована. Бот запущено...")
    # Start the bot's polling loop
    bot.polling(non_stop=True)

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from telebot import TeleBot, types
from datetime import datetime, timedelta
import json
import re

load_dotenv()

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
# This is a specific channel ID. Ensure the bot has necessary permissions in this channel.
CHANNEL_ID = -1002510470267
DATABASE_URL = os.getenv('DATABASE_URL')

bot = TeleBot(TOKEN)
logging.basicConfig(level=logging.INFO)

# Dictionary to store temporary user data for multi-step conversations
user_states = {}

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
    'вишневе': '#Вишневе', # Highlighted as requested
    'переяслав': '#Переяслав',
    'васильків': '#Васильків',
    'вишгород': '#Вишгород',
    ''славутич': '#Славутич',
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
    'прип'ять': '#Припять' # Similar to Chernobyl, for completeness
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

            # Tables for managing target channels and groups
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
        types.InlineKeyboardButton("🔧 Налаштування", callback_data="admin_settings")
    )
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
    # IMPORTANT: Replace with actual admin chat_ids for security
    # For testing, allows the sender to be admin. In production, hardcode admin IDs.
    ALLOWED_ADMINS = [admin_chat_id]

    if admin_chat_id not in ALLOWED_ADMINS:
        bot.send_message(admin_chat_id, "❌ У вас немає прав доступу до адмін-панелі.")
        return

    bot.send_message(admin_chat_id, "🔧 Панель адміністратора", reply_markup=get_admin_menu())

# ============ CALLBACK HANDLERS ============

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    """Handles all inline keyboard callback queries."""
    chat_id = call.message.chat.id

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

        # New callback handlers (placeholders for now)
        elif call.data == "channels_by_city":
            bot.answer_callback_query(call.id, "Функція 'Канали за містами' ще не реалізована.")
            # TODO: Implement show_channels_by_city(call)
        elif call.data == "my_channels":
            bot.answer_callback_query(call.id, "Функція 'Мої канали' ще не реалізована.")
            # TODO: Implement show_my_channels(call)
        elif call.data == "my_groups":
            bot.answer_callback_query(call.id, "Функція 'Мої групи' ще не реалізована.")
            # TODO: Implement show_my_groups(call)
        elif call.data == "channels_stats":
            bot.answer_callback_query(call.id, "Функція 'Статистика каналів' ще не реалізована.")
            # TODO: Implement show_channels_stats(call)
        elif call.data == "stats":
            bot.answer_callback_query(call.id, "Загальна статистика ще не реалізована.")
            # TODO: Implement show_overall_stats(call)
        elif call.data == "settings":
            bot.answer_callback_query(call.id, "Налаштування ще не реалізовані.")
            # TODO: Implement user_settings(call)
        elif call.data == "help":
            bot.answer_callback_query(call.id, "Допомога ще не реалізована.")
            # TODO: Implement help_message(call)
        elif call.data == "skip_rating":
            bot.edit_message_text("Добре, ви пропустили оцінку.", chat_id, call.message.message_id,
                                  reply_markup=get_main_menu())
            bot.answer_callback_query(call.id)
            return

        # Answer the callback query to remove the "loading" state on the button
        bot.answer_callback_query(call.id)

    except Exception as e:
        logging.error(f"Помилка в callback_handler: {e}")
        bot.answer_callback_query(call.id, "Сталася помилка. Спробуйте ще раз.")

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
        bot.answer_callback_query(call.id, "Сталася помилка при реєстрації")

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
        bot.answer_callback_query(call.id, "Помилка при збереженні оцінки")

# ============ SEGMENTED BROADCAST ============

def send_broadcast_by_city(message_text, target_cities=None, template_id=None):
    """
    Sends a broadcast message to users, optionally filtered by city,
    and includes a rating button if a template_id is provided.
    """
    conn = get_db_connection()
    users = []
    try:
        with conn:
            with conn.cursor() as cur:
                if target_cities:
                    placeholders = ','.join(['%s'] * len(target_cities))
                    cur.execute(f"""
                        SELECT chat_id, city FROM users
                        WHERE is_active = TRUE AND notifications = TRUE
                        AND city IN ({placeholders});
                    """, target_cities)
                else:
                    cur.execute("""
                        SELECT chat_id, city FROM users
                        WHERE is_active = TRUE AND notifications = TRUE;
                    """)
                users = cur.fetchall()
    except Exception as e:
        logging.error(f"Помилка при отриманні користувачів для розсилки: {e}")
    finally:
        conn.close()

    success_count = 0
    for user in users:
        try:
            chat_id = user['chat_id']
            user_city = user['city']
            city_hashtag = UKRAINIAN_CITIES.get(user_city, f"#{user_city.replace('_', ' ').title()}")

            # Adding city hashtag to the message
            full_message = f"{message_text}\n\n🏙️ {city_hashtag}"

            # Add rating button if template_id is provided
            keyboard = None
            if template_id:
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
        conn.close()
    return result['city'] if result else 'київ'

def get_channels_by_city(city):
    """Retrieves active channels filtered by city."""
    conn = get_db_connection()
    channels = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT channel_name, channel_link FROM target_channels
                    WHERE city = %s AND is_active = TRUE;
                """, (city,))
                channels = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching channels by city {city}: {e}")
    finally:
        conn.close()
    return channels

def get_groups_by_city(city):
    """Retrieves active groups filtered by city."""
    conn = get_db_connection()
    groups = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT group_name, group_link FROM target_groups
                    WHERE city = %s AND is_active = TRUE;
                """, (city,))
                groups = cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching groups by city {city}: {e}")
    finally:
        conn.close()
    return groups

def get_broadcast_rating_stats(template_id):
    """Retrieves rating statistics for a specific broadcast template."""
    conn = get_db_connection()
    stats = None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) as total_ratings,
                        AVG(rating) as avg_rating,
                        COUNT(CASE WHEN rating >= 4 THEN 1 END) as positive_ratings
                    FROM broadcast_ratings
                    WHERE template_id = %s;
                """, (template_id,))
                stats = cur.fetchone()
    except Exception as e:
        logging.error(f"Error fetching broadcast rating stats for template {template_id}: {e}")
    finally:
        conn.close()
    return stats

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
    action = call.data.replace("admin_", "")

    if action == "broadcast":
        handle_admin_broadcast_menu(call)
    elif action == "users":
        show_users_stats_by_city(call)
    elif action == "channels":
        show_channels_stats(call)
    elif action == "ratings":
        show_ratings_stats(call)
    elif action == "cities":
        show_city_hashtags(call)
    elif action == "settings":
        bot.send_message(chat_id, "Налаштування адмін-панелі ще не реалізовані.")
    elif action == "menu":
        bot.edit_message_text("🔧 Панель адміністратора", chat_id, call.message.message_id, reply_markup=get_admin_menu())

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

def handle_admin_broadcast_menu(call):
    """Admin menu for broadcast management (placeholder)."""
    bot.send_message(call.message.chat.id, "Меню розсилок для адміністратора ще не реалізовано.")

def show_city_hashtags(call):
    """Admin function to show city hashtags (placeholder)."""
    bot.send_message(call.message.chat.id, "Управління містами та хештегами ще не реалізовано.")

# ============ MAIN FUNCTION ============

if __name__ == '__main__':
    # Initialize the database and create tables if they don't exist
    init_db()
    logging.info("База даних ініціалізована. Бот запущено...")
    # Start the bot's polling loop
    bot.polling(non_stop=True)

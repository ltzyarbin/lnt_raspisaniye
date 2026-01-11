import asyncio
import hashlib
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import re
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton
from telegram.request import HTTPXRequest
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler, ConversationHandler, filters

# ========== НАСТРОЙКИ ==========
# Загрузка переменных окружения из .env файла
load_dotenv()

TOKEN = os.getenv('BOT_TOKEN')

# Проверка что секреты загружены
if not TOKEN:
    raise ValueError("❌ Не найден BOT_TOKEN в .env файле!")
# Константы
DATABASE_URL = os.getenv('DATABASE_URL')
CHECK_INTERVAL = 15 * 60  # 15 минут

MAX_EXTRA_GROUPS = 4  # Максимальное количество дополнительных групп

# ========== UI: КНОПКИ ==========
# Текст кнопок главного меню
BTN_SCHEDULE = "📅 Расписание"
BTN_GROUPS = "👥 Группы"
BTN_OTHER = "⚙️ Прочее"

# Callback data для inline-кнопок
CB_TEACHER_SEARCH = "teacher_search"
CB_GROUPS_LIST = "groups_list"
CB_ADD_GROUP = "add_group"
CB_REMOVE_GROUP = "remove_group"
CB_SET_MAIN_GROUP = "set_main_group"
CB_SUBSCRIBE = "subscribe"
CB_UNSUBSCRIBE = "unsubscribe"
CB_HELP = "help"
CB_BACK = "back"
CB_REMOVE_GROUP_PREFIX = "rmg_"  # Префикс для удаления конкретной группы
CB_SHOW_MY_SCHEDULE = "show_my_schedule"
CB_START_TEACHER_SEARCH = "start_teacher_search"
CB_SELECT_TEACHER_PREFIX = "sel_teacher_" # Префикс для выбора преподавателя

# Состояния для ConversationHandler
STATE_WAITING_GROUP = 1
STATE_WAITING_TEACHER = 2
STATE_WAITING_MAIN_GROUP = 3

def get_main_keyboard():
    """Создать главную клавиатуру"""
    keyboard = [
        [KeyboardButton(BTN_SCHEDULE)],
        [KeyboardButton(BTN_GROUPS), KeyboardButton(BTN_OTHER)]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Временное хранилище (кэш)
available_groups = set()

# ========== RATE LIMITING ==========
user_last_request = {}  # {user_id: timestamp}

def check_rate_limit(user_id, cooldown=3):
    """Проверка rate limiting (защита от спама)"""
    now = datetime.now()

    if user_id in user_last_request:
        time_passed = (now - user_last_request[user_id]).total_seconds()
        if time_passed < cooldown:
            return False, cooldown - time_passed

    user_last_request[user_id] = now
    return True, 0

# ========== ВАЛИДАЦИЯ ==========

def validate_group_name(group_name):
    """Валидация названия группы"""
    if not group_name:
        return False, "Название группы не может быть пустым"

    if len(group_name) > 20:
        return False, "Название группы слишком длинное (макс. 20 символов)"

    if len(group_name) < 2:
        return False, "Название группы слишком короткое (мин. 2 символа)"

    # Разрешены только буквы (русские/английские), цифры и дефис
    if not re.match(r'^[А-Яа-яA-Za-z0-9\-]+$', group_name):
        return False, "Название группы может содержать только буквы, цифры и дефис"

    return True, None

# ========== БАЗА ДАННЫХ ==========

@contextmanager
def get_db():
    """Контекстный менеджер для работы с PostgreSQL"""
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def init_db():
    """Инициализация базы данных"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                group_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица дополнительных групп
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_extra_groups (
                user_id BIGINT,
                group_name TEXT,
                PRIMARY KEY (user_id, group_name),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица подписок
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        
        logger.info("✅ База данных PostgreSQL инициализирована")

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С БД ==========

def get_user(user_id):
    """Получить данные пользователя"""
    with get_db() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM users WHERE user_id = %s', (user_id,))
        return cursor.fetchone()

def set_user_group(user_id, group_name):
    """Установить основную группу пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO users (user_id, group_name, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
                group_name = EXCLUDED.group_name,
                updated_at = EXCLUDED.updated_at
        ''', (user_id, group_name))

    logger.info(f"👥 Пользователь {user_id} установил группу: {group_name}")

def get_user_group(user_id):
    """Получить основную группу пользователя"""
    user = get_user(user_id)
    return user['group_name'] if user else None

def subscribe_user(user_id):
    """Подписать пользователя на уведомления"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO subscriptions (user_id) 
            VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING
        ''', (user_id,))

def unsubscribe_user(user_id):
    """Отписать пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM subscriptions WHERE user_id = %s', (user_id,))

def is_subscribed(user_id):
    """Проверить подписку"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM subscriptions WHERE user_id = %s', (user_id,))
        return cursor.fetchone() is not None

def get_all_subscribers():
    """Получить список всех подписчиков"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM subscriptions')
        return [row[0] for row in cursor.fetchall()]

def get_stats():
    """Получить статистику бота"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM subscriptions')
        total_subs = cursor.fetchone()[0]
        return {'total': total_users, 'subscribed': total_subs}

# ========== ДОПОЛНИТЕЛЬНЫЕ ГРУППЫ ==========

def add_extra_group(user_id, group_name):
    """Добавить дополнительную группу"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_extra_groups (user_id, group_name)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            ''', (user_id, group_name))
            if cursor.rowcount > 0:
                logger.info(f"➕ Пользователь {user_id} добавил доп. группу: {group_name}")
                return True
            return False
    except Exception as e:
        logger.error(f"Ошибка при добавлении группы: {e}")
        return False

def remove_extra_group(user_id, group_name):
    """Удалить дополнительную группу"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM user_extra_groups 
            WHERE user_id = %s AND group_name = %s
        ''', (user_id, group_name))
        deleted = cursor.rowcount > 0
        if deleted:
            logger.info(f"➖ Пользователь {user_id} удалил доп. группу: {group_name}")
        return deleted

def get_user_extra_groups(user_id):
    """Получить список доп. групп пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT group_name FROM user_extra_groups WHERE user_id = %s', (user_id,))
        return [row[0] for row in cursor.fetchall()]

def get_user_all_groups(user_id):
    """Получить все группы пользователя (основная + дополнительные)"""
    main_group = get_user_group(user_id)
    extra_groups = get_user_extra_groups(user_id)
    
    all_groups = []
    if main_group:
        all_groups.append(main_group)
    all_groups.extend(extra_groups)
    
    return all_groups

def count_extra_groups(user_id):
    """Подсчитать количество дополнительных групп пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM user_extra_groups WHERE user_id = %s', (user_id,))
        return cursor.fetchone()[0]

# ========== ПОИСК ПРЕПОДАВАТЕЛЯ ==========

def get_all_teachers(schedule_data):
    """Получить список всех преподавателей из расписания"""
    if not schedule_data or 'groups' not in schedule_data:
        return set()
    
    teachers = set()
    for group_name, pairs in schedule_data['groups'].items():
        for pair in pairs:
            teacher = pair.get('teacher', '')
            if teacher:
                teachers.add(teacher)
    return teachers

def search_teachers(query, schedule_data):
    """
    Поиск преподавателей по запросу.
    Возвращает список полных имен преподавателей.
    """
    all_teachers = get_all_teachers(schedule_data)
    query_lower = query.lower()
    
    # Точное совпадение
    exact_matches = [t for t in all_teachers if t.lower() == query_lower]
    if exact_matches:
        return exact_matches
        
    # Частичное совпадение
    matches = [t for t in all_teachers if query_lower in t.lower()]
    return sorted(list(matches))

def find_teacher_schedule(teacher_name, schedule_data):
    """
    Поиск пар преподавателя во всех группах.
    Возвращает словарь: {group_name: [pairs]}
    """
    if not schedule_data or 'groups' not in schedule_data:
        return {}
    
    result = {}
    teacher_lower = teacher_name.lower()
    
    for group_name, pairs in schedule_data['groups'].items():
        matching_pairs = []
        for pair in pairs:
            teacher = pair.get('teacher', '')
            # Ищем точное совпадение или вхождение (если передано полное имя)
            if teacher and (teacher_lower == teacher.lower() or teacher_lower in teacher.lower()):
                matching_pairs.append(pair)
        
        if matching_pairs:
            result[group_name] = matching_pairs
    
    return result

def format_teacher_schedule(teacher_name, teacher_data, schedule_date):
    """Форматирование расписания преподавателя"""
    if not teacher_data:
        return f"😕 Преподаватель с фамилией *{teacher_name}* не найден в расписании на сегодня"
    
    text = f"📅 *{schedule_date}*\n"
    text += f"👨‍🏫 Расписание преподавателя: *{teacher_name}*\n\n"
    
    total_pairs = 0
    for group_name, pairs in sorted(teacher_data.items()):
        text += f"👥 *{group_name}*\n"
        for pair in pairs:
            text += f"   📚 {pair['pair_number']} пара — {pair['subject']}\n"
            total_pairs += 1
        text += "\n"
    
    text += f"_Всего пар: {total_pairs}_"
    return text

# ========== ПАРСИНГ ==========

def get_schedule(group_filter=None):
    """Получить расписание всех групп"""
    timestamp = int(time.time() * 1000)
    url = f"http://lntrt.ru/schedule/daySchedule?_={timestamp}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if not response.text or response.text.strip() == '':
            print("⚠️ Пустой ответ")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Дата
        schedule_date = "Дата не указана"
        date_div = soup.find('div', style=lambda s: s and 'width:980px' in s)
        if date_div:
            date_text = date_div.get_text()
            for line in date_text.split('\n'):
                if any(month in line.lower() for month in ['ноября', 'декабря', 'января', 'февраля', 'марта']):
                    schedule_date = line.strip()
                    break
        
        print(f"📅 Дата: {schedule_date}")
        
        # Таблица
        table = soup.find('table', class_='border')
        if not table:
            print("❌ Таблица не найдена")
            return None
        
        rows = table.find_all('tr')
        print(f"📊 Всего строк в таблице: {len(rows)}")
        
        # Парсинг всех блоков
        schedule_by_group = {}
        i = 0
        block_number = 0
        
        while i < len(rows):
            row = rows[i]
            group_headers = row.find_all('th')
            
            if len(group_headers) > 0:
                groups_in_block = []
                
                for th in group_headers:
                    group_name = th.get_text(strip=True)
                    
                    if group_name and 3 <= len(group_name) <= 15:
                        has_digit = any(c.isdigit() for c in group_name)
                        has_letter = any(c.isalpha() for c in group_name)
                        
                        if has_digit and has_letter:
                            groups_in_block.append(group_name)
                            available_groups.add(group_name)
                
                if len(groups_in_block) > 0:
                    block_number += 1
                    
                    print(f"  📋 Блок {block_number}: {len(groups_in_block)} групп: {groups_in_block}")
                    
                    for group_name in groups_in_block:
                        if group_name not in schedule_by_group:
                            schedule_by_group[group_name] = []
                    
                    if i + 1 < len(rows):
                        schedule_row = rows[i + 1]
                        schedule_cells = schedule_row.find_all('td', recursive=False)
                        
                        for col_index, big_cell in enumerate(schedule_cells):
                            if col_index >= len(groups_in_block):
                                break
                            
                            group_name = groups_in_block[col_index]
                            inner_tables = big_cell.find_all('table', recursive=True)
                            
                            for inner_table in inner_tables:
                                pair_number_th = inner_table.find('th')
                                pair_number = pair_number_th.get_text(strip=True) if pair_number_th else '?'
                                
                                content_td = inner_table.find('td', style=lambda s: s and 'overflow' in s)
                                
                                if content_td:
                                    subject_text = content_td.get_text(separator=' ', strip=True)
                                    teacher_small = content_td.find('small')
                                    teacher = teacher_small.get_text(strip=True) if teacher_small else ''
                                    
                                    if teacher:
                                        subject = subject_text.replace(teacher, '').strip()
                                    else:
                                        subject = subject_text
                                    
                                    if 'нет' in subject.lower() and len(subject) < 15:
                                        continue
                                    
                                    schedule_by_group[group_name].append({
                                        'pair_number': pair_number,
                                        'subject': subject,
                                        'teacher': teacher
                                    })
                        
                        i += 2
                    else:
                        i += 1
                else:
                    i += 1
            else:
                i += 1
        
        print(f"✅ Всего найдено групп: {len(schedule_by_group)}")
        
        result = {
            'date': schedule_date,
            'groups': schedule_by_group
        }
        
        if group_filter:
            if group_filter in schedule_by_group:
                return {
                    'date': result['date'],
                    'groups': {group_filter: schedule_by_group[group_filter]}
                }
            else:
                print(f"⚠️ Группа {group_filter} не найдена")
                return None
        
        return result
        
    except Exception as e:
        print(f"❌ Ошибка парсинга: {e}")
        import traceback
        traceback.print_exc()
        return None

def format_schedule(schedule_data, group_name=None):
    """Форматирование расписания"""
    
    if not schedule_data:
        return "📭 Расписание еще не опубликовано"
    
    schedule_date = schedule_data.get('date', 'Дата не указана')
    groups_data = schedule_data.get('groups', {})
    
    if group_name and group_name in groups_data:
        text = f"📅 *{schedule_date}*\n"
        text += f"👥 Группа: *{group_name}*\n\n"
        
        items = groups_data[group_name]
        
        if not items:
            return f"📭 У группы *{group_name}* пар нет\n\n_(или все пары отменены)_"
        
        for item in items:
            pair_num = item['pair_number']
            text += f"📚 *{pair_num} пара*\n"
            text += f"   📖 {item['subject']}\n"
            
            if item['teacher']:
                text += f"   👨‍🏫 _{item['teacher']}_\n"
            
            text += "\n"
        
        return text
    
    text = f"📅 *{schedule_date}*\n\n"
    text += f"Найдено групп: {len(groups_data)}\n\n"
    
    for group, items in sorted(groups_data.items()):
        active_pairs = len(items)
        text += f"👥 *{group}*: {active_pairs} пар\n"
    
    text += "\n_Выбери свою группу: /setgroup_"
    
    return text

async def send_long_message(update: Update, text: str, max_length: int = 4000):
    """Отправить длинное сообщение по частям"""
    message = update.message or update.callback_query.message
    
    if len(text) <= max_length:
        await message.reply_text(text, parse_mode='Markdown')
        return
    
    parts = []
    current_part = ""
    
    for line in text.split('\n'):
        if len(current_part) + len(line) + 1 > max_length:
            parts.append(current_part)
            current_part = line + '\n'
        else:
            current_part += line + '\n'
    
    if current_part:
        parts.append(current_part)
    
    for i, part in enumerate(parts):
        if i == 0:
            await message.reply_text(part, parse_mode='Markdown')
        else:
            await message.reply_text(
                f"_(часть {i+1})_\n\n{part}", 
                parse_mode='Markdown'
            )



# ========== КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие"""
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name
    
    # Регистрируем пользователя если его нет
    set_user_group(user_id, None)
    
    await update.message.reply_text(
        f"👋 *Привет, {username}! Я бот расписания ЛНТ*\n\n"

        "👇 *Используй кнопки внизу для навигации:*\n"
        "📅 *Расписание* — твои пары и поиск преподавателей\n"
        "👥 *Группы* — управление твоими группами\n"
        "⚙️ *Прочее* — подписка и помощь",
        parse_mode='Markdown',
        reply_markup=get_main_keyboard()
    )



async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Расписание на сегодня для всех групп пользователя"""
    user_id = update.effective_user.id
    message = update.message or update.callback_query.message

    # Проверка rate limiting
    allowed, wait_time = check_rate_limit(user_id, cooldown=5)
    if not allowed:
        await message.reply_text(
            f"⏱️ Подожди {int(wait_time)} сек. перед следующим запросом",
            parse_mode='Markdown'
        )
        logger.warning(f"Rate limit для пользователя {user_id}")
        return

    # Получаем все группы пользователя
    all_groups = get_user_all_groups(user_id)
    
    if not all_groups:
        await message.reply_text(
            "❌ Группа не выбрана\n\n"
            "Сначала выбери группу: /setgroup",
            parse_mode='Markdown'
        )
        return

    await message.reply_text("⏳ Загружаю расписание...")

    # Получаем полное расписание один раз
    schedule = get_schedule()
    
    if not schedule:
        await message.reply_text(
            "📭 Расписание еще не опубликовано",
            parse_mode='Markdown'
        )
        return
    
    # Отправляем расписание для каждой группы отдельным сообщением
    sent_count = 0
    for group_name in all_groups:
        if group_name in schedule.get('groups', {}):
            group_schedule = {
                'date': schedule['date'],
                'groups': {group_name: schedule['groups'][group_name]}
            }
            text = format_schedule(group_schedule, group_name)
            await send_long_message(update, text)
            sent_count += 1
        else:
            await message.reply_text(
                f"⚠️ Группа *{group_name}* не найдена в расписании\n"
                "_Возможно, её нет на сегодня или название указано неверно_",
                parse_mode='Markdown'
            )
    
    if sent_count == 0:
        await message.reply_text(
            "📭 Расписание для ваших групп не найдено\n\n"
            "Проверьте названия групп: /mygroups",
            parse_mode='Markdown'
        )

async def setgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбрать группу"""
    user_id = update.effective_user.id

    if context.args:
        group = ' '.join(context.args).upper()

        # Валидация названия группы
        is_valid, error_message = validate_group_name(group)
        if not is_valid:
            await update.message.reply_text(
                f"❌ *Ошибка валидации:*\n{error_message}\n\n"
                "Попробуй еще раз: `/setgroup ИС-1-23`",
                parse_mode='Markdown'
            )
            logger.warning(f"Невалидное название группы от {user_id}: {group}")
            return

        set_user_group(user_id, group)

        await update.message.reply_text(
            f"✅ *Группа установлена: {group}*\n\n"
            "Проверь расписание: /today",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("⏳ Загружаю список групп...")
        
        schedule = get_schedule()
        
        if schedule and 'groups' in schedule:
            groups = sorted(schedule['groups'].keys())
            
            text = "📋 *Доступные группы:*\n\n"
            
            for group in groups:
                text += f"• `{group}`\n"
            
            text += f"\n💡 Пример: `/setgroup {groups[0]}`"
            
            await send_long_message(update, text)
        else:
            await update.message.reply_text(
                "⚠️ Не удалось загрузить список групп.\n"
                "Укажи группу вручную: `/setgroup ИС-1-23`",
                parse_mode='Markdown'
            )

async def mygroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать выбранную группу"""
    user_id = update.effective_user.id
    group = get_user_group(user_id)
    
    if group:
        await update.message.reply_text(
            f"👥 Твоя группа: *{group}*\n\n"
            "Изменить: `/setgroup НОВАЯ_ГРУППА`",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            "❌ Группа не выбрана\n\n"
            "Выбери группу: /setgroup"
        )

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подписка на уведомления"""
    user_id = update.effective_user.id
    subscribe_user(user_id)
    
    await update.message.reply_text(
        "✅ *Подписка активирована!*\n\n"
        f"Проверяю сайт каждые {CHECK_INTERVAL // 60} минут.\n"
        "Пришлю уведомление когда появится расписание.",
        parse_mode='Markdown'
    )

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отписка"""
    user_id = update.effective_user.id
    unsubscribe_user(user_id)
    await update.message.reply_text("❌ Подписка отменена")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Помощь — список всех команд"""
    message = update.message or update.callback_query.message
    await message.reply_text(
        "📖 *Все команды бота:*\n\n"
        "*📱 Навигация:*\n"
        "`/start` — перезапустить бота\n\n"
        "*📅 Расписание:*\n"
        "`/today` — расписание на сегодня\n"
        "`/teacher <фамилия>` — поиск преподавателя\n\n"
        "*👥 Группы:*\n"
        "`/setgroup <группа>` — установить основную группу\n"
        "`/mygroup` — показать основную группу\n"
        "`/addgroup <группа>` — добавить доп. группу\n"
        "`/removegroup <группа>` — удалить доп. группу\n"
        "`/mygroups` — все отслеживаемые группы\n\n"
        "*🔔 Уведомления:*\n"
        "`/subscribe` — подписаться на обновления\n"
        "`/unsubscribe` — отписаться\n\n"
        f"_Бот проверяет сайт каждые {CHECK_INTERVAL // 60} минут_",
        parse_mode='Markdown'
    )

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать главное меню"""
    await update.message.reply_text(
        "📋 *Главное меню*",
        parse_mode='Markdown',
        reply_markup=get_main_keyboard()
    )

# ========== КОМАНДЫ: ПОИСК ПРЕПОДАВАТЕЛЯ ==========

async def teacher(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск расписания преподавателя по фамилии"""
    user_id = update.effective_user.id
    
    # Проверка rate limiting
    allowed, wait_time = check_rate_limit(user_id, cooldown=5)
    if not allowed:
        await update.message.reply_text(
            f"⏱️ Подожди {int(wait_time)} сек. перед следующим запросом",
            parse_mode='Markdown'
        )
        return
    
    if not context.args:
        await update.message.reply_text(
            "🔍 *Поиск по преподавателю*\n\n"
            "Укажи фамилию преподавателя:\n"
            "`/teacher Иванов`\n\n"
            "_Поиск ищет по части фамилии_",
            parse_mode='Markdown'
        )
        return
    
    teacher_name = ' '.join(context.args)
    
    await update.message.reply_text("⏳ Ищу преподавателя...")
    
    schedule = get_schedule()
    
    if not schedule:
        await update.message.reply_text(
            "📭 Расписание еще не опубликовано",
            parse_mode='Markdown'
        )
        return
    
    # Умный поиск преподавателей
    found_teachers = search_teachers(teacher_name, schedule)
    
    if not found_teachers:
        await update.message.reply_text(
            f"😕 Преподаватель *{teacher_name}* не найден в расписании на сегодня.",
            parse_mode='Markdown'
        )
    elif len(found_teachers) == 1:
        # Найден один - показываем расписание
        full_name = found_teachers[0]
        teacher_data = find_teacher_schedule(full_name, schedule)
        schedule_date = schedule.get('date', 'Дата не указана')
        text = format_teacher_schedule(full_name, teacher_data, schedule_date)
        await send_long_message(update, text)
    else:
        # Найдено несколько - предлагаем выбор
        await update.message.reply_text(
            f"🔎 Найдено несколько преподавателей по запросу *{teacher_name}*.\n"
            "Выберите нужного:",
            parse_mode='Markdown'
        )
        
        keyboard = []
        for name in found_teachers:
            keyboard.append([InlineKeyboardButton(name, callback_data=f"{CB_SELECT_TEACHER_PREFIX}{name}")])
            
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Список:", reply_markup=reply_markup)

# ========== КОМАНДЫ: ДОПОЛНИТЕЛЬНЫЕ ГРУППЫ ==========

async def addgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавить дополнительную группу для отслеживания"""
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text(
            "➕ *Добавить дополнительную группу*\n\n"
            "Укажи название группы:\n"
            "`/addgroup ИС-1-23`\n\n"
            f"_Можно добавить до {MAX_EXTRA_GROUPS} доп. групп_",
            parse_mode='Markdown'
        )
        return
    
    group = ' '.join(context.args).upper()
    
    # Валидация
    is_valid, error_message = validate_group_name(group)
    if not is_valid:
        await update.message.reply_text(
            f"❌ *Ошибка:* {error_message}",
            parse_mode='Markdown'
        )
        return
    
    # Проверка лимита
    current_count = count_extra_groups(user_id)
    if current_count >= MAX_EXTRA_GROUPS:
        await update.message.reply_text(
            f"❌ Достигнут лимит дополнительных групп ({MAX_EXTRA_GROUPS})\n\n"
            "Удали ненужную группу: `/removegroup ГРУППА`",
            parse_mode='Markdown'
        )
        return
    
    # Проверка что это не основная группа
    main_group = get_user_group(user_id)
    if main_group and main_group.upper() == group:
        await update.message.reply_text(
            f"⚠️ *{group}* уже установлена как основная группа",
            parse_mode='Markdown'
        )
        return
    
    # Добавление
    if add_extra_group(user_id, group):
        extra_groups = get_user_extra_groups(user_id)
        await update.message.reply_text(
            f"✅ Группа *{group}* добавлена!\n\n"
            f"📋 Доп. группы ({len(extra_groups)}/{MAX_EXTRA_GROUPS}):\n" +
            '\n'.join([f"• `{g}`" for g in extra_groups]) +
            "\n\nПроверь расписание: /today",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            f"⚠️ Группа *{group}* уже добавлена",
            parse_mode='Markdown'
        )

async def removegroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удалить дополнительную группу"""
    user_id = update.effective_user.id
    
    if not context.args:
        extra_groups = get_user_extra_groups(user_id)
        if not extra_groups:
            await update.message.reply_text(
                "📋 У тебя нет дополнительных групп\n\n"
                "Добавь: `/addgroup ГРУППА`",
                parse_mode='Markdown'
            )
            return
        
        await update.message.reply_text(
            "➖ *Удалить дополнительную группу*\n\n"
            "Твои доп. группы:\n" +
            '\n'.join([f"• `{g}`" for g in extra_groups]) +
            "\n\nПример: `/removegroup " + extra_groups[0] + "`",
            parse_mode='Markdown'
        )
        return
    
    group = ' '.join(context.args).upper()
    
    if remove_extra_group(user_id, group):
        await update.message.reply_text(
            f"✅ Группа *{group}* удалена",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            f"❌ Группа *{group}* не найдена в твоих доп. группах",
            parse_mode='Markdown'
        )

async def mygroups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать все отслеживаемые группы"""
    user_id = update.effective_user.id
    
    main_group = get_user_group(user_id)
    extra_groups = get_user_extra_groups(user_id)
    
    text = "👥 *Твои группы:*\n\n"
    
    if main_group:
        text += f"🏠 Основная: *{main_group}*\n"
    else:
        text += "🏠 Основная: _не выбрана_\n"
    
    if extra_groups:
        text += f"\n📋 Дополнительные ({len(extra_groups)}/{MAX_EXTRA_GROUPS}):\n"
        for g in extra_groups:
            text += f"• `{g}`\n"
    else:
        text += "\n_Дополнительных групп нет_\n"
    
    text += "\n*Управление:*\n"
    text += "`/setgroup` — изменить основную\n"
    text += "`/addgroup` — добавить доп.\n"
    text += "`/removegroup` — удалить доп."
    
    await update.message.reply_text(text, parse_mode='Markdown')

# ========== UI: ОБРАБОТЧИКИ ==========

async def show_schedule_ui(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать меню расписания"""
    message = update.message or update.callback_query.message
    keyboard = [
        [InlineKeyboardButton("🎓 Моя группа", callback_data=CB_SHOW_MY_SCHEDULE)],
        [InlineKeyboardButton("👨‍🏫 Поиск преподавателя", callback_data=CB_START_TEACHER_SEARCH)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await message.reply_text("Выберите действие:", reply_markup=reply_markup)

async def show_groups_ui(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать меню управления группами"""
    user_id = update.effective_user.id
    message = update.message or update.callback_query.message
    main_group = get_user_group(user_id)
    extra_groups = get_user_extra_groups(user_id)
    
    text = "👥 *Управление группами*\n\n"
    text += f"🏠 Основная: *{main_group or 'не выбрана'}*\n"
    
    if extra_groups:
        text += f"📋 Дополнительные: {', '.join(extra_groups)}\n"
    else:
        text += "📋 Дополнительные: _нет_\n"
        
    keyboard = [
        [InlineKeyboardButton("➕ Добавить", callback_data=CB_ADD_GROUP),
         InlineKeyboardButton("➖ Удалить", callback_data=CB_REMOVE_GROUP)],
        [InlineKeyboardButton("🏠 Изменить основную", callback_data=CB_SET_MAIN_GROUP)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def show_other_ui(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать меню 'Прочее'"""
    user_id = update.effective_user.id
    message = update.message or update.callback_query.message
    subscribed = is_subscribed(user_id)
    
    sub_text = "✅ Подписка активна" if subscribed else "❌ Подписка выключена"
    sub_action = CB_UNSUBSCRIBE if subscribed else CB_SUBSCRIBE
    sub_btn_text = "🔕 Отписаться" if subscribed else "🔔 Подписаться"
    
    text = "⚙️ *Прочее*\n\n"
    text += f"Статус подписки: {sub_text}\n"
    
    keyboard = [
        [InlineKeyboardButton(sub_btn_text, callback_data=sub_action)],
        [InlineKeyboardButton("ℹ️ Помощь", callback_data=CB_HELP)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def handle_menu_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий кнопок главного меню"""
    user_id = update.effective_user.id
    text = update.message.text
    
    # Проверка авторизации удалена

    if text == BTN_SCHEDULE:
        await show_schedule_ui(update, context)
    elif text == BTN_GROUPS:
        await show_groups_ui(update, context)
    elif text == BTN_OTHER:
        await show_other_ui(update, context)
    else:
        # Если текст не распознан, просто показываем меню
        await update.message.reply_text(
            "🤔 Не понял команду.\nВот главное меню:",
            reply_markup=get_main_keyboard()
        )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка inline-кнопок"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == CB_SHOW_MY_SCHEDULE:
        await today(update, context)

    elif data == CB_START_TEACHER_SEARCH:
        await query.message.reply_text("Для поиска преподавателя введите команду:\n`/teacher Фамилия`", parse_mode='Markdown')
    
    elif data == CB_TEACHER_SEARCH:
        # Старый callback, оставим для совместимости
        await query.message.reply_text("Для поиска преподавателя введите команду:\n`/teacher Фамилия`", parse_mode='Markdown')
    
    elif data.startswith(CB_SELECT_TEACHER_PREFIX):
        teacher_name = data.replace(CB_SELECT_TEACHER_PREFIX, "")
        schedule = get_schedule()
        teacher_data = find_teacher_schedule(teacher_name, schedule)
        schedule_date = schedule.get('date', 'Дата не указана')
        text = format_teacher_schedule(teacher_name, teacher_data, schedule_date)
        await send_long_message(update, text)

    elif data == CB_ADD_GROUP:
        await query.message.reply_text("Для добавления группы введите:\n`/addgroup Группа`", parse_mode='Markdown')
        
    elif data == CB_REMOVE_GROUP:
        user_id = update.effective_user.id
        extra_groups = get_user_extra_groups(user_id)
        if not extra_groups:
            await query.message.reply_text("У вас нет дополнительных групп для удаления.")
            return
            
        keyboard = []
        for group in extra_groups:
            keyboard.append([InlineKeyboardButton(f"❌ {group}", callback_data=f"{CB_REMOVE_GROUP_PREFIX}{group}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Выберите группу для удаления:", reply_markup=reply_markup)

    elif data.startswith(CB_REMOVE_GROUP_PREFIX):
        group_to_remove = data.replace(CB_REMOVE_GROUP_PREFIX, "")
        user_id = update.effective_user.id
        if remove_extra_group(user_id, group_to_remove):
            await query.message.edit_text(f"✅ Группа {group_to_remove} удалена.")
        else:
            await query.message.edit_text(f"❌ Не удалось удалить группу {group_to_remove}.")

    elif data == CB_SET_MAIN_GROUP:
        await query.message.reply_text("Для изменения основной группы введите:\n`/setgroup Группа`", parse_mode='Markdown')

    elif data == CB_SUBSCRIBE:
        subscribe_user(update.effective_user.id)
        # Обновляем сообщение чтобы показать новый статус
        await show_other_ui(update, context)
        
    elif data == CB_UNSUBSCRIBE:
        unsubscribe_user(update.effective_user.id)
        # Обновляем сообщение чтобы показать новый статус
        await show_other_ui(update, context)
        
    elif data == CB_HELP:
        await help_command(update, context)

# ========== МОНИТОРИНГ ==========

async def monitor_schedule(app):
    """Фоновый мониторинг"""
    previous_hashes = {}  # Словарь: {group_name: hash}

    print("🔍 Мониторинг запущен...")

    while True:
        try:
            schedule = get_schedule()

            if schedule and 'groups' in schedule:
                current_hashes = {}
                changed_groups = []

                # Вычисляем хэши для каждой группы
                for group_name, group_schedule in schedule['groups'].items():
                    group_hash = hashlib.sha256(str(group_schedule).encode()).hexdigest()
                    current_hashes[group_name] = group_hash

                    # Проверяем, изменилась ли группа
                    if group_name in previous_hashes:
                        if previous_hashes[group_name] != group_hash:
                            changed_groups.append(group_name)
                    # Если группа новая (первый запуск или добавлена новая группа)
                    elif previous_hashes:  # Не первый запуск
                        changed_groups.append(group_name)

                # Если есть изменения
                if changed_groups:
                    print(f"\n🔔 Обнаружены изменения в расписании!")
                    print(f"📅 Дата: {schedule.get('date', 'Не указана')}")
                    print(f"📝 Изменённые группы ({len(changed_groups)}):")

                    for group in changed_groups:
                        if group in schedule['groups']:
                            pairs_count = len(schedule['groups'][group])
                            print(f"   • {group}: {pairs_count} пар")

                            # Детальная информация о парах
                            for pair in schedule['groups'][group]:
                                print(f"      - {pair['pair_number']} пара: {pair['subject']}")

                    # Получаем всех подписчиков
                    subscribers = get_all_subscribers()
                    notified_count = 0
                    notified_groups = {}

                    # Отправляем уведомления только тем, у кого изменилась хотя бы одна группа
                    for subscriber in subscribers:
                        try:
                            user_id = subscriber['user_id']
                            
                            # Получаем все группы пользователя (основная + дополнительные)
                            user_groups = get_user_all_groups(user_id)
                            
                            # Находим пересечение групп пользователя и изменённых групп
                            user_changed_groups = [g for g in user_groups if g in changed_groups]
                            
                            if not user_changed_groups:
                                continue
                            
                            # Отправляем уведомление для каждой изменённой группы
                            for user_group in user_changed_groups:
                                if user_group in schedule['groups']:
                                    group_schedule = {
                                        'date': schedule['date'],
                                        'groups': {user_group: schedule['groups'][user_group]}
                                    }

                                    text = "🔔 *РАСПИСАНИЕ ОБНОВЛЕНО!*\n\n"
                                    text += format_schedule(group_schedule, user_group)

                                    await app.bot.send_message(
                                        chat_id=user_id,
                                        text=text,
                                        parse_mode='Markdown'
                                    )
                                    notified_count += 1

                                    # Подсчитываем уведомления по группам
                                    if user_group not in notified_groups:
                                        notified_groups[user_group] = 0
                                    notified_groups[user_group] += 1
                        except Exception as e:
                            print(f"⚠️ Не удалось отправить {user_id}: {e}")

                    print(f"\n✅ Уведомлено пользователей: {notified_count}")
                    if notified_groups:
                        print("📊 По группам:")
                        for group, count in notified_groups.items():
                            print(f"   • {group}: {count} чел.")
                    print()

                previous_hashes = current_hashes
            else:
                print("📭 Расписание еще не опубликовано")

        except Exception as e:
            print(f"❌ Ошибка мониторинга: {e}")
            import traceback
            traceback.print_exc()

        await asyncio.sleep(CHECK_INTERVAL)

# ========== ЗАПУСК ==========

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логирование ошибок"""
    logger.error(msg="Exception while handling an update:", exc_info=context.error)

async def main_async():
    """Асинхронная главная функция"""
    print("🤖 Запуск бота...")
    
    # Инициализация БД
    init_db()
    
    # Статистика
    stats = get_stats()
    print(f"👥 Пользователей: {stats['total']}")
    print(f"🔔 Подписчиков: {stats['subscribed']}")
    
    # Настройка запросов с увеличенными таймаутами
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=20.0,
        write_timeout=20.0,
        connect_timeout=20.0,
        pool_timeout=20.0
    )
    
    # Создание приложения
    app = Application.builder().token(TOKEN).request(request).build()
    app.add_error_handler(error_handler)
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("setgroup", setgroup))
    app.add_handler(CommandHandler("mygroup", mygroup))
    app.add_handler(CommandHandler("subscribe", subscribe))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("menu", menu_command))
    # Новые команды
    app.add_handler(CommandHandler("teacher", teacher))
    app.add_handler(CommandHandler("addgroup", addgroup))
    app.add_handler(CommandHandler("removegroup", removegroup))
    app.add_handler(CommandHandler("mygroups", mygroups))
    
    # Обработчики кнопок
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_buttons))
    app.add_handler(CallbackQueryHandler(handle_callback_query))
    
    print("✅ Бот запущен!")
    print(f"📊 Интервал проверки: {CHECK_INTERVAL // 60} минут")
    
    # Инициализация и запуск
    async with app:
        # Запускаем мониторинг как фоновую задачу
        asyncio.create_task(monitor_schedule(app))
        # Запускаем polling
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        
        # Держим бота запущенным до прерывания
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await app.updater.stop()
            await app.stop()

def main():
    """Главная функция"""
    # Исправление кодировки для Windows консоли
    import sys
    import io
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    # Запуск через asyncio.run() для Python 3.10+
    try:
        # Запускаем dummy-сервер для Render
        from http.server import HTTPServer, BaseHTTPRequestHandler
        from threading import Thread
        
        class SimpleHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"I am alive!")
        
        def run_server():
            port = int(os.environ.get("PORT", 10000))
            server = HTTPServer(('0.0.0.0', port), SimpleHandler)
            print(f"🌍 Dummy server started on port {port}")
            server.serve_forever()
            
        Thread(target=run_server, daemon=True).start()
        
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")

if __name__ == '__main__':
    main()
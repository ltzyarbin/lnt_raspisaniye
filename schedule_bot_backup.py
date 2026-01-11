import asyncio
import hashlib
import time
import sqlite3
import os
import re
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ========== НАСТРОЙКИ ==========
# Загрузка переменных окружения из .env файла
load_dotenv()

TOKEN = os.getenv('BOT_TOKEN')
ACCESS_CODE = os.getenv('ACCESS_CODE')

# Проверка что секреты загружены
if not TOKEN or not ACCESS_CODE:
    raise ValueError("❌ Не найдены BOT_TOKEN или ACCESS_CODE в .env файле!")

CHECK_INTERVAL = 15 * 60  # 15 минут
DB_FILE = "schedule_bot.db"  # Файл базы данных
MAX_EXTRA_GROUPS = 4  # Максимальное количество дополнительных групп

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
    """Контекстный менеджер для работы с БД"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Доступ к колонкам по имени
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
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                authorized INTEGER DEFAULT 0,
                group_name TEXT,
                subscribed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица для дополнительных групп (для отслеживания расписания друзей/партнёров)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_extra_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                group_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, group_name),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        
        print("✅ База данных инициализирована")

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С БД ==========

def get_user(user_id):
    """Получить пользователя из БД"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        return cursor.fetchone()

def is_authorized(user_id):
    """Проверить авторизован ли пользователь"""
    user = get_user(user_id)
    return user and user['authorized'] == 1

def authorize_user(user_id, username):
    """Авторизовать пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO users (user_id, username, authorized)
            VALUES (?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                authorized = 1,
                username = excluded.username,
                updated_at = CURRENT_TIMESTAMP
        ''', (user_id, username))

    logger.info(f"✅ Пользователь авторизован: {username} (ID: {user_id})")

def set_user_group(user_id, group_name):
    """Установить группу пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users
            SET group_name = ?, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        ''', (group_name, user_id))

    logger.info(f"👥 Пользователь {user_id} установил группу: {group_name}")

def get_user_group(user_id):
    """Получить группу пользователя"""
    user = get_user(user_id)
    return user['group_name'] if user else None

def subscribe_user(user_id):
    """Подписать пользователя на уведомления"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users 
            SET subscribed = 1, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        ''', (user_id,))

def unsubscribe_user(user_id):
    """Отписать пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users 
            SET subscribed = 0, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        ''', (user_id,))

def is_subscribed(user_id):
    """Проверить подписан ли пользователь"""
    user = get_user(user_id)
    return user and user['subscribed'] == 1

def get_all_subscribers():
    """Получить всех подписчиков"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, group_name 
            FROM users 
            WHERE subscribed = 1 AND authorized = 1
        ''')
        return cursor.fetchall()

def get_stats():
    """Статистика по пользователям"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) as total FROM users WHERE authorized = 1')
        total = cursor.fetchone()['total']
        
        cursor.execute('SELECT COUNT(*) as subscribed FROM users WHERE subscribed = 1')
        subscribed = cursor.fetchone()['subscribed']
        
        return {
            'total': total,
            'subscribed': subscribed
        }

# ========== ДОПОЛНИТЕЛЬНЫЕ ГРУППЫ ==========

def add_extra_group(user_id, group_name):
    """Добавить дополнительную группу для отслеживания"""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO user_extra_groups (user_id, group_name)
                VALUES (?, ?)
            ''', (user_id, group_name))
            logger.info(f"➕ Пользователь {user_id} добавил доп. группу: {group_name}")
            return True
        except sqlite3.IntegrityError:
            # Группа уже добавлена
            return False

def remove_extra_group(user_id, group_name):
    """Удалить дополнительную группу"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM user_extra_groups 
            WHERE user_id = ? AND group_name = ?
        ''', (user_id, group_name))
        deleted = cursor.rowcount > 0
        if deleted:
            logger.info(f"➖ Пользователь {user_id} удалил доп. группу: {group_name}")
        return deleted

def get_user_extra_groups(user_id):
    """Получить список дополнительных групп пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT group_name FROM user_extra_groups 
            WHERE user_id = ?
            ORDER BY created_at
        ''', (user_id,))
        return [row['group_name'] for row in cursor.fetchall()]

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
        cursor.execute('''
            SELECT COUNT(*) as count FROM user_extra_groups 
            WHERE user_id = ?
        ''', (user_id,))
        return cursor.fetchone()['count']

# ========== ПОИСК ПРЕПОДАВАТЕЛЯ ==========

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
            if teacher and teacher_lower in teacher.lower():
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
    
    if len(text) <= max_length:
        await update.message.reply_text(text, parse_mode='Markdown')
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
            await update.message.reply_text(part, parse_mode='Markdown')
        else:
            await update.message.reply_text(
                f"_(часть {i+1})_\n\n{part}", 
                parse_mode='Markdown'
            )

# ========== ДЕКОРАТОР АВТОРИЗАЦИИ ==========

def require_auth(func):
    """Декоратор для команд требующих авторизацию"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if not is_authorized(user_id):
            await update.message.reply_text(
                "🔒 *Доступ ограничен*\n\n"
                "Для использования бота введи кодовое слово:\n"
                "`/auth кодовое_слово`",
                parse_mode='Markdown'
            )
            return
        
        return await func(update, context)
    
    return wrapper

# ========== КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие"""
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name
    
    if not is_authorized(user_id):
        await update.message.reply_text(
            f"👋 Привет, *{username}*!\n\n"
            "🔒 Этот бот доступен только для студентов ЛНТ.\n\n"
            "Для доступа введи кодовое слово:\n"
            "`/auth кодовое_слово`\n\n"
            "_Спроси кодовое слово у @o9dos_",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            "👋 *Привет! Я бот расписания ЛНТ*\n\n"
            "🔸 `/setgroup` — выбрать свою группу\n"
            "🔸 `/today` — расписание на сегодня\n"
            "🔸 `/mygroup` — какая группа выбрана\n"
            "🔸 `/subscribe` — подписаться на уведомления\n"
            "🔸 `/unsubscribe` — отписаться\n\n"
            "*Сначала выбери группу: /setgroup*",
            parse_mode='Markdown'
        )

async def auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Авторизация по кодовому слову"""
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    if not context.args:
        await update.message.reply_text(
            "❌ Укажи кодовое слово:\n`/auth кодовое_слово`",
            parse_mode='Markdown'
        )
        return

    code = ' '.join(context.args).lower().strip()

    if code == ACCESS_CODE:
        authorize_user(user_id, username)

        await update.message.reply_text(
            "✅ *Доступ разрешён!*\n\n"
            "Теперь можешь пользоваться ботом.\n"
            "Выбери свою группу: /setgroup",
            parse_mode='Markdown'
        )
    else:
        logger.warning(f"⚠️ Неудачная попытка авторизации: {username} (ID: {user_id}, код: {code})")
        await update.message.reply_text(
            "❌ *Неверное кодовое слово*\n\n"
            "Спроси правильное кодовое слово у одногруппников.",
            parse_mode='Markdown'
        )

@require_auth
async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Расписание на сегодня для всех групп пользователя"""
    user_id = update.effective_user.id

    # Проверка rate limiting
    allowed, wait_time = check_rate_limit(user_id, cooldown=5)
    if not allowed:
        await update.message.reply_text(
            f"⏱️ Подожди {int(wait_time)} сек. перед следующим запросом",
            parse_mode='Markdown'
        )
        logger.warning(f"Rate limit для пользователя {user_id}")
        return

    # Получаем все группы пользователя
    all_groups = get_user_all_groups(user_id)
    
    if not all_groups:
        await update.message.reply_text(
            "❌ Группа не выбрана\n\n"
            "Сначала выбери группу: /setgroup",
            parse_mode='Markdown'
        )
        return

    await update.message.reply_text("⏳ Загружаю расписание...")

    # Получаем полное расписание один раз
    schedule = get_schedule()
    
    if not schedule:
        await update.message.reply_text(
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
            await update.message.reply_text(
                f"⚠️ Группа *{group_name}* не найдена в расписании\n"
                "_Возможно, её нет на сегодня или название указано неверно_",
                parse_mode='Markdown'
            )
    
    if sent_count == 0:
        await update.message.reply_text(
            "📭 Расписание для ваших групп не найдено\n\n"
            "Проверьте названия групп: /mygroups",
            parse_mode='Markdown'
        )

@require_auth
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

@require_auth
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

@require_auth
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

@require_auth
async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отписка"""
    user_id = update.effective_user.id
    unsubscribe_user(user_id)
    await update.message.reply_text("❌ Подписка отменена")

@require_auth
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Помощь — список всех команд"""
    await update.message.reply_text(
        "📖 *Все команды бота:*\n\n"
        "*📅 Расписание:*\n"
        "`/today` — расписание на сегодня\n"
        "`/teacher <фамилия>` — пары преподавателя\n\n"
        "*👥 Группы:*\n"
        "`/setgroup <группа>` — установить основную группу\n"
        "`/mygroup` — показать основную группу\n"
        "`/addgroup <группа>` — добавить доп. группу\n"
        "`/removegroup <группа>` — удалить доп. группу\n"
        "`/mygroups` — все отслеживаемые группы\n\n"
        "*🔔 Уведомления:*\n"
        "`/subscribe` — подписаться на обновления\n"
        "`/unsubscribe` — отписаться\n\n"
        "*ℹ️ Прочее:*\n"
        "`/help` — эта справка\n"
        "`/start` — начало работы\n\n"
        f"_Бот проверяет сайт каждые {CHECK_INTERVAL // 60} минут_",
        parse_mode='Markdown'
    )

# ========== КОМАНДЫ: ПОИСК ПРЕПОДАВАТЕЛЯ ==========

@require_auth
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
    
    await update.message.reply_text("⏳ Ищу расписание преподавателя...")
    
    schedule = get_schedule()
    
    if not schedule:
        await update.message.reply_text(
            "📭 Расписание еще не опубликовано",
            parse_mode='Markdown'
        )
        return
    
    teacher_data = find_teacher_schedule(teacher_name, schedule)
    schedule_date = schedule.get('date', 'Дата не указана')
    
    text = format_teacher_schedule(teacher_name, teacher_data, schedule_date)
    await send_long_message(update, text)

# ========== КОМАНДЫ: ДОПОЛНИТЕЛЬНЫЕ ГРУППЫ ==========

@require_auth
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

@require_auth
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

@require_auth
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

async def main_async():
    """Асинхронная главная функция"""
    print("🤖 Запуск бота...")
    print(f"🔐 Кодовое слово: {ACCESS_CODE}")
    
    # Инициализация БД
    init_db()
    
    # Статистика
    stats = get_stats()
    print(f"👥 Пользователей: {stats['total']}")
    print(f"🔔 Подписчиков: {stats['subscribed']}")
    
    # Создание приложения
    app = Application.builder().token(TOKEN).build()
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("auth", auth))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("setgroup", setgroup))
    app.add_handler(CommandHandler("mygroup", mygroup))
    app.add_handler(CommandHandler("subscribe", subscribe))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe))
    app.add_handler(CommandHandler("help", help_command))
    # Новые команды
    app.add_handler(CommandHandler("teacher", teacher))
    app.add_handler(CommandHandler("addgroup", addgroup))
    app.add_handler(CommandHandler("removegroup", removegroup))
    app.add_handler(CommandHandler("mygroups", mygroups))
    
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
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")

if __name__ == '__main__':
    main()
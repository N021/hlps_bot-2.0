# ===============================
# ЧАСТИНА 1: ІМПОРТИ ТА БАЗОВІ НАЛАШТУВАННЯ
# ===============================

import logging
import pandas as pd
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, CallbackQueryHandler
import os
import json
import asyncio
from telegram.ext import ApplicationBuilder
import ssl
from aiohttp import web

# ===============================
# ЧАСТИНА 2: КОНФІГУРАЦІЯ ТА ГЛОБАЛЬНІ ЗМІННІ
# ===============================

# Налаштування порту
PORT = int(os.environ.get("PORT", "10000"))

# Налаштування логування
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# ДОДАНО: Налаштування для детального дебагу розрахунків
DEBUG_SCORING = os.environ.get("DEBUG_SCORING", "true").lower() == "true"
VALIDATE_CALCULATIONS = True  # Перевіряти правильність підрахунків

# Етапи розмови
REGION, WAITING_REGION_SUBMIT, CATEGORY, WAITING_STYLE_SUBMIT, WAITING_PURPOSE_SUBMIT = range(5)

# Зберігання даних користувача
user_data_global = {}
hotel_data = None  # Глобальна змінна для даних готелів

# ДОДАНО: Константи для системи балів (для легшого налаштування)
MAIN_CATEGORY_POINTS = [21, 18, 15, 12, 9, 6, 3]
ADJACENT_CATEGORY_POINTS = [7, 6, 5, 4, 3, 2, 1]
REGION_POINTS = [21, 18, 15, 12, 9, 6, 3]

# ДОДАНО: Середні рейтинги програм лояльності (на основі Google Maps)
LOYALTY_PROGRAM_RATINGS = {
    "ALL - Accor Live Limitless": 4.11,
    "Choice Privileges": 3.68,
    "Hilton Honors": 4.19,
    "IHG One Rewards": 4.15,
    "Marriott Bonvoy": 4.22,
    "World of Hyatt": 4.28,
    "Wyndham Rewards": 3.17
}

# ДОДАНО: Глобальна змінна для зберігання останніх результатів користувача (для команди /more)
user_last_results = {}

# ДОДАНО: Функція для логування дебагу (якщо потрібно)
def debug_log(message):
    """Логування для дебагу розрахунків"""
    if DEBUG_SCORING:
        logger.info(f"[DEBUG] {message}")

# ВИПРАВЛЕНО: Функція для валідації розрахунків
def validate_score_calculation(calculated_total, detailed_breakdown, program_name="Unknown"):
    """
    Перевіряє, чи сума детальних балів дорівнює загальному балу
    
    Args:
        calculated_total: загальний розрахований бал
        detailed_breakdown: словник з детальними балами {'region': X, 'category': Y, ...}
        program_name: назва програми для логування
    
    Returns:
        bool: True якщо розрахунки правильні
    """
    if not VALIDATE_CALCULATIONS:
        return True
    
    detailed_sum = sum(detailed_breakdown.values())
    difference = abs(calculated_total - detailed_sum)
    
    if difference > 0.01:  # Допускаємо невелику похибку через округлення
        logger.warning(
            f"VALIDATION ERROR for {program_name}: "
            f"Total={calculated_total:.2f}, Detailed sum={detailed_sum:.2f}, "
            f"Difference={difference:.2f}, Breakdown={detailed_breakdown}"
        )
        return False
    
    debug_log(f"Validation OK for {program_name}: {calculated_total:.2f} = {detailed_breakdown}")
    return True

# ДОДАНО: Функція для отримання рейтингу програми лояльності
def get_program_rating(program_name):
    """
    Повертає середній рейтинг програми лояльності
    
    Args:
        program_name: назва програми лояльності
    
    Returns:
        float: середній рейтинг (або 4.0 за замовчуванням)
    """
    return LOYALTY_PROGRAM_RATINGS.get(program_name, 4.0)

# ДОДАНО: Функція для розрахунку рейтинг-коефіцієнта
def calculate_rating_coefficient(program_rating):
    """
    Розраховує коефіцієнт на основі рейтингу програми
    
    Args:
        program_rating: рейтинг програми (1-5)
    
    Returns:
        float: коефіцієнт (рейтинг/5.0)
    """
    return program_rating / 5.0


# ===============================
# ЧАСТИНА 2.5: ФУНКЦІЇ ПЕРЕКЛАДУ
# ===============================

def translate_regions_to_english(regions):
    """Переводить список регіонів з української на англійську"""
    translation_map = {
        "Європа": "Europe",
        "Північна Америка": "North America", 
        "Азія": "Asia",
        "Близький Схід": "Middle East",
        "Африка": "Africa",
        "Південна Америка": "South America",
        "Карибський басейн": "Caribbean",
        "Океанія": "Oceania"
    }
    
    if not regions:
        return []
    
    translated = []
    for region in regions:
        # Якщо регіон вже англійською, залишаємо як є
        if region in translation_map.values():
            translated.append(region)
        # Якщо українською, перекладаємо
        elif region in translation_map:
            translated.append(translation_map[region])
        else:
            # Якщо не знайдено переклад, залишаємо оригінал
            translated.append(region)
            logger.warning(f"Не знайдено переклад для регіону: {region}")
    
    return translated

def translate_styles_to_english(styles):
    """Переводить список стилів з української на англійську"""
    translation_map = {
        "Розкішний і вишуканий": "Luxurious and refined",
        "Бутік і унікальний": "Boutique and unique", 
        "Класичний і традиційний": "Classic and traditional",
        "Сучасний і дизайнерський": "Modern and designer",
        "Затишний і сімейний": "Cozy and family-friendly",
        "Практичний і економічний": "Practical and economical"
    }
    
    if not styles:
        return []
    
    translated = []
    for style in styles:
        # Якщо стиль вже англійською, залишаємо як є
        if style in translation_map.values():
            translated.append(style)
        # Якщо українською, перекладаємо
        elif style in translation_map:
            translated.append(translation_map[style])
        else:
            # Якщо не знайдено переклад, залишаємо оригінал
            translated.append(style)
            logger.warning(f"Не знайдено переклад для стилю: {style}")
    
    return translated

def translate_purposes_to_english(purposes):
    """Переводить список цілей з української на англійську"""
    translation_map = {
        "Бізнес-подорожі / відрядження": "Business travel",
        "Відпустка / релакс": "Vacation / relaxation",
        "Сімейний відпочинок": "Family vacation", 
        "Довготривале проживання": "Long-term stay"
    }
    
    if not purposes:
        return []
    
    translated = []
    for purpose in purposes:
        # Якщо мета вже англійською, залишаємо як є
        if purpose in translation_map.values():
            translated.append(purpose)
        # Якщо українською, перекладаємо
        elif purpose in translation_map:
            translated.append(translation_map[purpose])
        else:
            # Якщо не знайдено переклад, залишаємо оригінал
            translated.append(purpose)
            logger.warning(f"Не знайдено переклад для мети: {purpose}")
    
    return translated



# ===============================
# ЧАСТИНА 3: ФУНКЦІЇ АНАЛІЗУ CSV ТА ЗАВАНТАЖЕННЯ ДАНИХ
# ===============================

def analyze_csv_structure(df):
    """
    Аналізує структуру CSV файлу та записує інформацію в лог
    
    Args:
        df: DataFrame з даними про готелі
    """
    logger.info("CSV structure analysis:")
    logger.info(f"Number of rows: {len(df)}")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Перевірка унікальних значень
    if 'loyalty_program' in df.columns:
        logger.info(f"Loyalty programs: {df['loyalty_program'].unique()}")
    
    if 'region' in df.columns:
        logger.info(f"Regions: {df['region'].unique()}")
    
    if 'segment' in df.columns:
        logger.info(f"Segments: {df['segment'].unique()}")
    
    # Перевірка на відсутні значення
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"Missing values: {null_counts[null_counts > 0]}")
    
    # Перевірка типів даних
    logger.info(f"Data types: {df.dtypes}")

def load_hotel_data(csv_path):
    """Завантаження даних програм лояльності з CSV файлу"""
    try:
        # Перевірка існування файлу
        if not os.path.exists(csv_path):
            logger.error(f"File not found: {csv_path}")
            return None
            
        df = pd.read_csv(csv_path)
        
        # Аналіз структури CSV
        analyze_csv_structure(df)
        
        # Базова валідація даних - з очікуваними назвами колонок
        expected_columns = ['loyalty_program', 'region', 'country', 'Hotel Brand', 'segment',
                            'Total hotels of Corporation / Loyalty Program in this region',
                            'Total hotels of Corporation / Loyalty Program in this country']
        
        # Перевірка колонок та створення маппінгу для перейменування
        rename_mapping = {}
        
        # Перевірка на 'Hotel Brand' або 'brand' колонку
        if 'brand' in df.columns and 'Hotel Brand' not in df.columns:
            rename_mapping['brand'] = 'Hotel Brand'
            logger.info("Renamed column 'brand' to 'Hotel Brand'")
        
        # Перевірка на 'segment' або 'category' колонку
        if 'category' in df.columns and 'segment' not in df.columns:
            rename_mapping['category'] = 'segment'
            logger.info("Renamed column 'category' to 'segment'")
        
        # Якщо є колонка з коротшою назвою для регіонів
        if 'region_hotels' in df.columns and 'Total hotels of Corporation / Loyalty Program in this region' not in df.columns:
            rename_mapping['region_hotels'] = 'Total hotels of Corporation / Loyalty Program in this region'
            logger.info("Renamed column 'region_hotels'")
        
        # Якщо є колонка з коротшою назвою для країн
        if 'country_hotels' in df.columns and 'Total hotels of Corporation / Loyalty Program in this country' not in df.columns:
            rename_mapping['country_hotels'] = 'Total hotels of Corporation / Loyalty Program in this country'
            logger.info("Renamed column 'country_hotels'")
        
        # Застосувати перейменування, якщо потрібно
        if rename_mapping:
            df = df.rename(columns=rename_mapping)
            logger.info(f"Renamed columns: {rename_mapping}")
        
        # Перевірка чи існують необхідні колонки після перейменування
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"After renaming, still missing columns: {missing_columns}")
            
            # Створення відсутніх колонок з порожніми значеннями
            for col in missing_columns:
                df[col] = ''
                logger.warning(f"Created empty column: {col}")
        
        return df
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return None


# ===============================
# ЧАСТИНА 4: ОСНОВНІ TELEGRAM ОБРОБНИКИ
# ===============================

# Функція старту бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    
    # Завжди очищати дані користувача при використанні команди /start
    if user_id in user_data_global:
        del user_data_global[user_id]
    
    # Ініціалізація нових даних з українською мовою за замовчуванням
    user_data_global[user_id] = {'language': 'uk'}
    
    # Логування початку нової розмови
    logger.info(f"User {user_id} started a new conversation. Data cleared.")
    
    # Одразу переходимо до першого питання про регіони
    return await ask_region(update, context)

# Функція обробки вибору мови
async def language_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір мови користувачем через InlineKeyboard"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    if callback_data == 'lang_uk':
        user_data_global[user_id]['language'] = 'uk'
        await query.edit_message_text(
            "Дякую! Я продовжу спілкування українською мовою."
        )
        # Коротка пауза перед наступним питанням
        await asyncio.sleep(0.3)
        return await ask_region(update, context)
    
    elif callback_data == 'lang_en':
        user_data_global[user_id]['language'] = 'en'
        await query.edit_message_text(
            "Thank you! I will continue our conversation in English."
        )
        # Коротка пауза перед наступним питанням
        await asyncio.sleep(0.3)
        return await ask_region(update, context)
    
    else:
        user_data_global[user_id]['language'] = 'en'  # За замовчуванням англійська
        await query.edit_message_text(
            "I'll continue in English. If you need another language, please let me know."
        )
        # Коротка пауза перед наступним питанням
        await asyncio.sleep(0.3)
        return await ask_region(update, context)

# ДОДАНО: Функція для команди /more
async def show_more_details(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показує детальний розбір останніх результатів - ОНОВЛЕНА версія без пояснень"""
    user_id = update.effective_user.id
    
    # Перевіряємо, чи є збережені результати для цього користувача
    if user_id not in user_last_results:
        lang = user_data_global.get(user_id, {}).get('language', 'uk')
        
        if lang == 'uk':
            await update.message.reply_text(
                "У вас немає останніх результатів для відображення деталей.\n"
                "Спочатку пройдіть опитування командою /start."
            )
        else:
            await update.message.reply_text(
                "You don't have recent results to show details.\n"
                "Please complete the survey first with /start command."
            )
        return ConversationHandler.END
    
    # Отримуємо збережені дані
    saved_data = user_last_results[user_id]
    user_data = saved_data['user_data']
    scores_df = saved_data['scores_df']
    lang = user_data.get('language', 'uk')
    
    try:
        # Генерируем ПРОСТИЙ детальний звіт
        detailed_results = format_detailed_results_with_ratings(user_data, scores_df, lang)
        
        # Відправляємо детальні результати БЕЗ пояснень системи балів
        if lang == 'uk':
            intro_text = "🎉 Детальний аналіз завершено!\n\n"
            outro_text = "\n\nЩоб почати нове опитування, надішліть команду /start."
        else:
            intro_text = "🎉 Detailed analysis completed!\n\n"
            outro_text = "\n\nTo start a new survey, send the /start command."
        
        # Відправляємо простий детальний звіт
        full_message = intro_text + detailed_results + outro_text
        await send_long_message_to_chat(context, update.message.chat_id, full_message)
        
    except Exception as e:
        logger.error(f"Помилка при показі детальних результатів: {e}")
        
        if lang == 'uk':
            await update.message.reply_text(
                "Виникла помилка при відображенні детальних результатів. "
                "Спробуйте пройти опитування знову командою /start."
            )
        else:
            await update.message.reply_text(
                "An error occurred while displaying detailed results. "
                "Please try taking the survey again with /start command."
            )
    
    return ConversationHandler.END

# ДОДАНО: Функція для команди /21 (адміністративна)
async def show_admin_scoring_breakdown(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показує повний розбір нарахування балів для всіх програм - АДМІНІСТРАТИВНА КОМАНДА"""
    user_id = update.effective_user.id
    
    # Перевіряємо, чи є збережені результати для цього користувача
    if user_id not in user_last_results:
        await update.message.reply_text(
            "Немає останніх результатів для відображення розбору балів.\n"
            "Спочатку пройдіть опитування командою /start."
        )
        return ConversationHandler.END
    
    # Отримуємо збережені дані
    saved_data = user_last_results[user_id]
    user_data = saved_data['user_data']
    scores_df = saved_data['scores_df']
    
    try:
        # Генеруємо ПОВНИЙ адміністративний звіт
        admin_report = format_admin_scoring_report(user_data, scores_df)
        
        # Відправляємо адміністративний звіт
        intro_text = "🎉 Звіт по нарахуванню балів!\n\nОсь 7 програм лояльності готелів з повним розбором балів:\n\n"
        full_message = intro_text + admin_report
        await send_long_message_to_chat(context, update.message.chat_id, full_message)
        
    except Exception as e:
        logger.error(f"Помилка при показі адміністративного розбору: {e}")
        await update.message.reply_text(
            "Виникла помилка при відображенні розбору балів. "
            "Спробуйте пройти опитування знову командою /start."
        )
    
    return ConversationHandler.END


# Функція скасування
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Скасовує розмову з командою /cancel"""
    user = update.message.from_user
    user_id = user.id
    logger.info(f"User {user_id} canceled the conversation.")
    
    lang = user_data_global.get(user_id, {}).get('language', 'en')
    
    # Повідомлення про завершення розмови
    if lang == 'uk':
        await update.message.reply_text(
            "Розмову завершено. Щоб почати знову, надішліть команду /start."
        )
    else:
        await update.message.reply_text(
            "Conversation ended. To start again, send the /start command."
        )
    
    # Видаляємо дані користувача
    if user_id in user_data_global:
        del user_data_global[user_id]
        logger.info(f"User data {user_id} successfully deleted")
    
    # ДОДАНО: Також видаляємо збережені результати
    if user_id in user_last_results:
        del user_last_results[user_id]
        logger.info(f"User last results {user_id} successfully deleted")
    
    # Очищаємо контекст, якщо він доступний
    if hasattr(context, 'user_data'):
        context.user_data.clear()
    
    return ConversationHandler.END

# ===============================
# ЧАСТИНА 5: ОБРОБНИКИ РЕГІОНІВ
# ===============================

# Функції вибору регіону
async def ask_region(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про регіони подорожі з чекбоксами"""
    # Визначаємо, чи це відповідь на callback_query або новий запит
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
        message_id = query.message.message_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
        message_id = None
    
    lang = user_data_global[user_id]['language']
    
    # Ініціалізуємо вибрані регіони, якщо їх ще не обрано
    if 'selected_regions' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_regions'] = []
    
    # Створюємо InlineKeyboard з чекбоксами
    if lang == 'uk':
        regions = [
            "Європа", "Північна Америка", "Азія",
            "Близький Схід", "Африка", "Південна Америка",
            "Карибський басейн", "Океанія"
        ]
        
        regions_description = (
            "Питання 1/4:\n"
            "У яких регіонах світу ви плануєте подорожувати?\n"
            "*(Оберіть один або декілька варіантів)*\n\n"
            "1. Європа\n"
            "2. Північна Америка\n"
            "3. Азія\n"
            "4. Близький Схід\n"
            "5. Африка\n"
            "6. Південна Америка\n"
            "7. Карибський басейн\n"
            "8. Океанія"
        )
        
        title_text = regions_description
        submit_text = "Відповісти"
    else:
        regions = [
            "Europe", "North America", "Asia",
            "Middle East", "Africa", "South America",
            "Caribbean", "Oceania"
        ]
        
        regions_description = (
            "Question 1/4:\n"
            "In which regions of the world are you planning to travel?\n"
            "*(Select one or multiple options)*\n\n"
            "1. Europe\n"
            "2. North America\n"
            "3. Asia\n"
            "4. Middle East\n"
            "5. Africa\n"
            "6. South America\n"
            "7. Caribbean\n"
            "8. Oceania"
        )
        
        title_text = regions_description
        submit_text = "Submit"
    
    # Створюємо клавіатуру з чекбоксами для регіонів
    keyboard = []
    selected_regions = user_data_global[user_id]['selected_regions']
    
    # Групуємо регіони по 2 в ряду з номерами
    for i in range(0, len(regions), 2):
        row = []
        for j in range(2):
            if i + j < len(regions):
                region = regions[i + j]
                region_index = i + j + 1
                checkbox = "✅ " if region in selected_regions else "☐ "
                row.append(InlineKeyboardButton(
                    f"{checkbox}{region_index}. {region}", 
                    callback_data=f"region_{region}"
                ))
        keyboard.append(row)
    
    # Додаємо кнопку "Відповісти" внизу
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="region_submit")])
    
    # Використовуємо edit_message_text, якщо це оновлення існуючого повідомлення
    if message_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=title_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Error updating message: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=title_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=title_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    return WAITING_REGION_SUBMIT

async def region_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір регіону через чекбокси"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    # Якщо користувач натиснув "Відповісти"
    if callback_data == "region_submit":
        selected_regions = user_data_global[user_id]['selected_regions']
        lang = user_data_global[user_id]['language']
        
        # Перевіряємо, чи вибрано хоча б один регіон
        if not selected_regions:
            if lang == 'uk':
                await query.answer("Будь ласка, виберіть хоча б один регіон", show_alert=True)
            else:
                await query.answer("Please select at least one region", show_alert=True)
            return WAITING_REGION_SUBMIT
        
        # Зберігаємо вибрані регіони
        user_data_global[user_id]['regions'] = selected_regions
        user_data_global[user_id]['countries'] = None
        
        # Оновлюємо повідомлення, видаляючи клавіатуру
        await query.edit_message_text(text=query.message.text)
        
        # Надсилаємо нове повідомлення з підтвердженням
        if lang == 'uk':
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Дякую! Ви обрали наступні регіони: {', '.join(selected_regions)}."
            )
        else:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Thank you! You have chosen the following regions: {', '.join(selected_regions)}."
            )
        
        await asyncio.sleep(0.3)
        return await ask_category(update, context)
    
    # Якщо це вибір регіону
    else:
        region = callback_data.replace("region_", "")
        
        # Перемикаємо стан вибору регіону
        if region in user_data_global[user_id]['selected_regions']:
            user_data_global[user_id]['selected_regions'].remove(region)
        else:
            user_data_global[user_id]['selected_regions'].append(region)
        
        # Оновлюємо клавіатуру
        return await ask_region(update, context)

# ===============================
# ЧАСТИНА 6: ОБРОБНИКИ КАТЕГОРІЙ
# ===============================

# Функції категорії
async def ask_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про категорію готелю"""
    # Визначаємо, чи це відповідь на callback_query
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
    
    lang = user_data_global[user_id]['language']
    
    # Створюємо InlineKeyboard для вибору категорії
    if lang == 'uk':
        keyboard = [
            [InlineKeyboardButton("1. Luxury (преміум-клас)", callback_data='category_Luxury')],
            [InlineKeyboardButton("2. Comfort (середній клас)", callback_data='category_Comfort')],
            [InlineKeyboardButton("3. Standard (економ-клас)", callback_data='category_Standard')]
        ]
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "Питання 2/4:\n"
                "Яку категорію готелів ви зазвичай обираєте?\n\n"
                "1. Luxury (преміум-клас)\n"
                "2. Comfort (середній клас)\n"
                "3. Standard (економ-клас)\n"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        keyboard = [
            [InlineKeyboardButton("1. Luxury (premium class)", callback_data='category_Luxury')],
            [InlineKeyboardButton("2. Comfort (middle class)", callback_data='category_Comfort')],
            [InlineKeyboardButton("3. Standard (economy class)", callback_data='category_Standard')]
        ]
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "Question 2/4:\n"
                "Which hotel category do you usually choose?\n\n"
                "1. Luxury (premium class)\n"
                "2. Comfort (middle class)\n"
                "3. Standard (economy class)\n"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    return CATEGORY

async def category_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    callback_data = query.data
    lang = user_data_global[user_id]['language']

    category = callback_data.replace("category_", "")
    user_data_global[user_id]['category'] = category

    # Видаляємо клавіатуру з попереднього повідомлення
    await query.edit_message_text(
        text=query.message.text,
        reply_markup=None
    )

    # Надсилаємо НОВЕ повідомлення з підтвердженням вибору
    if lang == 'uk':
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"Дякую! Ви обрали категорію: {category}."
        )
    else:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"Thank you! You have chosen the category: {category}."
        )

    await asyncio.sleep(0.3)

    return await ask_style(update, context)

# ===============================
# ЧАСТИНА 7: ОБРОБНИКИ СТИЛЮ
# ===============================

def get_styles_for_category(category, lang='uk'):
    """
    Повертає список стилів, які відповідають обраній категорії готелю
    """
    if lang == 'uk':
        all_styles = [
            "Розкішний і вишуканий", 
            "Бутік і унікальний", 
            "Класичний і традиційний", 
            "Сучасний і дизайнерський",
            "Затишний і сімейний", 
            "Практичний і економічний"
        ]
        
        if category == "Luxury":
            # Для Luxury виключаємо "Практичний і економічний"
            return [style for style in all_styles if style != "Практичний і економічний"]
        elif category == "Standard":
            # Для Standard виключаємо "Розкішний і вишуканий" і "Бутік і унікальний"
            return [style for style in all_styles if style not in ["Розкішний і вишуканий", "Бутік і унікальний"]]
        else:
            # Для Comfort залишаємо всі стилі
            return all_styles
    else:
        all_styles = [
            "Luxurious and refined", 
            "Boutique and unique",
            "Classic and traditional", 
            "Modern and designer",
            "Cozy and family-friendly", 
            "Practical and economical"
        ]
        
        if category == "Luxury":
            # Для Luxury виключаємо "Practical and economical"
            return [style for style in all_styles if style != "Practical and economical"]
        elif category == "Standard":
            # Для Standard виключаємо "Luxurious and refined" і "Boutique and unique"
            return [style for style in all_styles if style not in ["Luxurious and refined", "Boutique and unique"]]
        else:
            # Для Comfort залишаємо всі стилі
            return all_styles

async def ask_style(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про стиль готелю з чекбоксами та фільтрацією по категорії"""
    
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
    
    lang = user_data_global[user_id]['language']
    
    # Отримуємо обрану категорію
    category = user_data_global[user_id].get('category')
    
    # Ініціалізуємо вибрані стилі, якщо їх ще не обрано
    if 'selected_styles' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_styles'] = []
    
    # НОВОЕ: Отримуємо фільтровані стилі відповідно до категорії
    styles = get_styles_for_category(category, lang)
    
    # НОВОЕ: Очищуємо вибрані стилі, якщо вони більше не доступні для цієї категорії
    user_data_global[user_id]['selected_styles'] = [
        style for style in user_data_global[user_id]['selected_styles'] 
        if style in styles
    ]
    
    # Створюємо InlineKeyboard з чекбоксами для стилів
    if lang == 'uk':
        styles_description = (
            "Питання 3/4:\n"
            "Який стиль готелю ви зазвичай обираєте?\n"
            "*(Оберіть до трьох варіантів)*\n\n"
        )
        
        # НОВОЕ: Динамічно генеруємо описи тільки для доступних стилів
        style_descriptions = {
            "Розкішний і вишуканий": "**Розкішний і вишуканий** (преміум-матеріали, елегантний дизайн, високий рівень сервісу)",
            "Бутік і унікальний": "**Бутік і унікальний** (оригінальний інтер'єр, творча атмосфера, відчуття ексклюзивності)",
            "Класичний і традиційний": "**Класичний і традиційний** (перевірений часом стиль, консервативність, історичність)",
            "Сучасний і дизайнерський": "**Сучасний і дизайнерський** (модні інтер'єри, мінімалізм, технологічність)",
            "Затишний і сімейний": "**Затишний і сімейний** (тепла атмосфера, комфорт, дружній до дітей)",
            "Практичний і економічний": "**Практичний і економічний** (без зайвих деталей, функціональний, доступний)"
        }
        
        # Додаємо описи тільки для доступних стилів
        for i, style in enumerate(styles):
            styles_description += f"{i+1}. {style_descriptions[style]}\n"
        
        title_text = styles_description
        submit_text = "Відповісти"
    else:
        styles_description = (
            "Question 3/4:\n"
            "What hotel style do you usually choose?\n"
            "*(Choose up to three options)*\n\n"
        )
        
        # НОВОЕ: Динамічно генеруємо описи тільки для доступних стилів
        style_descriptions = {
            "Luxurious and refined": "**Luxurious and refined** (premium materials, elegant design, high level of service)",
            "Boutique and unique": "**Boutique and unique** (original interior, creative atmosphere, sense of exclusivity)",
            "Classic and traditional": "**Classic and traditional** (time-tested style, conservatism, historical ambiance)",
            "Modern and designer": "**Modern and designer** (fashionable interiors, minimalism, technological features)",
            "Cozy and family-friendly": "**Cozy and family-friendly** (warm atmosphere, comfort, child-friendly)",
            "Practical and economical": "**Practical and economical** (no unnecessary details, functional, affordable)"
        }
        
        # Додаємо описи тільки для доступних стилів
        for i, style in enumerate(styles):
            styles_description += f"{i+1}. {style_descriptions[style]}\n"
        
        title_text = styles_description
        submit_text = "Submit"
    
    # Створюємо клавіатуру з чекбоксами для стилів
    keyboard = []
    selected_styles = user_data_global[user_id]['selected_styles']
    
    # Додаємо стилі з номерами
    for i, style in enumerate(styles):
        checkbox = "✅ " if style in selected_styles else "☐ "
        keyboard.append([InlineKeyboardButton(
            f"{checkbox}{i+1}. {style}", 
            callback_data=f"style_{style}"
        )])
    
    # Додаємо кнопку "Відповісти" внизу
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="style_submit")])
    
    # Перевіряємо, чи це оновлення існуючого повідомлення зі стилями
    if 'style_message_id' in user_data_global[user_id]:
        try:
            # Оновлюємо існуюче повідомлення зі стилями
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=user_data_global[user_id]['style_message_id'],
                text=title_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
            return WAITING_STYLE_SUBMIT
        except Exception as e:
            logger.error(f"Error updating style message: {e}")
            # Видаляємо недійсний ID повідомлення
            del user_data_global[user_id]['style_message_id']
    
    # Надсилаємо НОВЕ повідомлення для питання 3/4
    try:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=title_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        # Зберігаємо ID повідомлення для майбутніх оновлень
        user_data_global[user_id]['style_message_id'] = message.message_id
    except Exception as e:
        logger.error(f"Error sending style message: {e}")
        # Відправляємо без Markdown, якщо є проблеми з форматуванням
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=title_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        user_data_global[user_id]['style_message_id'] = message.message_id
    
    return WAITING_STYLE_SUBMIT

async def style_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір стилю через чекбокси"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    # Якщо користувач натиснув "Відповісти"
    if callback_data == "style_submit":
        selected_styles = user_data_global[user_id]['selected_styles']
        lang = user_data_global[user_id]['language']
        
        # Перевіряємо, чи вибрано хоча б один стиль
        if not selected_styles:
            if lang == 'uk':
                await query.answer("Будь ласка, виберіть хоча б один стиль", show_alert=True)
            else:
                await query.answer("Please select at least one style", show_alert=True)
            return WAITING_STYLE_SUBMIT
        
        # Обмеження до трьох варіантів
        if len(selected_styles) > 3:
            original_count = len(selected_styles)
            user_data_global[user_id]['selected_styles'] = selected_styles[:3]
            
            if lang == 'uk':
                await query.answer(
                    f"Ви обрали {original_count} стилів, але дозволено максимум 3. "
                    f"Враховано тільки перші три стилі.", 
                    show_alert=True
                )
            else:
                await query.answer(
                    f"You selected {original_count} styles, but a maximum of 3 is allowed. "
                    f"Only the first three have been considered.", 
                    show_alert=True
                )
            # Оновлюємо вибір та клавіатуру
            return await ask_style(update, context)
        
        # Зберігаємо вибрані стилі
        user_data_global[user_id]['styles'] = selected_styles
        
        # Видаляємо клавіатуру, але зберігаємо текст питання 3/4
        try:
            await query.edit_message_text(text=query.message.text, reply_markup=None, parse_mode="Markdown")
        except:
            await query.edit_message_text(text=query.message.text, reply_markup=None)
        
        # Надсилаємо НОВЕ повідомлення з підтвердженням вибору
        if lang == 'uk':
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Дякую! Ви обрали наступні стилі: {', '.join(selected_styles)}."
            )
        else:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Thank you! You have chosen the following styles: {', '.join(selected_styles)}."
            )
        
        # Очищуємо ID повідомлення зі стилем
        if 'style_message_id' in user_data_global[user_id]:
            del user_data_global[user_id]['style_message_id']
        
        await asyncio.sleep(0.3)
        return await ask_purpose(update, context)
    
    # Якщо це вибір або скасування вибору стилю
    else:
        style = callback_data.replace("style_", "")
        
        # Перевіряємо, чи не перевищено максимальну кількість стилів (3)
        if style not in user_data_global[user_id]['selected_styles'] and len(user_data_global[user_id]['selected_styles']) >= 3:
            lang = user_data_global[user_id]['language']
            if lang == 'uk':
                await query.answer("Ви вже обрали максимальну кількість стилів (3)", show_alert=True)
            else:
                await query.answer("You have already selected the maximum number of styles (3)", show_alert=True)
            return WAITING_STYLE_SUBMIT
        
        # Перемикаємо стан вибору стилю
        if style in user_data_global[user_id]['selected_styles']:
            user_data_global[user_id]['selected_styles'].remove(style)
        else:
            user_data_global[user_id]['selected_styles'].append(style)
        
        # Оновлюємо клавіатуру з новим вибором
        return await ask_style(update, context)

# ===============================
# ЧАСТИНА 8: ОБРОБНИКИ МЕТИ ПОДОРОЖІ
# ===============================

async def ask_purpose(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про мету подорожі з чекбоксами та детальними описами"""
    
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
    
    lang = user_data_global[user_id]['language']
    
    # Ініціалізуємо вибрані цілі, якщо їх ще не обрано
    if 'selected_purposes' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_purposes'] = []
    
    # Створюємо InlineKeyboard з чекбоксами для цілей
    if lang == 'uk':
        purposes = [
            "Бізнес-подорожі / відрядження",
            "Відпустка / релакс",
            "Сімейний відпочинок",
            "Довготривале проживання"
        ]
        
        purpose_description = (
            "Питання 4/4:\n"
            "З якою метою ви зазвичай зупиняєтесь у готелі?\n"
            "*(Оберіть до двох варіантів)*\n\n"
            "1. **Бізнес-подорожі / відрядження** (зручність для роботи, доступ до ділових центрів)\n"
            "2. **Відпустка / релакс** (комфорт, розваги, відпочинок)\n"
            "3. **Сімейний відпочинок** (розваги для дітей, сімейні номери)\n"
            "4. **Довготривале проживання** (відчуття дому, кухня, пральня)"
        )
        
        title_text = purpose_description
        submit_text = "Відповісти"
    else:
        purposes = [
            "Business travel",
            "Vacation / relaxation",
            "Family vacation",
            "Long-term stay"
        ]
        
        purpose_description = (
            "Question 4/4:\n"
            "For what purpose do you usually stay at a hotel?\n"
            "*(Choose up to two options)*\n\n"
            "1. **Business travel** (convenience for work, access to business centers)\n"
            "2. **Vacation / relaxation** (comfort, entertainment, rest)\n"
            "3. **Family vacation** (activities for children, family rooms)\n"
            "4. **Long-term stay** (home feeling, kitchen, laundry)"
        )
        
        title_text = purpose_description
        submit_text = "Submit"
    
    # Створюємо клавіатуру з чекбоксами для цілей з номерами
    keyboard = []
    selected_purposes = user_data_global[user_id]['selected_purposes']
    
    # Додаємо цілі з номерами
    for i, purpose in enumerate(purposes):
        checkbox = "✅ " if purpose in selected_purposes else "☐ "
        keyboard.append([InlineKeyboardButton(
            f"{checkbox}{i+1}. {purpose}", 
            callback_data=f"purpose_{purpose}"
        )])
    
    # Додаємо кнопку "Відповісти" внизу
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="purpose_submit")])
    
    # Перевіряємо, чи це оновлення існуючого повідомлення з метою
    if 'purpose_message_id' in user_data_global[user_id]:
        try:
            # Оновлюємо існуюче повідомлення з метою
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=user_data_global[user_id]['purpose_message_id'],
                text=title_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
            return WAITING_PURPOSE_SUBMIT
        except Exception as e:
            logger.error(f"Error updating purpose message: {e}")
            # Видаляємо недійсний ID повідомлення
            del user_data_global[user_id]['purpose_message_id']
    
    # Надсилаємо НОВЕ повідомлення для питання 4/4
    try:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=title_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        # Зберігаємо ID повідомлення для майбутніх оновлень
        user_data_global[user_id]['purpose_message_id'] = message.message_id
    except Exception as e:
        logger.error(f"Error sending purpose message: {e}")
        # Відправляємо без Markdown, якщо є проблеми з форматуванням
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=title_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        user_data_global[user_id]['purpose_message_id'] = message.message_id
    
    return WAITING_PURPOSE_SUBMIT

async def purpose_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір мети через чекбокси"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    # Якщо користувач натиснув "Відповісти"
    if callback_data == "purpose_submit":
        selected_purposes = user_data_global[user_id]['selected_purposes']
        lang = user_data_global[user_id]['language']
        
        # Перевіряємо, чи вибрано хоча б одну мету
        if not selected_purposes:
            if lang == 'uk':
                await query.answer("Будь ласка, виберіть хоча б одну мету", show_alert=True)
            else:
                await query.answer("Please select at least one purpose", show_alert=True)
            return WAITING_PURPOSE_SUBMIT
        
        # Обмеження до двох варіантів
        if len(selected_purposes) > 2:
            original_count = len(selected_purposes)
            user_data_global[user_id]['selected_purposes'] = selected_purposes[:2]
            
            if lang == 'uk':
                await query.answer(
                    f"Ви обрали {original_count} цілей, але дозволено максимум 2. "
                    f"Враховано тільки перші дві цілі.", 
                    show_alert=True
                )
            else:
                await query.answer(
                    f"You selected {original_count} purposes, but a maximum of 2 is allowed. "
                    f"Only the first two have been considered.", 
                    show_alert=True
                )
            # Оновлюємо вибір та клавіатуру
            return await ask_purpose(update, context)
        
        # Зберігаємо вибрані цілі
        user_data_global[user_id]['purposes'] = selected_purposes
        
        # Видаляємо клавіатуру, але зберігаємо текст питання 4/4
        try:
            await query.edit_message_text(text=query.message.text, reply_markup=None, parse_mode="Markdown")
        except:
            await query.edit_message_text(text=query.message.text, reply_markup=None)
        
        # Надсилаємо НОВЕ повідомлення з підтвердженням вибору
        if lang == 'uk':
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Дякую! Ви обрали наступні мети: {', '.join(selected_purposes)}.\n"
                "Зачекайте, будь ласка, поки я проаналізую ваші відповіді та підберу найкращі програми лояльності для вас."
            )
        else:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Thank you! You have chosen the following purposes: {', '.join(selected_purposes)}.\n"
                "Please wait while I analyze your answers and select the best loyalty programs for you."
            )
        
        # Очищуємо ID повідомлення з метою
        if 'purpose_message_id' in user_data_global[user_id]:
            del user_data_global[user_id]['purpose_message_id']
        
        # ОНОВЛЕНО: Розрахунок і відображення результатів з рейтингами + збереження для /more
        return await calculate_and_show_results_with_ratings(update, context)
    
    # Якщо це вибір або скасування вибору мети
    else:
        purpose = callback_data.replace("purpose_", "")
        
        # Перевіряємо, чи не перевищено максимальну кількість цілей (2)
        if purpose not in user_data_global[user_id]['selected_purposes'] and len(user_data_global[user_id]['selected_purposes']) >= 2:
            lang = user_data_global[user_id]['language']
            if lang == 'uk':
                await query.answer("Ви вже обрали максимальну кількість цілей (2)", show_alert=True)
            else:
                await query.answer("You have already selected the maximum number of purposes (2)", show_alert=True)
            return WAITING_PURPOSE_SUBMIT
        
        # Перемикаємо стан вибору мети
        if purpose in user_data_global[user_id]['selected_purposes']:
            user_data_global[user_id]['selected_purposes'].remove(purpose)
        else:
            user_data_global[user_id]['selected_purposes'].append(purpose)
        
        # Оновлюємо клавіатуру з новим вибором
        return await ask_purpose(update, context)

# ===============================
# ЧАСТИНА 9: ФУНКЦІЇ MAPPING ГОТЕЛІВ ЗІ СТИЛЯМИ ТА МЕТОЮ
# ===============================

def map_hotel_style(hotel_brand):
    """
    Зіставляє бренд готелю зі стилями
    
    Args:
        hotel_brand: бренд готелю (один рядок, не список)
    
    Returns:
        Словник стилів із відповідними значеннями True/False
    """
    # Переконуємося, що hotel_brand є рядком
    if not isinstance(hotel_brand, str):
        hotel_brand = str(hotel_brand)
    
    hotel_brand = hotel_brand.lower()
    
    # Оновлений повний словник стилів і брендів
    style_mapping = {
        "Розкішний і вишуканий": [
            "JW Marriott", "The Ritz-Carlton", "Conrad Hotels & Resorts", 
            "Waldorf Astoria Hotels & Resorts", "InterContinental Hotels & Resorts", 
            "Wyndham Grand", "Registry Collection Hotels", "Fairmont Hotels", 
            "Raffles Hotels & Resorts", "Park Hyatt Hotels", "Alila Hotels", 
            "Hyatt Regency", "Grand Hyatt", "Ascend Hotel Collection"
        ],
        
        "Бутік і унікальний": [
            "Kimpton Hotels & Restaurants", "Registry Collection Hotels", 
            "Mercure Hotels", "ibis Styles", "Park Hyatt Hotels", 
            "Alila Hotels", "Ascend Hotel Collection"
        ],
        
        "Класичний і традиційний": [
            "The Ritz-Carlton", "Marriott Hotels", "Sheraton", 
            "Waldorf Astoria Hotels & Resorts", "Hilton Hotels & Resorts", 
            "InterContinental Hotels & Resorts", "Holiday Inn Hotels & Resorts", 
            "Wyndham", "Fairmont Hotels", "Raffles Hotels & Resorts", 
            "Ascend Hotel Collection"
        ],
        
        "Сучасний і дизайнерський": [
            "Conrad Hotels & Resorts", "Kimpton Hotels & Restaurants", 
            "Crowne Plaza", "Wyndham Grand", "Novotel Hotels", 
            "Ibis Hotels", "ibis Styles", "Cambria Hotels", 
            "Park Hyatt Hotels", "Grand Hyatt", "Hyatt Place"
        ],
        
        "Затишний і сімейний": [
            "Fairfield Inn & Suites", "DoubleTree by Hilton", 
            "Hampton by Hilton", "Holiday Inn Hotels & Resorts", 
            "Candlewood Suites", "Wyndham", "Days Inn by Wyndham", 
            "Mercure Hotels", "Novotel Hotels", "Quality Inn Hotels", 
            "Comfort Inn Hotels", "Hyatt House"
        ],
        
        "Практичний і економічний": [
            "Fairfield Inn & Suites", "Courtyard by Marriott", 
            "Hampton by Hilton", "Hilton Garden Inn", 
            "Holiday Inn Hotels & Resorts", "Holiday Inn Express", 
            "Candlewood Suites", "Wingate by Wyndham", 
            "Super 8 by Wyndham", "Days Inn by Wyndham", 
            "Ibis Hotels", "ibis Styles", "Quality Inn Hotels", 
            "Comfort Inn Hotels", "Econo Lodge Hotels", 
            "Rodeway Inn Hotels", "Hyatt Place", "Hyatt House"
        ]
    }
    
    # Додаємо англійські ключі для стилів
    style_mapping_en = {
        "Luxurious and refined": style_mapping["Розкішний і вишуканий"],
        "Boutique and unique": style_mapping["Бутік і унікальний"],
        "Classic and traditional": style_mapping["Класичний і традиційний"],
        "Modern and designer": style_mapping["Сучасний і дизайнерський"],
        "Cozy and family-friendly": style_mapping["Затишний і сімейний"],
        "Practical and economical": style_mapping["Практичний і економічний"]
    }
    
    # Об'єднуємо словники
    combined_mapping = {**style_mapping, **style_mapping_en}
    
    result = {}
    for style, brands in combined_mapping.items():
        # Більш гнучке порівняння назв брендів
        is_match = False
        for brand in brands:
            brand_lower = brand.lower()
            # Перевіряємо, чи містить бренд готелю назву бренду зі списку
            if brand_lower in hotel_brand:
                is_match = True
                break
        result[style] = is_match
    
    return result

def map_hotel_purpose(hotel_brand):
    """
    Зіставляє бренд готелю з метою подорожі
    
    Args:
        hotel_brand: бренд готелю (один рядок, не список)
    
    Returns:
        Словник цілей із відповідними значеннями True/False
    """
    # Переконуємося, що hotel_brand є рядком
    if not isinstance(hotel_brand, str):
        hotel_brand = str(hotel_brand)
    
    hotel_brand = hotel_brand.lower()
    
    purpose_mapping = {
        "Бізнес-подорожі / відрядження": ["Marriott Hotels", "InterContinental Hotels & Resorts", "Crowne Plaza", 
                                      "Hyatt Regency", "Grand Hyatt", "Courtyard by Marriott", "Hilton Garden Inn", 
                                      "Sheraton", "DoubleTree by Hilton", "Novotel Hotels", "Cambria Hotels", 
                                      "Fairfield Inn & Suites", "Holiday Inn Express", "Wingate by Wyndham", 
                                      "Quality Inn Hotels", "ibis Hotels", "Econo Lodge Hotels", "Hyatt Place", "Rodeway Inn Hotels"],
        
        "Відпустка / релакс": ["The Ritz-Carlton", "JW Marriott", "Waldorf Astoria Hotels & Resorts", 
                             "Conrad Hotels & Resorts", "Park Hyatt Hotels", "Fairmont Hotels", 
                             "Raffles Hotels & Resorts", "InterContinental Hotels & Resorts", 
                             "Kimpton Hotels & Restaurants", "Alila Hotels", "Registry Collection Hotels", 
                             "Ascend Hotel Collection", "Hilton Hotels & Resorts", "Wyndham Grand", "Grand Hyatt"],
        
        "Сімейний відпочинок": ["JW Marriott", "Hyatt Regency", "Sheraton", "Holiday Inn Hotels & Resorts", 
                              "DoubleTree by Hilton", "Wyndham", "Mercure Hotels", "Novotel Hotels", 
                              "Comfort Inn Hotels", "Hampton by Hilton", "Holiday Inn Express", 
                              "Days Inn by Wyndham", "Super 8 by Wyndham", "Hilton Hotels & Resorts", "Wyndham Grand", "Marriott Hotels", 
                              "Courtyard by Marriott", "Crowne Plaza", "The Ritz-Carlton"],
        
        "Довготривале проживання": ["Hyatt House", "Candlewood Suites", "ibis Styles"]
    }
    
    # Переклад для англійської мови
    purpose_mapping_en = {
        "Business travel": purpose_mapping["Бізнес-подорожі / відрядження"],
        "Vacation / relaxation": purpose_mapping["Відпустка / релакс"],
        "Family vacation": purpose_mapping["Сімейний відпочинок"],
        "Long-term stay": purpose_mapping["Довготривале проживання"]
    }
    
    # Об'єднуємо обидва словники
    combined_mapping = {**purpose_mapping, **purpose_mapping_en}
    
    result = {}
    for purpose, brands in combined_mapping.items():
        # Більш гнучке порівняння назв брендів
        is_match = False
        for brand in brands:
            brand_lower = brand.lower()
            # Перевіряємо, чи бренд готелю містить назву бренду зі списку
            if brand_lower in hotel_brand:
                is_match = True
                break
        result[purpose] = is_match
    
    return result

def convert_rating_column_to_numeric(df):
    """
    Конвертує колонку рейтингу в числовий формат
    
    Args:
        df: DataFrame з готелями
    
    Returns:
        DataFrame з правильним типом колонки рейтингу
    """
    if 'Weighted rating of each unique hotel' in df.columns:
        # Конвертуємо в числовий формат, некоректні значення стають NaN
        df['Weighted rating of each unique hotel'] = pd.to_numeric(
            df['Weighted rating of each unique hotel'], 
            errors='coerce'
        )
        
        # Заповнюємо NaN нулями
        df['Weighted rating of each unique hotel'].fillna(0.0, inplace=True)
        
        debug_log(f"Converted rating column to numeric. Sample values: {df['Weighted rating of each unique hotel'].head().tolist()}")
    
    return df

def select_diverse_hotels(hotels_df, max_count=2):
    """
    Вибирає готелі з різних брендів, максимум 1 готель від одного бренду
    
    Args:
        hotels_df: DataFrame з готелями
        max_count: максимальна кількість готелів для вибору (тепер 2)
    
    Returns:
        DataFrame з обраними готелями
    """
    if hotels_df.empty:
        return hotels_df
    
    # Сортуємо за рейтингом (найкращі спочатку)
    sorted_hotels = hotels_df.sort_values('Weighted rating of each unique hotel', ascending=False)
    
    selected_hotels = []
    used_brands = set()
    
    for index, hotel in sorted_hotels.iterrows():
        hotel_brand = hotel.get('Hotel Brand', 'Unknown')
        
        # Якщо бренд ще не використаний і ми не набрали максимум
        if hotel_brand not in used_brands and len(selected_hotels) < max_count:
            selected_hotels.append(hotel)
            used_brands.add(hotel_brand)
            debug_log(f"Обрано готель: {hotel.get('hotel_name')} (бренд: {hotel_brand})")
    
    # Якщо не набрали достатньо готелів, додаємо решту (може бути дублікат брендів)
    if len(selected_hotels) < max_count:
        for index, hotel in sorted_hotels.iterrows():
            if len(selected_hotels) >= max_count:
                break
            
            # Перевіряємо, чи цей готель вже не додано
            if not any(h.get('hotel_name') == hotel.get('hotel_name') for h in selected_hotels):
                selected_hotels.append(hotel)
    
    # Конвертуємо назад в DataFrame
    if selected_hotels:
        result_df = pd.DataFrame(selected_hotels)
        debug_log(f"Підсумок: обрано {len(result_df)} готелів з {len(used_brands)} різних брендів")
        return result_df
    else:
        return pd.DataFrame()

def find_top_2_hotels_for_program(program_name, user_data, hotel_data):
    """
    Знаходить топ-2 готелі для програми лояльності, які відповідають критеріям користувача
    З диверсифікацією брендів - максимум 1 готель від одного бренду
    
    Args:
        program_name: назва програми лояльності
        user_data: дані користувача з відповідями
        hotel_data: повні дані готелів
    
    Returns:
        tuple: (DataFrame з топ-2 готелів, тип вибірки)
    """
    # Переводимо критерії користувача
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    english_regions = translate_regions_to_english(regions)
    english_countries = translate_regions_to_english(countries)
    english_styles = translate_styles_to_english(styles)
    english_purposes = translate_purposes_to_english(purposes)
    
    debug_log(f"Пошук топ-2 готелів для програми: {program_name}")
    debug_log(f"Критерії: regions={english_regions}, category={category}, styles={english_styles}, purposes={english_purposes}")
    
    # 1. Фільтруємо за регіоном
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions, english_countries)
    
    # 2. Фільтруємо за програмою лояльності
    program_hotels = filtered_by_region[filtered_by_region['loyalty_program'] == program_name]
    
    # ВИПРАВЛЕННЯ: Конвертуємо рейтинг в числовий формат
    program_hotels = convert_rating_column_to_numeric(program_hotels)
    
    if program_hotels.empty:
        debug_log(f"Немає готелів для програми {program_name} в регіоні")
        return pd.DataFrame(), "no_hotels"
    
    # 3. ПРІОРИТЕТ: основна категорія з усіма критеріями
    main_category_hotels = filter_hotels_by_category(program_hotels, category)
    main_filtered = filter_hotels_by_style(main_category_hotels, english_styles)
    main_filtered = filter_hotels_by_purpose(main_filtered, english_purposes)
    
    debug_log(f"Готелів в основній категорії {category} з усіма критеріями: {len(main_filtered)}")
    
    # 4. ОНОВЛЕНО: Якщо в основній категорії є готелі
    if len(main_filtered) > 0:
        # НОВА ЛОГІКА: Диверсифікація брендів (2 готелі)
        top_2 = select_diverse_hotels(main_filtered, 2)
        if len(top_2) >= 2:
            debug_log(f"Обрано 2 готелі з основної категорії (різні бренди)")
            return top_2, "main_category"
    else:
        main_filtered = pd.DataFrame()
    
    # 5. ДОПОВНЮЄМО з суміжних категорій
    all_suitable_hotels = main_filtered.copy()
    
    adjacent_categories = get_adjacent_categories(category)
    debug_log(f"Суміжні категорії: {adjacent_categories}")
    
    for adj_category in adjacent_categories:
        adj_category_hotels = filter_hotels_by_category(program_hotels, adj_category)
        adj_filtered = filter_hotels_by_style(adj_category_hotels, english_styles)
        adj_filtered = filter_hotels_by_purpose(adj_filtered, english_purposes)
        
        debug_log(f"Готелів в суміжній категорії {adj_category}: {len(adj_filtered)}")
        all_suitable_hotels = pd.concat([all_suitable_hotels, adj_filtered], ignore_index=True)
    
    # 6. ОНОВЛЕНО: Видаляємо дублікати та диверсифікуємо бренди
    all_suitable_hotels = all_suitable_hotels.drop_duplicates(subset=['hotel_name', 'Place ID'])
    
    if len(all_suitable_hotels) > 0:
        top_2 = select_diverse_hotels(all_suitable_hotels, 2)
        debug_log(f"Обрано {len(top_2)} готелі з комбінації категорій (різні бренди)")
        return top_2, "mixed"
    else:
        debug_log(f"Не знайдено готелів для програми {program_name}")
        return pd.DataFrame(), "no_hotels"

def format_hotel_examples_for_integration(top_hotels, program_name, lang='uk'):
    """
    Форматує інформацію про топ-2 готелі для інтеграції в основний звіт
    
    Args:
        top_hotels: DataFrame з готелями
        program_name: назва програми лояльності
        lang: мова інтерфейсу
    
    Returns:
        str: відформатований текст для додавання до звіту
    """
    if top_hotels.empty:
        if lang == 'uk':
            return "\n❌ Не знайдено готелів, що відповідають всім вашим критеріям."
        else:
            return "\n❌ No hotels found matching all your criteria."
    
    if lang == 'uk':
        result = f"\n🏆 Ось приклад {len(top_hotels)} кращих готелів цієї програми, які відповідають вашому запиту:\n\n"
    else:
        result = f"\n🏆 Here are the top {len(top_hotels)} hotels from this program that match your request:\n\n"
    
    for i, (index, hotel) in enumerate(top_hotels.iterrows()):
        # Простий текст БЕЗ Markdown
        hotel_name = str(hotel.get('hotel_name', 'N/A'))
        hotel_brand = str(hotel.get('Hotel Brand', 'N/A'))
        rating = float(hotel.get('Weighted rating of each unique hotel', 0))
        address = str(hotel.get('address', 'N/A'))
        place_id = str(hotel.get('Place ID', ''))
        
        result += f"{i+1}. {hotel_name}"
        
        # Показуємо рейтинг тільки якщо він > 0
        if rating > 0:
            result += f" ⭐{rating:.1f}"
        
        result += f"\n   🏢 {hotel_brand}\n"
        result += f"   📍 {address}\n"
        result += f"   🔗 Place ID: {place_id}\n\n"
    
    return result

def add_hotels_to_results(detailed_results, user_data, scores_df, lang='uk'):
    """
    Додає готелі до детального звіту для кожної програми
    
    Args:
        detailed_results: оригінальний детальний звіт
        user_data: дані користувача
        scores_df: DataFrame з результатами програм
        lang: мова інтерфейсу
    
    Returns:
        str: розширений звіт з готелями
    """
    # Розбиваємо звіт на секції для кожної програми
    sections = detailed_results.split("=" * 50)
    enhanced_sections = []
    
    top_programs = scores_df.head(3)
    
    for i, section in enumerate(sections):
        enhanced_sections.append(section)
        
        # Додаємо готелі після кожної програми (крім останньої секції)
        if i < len(top_programs):
            try:
                program_name = top_programs.iloc[i]['loyalty_program']
                
                # Знаходимо топ-2 готелі для цієї програми
                top_hotels, selection_type = find_top_2_hotels_for_program(program_name, user_data, hotel_data)
                
                # Форматуємо готелі для інтеграції
                hotels_text = format_hotel_examples_for_integration(top_hotels, program_name, lang)
                
                # Додаємо готелі до секції
                enhanced_sections.append(hotels_text)
                
            except Exception as e:
                debug_log(f"Помилка додавання готелів для програми: {e}")
    
    # Об'єднуємо всі секції назад
    return ("=" * 50).join(enhanced_sections)

# ===============================
# ЧАСТИНА 10: ВИПРАВЛЕНІ ФУНКЦІЇ РОЗРАХУНКУ БАЛІВ ТА ГОЛОВНІ ФУНКЦІЇ
# ===============================
# ===============================
# ЧАСТИНА 10: ВИПРАВЛЕНІ ФУНКЦІЇ РОЗРАХУНКУ БАЛІВ ТА ГОЛОВНІ ФУНКЦІЇ
# ===============================

def get_total_hotels_for_regions(df, program, regions=None, countries=None):
    """
    Підраховує загальну кількість готелів програми лояльності у всіх обраних регіонах/країнах
    """
    total_hotels = 0
    
    # Фільтруємо дані за програмою лояльності
    program_data = df[df['loyalty_program'] == program]
    
    if regions and len(regions) > 0:
        for region in regions:
            region_data = program_data[program_data['region'].str.contains(region, case=False, na=False)]
            
            if not region_data.empty and 'Total hotels of Corporation / Loyalty Program in this region' in region_data.columns:
                region_hotels = region_data['Total hotels of Corporation / Loyalty Program in this region'].iloc[0]
                if pd.notna(region_hotels):
                    total_hotels += int(region_hotels)
    
    elif countries and len(countries) > 0:
        for country in countries:
            country_data = program_data[program_data['country'].str.contains(country, case=False, na=False)]
            
            if not country_data.empty and 'Total hotels of Corporation / Loyalty Program in this country' in country_data.columns:
                country_hotels = country_data['Total hotels of Corporation / Loyalty Program in this country'].iloc[0]
                if pd.notna(country_hotels):
                    total_hotels += int(country_hotels)
    
    return total_hotels

## Функції фільтрації готелів (залишаються без змін)
def filter_hotels_by_region(df, regions=None, countries=None):
    """Фільтрує готелі за регіоном або країною"""
    if not regions and not countries:
        return df
    
    filtered_df = df.copy()
    
    if regions and len(regions) > 0:
        region_mask = filtered_df['region'].apply(lambda x: any(region.lower() in str(x).lower() for region in regions))
        filtered_df = filtered_df[region_mask]
    
    if countries and len(countries) > 0:
        country_mask = filtered_df['country'].apply(lambda x: any(country.lower() in str(x).lower() for country in countries))
        filtered_df = filtered_df[country_mask]
    
    return filtered_df

def filter_hotels_by_category(df, category):
    """Фільтрує готелі за категорією"""
    category_mapping = {
        "Luxury": ["Luxury"],
        "Comfort": ["Comfort"],
        "Standard": ["Standard", "Standart"],
    }
    
    if category in category_mapping:
        if 'segment' in df.columns:
            mask = df['segment'].apply(lambda x: any(cat.lower() in str(x).lower() for cat in category_mapping[category]))
            return df[mask]
    
    return df

def filter_hotels_by_style(df, styles):
    """Фільтрує готелі за стилем"""
    if not styles or len(styles) == 0:
        return df
    
    debug_log(f"Фільтрація за стилями: {styles}")
    
    style_mask = pd.Series(False, index=df.index)
    
    for idx, row in df.iterrows():
        if 'Hotel Brand' in df.columns and pd.notna(row['Hotel Brand']):
            hotel_brand = row['Hotel Brand']
            hotel_styles = map_hotel_style(hotel_brand)
            
            for style in styles:
                style_lower = style.lower()
                for hotel_style, matches in hotel_styles.items():
                    if matches and (hotel_style.lower() == style_lower or 
                                    style_lower in hotel_style.lower() or
                                    hotel_style.lower() in style_lower):
                        style_mask.loc[idx] = True
                        break
    
    filtered_df = df[style_mask]
    debug_log(f"Готелів після фільтрації за стилем: {len(filtered_df)}")
    
    return filtered_df

def filter_hotels_by_purpose(df, purposes):
    """Фільтрує готелі за метою подорожі"""
    if not purposes or len(purposes) == 0:
        return df
    
    debug_log(f"Фільтрація за метою: {purposes}")
    
    purpose_mask = pd.Series(False, index=df.index)
    
    for idx, row in df.iterrows():
        if 'Hotel Brand' in df.columns and pd.notna(row['Hotel Brand']):
            hotel_brand = row['Hotel Brand']
            hotel_purposes = map_hotel_purpose(hotel_brand)
            
            for purpose in purposes:
                purpose_lower = purpose.lower()
                for hotel_purpose, matches in hotel_purposes.items():
                    if matches and (hotel_purpose.lower() == purpose_lower or 
                                    purpose_lower in hotel_purpose.lower() or
                                    hotel_purpose.lower() in purpose_lower):
                        purpose_mask.loc[idx] = True
                        break
    
    filtered_df = df[purpose_mask]
    debug_log(f"Готелів після фільтрації за метою: {len(filtered_df)}")
    
    return filtered_df

def get_adjacent_categories(category):
    """Повертає суміжні категорії"""
    adjacent_mapping = {
        "Luxury": ["Comfort"],
        "Comfort": ["Luxury", "Standard"],
        "Standard": ["Comfort"],
    }
    return adjacent_mapping.get(category, [])

def distribute_scores_with_ties(counts_dict, score_values):
    """
    Universal function for distributing scores with tie handling
    
    Args:
        counts_dict: dictionary {program: hotel_count}
        score_values: list of scores [21, 18, 15, 12, 9, 6, 3] or [7, 6, 5, 4, 3, 2, 1]
    
    Returns:
        dictionary {program: points}
    """
    if not counts_dict or not score_values:
        return {program: 0.0 for program in counts_dict.keys()}
    
    # КРИТИЧНО ВАЖЛИВО: Фільтруємо програми з кількістю > 0
    filtered_counts = {prog: count for prog, count in counts_dict.items() if count > 0}
    
    if not filtered_counts:
        debug_log("Немає програм з готелями > 0, повертаємо нульові бали")
        return {program: 0.0 for program in counts_dict.keys()}
    
    # Групуємо програми за кількістю готелів
    count_groups = {}
    for program, count in filtered_counts.items():
        if count not in count_groups:
            count_groups[count] = []
        count_groups[count].append(program)
    
    # Сортуємо групи за кількістю готелів (по спаданню)
    sorted_counts = sorted(count_groups.keys(), reverse=True)
    
    # Розподіляємо бали
    result_scores = {program: 0.0 for program in counts_dict.keys()}
    current_position = 0
    
    for count in sorted_counts:
        programs_in_group = count_groups[count]
        group_size = len(programs_in_group)
        
        # Перевіряємо, чи ще є доступні бали
        if current_position >= len(score_values):
            break
        
        # Беремо бал для поточної позиції
        score_for_group = score_values[current_position]
        
        # Всі програми в групі отримують однакові бали
        for program in programs_in_group:
            result_scores[program] = float(score_for_group)
        
        debug_log(f"Позиція {current_position+1}: {len(programs_in_group)} програм з {count} готелів отримують {score_for_group} балів")
        
        # Переходимо до наступної позиції, пропускаючи зайняті місця
        current_position += group_size
    
    return result_scores

def get_region_score(df, regions=None, countries=None):
    """Обчислює бали для програм лояльності за регіонами/країнами з правильним розподілом при ties"""
    try:
        if regions and len(regions) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this region' in df.columns:
                region_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this region']]
                region_counts = region_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this region'].to_dict()
            else:
                region_counts = df.groupby('loyalty_program').size().to_dict()
                logger.warning("Колонка 'Total hotels of Corporation / Loyalty Program in this region' відсутня. Використовуємо кількість рядків.")
        
        elif countries and len(countries) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this country' in df.columns:
                country_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this country']]
                region_counts = country_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this country'].to_dict()
            else:
                region_counts = df.groupby('loyalty_program').size().to_dict()
                logger.warning("Колонка 'Total hotels of Corporation / Loyalty Program in this country' відсутня. Використовуємо кількість рядків.")
        
        else:
            return {}
        
        # Переводимо в float та заповнюємо NaN як 0
        region_counts = {prog: float(count) if pd.notna(count) else 0.0 for prog, count in region_counts.items()}
        
        # Використовуємо константи з ЧАСТИНИ 2
        region_scores = distribute_scores_with_ties(region_counts, REGION_POINTS)
        
        # Нормалізуємо, якщо обрано кілька регіонів
        normalization_factor = 1.0
        if regions and len(regions) > 1:
            normalization_factor = float(len(regions))
        elif countries and len(countries) > 1:
            normalization_factor = float(len(countries))
        
        if normalization_factor > 1.0:
            region_scores = {program: score / normalization_factor for program, score in region_scores.items()}
            debug_log(f"Нормалізація регіонів на фактор {normalization_factor}")
                
    except Exception as e:
        logger.error(f"Помилка обчислення балів за регіоном: {e}")
        return {}
    
    return region_scores

def calculate_scores_with_ratings(user_data, hotel_data):
    """
    ОНОВЛЕНА функція розрахунку балів з урахуванням рейтингів та правильним підрахунком готелів
    """
    debug_log(f"=== STARTING SCORE CALCULATION WITH RATINGS ===")
    debug_log(f"User data: {user_data}")
    
    # Спочатку розраховуємо базові бали (без рейтингів)
    base_scores_df = calculate_scores_fixed(user_data, hotel_data)
    
    if base_scores_df.empty:
        debug_log("Base scores calculation returned empty DataFrame")
        return base_scores_df
    
    # Додаємо колонки для рейтингів
    base_scores_df['program_rating'] = 0.0
    base_scores_df['rating_coefficient'] = 0.0
    base_scores_df['base_score'] = base_scores_df['total_score'].copy()
    
    # ВИПРАВЛЕННЯ: Оновлюємо region_hotels з правильним підрахунком для кількох регіонів
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    english_regions = translate_regions_to_english(regions)
    english_countries = translate_regions_to_english(countries)
    
    # Застосовуємо рейтинг-коефіцієнти до кожної програми
    for index, row in base_scores_df.iterrows():
        program = row['loyalty_program']
        base_score = row['base_score']
        
        # ВИПРАВЛЕННЯ: Правильний підрахунок готелів для кількох регіонів
        total_hotels = get_total_hotels_for_regions(hotel_data, program, english_regions, english_countries)
        base_scores_df.at[index, 'region_hotels'] = total_hotels
        
        # Отримуємо рейтинг програми
        program_rating = get_program_rating(program)
        rating_coefficient = calculate_rating_coefficient(program_rating)
        
        # Розраховуємо фінальний бал з урахуванням рейтингу
        final_score = base_score * rating_coefficient
        
        # Оновлюємо DataFrame
        base_scores_df.at[index, 'program_rating'] = program_rating
        base_scores_df.at[index, 'rating_coefficient'] = rating_coefficient
        base_scores_df.at[index, 'total_score'] = final_score
        
        debug_log(f"{program}: hotels={total_hotels}, base={base_score:.2f}, rating={program_rating:.2f}, "
                 f"coeff={rating_coefficient:.3f}, final={final_score:.2f}")
    
    # Пересортовуємо за новими фінальними балами
    base_scores_df = base_scores_df.sort_values('total_score', ascending=False)
    
    debug_log(f"=== CALCULATION WITH RATINGS COMPLETE ===")
    
    return base_scores_df

def calculate_style_scores_simple(filtered_by_region, loyalty_programs, category, styles):
    """
    ПРОСТА логіка розрахунку балів за стилем:
    1. Рахуємо всі бали БЕЗ нормалізації
    2. Сумуємо
    3. Ділимо на кількість стилів
    """
    if not styles or len(styles) == 0:
        return {program: 0.0 for program in loyalty_programs}, {program: 0 for program in loyalty_programs}
    
    debug_log(f"=== SIMPLE STYLE CALCULATION ===")
    debug_log(f"Category: {category}, Styles: {styles}")
    
    adjacent_categories = get_adjacent_categories(category) if category else []
    raw_style_scores = {program: 0.0 for program in loyalty_programs}  # БЕЗ нормалізації
    main_style_counts = {program: 0 for program in loyalty_programs}
    
    # Для кожного стилю окремо розраховуємо бали БЕЗ нормалізації
    for style in styles:
        debug_log(f"Processing style: {style}")
        
        # MAIN категорія для ОДНОГО стилю
        if category:
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_style_filtered = filter_hotels_by_style(main_category_hotels, [style])
            
            main_counts_for_style = {}
            for program in loyalty_programs:
                count = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
                main_counts_for_style[program] = count
                main_style_counts[program] += count
            
            debug_log(f"Main category ({category}) for style '{style}': {main_counts_for_style}")
            
            # БЕЗ нормалізації - повні бали
            main_scores = distribute_scores_with_ties(main_counts_for_style, MAIN_CATEGORY_POINTS)
            
            for program in loyalty_programs:
                raw_style_scores[program] += main_scores.get(program, 0.0)
        
        # ADJACENT категорії для ОДНОГО стилю
        adj_score_for_style = {program: 0.0 for program in loyalty_programs}
        
        for adj_cat in adjacent_categories:
            adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_style_filtered = filter_hotels_by_style(adj_category_hotels, [style])
            
            adj_counts_for_style = {}
            for program in loyalty_programs:
                count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                adj_counts_for_style[program] = count
            
            debug_log(f"Adjacent category ({adj_cat}) for style '{style}': {adj_counts_for_style}")
            
            # БЕЗ нормалізації - повні бали
            adj_scores = distribute_scores_with_ties(adj_counts_for_style, ADJACENT_CATEGORY_POINTS)
            
            # ДОДАЄМО всі суміжні бали для цього стилю
            for program in loyalty_programs:
                score = adj_scores.get(program, 0.0)
                adj_score_for_style[program] += score
        
        # Додаємо всі adjacent бали для цього стилю
        for program in loyalty_programs:
            raw_style_scores[program] += adj_score_for_style[program]
    
    # НОРМАЛІЗАЦІЯ ТІЛЬКИ В КІНЦІ
    final_style_scores = {}
    normalization_factor = len(styles)
    
    for program in loyalty_programs:
        if normalization_factor > 1:
            final_style_scores[program] = raw_style_scores[program] / normalization_factor
        else:
            final_style_scores[program] = raw_style_scores[program]
    
    debug_log(f"Raw total scores: {raw_style_scores}")
    debug_log(f"Applied normalization factor: {normalization_factor}")
    debug_log(f"Final style scores: {final_style_scores}")
    
    return final_style_scores, main_style_counts

def calculate_purpose_scores_simple(filtered_by_region, loyalty_programs, category, purposes):
    """
    ПРОСТА логіка розрахунку балів за метою:
    1. Рахуємо всі бали БЕЗ нормалізації
    2. Сумуємо
    3. Ділимо на кількість цілей
    """
    if not purposes or len(purposes) == 0:
        return {program: 0.0 for program in loyalty_programs}, {program: 0 for program in loyalty_programs}
    
    debug_log(f"=== SIMPLE PURPOSE CALCULATION ===")
    debug_log(f"Category: {category}, Purposes: {purposes}")
    
    adjacent_categories = get_adjacent_categories(category) if category else []
    raw_purpose_scores = {program: 0.0 for program in loyalty_programs}  # БЕЗ нормалізації
    main_purpose_counts = {program: 0 for program in loyalty_programs}
    
    # Для кожної мети окремо розраховуємо бали БЕЗ нормалізації
    for purpose in purposes:
        debug_log(f"Processing purpose: {purpose}")
        
        # MAIN категорія для ОДНІЄЇ мети
        if category:
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, [purpose])
            
            main_counts_for_purpose = {}
            for program in loyalty_programs:
                count = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
                main_counts_for_purpose[program] = count
                main_purpose_counts[program] += count
            
            debug_log(f"Main category ({category}) for purpose '{purpose}': {main_counts_for_purpose}")
            
            # БЕЗ нормалізації - повні бали
            main_scores = distribute_scores_with_ties(main_counts_for_purpose, MAIN_CATEGORY_POINTS)
            
            for program in loyalty_programs:
                raw_purpose_scores[program] += main_scores.get(program, 0.0)
        
        # ADJACENT категорії для ОДНІЄЇ мети
        adj_score_for_purpose = {program: 0.0 for program in loyalty_programs}
        
        for adj_cat in adjacent_categories:
            adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, [purpose])
            
            adj_counts_for_purpose = {}
            for program in loyalty_programs:
                count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                adj_counts_for_purpose[program] = count
            
            debug_log(f"Adjacent category ({adj_cat}) for purpose '{purpose}': {adj_counts_for_purpose}")
            
            # БЕЗ нормалізації - повні бали
            adj_scores = distribute_scores_with_ties(adj_counts_for_purpose, ADJACENT_CATEGORY_POINTS)
            
            # ДОДАЄМО всі суміжні бали для цієї мети
            for program in loyalty_programs:
                score = adj_scores.get(program, 0.0)
                adj_score_for_purpose[program] += score
        
        # Додаємо всі adjacent бали для цієї мети
        for program in loyalty_programs:
            raw_purpose_scores[program] += adj_score_for_purpose[program]
    
    # НОРМАЛІЗАЦІЯ ТІЛЬКИ В КІНЦІ
    final_purpose_scores = {}
    normalization_factor = len(purposes)
    
    for program in loyalty_programs:
        if normalization_factor > 1:
            final_purpose_scores[program] = raw_purpose_scores[program] / normalization_factor
        else:
            final_purpose_scores[program] = raw_purpose_scores[program]
    
    debug_log(f"Raw total scores: {raw_purpose_scores}")
    debug_log(f"Applied normalization factor: {normalization_factor}")
    debug_log(f"Final purpose scores: {final_purpose_scores}")
    
    return final_purpose_scores, main_purpose_counts

def calculate_scores_fixed(user_data, hotel_data):
    """
    БАЗОВА функція розрахунку балів БЕЗ рейтингів (для внутрішнього використання)
    """
    debug_log(f"=== STARTING BASE SCORE CALCULATION ===")
    debug_log(f"Original user data: {user_data}")
    
    # Отримуємо відповіді користувача
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    # ПЕРЕВОДИМО українські відповіді на англійську для обробки
    english_regions = translate_regions_to_english(regions)
    english_countries = translate_regions_to_english(countries)
    english_styles = translate_styles_to_english(styles)
    english_purposes = translate_purposes_to_english(purposes)
    
    debug_log(f"Translated data - regions: {english_regions}, styles: {english_styles}, purposes: {english_purposes}")
    
    # Ініціалізуємо DataFrame для зберігання результатів
    loyalty_programs = hotel_data['loyalty_program'].unique()
    scores_df = pd.DataFrame({
        'loyalty_program': loyalty_programs,
        'region_score': 0.0,
        'category_score': 0.0,
        'style_score': 0.0,
        'purpose_score': 0.0,
        'total_score': 0.0,
        'region_hotels': 0,
        'category_hotels': 0,
        'style_hotels': 0,
        'purpose_hotels': 0
    })
    
    # Крок 1: Фільтруємо готелі за регіоном
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions, english_countries)
    debug_log(f"Hotels after region filter: {len(filtered_by_region)}")
    
    # Розподіляємо бали за регіонами/країнами
    region_scores = get_region_score(filtered_by_region, english_regions, english_countries)
    debug_log(f"Region scores: {region_scores}")
    
    for index, row in scores_df.iterrows():
        program = row['loyalty_program']
        scores_df.at[index, 'region_score'] = region_scores.get(program, 0.0)
        
        # ВИПРАВЛЕННЯ: Правильний підрахунок готелів для region_hotels
        total_hotels = get_total_hotels_for_regions(hotel_data, program, english_regions, english_countries)
        scores_df.at[index, 'region_hotels'] = total_hotels
    
    # Крок 2: ВИПРАВЛЕНИЙ розрахунок балів за категорією
    if category:
        # ОСНОВНА категорія
        main_filtered = filter_hotels_by_category(filtered_by_region, category)
        main_counts = main_filtered.groupby('loyalty_program').size().to_dict()
        main_scores = distribute_scores_with_ties(main_counts, MAIN_CATEGORY_POINTS)
        
        # СУМІЖНІ категорії
        adjacent_categories = get_adjacent_categories(category)
        adjacent_scores = {program: 0.0 for program in loyalty_programs}
        
        for adj_cat in adjacent_categories:
            adj_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_counts = adj_filtered.groupby('loyalty_program').size().to_dict()
            adj_scores = distribute_scores_with_ties(adj_counts, ADJACENT_CATEGORY_POINTS)
            
            # ✅ ВИПРАВЛЕНО: ДОДАЄМО всі суміжні бали замість max()
            for program in loyalty_programs:
                score = adj_scores.get(program, 0.0)
                adjacent_scores[program] += score  # ЗМІНЕНО: += замість max()
        
        # ВИПРАВЛЕНО: правильне підсумовування
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            main_score = main_scores.get(program, 0.0)
            adj_score = adjacent_scores.get(program, 0.0)
            total_category_score = main_score + adj_score  # СУМА замість main + max(adj)
            
            scores_df.at[index, 'category_score'] = total_category_score
            scores_df.at[index, 'category_hotels'] = main_counts.get(program, 0)
            
            debug_log(f"{program} category: main={main_score:.1f} + adj_total={adj_score:.1f} = {total_category_score:.1f}")
    
    # Крок 3: ПРОСТА ЛОГІКА розрахунку балів за стилем
    if english_styles and len(english_styles) > 0:
        style_scores, style_counts = calculate_style_scores_simple(
            filtered_by_region, loyalty_programs, category, english_styles
        )
        
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            scores_df.at[index, 'style_score'] = style_scores.get(program, 0.0)
            scores_df.at[index, 'style_hotels'] = style_counts.get(program, 0)
    
    # Крок 4: ПРОСТА ЛОГІКА розрахунку балів за метою
    if english_purposes and len(english_purposes) > 0:
        purpose_scores, purpose_counts = calculate_purpose_scores_simple(
            filtered_by_region, loyalty_programs, category, english_purposes
        )
        
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            scores_df.at[index, 'purpose_score'] = purpose_scores.get(program, 0.0)
            scores_df.at[index, 'purpose_hotels'] = purpose_counts.get(program, 0)
    
    # Обчислюємо загальний рейтинг
    scores_df['total_score'] = (
        scores_df['region_score'] + 
        scores_df['category_score'] + 
        scores_df['style_score'] + 
        scores_df['purpose_score']
    )
    
    # ДОДАНО: валідація розрахунків
    if VALIDATE_CALCULATIONS:
        for _, row in scores_df.head(5).iterrows():
            program = row['loyalty_program']
            breakdown = {
                'region': row['region_score'],
                'category': row['category_score'], 
                'style': row['style_score'],
                'purpose': row['purpose_score']
            }
            validate_score_calculation(row['total_score'], breakdown, program)
    
    debug_log(f"=== BASE CALCULATION COMPLETE ===")
    for _, row in scores_df.head(3).iterrows():
        debug_log(f"{row['loyalty_program']}: region={row['region_score']:.1f}, "
                   f"category={row['category_score']:.1f}, style={row['style_score']:.1f}, "
                   f"purpose={row['purpose_score']:.1f}, total={row['total_score']:.1f}")
    
    # Сортуємо за загальним рейтингом
    scores_df = scores_df.sort_values('total_score', ascending=False)
    
    return scores_df

async def calculate_and_show_results_with_ratings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    ОНОВЛЕНА функція обчислення та відображення результатів з урахуванням рейтингів
    Показує кожну програму з готелями в одному повідомленні
    """
    
    user_id = update.effective_user.id
    user_data = user_data_global[user_id]
    lang = user_data['language']
    
    try:
        debug_log(f"Розрахунок балів з рейтингами для користувача {user_id}")
        
        if hotel_data is None or hotel_data.empty:
            logger.error("Дані готелів відсутні або порожні!")
            if lang == 'uk':
                await context.bot.send_message(
                    chat_id=update.callback_query.message.chat_id,
                    text="На жаль, виникла проблема з даними готелів. Спробуйте пізніше."
                )
            else:
                await context.bot.send_message(
                    chat_id=update.callback_query.message.chat_id,
                    text="Unfortunately, there is a problem with the hotel data. Please try again later."
                )
            return ConversationHandler.END
        
        # Розрахунок балів з урахуванням рейтингів
        scores_df = calculate_scores_with_ratings(user_data, hotel_data)
        
        if scores_df.empty:
            if lang == 'uk':
                await context.bot.send_message(
                    chat_id=update.callback_query.message.chat_id,
                    text="На жаль, не вдалося знайти програми лояльності, які відповідають вашим уподобанням. "
                    "Спробуйте змінити параметри пошуку, надіславши команду /start знову."
                )
            else:
                await context.bot.send_message(
                    chat_id=update.callback_query.message.chat_id,
                    text="Unfortunately, I couldn't find any loyalty programs that match your preferences. "
                    "Try changing your search parameters by sending the /start command again."
                )
            return ConversationHandler.END
        
        # ЗБЕРІГАЄМО результати для команди /more
        user_last_results[user_id] = {
            'user_data': user_data.copy(),
            'scores_df': scores_df.copy()
        }
        debug_log(f"Збережено результати для користувача {user_id} для команди /more")
        
        # Відправляємо вступне повідомлення
        if lang == 'uk':
            intro_text = ("🎉 **Аналіз завершено!**\n\n"
                         "**Ось топ-3 програми лояльності готелів з детальним розбором балів:**")
        else:
            intro_text = ("🎉 **Analysis completed!**\n\n"
                         "**Here are the top 3 hotel loyalty programs with detailed score breakdown:**")
        
        await context.bot.send_message(
            chat_id=update.callback_query.message.chat_id,
            text=intro_text,
            parse_mode="Markdown"
        )
        
        # НОВЕ: Відправляємо кожну програму окремо з готелями та фото
        await send_programs_with_integrated_hotels_and_photos(
            context, 
            update.callback_query.message.chat_id, 
            user_data, 
            scores_df, 
            lang
        )
        
        # Відправляємо заключне повідомлення
        if lang == 'uk':
            outro_text = ("💡 **Хочете ще більше деталей?**\n"
                         "Натисніть /more для розширеного аналізу або /start для нового пошуку")
        else:
            outro_text = ("💡 **Want even more details?**\n"
                         "Type /more for extended analysis or /start for a new search")
        
        await context.bot.send_message(
            chat_id=update.callback_query.message.chat_id,
            text=outro_text,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Помилка при обчисленні результатів з рейтингами: {e}")
        
        if lang == 'uk':
            await context.bot.send_message(
                chat_id=update.callback_query.message.chat_id,
                text="Виникла помилка при аналізі ваших відповідей. Будь ласка, спробуйте знову, надіславши команду /start."
            )
        else:
            await context.bot.send_message(
                chat_id=update.callback_query.message.chat_id,
                text="An error occurred while analyzing your answers. Please try again by sending the /start command."
            )
    
    return ConversationHandler.END

async def send_long_message_to_chat(context, chat_id, text, max_length=4000):
    """Відправляє довге повідомлення частинами до чату"""
    if len(text) <= max_length:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        except Exception as e:
            # Якщо Markdown не працює, відправляємо без форматування
            logger.warning(f"Markdown parsing failed: {e}")
            await context.bot.send_message(chat_id=chat_id, text=text)
        return
    
    # Розбиваємо повідомлення на частини
    parts = []
    current_part = ""
    
    for line in text.split('\n'):
        if len(current_part + line + '\n') > max_length:
            if current_part:
                parts.append(current_part.strip())
                current_part = line + '\n'
            else:
                # Якщо один рядок занадто довгий, розбиваємо його
                parts.append(line[:max_length])
        else:
            current_part += line + '\n'
    
    if current_part:
        parts.append(current_part.strip())
    
    # Відправляємо частинами
    for i, part in enumerate(parts):
        try:
            await context.bot.send_message(chat_id=chat_id, text=part, parse_mode="Markdown")
        except Exception as e:
            # Якщо Markdown не працює, відправляємо без форматування
            logger.warning(f"Markdown parsing failed: {e}")
            await context.bot.send_message(chat_id=chat_id, text=part)
        
        # Невелика пауза між повідомленнями
        if i < len(parts) - 1:
            await asyncio.sleep(0.5)

# ===============================
# МОДЕРНІЗОВАНІ ФУНКЦІЇ ФОРМАТУВАННЯ РЕЗУЛЬТАТІВ
# ===============================

def get_style_counts_for_program_by_category(program, category_hotels, styles):
    """
    Допоміжна функція для підрахунку готелів за кожним стилем в певній категорії
    """
    style_counts = {}
    
    for style in styles:
        # Фільтруємо готелі цієї програми в цій категорії за конкретним стилем
        style_filtered = filter_hotels_by_style(category_hotels, [style])
        count = len(style_filtered[style_filtered['loyalty_program'] == program])
        style_counts[style] = count
    
    return style_counts

def get_purpose_counts_for_program_by_category(program, category_hotels, purposes):
    """
    Допоміжна функція для підрахунку готелів за кожною метою в певній категорії
    """
    purpose_counts = {}
    
    for purpose in purposes:
        # Фільтруємо готелі цієї програми в цій категорії за конкретною метою
        purpose_filtered = filter_hotels_by_purpose(category_hotels, [purpose])
        count = len(purpose_filtered[purpose_filtered['loyalty_program'] == program])
        purpose_counts[purpose] = count
    
    return purpose_counts

def format_simple_results(user_data, scores_df, lang='uk'):
    """
    ОНОВЛЕНА функція для звичайного звіту - новий компактний формат з абзацами
    """
    results = ""
    
    # Беремо тільки топ-3 програми
    top_programs = scores_df.head(3)
    
    # Отримуємо дані користувача для аналізу
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    # Переводимо для обробки
    english_regions = translate_regions_to_english(regions)
    english_countries = translate_regions_to_english(countries)
    english_styles = translate_styles_to_english(styles)
    english_purposes = translate_purposes_to_english(purposes)
    
    # Фільтруємо дані за регіоном для детального аналізу
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions, english_countries)
    
    for i, (index, row) in enumerate(top_programs.iterrows()):
        program = row['loyalty_program']
        
        # Замінюємо назву програми для відображення
        if program == "IHG One Rewards":
            display_program_name = "InterContinental Hotels One Rewards"
        else:
            display_program_name = program
        
        # Визначаємо емодзі та назву позиції
        if i == 0:
            emoji = "🥇"
            position_text = "Топ 1" if lang == 'uk' else "Top 1"
        elif i == 1:
            emoji = "🥈"
            position_text = "Топ 2" if lang == 'uk' else "Top 2"
        else:
            emoji = "🥉"
            position_text = "Топ 3" if lang == 'uk' else "Top 3"
        
        # ДОДАНО: Порожній рядок перед кожною програмою (крім першої)
        if i > 0:
            results += "\n"
        
        if lang == 'uk':
            results += f"{emoji} {position_text} – {display_program_name}\n\n"  # ДОДАНО: \n\n замість \n
            results += f"⭐{row['program_rating']:.2f} – середній рейтинг готелів, що входять до програми\n"
            results += f"(на основі відгуків з Google Maps):\n\n"  # ДОДАНО: \n\n замість \n
        else:
            results += f"{emoji} {position_text} – {display_program_name}\n\n"  # ДОДАНО: \n\n замість \n
            results += f"⭐{row['program_rating']:.2f} – average rating of hotels in the program\n"
            results += f"(based on Google Maps reviews):\n\n"  # ДОДАНО: \n\n замість \n
        
        # РЕГІОН
        if lang == 'uk':
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"📍 Регіон:\n"
            results += f" • {row['region_hotels']} готелів у {region_str}\n\n"  # ДОДАНО: \n\n замість \n
        else:
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"📍 Region:\n"
            results += f" • {row['region_hotels']} hotels in {region_str}\n\n"  # ДОДАНО: \n\n замість \n
        
        # КАТЕГОРІЯ - НОВИЙ КОМПАКТНИЙ ФОРМАТ
        if category:
            # Отримуємо дані для основної категорії
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_count = len(main_category_hotels[main_category_hotels['loyalty_program'] == program])
            
            # Отримуємо дані для суміжних категорій
            adjacent_categories = get_adjacent_categories(category)
            adjacent_total = 0
            adjacent_details = []
            
            for adj_cat in adjacent_categories:
                adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                adj_count = len(adj_category_hotels[adj_category_hotels['loyalty_program'] == program])
                adjacent_total += adj_count
                if adj_count > 0 or True:  # Завжди показуємо, навіть якщо 0
                    adjacent_details.append(adj_cat)
            
            if lang == 'uk':
                results += f"🏨 Сегмент:\n"
                results += f"Обраний – {category} – {main_count} готелів\n"
                if adjacent_details:
                    adj_cats_str = ' і '.join(adjacent_details)
                    results += f"Cуміжні – {adj_cats_str} – {adjacent_total} готелів\n\n"  # ДОДАНО: \n\n замість \n
                else:
                    results += "\n"  # ДОДАНО: порожній рядок, якщо немає суміжних
            else:
                results += f"🏨 Segment:\n"
                results += f"Selected – {category} – {main_count} hotels\n"
                if adjacent_details:
                    adj_cats_str = ' and '.join(adjacent_details)
                    results += f"Adjacent – {adj_cats_str} – {adjacent_total} hotels\n\n"  # ДОДАНО: \n\n замість \n
                else:
                    results += "\n"  # ДОДАНО: порожній рядок, якщо немає суміжних
        
        # СТИЛЬ - НОВИЙ КОМПАКТНИЙ ФОРМАТ
        if styles:
            if lang == 'uk':
                styles_str = '; '.join(styles)
                results += f"🎨 Стиль, позиціонування:\n{styles_str}.\n"
            else:
                styles_str = '; '.join(styles)
                results += f"🎨 Style, positioning:\n{styles_str}.\n"
            
            # Підрахунок готелів в обраних стилях для основної категорії
            main_style_total = 0
            if category:
                main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                main_style_filtered = filter_hotels_by_style(main_category_hotels, styles)
                main_style_total = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
            
            # Підрахунок готелів в обраних стилях для суміжних категорій
            adjacent_style_total = 0
            adjacent_categories_list = []
            if category:
                adjacent_categories = get_adjacent_categories(category)
                for adj_cat in adjacent_categories:
                    adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                    adj_style_filtered = filter_hotels_by_style(adj_category_hotels, styles)
                    adj_style_count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                    adjacent_style_total += adj_style_count
                    adjacent_categories_list.append(adj_cat)
            
            if lang == 'uk':
                results += f"  - {main_style_total} готелів в обраних стилях, в категорії {category}\n"
                if adjacent_style_total > 0 and adjacent_categories_list:
                    adj_cats_str = ' і '.join(adjacent_categories_list)
                    results += f"  - {adjacent_style_total} готелів в обраних стилях, в суміжних категоріях ({adj_cats_str})\n\n"  # ДОДАНО: \n\n замість \n
                else:
                    results += "\n"  # ДОДАНО: порожній рядок, якщо немає суміжних
            else:
                results += f"  - {main_style_total} hotels in selected styles, in {category} category\n"
                if adjacent_style_total > 0 and adjacent_categories_list:
                    adj_cats_str = ' and '.join(adjacent_categories_list)
                    results += f"  - {adjacent_style_total} hotels in selected styles, in adjacent categories ({adj_cats_str})\n\n"  # ДОДАНО: \n\n замість \n
                else:
                    results += "\n"  # ДОДАНО: порожній рядок, якщо немає суміжних
        
        # МЕТА - НОВИЙ КОМПАКТНИЙ ФОРМАТ
        if purposes:
            if lang == 'uk':
                purposes_str = '; '.join(purposes)
                results += f"🎯 Ціль подорожі:\n{purposes_str}:\n"
            else:
                purposes_str = '; '.join(purposes)
                results += f"🎯 Travel purpose:\n{purposes_str}:\n"
            
            # Підрахунок готелів для обраних цілей в основній категорії
            main_purpose_total = 0
            if category:
                main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, purposes)
                main_purpose_total = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
            
            # Підрахунок готелів для обраних цілей в суміжних категоріях
            adjacent_purpose_total = 0
            adjacent_categories_list = []
            if category:
                adjacent_categories = get_adjacent_categories(category)
                for adj_cat in adjacent_categories:
                    adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                    adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, purposes)
                    adj_purpose_count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                    adjacent_purpose_total += adj_purpose_count
                    adjacent_categories_list.append(adj_cat)
            
            if lang == 'uk':
                results += f"  - {main_purpose_total} готелів в обраних цілях, в категорії {category}\n"
                if adjacent_purpose_total > 0 and adjacent_categories_list:
                    adj_cats_str = ' і '.join(adjacent_categories_list)
                    results += f"  - {adjacent_purpose_total} готелів в обраних цілях, в суміжних категоріях ({adj_cats_str})\n"
                # ВИДАЛЕНО додатковий \n\n тут, оскільки це останній блок
            else:
                results += f"  - {main_purpose_total} hotels for selected purposes, in {category} category\n"
                if adjacent_purpose_total > 0 and adjacent_categories_list:
                    adj_cats_str = ' and '.join(adjacent_categories_list)
                    results += f"  - {adjacent_purpose_total} hotels for selected purposes, in adjacent categories ({adj_cats_str})\n"
                # ВИДАЛЕНО додатковий \n\n тут, оскільки це останній блок
        
        if i < 2:  # Додаємо роздільник між програмами (крім останньої)
            results += "\n" + "=" * 50 + "\n"  # ЗМІНЕНО: додано \n перед роздільником
    
    return results

def format_detailed_results_with_ratings(user_data, scores_df, lang='uk'):
    """
    НОВИЙ ПРОСТИЙ формат детального звіту /more без складних розрахунків - з абзацами
    """
    results = ""
    
    # Беремо всі програми для режиму /more
    all_programs = scores_df.head(7)  # Всі 7 програм лояльності
    
    # Отримуємо дані користувача
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    # Переводимо для обробки
    english_regions = translate_regions_to_english(regions)
    english_countries = translate_regions_to_english(countries)
    english_styles = translate_styles_to_english(styles)
    english_purposes = translate_purposes_to_english(purposes)
    
    # Фільтруємо дані за регіоном
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions, english_countries)
    
    for i, (index, row) in enumerate(all_programs.iterrows()):
        program = row['loyalty_program']
        
        # Замінюємо назву програми для відображення
        if program == "IHG One Rewards":
            display_program_name = "InterContinental Hotels One Rewards"
        else:
            display_program_name = program
        
        # Визначаємо емодзі для позиції
        emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
        
        # ДОДАНО: Порожній рядок перед кожною програмою (крім першої)
        if i > 0:
            results += "\n"
        
        if lang == 'uk':
            results += f"{emoji} Топ {i+1} – {display_program_name}\n\n"  # ДОДАНО: \n\n замість \n
            results += f"Середній рейтинг готелів, що входять до програми\n"
            results += f"(на основі відгуків з Google Maps): {row['program_rating']:.2f}⭐\n\n"  # ДОДАНО: \n\n замість \n
        else:
            results += f"{emoji} Top {i+1} – {display_program_name}\n\n"  # ДОДАНО: \n\n замість \n
            results += f"Average rating of hotels in the program\n"
            results += f"(based on Google Maps reviews): {row['program_rating']:.2f}⭐\n\n"  # ДОДАНО: \n\n замість \n
        
        # РЕГІОН
        if lang == 'uk':
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"📍 Регіон:\n"
            results += f" • {row['region_hotels']} готелів у {region_str}\n\n"  # ДОДАНО: \n\n замість \n
        else:
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"📍 Region:\n"
            results += f" • {row['region_hotels']} hotels in {region_str}\n\n"  # ДОДАНО: \n\n замість \n
        
        # СЕГМЕНТ
        if category:
            # Отримуємо дані для основної категорії
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_count = len(main_category_hotels[main_category_hotels['loyalty_program'] == program])
            
            # Отримуємо дані для суміжних категорій
            adjacent_categories = get_adjacent_categories(category)
            adjacent_total = 0
            adjacent_details = []
            
            for adj_cat in adjacent_categories:
                adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                adj_count = len(adj_category_hotels[adj_category_hotels['loyalty_program'] == program])
                adjacent_total += adj_count
                adjacent_details.append(adj_cat)
            
            if lang == 'uk':
                results += f"🏨 Сегмент:\n"
                results += f" • {main_count} готелів в категорії {category} (обраний сегмент)\n"
                if adjacent_details:
                    adj_cats_str = ' і '.join(adjacent_details)
                    results += f" • {adjacent_total} готелів в категоріях {adj_cats_str} (суміжний сегмент до обраного)\n\n"  # ДОДАНО: \n\n замість \n
                else:
                    results += "\n"  # ДОДАНО: порожній рядок, якщо немає суміжних
            else:
                results += f"🏨 Segment:\n"
                results += f" • {main_count} hotels in {category} category (selected segment)\n"
                if adjacent_details:
                    adj_cats_str = ' and '.join(adjacent_details)
                    results += f" • {adjacent_total} hotels in {adj_cats_str} categories (adjacent segment to selected)\n\n"  # ДОДАНО: \n\n замість \n
                else:
                    results += "\n"  # ДОДАНО: порожній рядок, якщо немає суміжних
        
        # СТИЛЬ - ДЕТАЛЬНИЙ РОЗБІР ПО КОЖНОМУ СТИЛЮ
        if styles:
            if lang == 'uk':
                results += f"🎨 Стиль, позиціонування:\n\n"  # ДОДАНО: \n\n замість \n
            else:
                results += f"🎨 Style, positioning:\n\n"  # ДОДАНО: \n\n замість \n
            
            for j, style in enumerate(styles):
                results += f"{style}:\n"
                
                # Основна категорія для цього стилю
                main_style_total = 0
                if category:
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_style_filtered = filter_hotels_by_style(main_category_hotels, [style])
                    main_style_total = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
                
                # Суміжні категорії для цього стилю
                adjacent_style_total = 0
                adjacent_style_details = []
                if category:
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_style_filtered = filter_hotels_by_style(adj_category_hotels, [style])
                        adj_style_count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                        adjacent_style_total += adj_style_count
                        adjacent_style_details.append(adj_cat)
                
                if lang == 'uk':
                    results += f"  - {main_style_total} готелів в стилі «{style}» в категорії {category}\n"
                    if adjacent_style_details:
                        adj_cats_str = ' i '.join(adjacent_style_details)
                        results += f"  - {adjacent_style_total} готелів в стилі «{style}» в суміжних категоріях ({adj_cats_str})\n"
                    # ДОДАНО: абзац після кожного стилю (крім останнього)
                    if j < len(styles) - 1:
                        results += "\n"
                else:
                    results += f"  - {main_style_total} hotels in «{style}» style in {category} category\n"
                    if adjacent_style_details:
                        adj_cats_str = ' and '.join(adjacent_style_details)
                        results += f"  - {adjacent_style_total} hotels in «{style}» style in adjacent categories ({adj_cats_str})\n"
                    # ДОДАНО: абзац після кожного стилю (крім останнього)
                    if j < len(styles) - 1:
                        results += "\n"
            
            # ДОДАНО: абзац після всіх стилів
            results += "\n"
        
        # ЦІЛЬ - ДЕТАЛЬНИЙ РОЗБІР ПО КОЖНІЙ ЦІЛІ
        if purposes:
            if lang == 'uk':
                results += f"🎯 Ціль подорожі:\n\n"  # ДОДАНО: \n\n замість \n
            else:
                results += f"🎯 Travel purpose:\n\n"  # ДОДАНО: \n\n замість \n
            
            for j, purpose in enumerate(purposes):
                results += f"{purpose}:\n"
                
                # Основна категорія для цієї мети
                main_purpose_total = 0
                if category:
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, [purpose])
                    main_purpose_total = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
                
                # Суміжні категорії для цієї мети
                adjacent_purpose_total = 0
                adjacent_purpose_details = []
                if category:
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, [purpose])
                        adj_purpose_count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                        adjacent_purpose_total += adj_purpose_count
                        adjacent_purpose_details.append(adj_cat)
                
                if lang == 'uk':
                    results += f"  - {main_purpose_total} готелів, що відповідають цілі «{purpose}» в категорії {category}\n"
                    if adjacent_purpose_details:
                        adj_cats_str = ' i '.join(adjacent_purpose_details)
                        results += f"  - {adjacent_purpose_total} готелів, що відповідають цілі «{purpose}» в суміжних сегментах ({adj_cats_str})\n"
                    # ДОДАНО: абзац після кожної мети (крім останньої)
                    if j < len(purposes) - 1:
                        results += "\n"
                else:
                    results += f"  - {main_purpose_total} hotels matching «{purpose}» purpose in {category} category\n"
                    if adjacent_purpose_details:
                        adj_cats_str = ' and '.join(adjacent_purpose_details)
                        results += f"  - {adjacent_purpose_total} hotels matching «{purpose}» purpose in adjacent segments ({adj_cats_str})\n"
                    # ДОДАНО: абзац після кожної мети (крім останньої)
                    if j < len(purposes) - 1:
                        results += "\n"
        
        if i < len(all_programs) - 1:  # Додаємо роздільник між програмами (крім останньої)
            results += "\n" + "=" * 50 + "\n"  # ЗМІНЕНО: додано \n перед роздільником
    
    return results

def format_admin_scoring_report(user_data, scores_df):
    """
    Форматує ПОВНИЙ адміністративний звіт з детальним розбором нарахування балів
    """
    results = ""
    
    # Беремо всі 7 програм
    all_programs = scores_df.head(7)
    
    # Отримуємо дані користувача
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    # Переводимо для обробки
    english_regions = translate_regions_to_english(regions)
    english_countries = translate_regions_to_english(countries)
    english_styles = translate_styles_to_english(styles)
    english_purposes = translate_purposes_to_english(purposes)
    
    # Фільтруємо дані за регіоном
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions, english_countries)
    
    for i, (index, row) in enumerate(all_programs.iterrows()):
        program = row['loyalty_program']
        
        # Замінюємо назву програми для відображення
        if program == "IHG One Rewards":
            display_program_name = "InterContinental Hotels One Rewards"
        else:
            display_program_name = program
        
        # Визначаємо емодзі для позиції
        emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
        
        results += f"{emoji} Топ {i+1}. {display_program_name}\n"
        results += f"Загальний бал: {row['total_score']:.2f}\n"
        results += "-" * 30 + "\n"
        
        # РЕГІОН - детальний розбір
        region_score = row['region_score']
        region_hotels = row['region_hotels']
        region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
        
        results += f"📍 Регіон: {region_score:.1f} балів\n"
        results += f"{region_hotels} готелів у {region_str}\n\n"
        
        # СЕГМЕНТ - детальний розбір з підрахунками
        if category:
            results += f"🏨 Сегмент: {row['category_score']:.1f} балів\n"
            
            # Основна категорія
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_count = len(main_category_hotels[main_category_hotels['loyalty_program'] == program])
            
            # Отримуємо бали для основної категорії
            main_counts_dict = main_category_hotels.groupby('loyalty_program').size().to_dict()
            main_scores_dict = distribute_scores_with_ties(main_counts_dict, MAIN_CATEGORY_POINTS)
            main_score = main_scores_dict.get(program, 0.0)
            
            results += f"(основний) {category} – {main_count} готелів – {main_score:.1f} балів\n"
            
            # Суміжні категорії
            adjacent_categories = get_adjacent_categories(category)
            adjacent_scores = []
            adjacent_total_score = 0.0
            
            for adj_cat in adjacent_categories:
                adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                adj_count = len(adj_category_hotels[adj_category_hotels['loyalty_program'] == program])
                
                # Отримуємо бали для суміжної категорії
                adj_counts_dict = adj_category_hotels.groupby('loyalty_program').size().to_dict()
                adj_scores_dict = distribute_scores_with_ties(adj_counts_dict, ADJACENT_CATEGORY_POINTS)
                adj_score = adj_scores_dict.get(program, 0.0)
                
                adjacent_scores.append(adj_score)
                adjacent_total_score += adj_score
                
                results += f"(суміжний) {adj_cat} – {adj_count} готелів – {adj_score:.1f} балів\n"
            
            # Підрахунок
            if adjacent_scores:
                adjacent_sum_str = " + ".join([f"{score:.1f}" for score in adjacent_scores])
                results += f"Підрахунок: {main_score:.1f} + ({adjacent_sum_str}) = {row['category_score']:.1f}\n\n"
            else:
                results += f"Підрахунок: {main_score:.1f} = {row['category_score']:.1f}\n\n"
        
        # СТИЛЬ - детальний розбір з нормалізацією
        if styles:
            results += f"🎨 Стиль, позиціонування: {row['style_score']:.1f} балів\n"
            
            total_style_points = 0.0
            style_details = []
            
            for style in styles:
                results += f"{style}:\n"
                
                # Основна категорія для стилю
                if category:
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_style_filtered = filter_hotels_by_style(main_category_hotels, [style])
                    main_style_count = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
                    
                    # Отримуємо бали для основного стилю
                    main_style_counts = main_style_filtered.groupby('loyalty_program').size().to_dict()
                    main_style_scores = distribute_scores_with_ties(main_style_counts, MAIN_CATEGORY_POINTS)
                    main_style_score = main_style_scores.get(program, 0.0)
                    
                    results += f"{style} в {category.lower()} – {main_style_count} готелів – {main_style_score:.1f} балів\n"
                    total_style_points += main_style_score
                    
                    # Суміжні категорії для стилю
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_style_filtered = filter_hotels_by_style(adj_category_hotels, [style])
                        adj_style_count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                        
                        # Отримуємо бали для суміжного стилю
                        adj_style_counts = adj_style_filtered.groupby('loyalty_program').size().to_dict()
                        adj_style_scores = distribute_scores_with_ties(adj_style_counts, ADJACENT_CATEGORY_POINTS)
                        adj_style_score = adj_style_scores.get(program, 0.0)
                        
                        results += f"{style} в {adj_cat.lower()} (суміжний) – {adj_style_count} готелів – {adj_style_score:.1f} балів\n"
                        total_style_points += adj_style_score
            
            # Нормалізація стилів
            normalization_factor = len(styles)
            results += f"Сума: {total_style_points:.1f} балів\n"
            results += f"Нормалізація: {total_style_points:.1f} ÷ {normalization_factor} стилі = {row['style_score']:.1f} балів\n\n"
        
        # ЦІЛЬ - детальний розбір з нормалізацією
        if purposes:
            results += f"🎯 Ціль подорожі: {row['purpose_score']:.1f} балів\n"
            
            total_purpose_points = 0.0
            
            for purpose in purposes:
                results += f"{purpose}:\n"
                
                # Основна категорія для мети
                if category:
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, [purpose])
                    main_purpose_count = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
                    
                    # Отримуємо бали для основної мети
                    main_purpose_counts = main_purpose_filtered.groupby('loyalty_program').size().to_dict()
                    main_purpose_scores = distribute_scores_with_ties(main_purpose_counts, MAIN_CATEGORY_POINTS)
                    main_purpose_score = main_purpose_scores.get(program, 0.0)
                    
                    results += f"{purpose} в {category.lower()} – {main_purpose_count} готелів – {main_purpose_score:.1f} балів\n"
                    total_purpose_points += main_purpose_score
                    
                    # Суміжні категорії для мети
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, [purpose])
                        adj_purpose_count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                        
                        # Отримуємо бали для суміжної мети
                        adj_purpose_counts = adj_purpose_filtered.groupby('loyalty_program').size().to_dict()
                        adj_purpose_scores = distribute_scores_with_ties(adj_purpose_counts, ADJACENT_CATEGORY_POINTS)
                        adj_purpose_score = adj_purpose_scores.get(program, 0.0)
                        
                        results += f"{purpose} в {adj_cat.lower()} (суміжний) – {adj_purpose_count} готелів – {adj_purpose_score:.1f} балів\n"
                        total_purpose_points += adj_purpose_score
            
            # Нормалізація цілей
            normalization_factor = len(purposes)
            results += f"Сума: {total_purpose_points:.1f} балів\n"
            results += f"Нормалізація: {total_purpose_points:.1f} ÷ {normalization_factor} цілі = {row['purpose_score']:.1f} балів\n\n"
        
        # ПІДСУМОК з рейтинг-коефіцієнтом
        base_score = row['base_score']
        program_rating = row['program_rating']
        rating_coefficient = row['rating_coefficient']
        final_score = row['total_score']
        
        results += "➕ ПІДСУМОК:\n"
        results += f"{row['region_score']:.1f} + {row['category_score']:.1f} + {row['style_score']:.1f} + {row['purpose_score']:.1f} = {base_score:.2f} балів\n"
        results += f"Рейтинг програми: {program_rating:.2f}★\n"
        results += f"Рейтинг-коефіцієнт: {program_rating:.2f} ÷ 5.0 = {rating_coefficient:.3f}\n"
        results += f"Фінальний результат: {base_score:.2f} × {rating_coefficient:.3f} = {final_score:.2f} балів\n"
        
        if i < len(all_programs) - 1:  # Додаємо роздільник між програмами (крім останньої)
            results += "\n" + "=" * 50 + "\n\n"
    
    return results

# ===============================
# ФУНКЦІЯ MAIN ТА ЗАПУСК БОТА
# ===============================

def main(token, csv_path, webhook_url=None, webhook_port=None, webhook_path=None):
    """Головна функція запуску бота з підтримкою webhook"""
    # Завантаження даних
    global hotel_data
    hotel_data = load_hotel_data(csv_path)
    
    if hotel_data is None:
        logger.error("Не вдалося завантажити дані. Бот не запущено.")
        return
    
    # Додаткова перевірка наявності необхідних колонок
    required_columns = ['loyalty_program', 'region', 'country', 'Hotel Brand']
    missing_required = [col for col in required_columns if col not in hotel_data.columns]
    
    if missing_required:
        logger.error(f"Відсутні критично важливі колонки: {missing_required}. Бот не запущено.")
        return
    
    # Переконуємося, що є колонка 'segment'
    if 'segment' not in hotel_data.columns:
        logger.error("Відсутня колонка 'segment'. Бот не запущено.")
        return
    
    # ДОДАНО: Логування успішного завантаження даних та рейтингів
    logger.info(f"Успішно завантажено дані готелів: {len(hotel_data)} записів")
    logger.info(f"Доступні програми лояльності з рейтингами: {list(LOYALTY_PROGRAM_RATINGS.keys())}")
    
    # Створення застосунку
    app = Application.builder().token(token)
    
    # Побудова застосунку
    application = app.build()
    
    # ОНОВЛЕНО: Налаштування обробників з додаванням команди /more
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            WAITING_REGION_SUBMIT: [CallbackQueryHandler(region_choice)],
            CATEGORY: [CallbackQueryHandler(category_choice)],
            WAITING_STYLE_SUBMIT: [CallbackQueryHandler(style_choice)],
            WAITING_PURPOSE_SUBMIT: [CallbackQueryHandler(purpose_choice)]
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", start)  # Додаємо /start як fallback
        ]
    )
    
    # Додаємо основний обробник розмови
    application.add_handler(conv_handler)
    
    # ДОДАНО: Обробник команди /more для детального розбору
    application.add_handler(CommandHandler("more", show_more_details))

    # ДОДАНО: Обробник команди /21 для адміністративного розбору балів
    application.add_handler(CommandHandler("21", show_admin_scoring_breakdown))
    
    # ДОДАНО: Логування доступних команд
    logger.info("Зареєстровані команди бота:")
    logger.info("  /start - початок опитування")
    logger.info("  /cancel - скасування розмови")
    logger.info("  /more - детальний розбір останніх результатів")
    logger.info("  /21 - адміністративний розбір нарахування балів")
    
    # ВИПРАВЛЕНО: Отримуємо порт з змінних середовища
    port = int(os.environ.get("PORT", "10000"))
    
    # ВИПРАВЛЕНО: Логування налаштувань
    logger.info(f"Port from environment: {port}")
    logger.info(f"Webhook URL: {webhook_url}")
    logger.info(f"Webhook path: {webhook_path}")
    
    if webhook_url and webhook_path:
        webhook_info = f"{webhook_url}{webhook_path}"
        logger.info(f"Запуск бота в режимі webhook на {webhook_info}")
        
        try:
            application.run_webhook(
                listen="0.0.0.0",  # ВАЖЛИВО: слухати на всіх інтерфейсах
                port=port,
                url_path=webhook_path,
                webhook_url=webhook_info,
                allowed_updates=Update.ALL_TYPES
            )
        except Exception as e:
            logger.error(f"Помилка запуску webhook: {e}")
            logger.info("Перехід на polling режим...")
            application.run_polling(allowed_updates=Update.ALL_TYPES)
    else:
        logger.info("WEBHOOK_URL не вказано. Запуск бота в режимі polling...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    
    logger.info("Бот успішно запущено з підтримкою рейтингових розрахунків")

if __name__ == "__main__":
    # Використовуємо змінні середовища або значення за замовчуванням
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
    CSV_PATH = os.environ.get("CSV_PATH", "hotel_data.csv")

    if not CSV_PATH:
        logger.error("CSV_PATH не задано. Завершення запуску.")
        exit(1)
    logger.info(f"Використовується шлях до CSV: {CSV_PATH}")
    
    # Параметри для webhook (опціонально)
    WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST", "").replace("https://", "")  # Очистити https://, якщо є
    WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", f"/webhook/{TOKEN}")
    
    # Формуємо повну URL для webhook, якщо вказано WEBHOOK_HOST
    WEBHOOK_URL = f"https://{WEBHOOK_HOST}" if WEBHOOK_HOST else None
    
    # Перевіряємо наявність токена
    if TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logger.warning("Токен бота не налаштовано! Встановіть змінну середовища TELEGRAM_BOT_TOKEN або змініть значення в коді.")
    
    # ДОДАНО: Логування версії бота
    logger.info("="*60)
    logger.info("🤖 HOTEL LOYALTY PROGRAM BOT v2.0 (WITH RATINGS)")
    logger.info("="*60)
    
    # ВИПРАВЛЕНО: Додаткове логування для діагностики
    logger.info(f"Starting bot with TOKEN: {TOKEN[:10]}...")
    logger.info(f"CSV_PATH: {CSV_PATH}")
    logger.info(f"WEBHOOK_HOST: {WEBHOOK_HOST}")
    logger.info(f"WEBHOOK_URL: {WEBHOOK_URL}")
    logger.info(f"PORT: {os.environ.get('PORT', '10000')}")
    
    # ДОДАНО: Логування налаштувань рейтингів
    logger.info(f"Loyalty program ratings loaded: {len(LOYALTY_PROGRAM_RATINGS)} programs")
    logger.info("Available ratings:")
    for program, rating in LOYALTY_PROGRAM_RATINGS.items():
        logger.info(f"  {program}: {rating}★")
    
    # Запускаємо бота з підтримкою webhook або polling
    main(TOKEN, CSV_PATH, WEBHOOK_URL, 10000, WEBHOOK_PATH)

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
    "IHG One Rewards": 4.19,
    "Marriott Bonvoy": 4.22,
    "World of Hyatt": 4.28,
    "Wyndham Rewards": 3.17
}

# ДОДАНО: Глобальна змінна для зберігання останніх результатів користувача (для команд /more та /scoring)
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

# ОНОВЛЕНО: Функція для команди /more - тепер показує всі 7 програм
async def show_more_details(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показує детальний розбір для всіх 7 програм лояльності"""
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
        # Генерируем детальний звіт для ВСІХ 7 програм
        detailed_results = format_detailed_results_all_programs(user_data, scores_df, lang)
        
        # Відправляємо детальні результати
        if lang == 'uk':
            intro_text = ("🎉 **Детальний аналіз завершено!**\n\n"
                         "Ось всі 7 програм лояльності готелів з детальним розбором:\n\n")
            outro_text = ("\n\n💡 **Хочете ще більше деталей про розрахунок балів?**\n"
                         "Натисніть /scoring для повного розбору балів або /start для нового пошуку")
        else:
            intro_text = ("🎉 **Detailed analysis completed!**\n\n"
                         "Here are all 7 hotel loyalty programs with detailed breakdown:\n\n")
            outro_text = ("\n\n💡 **Want even more details about scoring?**\n"
                         "Type /scoring for complete scoring breakdown or /start for a new search")
        
        # Відправляємо детальний звіт
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

# НОВА: Функція для команди /scoring - показує детальний розбір балів
async def show_scoring_details(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показує детальний розбір нарахування балів для топ-3 програм"""
    user_id = update.effective_user.id
    
    # Перевіряємо, чи є збережені результати для цього користувача
    if user_id not in user_last_results:
        lang = user_data_global.get(user_id, {}).get('language', 'uk')
        
        if lang == 'uk':
            await update.message.reply_text(
                "У вас немає останніх результатів для відображення розбору балів.\n"
                "Спочатку пройдіть опитування командою /start."
            )
        else:
            await update.message.reply_text(
                "You don't have recent results to show scoring breakdown.\n"
                "Please complete the survey first with /start command."
            )
        return ConversationHandler.END
    
    # Отримуємо збережені дані
    saved_data = user_last_results[user_id]
    user_data = saved_data['user_data']
    scores_df = saved_data['scores_df']
    lang = user_data.get('language', 'uk')
    
    try:
        # Генерируем детальний розбір балів для ТОП-3 програм
        scoring_results = format_scoring_breakdown(user_data, scores_df, lang)
        
        # Відправляємо результати розбору балів
        if lang == 'uk':
            intro_text = ("🎉 **Детальний аналіз завершено!**\n\n"
                         "Ось топ-3 програми лояльності готелів з повним розбором балів:\n\n")
            outro_text = ("\n\n📝 **Пояснення системи балів:**\n"
                         "• **Основна категорія**: бали за вибрану категорію (21,18,15,12,9,6,3)\n"
                         "• **Суміжні категорії**: СУМА всіх додаткових балів (7,6,5,4,3,2,1)\n"
                         "• **Luxury**: суміжна Comfort\n"
                         "• **Comfort**: суміжні Luxury + Standard\n"
                         "• **Standard**: суміжна Comfort\n"
                         "• **Готелі = 0**: бали = 0\n"
                         "• **Стиль/Мета**: спочатку сума всіх балів, потім ділення на кількість\n"
                         "• **Рейтинг-коефіцієнт**: середній рейтинг програми ÷ 5.0\n"
                         "• **Фінальний бал**: базовий бал × рейтинг-коефіцієнт\n\n"
                         "Щоб почати нове опитування, надішліть команду /start.")
        else:
            intro_text = ("🎉 **Detailed analysis completed!**\n\n"
                         "Here are the top 3 hotel loyalty programs with complete score breakdown:\n\n")
            outro_text = ("\n\n📝 **Scoring system explanation:**\n"
                         "• **Main category**: points for selected category (21,18,15,12,9,6,3)\n"
                         "• **Adjacent categories**: SUM of all additional points (7,6,5,4,3,2,1)\n"
                         "• **Luxury**: adjacent Comfort\n"
                         "• **Comfort**: adjacent Luxury + Standard\n"
                         "• **Standard**: adjacent Comfort\n"
                         "• **Hotels = 0**: points = 0\n"
                         "• **Style/Purpose**: first sum all points, then divide by quantity\n"
                         "• **Rating coefficient**: program average rating ÷ 5.0\n"
                         "• **Final score**: base score × rating coefficient\n\n"
                         "To start a new survey, send the /start command.")
        
        # Відправляємо повний розбір балів
        full_message = intro_text + scoring_results + outro_text
        await send_long_message_to_chat(context, update.message.chat_id, full_message)
        
    except Exception as e:
        logger.error(f"Помилка при показі розбору балів: {e}")
        
        if lang == 'uk':
            await update.message.reply_text(
                "Виникла помилка при відображенні розбору балів. "
                "Спробуйте пройти опитування знову командою /start."
            )
        else:
            await update.message.reply_text(
                "An error occurred while displaying scoring breakdown. "
                "Please try taking the survey again with /start command."
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

async def ask_style(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про стиль готелю з чекбоксами та детальними описами"""
    
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
    
    lang = user_data_global[user_id]['language']
    
    # Ініціалізуємо вибрані стилі, якщо їх ще не обрано
    if 'selected_styles' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_styles'] = []
    
    # Створюємо InlineKeyboard з чекбоксами для стилів
    if lang == 'uk':
        styles = [
            "Розкішний і вишуканий", 
            "Бутік і унікальний", 
            "Класичний і традиційний", 
            "Сучасний і дизайнерський",
            "Затишний і сімейний", 
            "Практичний і економічний"
        ]
        
        styles_description = (
            "Питання 3/4:\n"
            "Який стиль готелю ви зазвичай обираєте?\n"
            "*(Оберіть до трьох варіантів)*\n\n"
            "1. **Розкішний і вишуканий** (преміум-матеріали, елегантний дизайн, високий рівень сервісу)\n"
            "2. **Бутік і унікальний** (оригінальний інтер'єр, творча атмосфера, відчуття ексклюзивності)\n"
            "3. **Класичний і традиційний** (перевірений часом стиль, консервативність, історичність)\n"
            "4. **Сучасний і дизайнерський** (модні інтер'єри, мінімалізм, технологічність)\n"
            "5. **Затишний і сімейний** (тепла атмосфера, комфорт, дружній до дітей)\n"
            "6. **Практичний і економічний** (без зайвих деталей, функціональний, доступний)"
        )
        
        title_text = styles_description
        submit_text = "Відповісти"
    else:
        styles = [
            "Luxurious and refined", 
            "Boutique and unique",
            "Classic and traditional", 
            "Modern and designer",
            "Cozy and family-friendly", 
            "Practical and economical"
        ]
        
        styles_description = (
            "Question 3/4:\n"
            "What hotel style do you usually choose?\n"
            "*(Choose up to three options)*\n\n"
            "1. **Luxurious and refined** (premium materials, elegant design, high level of service)\n"
            "2. **Boutique and unique** (original interior, creative atmosphere, sense of exclusivity)\n"
            "3. **Classic and traditional** (time-tested style, conservatism, historical ambiance)\n"
            "4. **Modern and designer** (fashionable interiors, minimalism, technological features)\n"
            "5. **Cozy and family-friendly** (warm atmosphere, comfort, child-friendly)\n"
            "6. **Practical and economical** (no unnecessary details, functional, affordable)"
        )
        
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


# ===============================
# ЧАСТИНА 10: ОНОВЛЕНІ ФУНКЦІЇ ФОРМАТУВАННЯ РЕЗУЛЬТАТІВ ТА MAIN
# ===============================

# ВИПРАВЛЕНА: Функція для звичайного звіту (топ-3, компактний формат)
def format_simple_results(user_data, scores_df, lang='uk'):
    """
    Генерує звичайний звіт у форматі з документа - показує тільки перші ТОП 3
    """
    results = ""
    
    # Беремо тільки топ-3 програми
    top_programs = scores_df.head(3)
    
    # Отримуємо дані користувача для аналізу
    regions = user_data.get('regions', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    # Переводимо для обробки
    english_regions = translate_regions_to_english(regions)
    english_styles = translate_styles_to_english(styles)
    english_purposes = translate_purposes_to_english(purposes)
    
    # Фільтруємо дані за регіоном для детального аналізу
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions)
    
    for i, (index, row) in enumerate(top_programs.iterrows()):
        program = row['loyalty_program']
        
        # Визначаємо емодзі для позиції
        emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
        
        if lang == 'uk':
            results += f"{emoji} Топ {i+1}. {program}\n"
            results += f"Середній рейтинг готелів, що входять до програми\n"
            results += f"(на основі відгуків з Google Maps): {row['program_rating']:.2f} ⭐\n"
        else:
            results += f"{emoji} Top {i+1}. {program}\n"
            results += f"Average rating of hotels in the program\n"
            results += f"(based on Google Maps reviews): {row['program_rating']:.2f} ⭐\n"
        
        # РЕГІОН
        if lang == 'uk':
            region_str = ', '.join(regions) if regions else 'N/A'
            results += f"📍 REGION:\n"
            results += f" • {row['region_hotels']} готелів у {region_str}\n"
        else:
            region_str = ', '.join(regions) if regions else 'N/A'
            results += f"📍 REGION:\n"
            results += f" • {row['region_hotels']} hotels in {region_str}\n"
        
        # КАТЕГОРІЯ - НОВИЙ ФОРМАТ згідно документа
        if category:
            # Основна категорія
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_count = len(main_category_hotels[main_category_hotels['loyalty_program'] == program])
            
            # Суміжні категорії
            adjacent_categories = get_adjacent_categories(category)
            adjacent_total = 0
            adjacent_names = []
            
            for adj_cat in adjacent_categories:
                adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                adj_count = len(adj_category_hotels[adj_category_hotels['loyalty_program'] == program])
                adjacent_total += adj_count
                adjacent_names.append(adj_cat)
            
            if lang == 'uk':
                results += f"🏨 CATEGORY: Обрана категорія – {category} – {main_count} готелів"
                if adjacent_names:
                    adj_names_str = ' і '.join(adjacent_names)
                    results += f" Cуміжні категорії – {adj_names_str} – {adjacent_total} готелів\n"
                else:
                    results += "\n"
            else:
                results += f"🏨 CATEGORY: Selected category – {category} – {main_count} hotels"
                if adjacent_names:
                    adj_names_str = ' and '.join(adjacent_names)
                    results += f" Adjacent categories – {adj_names_str} – {adjacent_total} hotels\n"
                else:
                    results += "\n"
        
        # СТИЛЬ - НОВИЙ ФОРМАТ (як в документі: всі стилі в одному рядку)
        if styles:
            if lang == 'uk':
                results += f"🎨 STYLE: {'; '.join(styles)}.\n"
                
                # Підрахунок готелів в обраних стилях
                main_style_total = 0
                adj_style_total = 0
                
                if category:
                    # Основна категорія
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_style_filtered = filter_hotels_by_style(main_category_hotels, styles)
                    main_style_total = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
                    
                    # Суміжні категорії
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_style_filtered = filter_hotels_by_style(adj_category_hotels, styles)
                        adj_style_count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                        adj_style_total += adj_style_count
                
                results += f"  - {main_style_total} готелів в обраних стилях, в категорії {category}\n"
                if adjacent_categories:
                    adj_cats_str = ', '.join(adjacent_categories)
                    results += f"  - {adj_style_total} готелів в обраних стилях, в суміжних категоріях ({adj_cats_str})\n"
            else:
                results += f"🎨 STYLE: {'; '.join(styles)}.\n"
                
                # Підрахунок готелів в обраних стилях
                main_style_total = 0
                adj_style_total = 0
                
                if category:
                    # Основна категорія
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_style_filtered = filter_hotels_by_style(main_category_hotels, styles)
                    main_style_total = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
                    
                    # Суміжні категорії
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_style_filtered = filter_hotels_by_style(adj_category_hotels, styles)
                        adj_style_count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                        adj_style_total += adj_style_count
                
                results += f"  - {main_style_total} hotels in selected styles, in {category} category\n"
                if adjacent_categories:
                    adj_cats_str = ', '.join(adjacent_categories)
                    results += f"  - {adj_style_total} hotels in selected styles, in adjacent categories ({adj_cats_str})\n"
        
        # МЕТА - НОВИЙ ФОРМАТ (як в документі)
        if purposes:
            if lang == 'uk':
                results += f"🎯 Ціль:\n"
                purpose_str = '; '.join(purposes)
                results += f"{purpose_str}:\n"
                
                # Підрахунок готелів для обраних цілей
                main_purpose_total = 0
                adj_purpose_total = 0
                adj_details = []
                
                if category:
                    # Основна категорія
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, purposes)
                    main_purpose_total = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
                    
                    # Суміжні категорії
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, purposes)
                        adj_purpose_count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                        adj_purpose_total += adj_purpose_count
                        adj_details.append(f"{adj_cat}: {adj_purpose_count}")
                
                results += f"  - {main_purpose_total} в обраних цілях, в категорії {category}\n"
                if adj_details:
                    results += f"  - {adj_purpose_total} в обраних цілях, в суміжних категоріях ({', '.join(adj_details)})\n"
            else:
                results += f"🎯 Goal:\n"
                purpose_str = '; '.join(purposes)
                results += f"{purpose_str}:\n"
                
                # Підрахунок готелів для обраних цілей
                main_purpose_total = 0
                adj_purpose_total = 0
                adj_details = []
                
                if category:
                    # Основна категорія
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, purposes)
                    main_purpose_total = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
                    
                    # Суміжні категорії
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, purposes)
                        adj_purpose_count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                        adj_purpose_total += adj_purpose_count
                        adj_details.append(f"{adj_cat}: {adj_purpose_count}")
                
                results += f"  - {main_purpose_total} in selected goals, in {category} category\n"
                if adj_details:
                    results += f"  - {adj_purpose_total} in selected goals, in adjacent categories ({', '.join(adj_details)})\n"
        
        results += "\n"  # Відступ між програмами
    
    return results

# НОВА: Функція для детального звіту всіх програм (/more)
def format_detailed_results_all_programs(user_data, scores_df, lang='uk'):
    """
    Генерує детальний звіт для ВСІХ 7 програм лояльності
    """
    results = ""
    
    # Беремо ВСІ програми (не тільки топ-3)
    all_programs = scores_df.head(7)  # Всі 7 програм лояльності
    
    # Отримуємо дані користувача
    regions = user_data.get('regions', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    # Переводимо для обробки
    english_regions = translate_regions_to_english(regions)
    english_styles = translate_styles_to_english(styles)
    english_purposes = translate_purposes_to_english(purposes)
    
    # Фільтруємо дані за регіоном
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions)
    
    for i, (index, row) in enumerate(all_programs.iterrows()):
        program = row['loyalty_program']
        
        # Визначаємо емодзі для позиції
        emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
        
        if lang == 'uk':
            results += f"{emoji} Топ {i+1}. {program}\n"
            results += f"Фінальний бал: {row['total_score']:.2f}\n"
            results += f"Середній рейтинг готелів, що входять до програми\n"
            results += f"(на основі відгуків з Google Maps): {row['program_rating']:.2f}⭐\n"
        else:
            results += f"{emoji} Top {i+1}. {program}\n"
            results += f"Final score: {row['total_score']:.2f}\n"
            results += f"Average rating of hotels in the program\n"
            results += f"(based on Google Maps reviews): {row['program_rating']:.2f}⭐\n"
        
        # РЕГІОН
        if lang == 'uk':
            region_str = ', '.join(regions) if regions else 'N/A'
            results += f"📍 REGION:\n"
            results += f" • {row['region_hotels']} готелів у {region_str}\n"
        else:
            region_str = ', '.join(regions) if regions else 'N/A'
            results += f"📍 REGION:\n"
            results += f" • {row['region_hotels']} hotels in {region_str}\n"
        
        # КАТЕГОРІЯ з детальним розбором
        if category:
            # Отримуємо дані для основної категорії
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_count = len(main_category_hotels[main_category_hotels['loyalty_program'] == program])
            
            # Отримуємо дані для суміжних категорій
            adjacent_categories = get_adjacent_categories(category)
            adjacent_details = []
            
            for adj_cat in adjacent_categories:
                adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                adj_count = len(adj_category_hotels[adj_category_hotels['loyalty_program'] == program])
                adjacent_details.append(f"{adj_cat} i Standart" if adj_cat == "Luxury" else adj_cat)
            
            if lang == 'uk':
                results += f"🏨 CATEGORY:\n"
                results += f" • {main_count} готелів в категорії {category} (обрана категорія)\n"
                if adjacent_details:
                    adj_cats_str = ' і '.join(adjacent_details)
                    results += f" • готелів в категоріях {adj_cats_str} (суміжні категорії до обраної)\n"
            else:
                results += f"🏨 CATEGORY:\n"
                results += f" • {main_count} hotels in {category} category (selected category)\n"
                if adjacent_details:
                    adj_cats_str = ' and '.join(adjacent_details)
                    results += f" • hotels in {adj_cats_str} categories (adjacent to selected)\n"
        
        # СТИЛЬ з детальним розбором
        if styles:
            if lang == 'uk':
                results += f"🎨 STYLE: {'; '.join(styles)}:\n"
            else:
                results += f"🎨 STYLE: {'; '.join(styles)}:\n"
            
            # Детальний розбір для кожного стилю
            if category:
                # Основна категорія
                main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                main_style_filtered = filter_hotels_by_style(main_category_hotels, styles)
                main_style_count = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
                
                # Суміжні категорії
                adjacent_categories = get_adjacent_categories(category)
                adjacent_style_counts = []
                adjacent_style_total = 0
                
                for adj_cat in adjacent_categories:
                    adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                    adj_style_filtered = filter_hotels_by_style(adj_category_hotels, styles)
                    adj_style_count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                    adjacent_style_counts.append(f"{adj_cat}: {adj_style_count}")
                    adjacent_style_total += adj_style_count
                
                if lang == 'uk':
                    results += f"  - {main_style_count} готелів в обраних стилях в категорії {category}\n"
                    if adjacent_style_counts:
                        results += f"  - {adjacent_style_total} готелів в обраних стилях в суміжних категоріях ({', '.join(adjacent_style_counts)})\n"
                else:
                    results += f"  - {main_style_count} hotels in selected styles in {category} category\n"
                    if adjacent_style_counts:
                        results += f"  - {adjacent_style_total} hotels in selected styles in adjacent categories ({', '.join(adjacent_style_counts)})\n"
        
        # МЕТА з детальним розбором
        if purposes:
            if lang == 'uk':
                results += f"🎯 PURPOSE:\n"
            else:
                results += f"🎯 PURPOSE:\n"
            
            # Детальний розбір для кожної мети
            for purpose in purposes:
                if lang == 'uk':
                    results += f"{purpose}:\n"
                else:
                    results += f"{purpose}:\n"
                
                if category:
                    # Основна категорія
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, [purpose])
                    main_purpose_count = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
                    
                    # Суміжні категорії
                    adjacent_categories = get_adjacent_categories(category)
                    adjacent_purpose_counts = []
                    adjacent_purpose_total = 0
                    
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, [purpose])
                        adj_purpose_count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                        adjacent_purpose_counts.append(f"{adj_cat}: {adj_purpose_count}")
                        adjacent_purpose_total += adj_purpose_count
                    
                    if lang == 'uk':
                        results += f"  - {main_purpose_count} готелів для мети {purpose} в категорії {category} (обрана категорія)\n"
                        if adjacent_purpose_counts:
                            results += f"  - {adjacent_purpose_total} готелів для мети {purpose} в суміжних категоріях ({', '.join(adjacent_purpose_counts)})\n"
                    else:
                        results += f"  - {main_purpose_count} hotels for {purpose} purpose in {category} category (selected category)\n"
                        if adjacent_purpose_counts:
                            results += f"  - {adjacent_purpose_total} hotels for {purpose} purpose in adjacent categories ({', '.join(adjacent_purpose_counts)})\n"
        
        if i < len(all_programs) - 1:  # Додаємо роздільник (крім останньої)
            results += "\n" + "=" * 50 + "\n\n"
    
    return results

# ВИПРАВЛЕНА: Функція для розбору балів (/scoring) - тепер для ВСІХ 7 програм
def format_scoring_breakdown(user_data, scores_df, lang='uk'):
    """
    Генерує детальний розбір нарахування балів для ВСІХ 7 програм (як в документі)
    """
    results = ""
    
    # ВИПРАВЛЕНО: Беремо ВСІ 7 програм для розбору балів
    all_programs = scores_df.head(7)  # Всі 7 програм замість топ-3
    
    # Отримуємо дані користувача
    regions = user_data.get('regions', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    # Переводимо для обробки
    english_regions = translate_regions_to_english(regions)
    english_styles = translate_styles_to_english(styles)
    english_purposes = translate_purposes_to_english(purposes)
    
    # Фільтруємо дані за регіоном
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions)
    
    for i, (index, row) in enumerate(all_programs.iterrows()):
        program = row['loyalty_program']
        
        # Визначаємо емодзі для позиції
        emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
        
        if lang == 'uk':
            results += f"{emoji} Топ {i+1}. {program}\n"
            results += f"Загальний бал: {row['total_score']:.2f}\n"
            results += f"------------------------------\n"
        else:
            results += f"{emoji} Top {i+1}. {program}\n"
            results += f"Total score: {row['total_score']:.2f}\n"
            results += f"------------------------------\n"
        
        # РЕГІОН З БАЛАМИ (детальний розбір)
        if lang == 'uk':
            region_str = ', '.join(regions) if regions else 'N/A'
            results += f"📍 REGION: {row['region_score']:.1f} балів\n"
            results += f"{row['region_hotels']} готелів у {region_str}\n\n"
        else:
            region_str = ', '.join(regions) if regions else 'N/A'
            results += f"📍 REGION: {row['region_score']:.1f} points\n"
            results += f"{row['region_hotels']} hotels in {region_str}\n\n"
        
        # КАТЕГОРІЯ З ДЕТАЛЬНИМ РОЗБОРОМ БАЛІВ (як в документі)
        if category:
            # Розраховуємо бали для основної категорії
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_counts = main_category_hotels.groupby('loyalty_program').size().to_dict()
            main_scores = distribute_scores_with_ties(main_counts, MAIN_CATEGORY_POINTS)
            main_score = main_scores.get(program, 0.0)
            main_count = main_counts.get(program, 0)
            
            # Розраховуємо бали для суміжних категорій
            adjacent_categories = get_adjacent_categories(category)
            adjacent_total_score = 0.0
            adjacent_details = []
            
            for adj_cat in adjacent_categories:
                adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                adj_counts = adj_category_hotels.groupby('loyalty_program').size().to_dict()
                adj_scores = distribute_scores_with_ties(adj_counts, ADJACENT_CATEGORY_POINTS)
                adj_score = adj_scores.get(program, 0.0)
                adj_count = adj_counts.get(program, 0)
                
                adjacent_total_score += adj_score
                if lang == 'uk':
                    adjacent_details.append(f"(суміжна) {adj_cat} – {adj_count} готелів – {adj_score:.1f} балів")
                else:
                    adjacent_details.append(f"(adjacent) {adj_cat} – {adj_count} hotels – {adj_score:.1f} points")
            
            total_category_score = main_score + adjacent_total_score
            
            if lang == 'uk':
                results += f"🏨 CATEGORY: {total_category_score:.1f} балів\n"
                results += f"(основна) {category} – {main_count} готелів – {main_score:.1f} балів\n"
                for detail in adjacent_details:
                    results += f"{detail}\n"
                results += f"Підрахунок: {main_score:.1f} + ({adjacent_total_score:.1f}) = {total_category_score:.1f}\n\n"
            else:
                results += f"🏨 CATEGORY: {total_category_score:.1f} points\n"
                results += f"(main) {category} – {main_count} hotels – {main_score:.1f} points\n"
                for detail in adjacent_details:
                    results += f"{detail}\n"
                results += f"Calculation: {main_score:.1f} + ({adjacent_total_score:.1f}) = {total_category_score:.1f}\n\n"
        
        # СТИЛЬ З ДЕТАЛЬНИМ РОЗБОРОМ БАЛІВ
        if styles:
            if lang == 'uk':
                results += f"🎨 STYLE: {row['style_score']:.1f} балів\n"
            else:
                results += f"🎨 STYLE: {row['style_score']:.1f} points\n"
            
            total_raw_style_score = 0.0
            
            for style in styles:
                if lang == 'uk':
                    results += f"{style}:\n"
                else:
                    results += f"{style}:\n"
                
                style_raw_score = 0.0
                
                # Основна категорія для кожного стилю
                if category:
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_style_filtered = filter_hotels_by_style(main_category_hotels, [style])
                    main_style_counts = main_style_filtered.groupby('loyalty_program').size().to_dict()
                    main_style_scores = distribute_scores_with_ties(main_style_counts, MAIN_CATEGORY_POINTS)
                    main_style_score = main_style_scores.get(program, 0.0)
                    main_style_count = main_style_counts.get(program, 0)
                    
                    style_raw_score += main_style_score
                    
                    if lang == 'uk':
                        results += f"{style} в {category.lower()} – {main_style_count} готелів – {main_style_score:.1f} балів\n"
                    else:
                        results += f"{style} in {category.lower()} – {main_style_count} hotels – {main_style_score:.1f} points\n"
                    
                    # Суміжні категорії для кожного стилю
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_style_filtered = filter_hotels_by_style(adj_category_hotels, [style])
                        adj_style_counts = adj_style_filtered.groupby('loyalty_program').size().to_dict()
                        adj_style_scores = distribute_scores_with_ties(adj_style_counts, ADJACENT_CATEGORY_POINTS)
                        adj_style_score = adj_style_scores.get(program, 0.0)
                        adj_style_count = adj_style_counts.get(program, 0)
                        
                        style_raw_score += adj_style_score
                        
                        if lang == 'uk':
                            results += f"{style} в {adj_cat.lower()} (суміжна) – {adj_style_count} готелів – {adj_style_score:.1f} балів\n"
                        else:
                            results += f"{style} in {adj_cat.lower()} (adjacent) – {adj_style_count} hotels – {adj_style_score:.1f} points\n"
                
                total_raw_style_score += style_raw_score
            
            # Нормалізація стилів
            normalization_factor = len(styles)
            final_style_score = total_raw_style_score / normalization_factor if normalization_factor > 1 else total_raw_style_score
            
            if lang == 'uk':
                results += f"Сума: {total_raw_style_score:.1f} балів\n"
                if normalization_factor > 1:
                    results += f"Нормалізація: {total_raw_style_score:.1f} ÷ {normalization_factor} стилі = {final_style_score:.1f} балів\n\n"
                else:
                    results += f"Фінальний результат: {final_style_score:.1f} балів\n\n"
            else:
                results += f"Sum: {total_raw_style_score:.1f} points\n"
                if normalization_factor > 1:
                    results += f"Normalization: {total_raw_style_score:.1f} ÷ {normalization_factor} styles = {final_style_score:.1f} points\n\n"
                else:
                    results += f"Final result: {final_style_score:.1f} points\n\n"
        
        # МЕТА З ДЕТАЛЬНИМ РОЗБОРОМ БАЛІВ
        if purposes:
            if lang == 'uk':
                results += f"🎯 PURPOSE: {row['purpose_score']:.1f} балів\n"
            else:
                results += f"🎯 PURPOSE: {row['purpose_score']:.1f} points\n"
            
            total_raw_purpose_score = 0.0
            
            for purpose in purposes:
                if lang == 'uk':
                    results += f"{purpose}:\n"
                else:
                    results += f"{purpose}:\n"
                
                purpose_raw_score = 0.0
                
                # Основна категорія для кожної мети
                if category:
                    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
                    main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, [purpose])
                    main_purpose_counts = main_purpose_filtered.groupby('loyalty_program').size().to_dict()
                    main_purpose_scores = distribute_scores_with_ties(main_purpose_counts, MAIN_CATEGORY_POINTS)
                    main_purpose_score = main_purpose_scores.get(program, 0.0)
                    main_purpose_count = main_purpose_counts.get(program, 0)
                    
                    purpose_raw_score += main_purpose_score
                    
                    if lang == 'uk':
                        results += f"{purpose} в {category.lower()} – {main_purpose_count} готелів – {main_purpose_score:.1f} балів\n"
                    else:
                        results += f"{purpose} in {category.lower()} – {main_purpose_count} hotels – {main_purpose_score:.1f} points\n"
                    
                    # Суміжні категорії для кожної мети
                    adjacent_categories = get_adjacent_categories(category)
                    for adj_cat in adjacent_categories:
                        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
                        adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, [purpose])
                        adj_purpose_counts = adj_purpose_filtered.groupby('loyalty_program').size().to_dict()
                        adj_purpose_scores = distribute_scores_with_ties(adj_purpose_counts, ADJACENT_CATEGORY_POINTS)
                        adj_purpose_score = adj_purpose_scores.get(program, 0.0)
                        adj_purpose_count = adj_purpose_counts.get(program, 0)
                        
                        purpose_raw_score += adj_purpose_score
                        
                        if lang == 'uk':
                            results += f"{purpose} в {adj_cat.lower()} (суміжна) – {adj_purpose_count} готелів – {adj_purpose_score:.1f} балів\n"
                        else:
                            results += f"{purpose} in {adj_cat.lower()} (adjacent) – {adj_purpose_count} hotels – {adj_purpose_score:.1f} points\n"
                
                total_raw_purpose_score += purpose_raw_score
            
            # Нормалізація цілей
            normalization_factor = len(purposes)
            final_purpose_score = total_raw_purpose_score / normalization_factor if normalization_factor > 1 else total_raw_purpose_score
            
            if lang == 'uk':
                results += f"Сума: {total_raw_purpose_score:.1f} балів\n"
                if normalization_factor > 1:
                    results += f"Нормалізація: {total_raw_purpose_score:.1f} ÷ {normalization_factor} цілі = {final_purpose_score:.1f} балів\n\n"
                else:
                    results += f"Фінальний результат: {final_purpose_score:.1f} балів\n\n"
            else:
                results += f"Sum: {total_raw_purpose_score:.1f} points\n"
                if normalization_factor > 1:
                    results += f"Normalization: {total_raw_purpose_score:.1f} ÷ {normalization_factor} purposes = {final_purpose_score:.1f} points\n\n"
                else:
                    results += f"Final result: {final_purpose_score:.1f} points\n\n"
        
        # ПІДСУМОК З РЕЙТИНГОМ (як в документі)
        base_total = row['region_score'] + row['category_score'] + row['style_score'] + row['purpose_score']
        
        if lang == 'uk':
            results += f"➕ ПІДСУМОК:\n"
            results += f"{row['region_score']:.1f} + {row['category_score']:.1f} + {row['style_score']:.1f} + {row['purpose_score']:.1f} = {base_total:.2f} балів\n"
            results += f"Рейтинг програми: {row['program_rating']:.2f}★\n"
            results += f"Рейтинг-коефіцієнт: {row['program_rating']:.2f} ÷ 5.0 = {row['rating_coefficient']:.3f}\n"
            results += f"Фінальний результат: {base_total:.2f} × {row['rating_coefficient']:.3f} = {row['total_score']:.2f} балів\n"
        else:
            results += f"➕ SUMMARY:\n"
            results += f"{row['region_score']:.1f} + {row['category_score']:.1f} + {row['style_score']:.1f} + {row['purpose_score']:.1f} = {base_total:.2f} points\n"
            results += f"Program rating: {row['program_rating']:.2f}★\n"
            results += f"Rating coefficient: {row['program_rating']:.2f} ÷ 5.0 = {row['rating_coefficient']:.3f}\n"
            results += f"Final result: {base_total:.2f} × {row['rating_coefficient']:.3f} = {row['total_score']:.2f} points\n"
        
        if i < len(all_programs) - 1:  # Додаємо роздільник між програмами (крім останньої)
            results += "\n" + "=" * 50 + "\n\n"
    
    return results

# ОНОВЛЕНА: Функція main з додаванням команди /scoring
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
    
    # ОНОВЛЕНО: Налаштування обробників з додаванням команди /scoring
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
    
    # ДОДАНО: Обробник команди /more для детального розбору всіх програм
    application.add_handler(CommandHandler("more", show_more_details))
    
    # НОВЕ: Обробник команди /scoring для детального розбору балів
    application.add_handler(CommandHandler("scoring", show_scoring_details))
    
    # ОНОВЛЕНО: Логування доступних команд
    logger.info("Зареєстровані команди бота:")
    logger.info("  /start - початок опитування")
    logger.info("  /cancel - скасування розмови")
    logger.info("  /more - детальний розбір всіх 7 програм лояльності")
    logger.info("  /scoring - детальний розбір нарахування балів для всіх 7 програм")
    
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
    
    logger.info("Бот успішно запущено з підтримкою рейтингових розрахунків та трьох типів звітів")

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
    logger.info("🤖 HOTEL LOYALTY PROGRAM BOT v3.0 (WITH 3 REPORT TYPES)")
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
    
    # ОНОВЛЕНО: Логування доступних звітів
    logger.info("Available report types:")
    logger.info("  1. Simple report (/start) - Top 3 programs, compact format")
    logger.info("  2. Detailed report (/more) - All 7 programs, extended info")
    logger.info("  3. Scoring breakdown (/scoring) - All 7 programs, detailed calculations")
    
    # Запускаємо бота з підтримкою webhook або polling
    main(TOKEN, CSV_PATH, WEBHOOK_URL, 10000, WEBHOOK_PATH)

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
    
    # ДОДАНО: Логування доступних команд
    logger.info("Зареєстровані команди бота:")
    logger.info("  /start - початок опитування")
    logger.info("  /cancel - скасування розмови")
    logger.info("  /more - детальний розбір останніх результатів")
    
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

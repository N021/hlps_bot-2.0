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

# Етапи розмови
LANGUAGE, REGION, WAITING_REGION_SUBMIT, CATEGORY, WAITING_STYLE_SUBMIT, WAITING_PURPOSE_SUBMIT = range(6)

# Зберігання даних користувача
user_data_global = {}
hotel_data = None  # Глобальна змінна для даних готелів

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
    
    # Ініціалізація нових даних
    user_data_global[user_id] = {}
    
    # Логування початку нової розмови
    logger.info(f"User {user_id} started a new conversation. Data cleared.")
    
    # Клавіатура для вибору мови за допомогою InlineKeyboardMarkup
    keyboard = [
        [InlineKeyboardButton("Українська (Ukrainian)", callback_data='lang_uk')],
        [InlineKeyboardButton("English (Англійська)", callback_data='lang_en')]
    ]
    
    await update.message.reply_text(
        "Please select your preferred language for our conversation\n"
        "(будь ласка, оберіть мову, якою вам зручніше спілкуватися):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return LANGUAGE

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
        
        # Розрахунок і відображення результатів
        return await calculate_and_show_results(update, context)
    
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
# ЧАСТИНА 10: НОВА ЛОГІКА ПІДРАХУНКУ БАЛІВ ТА ГОЛОВНІ ФУНКЦІЇ
# ===============================

# Функції фільтрації готелів
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
    
    logger.info(f"Фільтрація за стилями: {styles}")
    
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
    logger.info(f"Готелів після фільтрації за стилем: {len(filtered_df)}")
    
    return filtered_df

def filter_hotels_by_purpose(df, purposes):
    """Фільтрує готелі за метою подорожі"""
    if not purposes or len(purposes) == 0:
        return df
    
    logger.info(f"Фільтрація за метою: {purposes}")
    
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
    logger.info(f"Готелів після фільтрації за метою: {len(filtered_df)}")
    
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
    Універсальна функція для розподілу балів з урахуванням однакових значень
    
    Args:
        counts_dict: словник {програма: кількість готелів}
        score_values: список балів [21, 18, 15, 12, 9, 6, 3] або [7, 6, 5, 4, 3, 2, 1]
    
    Returns:
        словник {програма: бали}
    """
    if not counts_dict or not score_values:
        return {program: 0.0 for program in counts_dict.keys()}
    
    # Фільтруємо програми з кількістю > 0
    filtered_counts = {prog: count for prog, count in counts_dict.items() if count > 0}
    
    if not filtered_counts:
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
            # Всі наступні програми отримують 0 балів
            break
        
        # Беремо бал для поточної позиції
        score_for_group = score_values[current_position]
        
        # Всі програми в групі отримують однакові бали
        for program in programs_in_group:
            result_scores[program] = float(score_for_group)
        
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
        
        # Використовуємо нову функцію розподілу балів
        score_values = [21, 18, 15, 12, 9, 6, 3]
        region_scores = distribute_scores_with_ties(region_counts, score_values)
        
        # Нормалізуємо, якщо обрано кілька регіонів
        normalization_factor = 1.0
        if regions and len(regions) > 1:
            normalization_factor = float(len(regions))
        elif countries and len(countries) > 1:
            normalization_factor = float(len(countries))
        
        if normalization_factor > 1.0:
            region_scores = {program: score / normalization_factor for program, score in region_scores.items()}
                
    except Exception as e:
        logger.error(f"Помилка обчислення балів за регіоном: {e}")
        return {}
    
    return region_scores

def get_detailed_category_scores(filtered_by_region, program, category):
    """Розраховує детальні бали для категорій з правильним розподілом при ties"""
    scores = {'main': {'hotels': 0, 'points': 0.0}, 'adjacent': {}}
    
    if not category:
        return scores
    
    # Основна категорія
    main_filtered = filter_hotels_by_category(filtered_by_region, category)
    main_program_hotels = len(main_filtered[main_filtered['loyalty_program'] == program])
    scores['main']['hotels'] = main_program_hotels
    
    # Розраховуємо бали для основної категорії
    if main_program_hotels > 0:
        main_counts = main_filtered.groupby('loyalty_program').size().to_dict()
        main_score_values = [21, 18, 15, 12, 9, 6, 3]
        main_scores = distribute_scores_with_ties(main_counts, main_score_values)
        scores['main']['points'] = main_scores.get(program, 0.0)
    
    # Суміжні категорії
    adjacent_categories = get_adjacent_categories(category)
    for adj_cat in adjacent_categories:
        adj_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
        adj_program_hotels = len(adj_filtered[adj_filtered['loyalty_program'] == program])
        
        adj_score = 0.0
        if adj_program_hotels > 0:
            adj_counts = adj_filtered.groupby('loyalty_program').size().to_dict()
            adj_score_values = [7, 6, 5, 4, 3, 2, 1]
            adj_scores = distribute_scores_with_ties(adj_counts, adj_score_values)
            adj_score = adj_scores.get(program, 0.0)
        
        scores['adjacent'][adj_cat] = {'hotels': adj_program_hotels, 'points': adj_score}
    
    return scores

def calculate_style_scores_new_logic(filtered_by_region, loyalty_programs, category, styles):
    """
    НОВА ЛОГІКА розрахунку балів за стилем з правильним розподілом при ties
    """
    if not styles or len(styles) == 0:
        return {program: 0.0 for program in loyalty_programs}, {program: 0 for program in loyalty_programs}
    
    logger.info(f"=== STYLE CALCULATION (NEW LOGIC WITH TIES) ===")
    logger.info(f"Category: {category}, Styles: {styles}")
    
    # Отримуємо суміжні категорії
    adjacent_categories = get_adjacent_categories(category) if category else []
    logger.info(f"Adjacent categories: {adjacent_categories}")
    
    # Розраховуємо готелі за стилем для MAIN категорії
    main_style_counts = {}
    main_style_scores = {}
    
    if category:
        main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
        main_style_filtered = filter_hotels_by_style(main_category_hotels, styles)
        
        for program in loyalty_programs:
            count = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
            main_style_counts[program] = count
        
        logger.info(f"Main category ({category}) style counts: {main_style_counts}")
        
        # Використовуємо нову функцію розподілу балів
        main_score_values = [21, 18, 15, 12, 9, 6, 3]
        main_style_scores = distribute_scores_with_ties(main_style_counts, main_score_values)
    else:
        main_style_counts = {program: 0 for program in loyalty_programs}
        main_style_scores = {program: 0.0 for program in loyalty_programs}
    
    logger.info(f"Main style scores: {main_style_scores}")
    
    # Розраховуємо готелі за стилем для ADJACENT категорій
    adjacent_style_scores = {program: 0.0 for program in loyalty_programs}
    
    if adjacent_categories:
        for adj_cat in adjacent_categories:
            logger.info(f"Processing adjacent category: {adj_cat}")
            
            adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_style_filtered = filter_hotels_by_style(adj_category_hotels, styles)
            
            adj_style_counts = {}
            for program in loyalty_programs:
                count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                adj_style_counts[program] = count
            
            logger.info(f"Adjacent category ({adj_cat}) style counts: {adj_style_counts}")
            
            # Використовуємо нову функцію розподілу балів
            adj_score_values = [7, 6, 5, 4, 3, 2, 1]
            adj_category_scores = distribute_scores_with_ties(adj_style_counts, adj_score_values)
            
            logger.info(f"Adjacent category ({adj_cat}) scores: {adj_category_scores}")
            
            # Для кожної програми беремо МАКСИМУМ з усіх adjacent категорій
            for program in loyalty_programs:
                current_score = adj_category_scores.get(program, 0.0)
                adjacent_style_scores[program] = max(adjacent_style_scores[program], current_score)
    
    logger.info(f"Final adjacent style scores: {adjacent_style_scores}")
    
    # Об'єднуємо бали (main + adjacent)
    final_style_scores = {}
    for program in loyalty_programs:
        main_score = main_style_scores.get(program, 0.0)
        adj_score = adjacent_style_scores.get(program, 0.0)
        final_style_scores[program] = main_score + adj_score
    
    # Нормалізуємо, якщо обрано кілька стилів
    if len(styles) > 1:
        normalization_factor = len(styles)
        final_style_scores = {program: score / normalization_factor 
                             for program, score in final_style_scores.items()}
        logger.info(f"Applied normalization factor: {normalization_factor}")
    
    logger.info(f"Final style scores after normalization: {final_style_scores}")
    
    return final_style_scores, main_style_counts

def calculate_purpose_scores_new_logic(filtered_by_region, loyalty_programs, category, purposes):
    """
    НОВА ЛОГІКА розрахунку балів за метою з правильним розподілом при ties
    """
    if not purposes or len(purposes) == 0:
        return {program: 0.0 for program in loyalty_programs}, {program: 0 for program in loyalty_programs}
    
    logger.info(f"=== PURPOSE CALCULATION (NEW LOGIC WITH TIES) ===")
    logger.info(f"Category: {category}, Purposes: {purposes}")
    
    # Отримуємо суміжні категорії
    adjacent_categories = get_adjacent_categories(category) if category else []
    logger.info(f"Adjacent categories: {adjacent_categories}")
    
    # Розраховуємо готелі за метою для MAIN категорії
    main_purpose_counts = {}
    main_purpose_scores = {}
    
    if category:
        main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
        main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, purposes)
        
        for program in loyalty_programs:
            count = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
            main_purpose_counts[program] = count
        
        logger.info(f"Main category ({category}) purpose counts: {main_purpose_counts}")
        
        # Використовуємо нову функцію розподілу балів
        main_score_values = [21, 18, 15, 12, 9, 6, 3]
        main_purpose_scores = distribute_scores_with_ties(main_purpose_counts, main_score_values)
    else:
        main_purpose_counts = {program: 0 for program in loyalty_programs}
        main_purpose_scores = {program: 0.0 for program in loyalty_programs}
    
    logger.info(f"Main purpose scores: {main_purpose_scores}")
    
    # Розраховуємо готелі за метою для ADJACENT категорій
    adjacent_purpose_scores = {program: 0.0 for program in loyalty_programs}
    
    if adjacent_categories:
        for adj_cat in adjacent_categories:
            logger.info(f"Processing adjacent category: {adj_cat}")
            
            adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, purposes)
            
            adj_purpose_counts = {}
            for program in loyalty_programs:
                count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                adj_purpose_counts[program] = count
            
            logger.info(f"Adjacent category ({adj_cat}) purpose counts: {adj_purpose_counts}")
            
            # Використовуємо нову функцію розподілу балів
            adj_score_values = [7, 6, 5, 4, 3, 2, 1]
            adj_category_scores = distribute_scores_with_ties(adj_purpose_counts, adj_score_values)
            
            logger.info(f"Adjacent category ({adj_cat}) scores: {adj_category_scores}")
            
            # Для кожної програми беремо МАКСИМУМ з усіх adjacent категорій
            for program in loyalty_programs:
                current_score = adj_category_scores.get(program, 0.0)
                adjacent_purpose_scores[program] = max(adjacent_purpose_scores[program], current_score)
    
    logger.info(f"Final adjacent purpose scores: {adjacent_purpose_scores}")
    
    # Об'єднуємо бали (main + adjacent)
    final_purpose_scores = {}
    for program in loyalty_programs:
        main_score = main_purpose_scores.get(program, 0.0)
        adj_score = adjacent_purpose_scores.get(program, 0.0)
        final_purpose_scores[program] = main_score + adj_score
    
    # Нормалізуємо, якщо обрано кілька цілей
    if len(purposes) > 1:
        normalization_factor = len(purposes)
        final_purpose_scores = {program: score / normalization_factor 
                               for program, score in final_purpose_scores.items()}
        logger.info(f"Applied normalization factor: {normalization_factor}")
    
    logger.info(f"Final purpose scores after normalization: {final_purpose_scores}")
    
    return final_purpose_scores, main_purpose_counts

def calculate_scores(user_data, hotel_data):
    """
    ОНОВЛЕНА функція розрахунку балів з правильним розподілом при ties
    """
    logger.info(f"=== STARTING SCORE CALCULATION WITH TIES HANDLING ===")
    logger.info(f"User data: {user_data}")
    
    # Отримуємо відповіді користувача
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
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
    filtered_by_region = filter_hotels_by_region(hotel_data, regions, countries)
    logger.info(f"Hotels after region filter: {len(filtered_by_region)}")
    
    # Розподіляємо бали за регіонами/країнами
    region_scores = get_region_score(filtered_by_region, regions, countries)
    logger.info(f"Region scores: {region_scores}")
    
    for index, row in scores_df.iterrows():
        program = row['loyalty_program']
        if program in region_scores:
            scores_df.at[index, 'region_score'] = region_scores[program]
        
        # Також заповнюємо region_hotels
        if regions and len(regions) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this region' in filtered_by_region.columns:
                program_data = filtered_by_region[filtered_by_region['loyalty_program'] == program]
                if not program_data.empty:
                    region_hotels = program_data['Total hotels of Corporation / Loyalty Program in this region'].iloc[0]
                    scores_df.at[index, 'region_hotels'] = region_hotels
            else:
                region_counts = filtered_by_region.groupby('loyalty_program').size()
                if program in region_counts:
                    scores_df.at[index, 'region_hotels'] = region_counts[program]
    
    # Крок 2: Розраховуємо бали за категорією з правильним розподілом при ties
    if category:
        filtered_by_category = filter_hotels_by_category(filtered_by_region, category)
        category_counts = filtered_by_category.groupby('loyalty_program').size().to_dict()
        
        if category_counts:
            # Використовуємо нову функцію розподілу балів
            category_score_values = [21, 18, 15, 12, 9, 6, 3]
            category_scores = distribute_scores_with_ties(category_counts, category_score_values)
            
            # Додаємо бали за суміжні категорії
            adjacent_categories = get_adjacent_categories(category)
            adjacent_scores = {}
            
            for adj_cat in adjacent_categories:
                adjacent_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
                adjacent_counts = adjacent_filtered.groupby('loyalty_program').size().to_dict()
                
                if adjacent_counts:
                    adjacent_score_values = [7, 6, 5, 4, 3, 2, 1]
                    adj_scores = distribute_scores_with_ties(adjacent_counts, adjacent_score_values)
                    
                    for program, score in adj_scores.items():
                        adjacent_scores[program] = max(adjacent_scores.get(program, 0.0), score)
            
            # Оновлюємо DataFrame з балами
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                
                main_score = category_scores.get(program, 0.0)
                adj_score = adjacent_scores.get(program, 0.0)
                scores_df.at[index, 'category_score'] = main_score + adj_score
                
                # Записуємо кількість готелів у категорії
                scores_df.at[index, 'category_hotels'] = category_counts.get(program, 0)
    
    # Крок 3: НОВА ЛОГІКА - Розраховуємо бали за стилем
    if styles and len(styles) > 0:
        style_scores, style_counts = calculate_style_scores_new_logic(
            filtered_by_region, loyalty_programs, category, styles
        )
        
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            scores_df.at[index, 'style_score'] = style_scores.get(program, 0.0)
            scores_df.at[index, 'style_hotels'] = style_counts.get(program, 0)
    
    # Крок 4: НОВА ЛОГІКА - Розраховуємо бали за метою
    if purposes and len(purposes) > 0:
        purpose_scores, purpose_counts = calculate_purpose_scores_new_logic(
            filtered_by_region, loyalty_programs, category, purposes
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
    
    logger.info(f"=== FINAL CALCULATION COMPLETE WITH TIES HANDLING ===")
    for _, row in scores_df.head(3).iterrows():
        logger.info(f"{row['loyalty_program']}: region={row['region_score']:.1f}, "
                   f"category={row['category_score']:.1f}, style={row['style_score']:.1f}, "
                   f"purpose={row['purpose_score']:.1f}, total={row['total_score']:.1f}")
    
    # Сортуємо за загальним рейтингом
    scores_df = scores_df.sort_values('total_score', ascending=False)
    
    return scores_df

def get_region_score(df, regions=None, countries=None):
    """Обчислює бали для програм лояльності за регіонами/країнами"""
    region_scores = {}
    
    try:
        if regions and len(regions) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this region' in df.columns:
                region_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this region']]
                region_counts = region_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this region']
            else:
                region_counts = df.groupby('loyalty_program').size()
                logger.warning("Колонка 'Total hotels of Corporation / Loyalty Program in this region' відсутня. Використовуємо кількість рядків.")
        
        elif countries and len(countries) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this country' in df.columns:
                country_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this country']]
                region_counts = country_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this country']
            else:
                region_counts = df.groupby('loyalty_program').size()
                logger.warning("Колонка 'Total hotels of Corporation / Loyalty Program in this country' відсутня. Використовуємо кількість рядків.")
        
        else:
            return {}
        
        region_counts = region_counts.fillna(0).astype(float)
        score_values = [21, 18, 15, 12, 9, 6, 3]
        ranked_programs = region_counts.sort_values(ascending=False)
        
        normalization_factor = 1.0
        if regions and len(regions) > 0:
            normalization_factor = float(len(regions))
        elif countries and len(countries) > 0:
            normalization_factor = float(len(countries))
        
        for i, (program, _) in enumerate(ranked_programs.items()):
            if i < len(score_values):
                region_scores[program] = score_values[i] / normalization_factor
            else:
                region_scores[program] = 0.0
                
    except Exception as e:
        logger.error(f"Помилка обчислення балів за регіоном: {e}")
    
    return region_scores

def get_detailed_style_data(filtered_by_region, loyalty_programs, category, styles):
    """
    Отримує детальну інформацію про стилі для кожної категорії
    """
    if not styles or len(styles) == 0:
        return {}
    
    # Отримуємо суміжні категорії
    adjacent_categories = get_adjacent_categories(category) if category else []
    
    detailed_data = {}
    
    # Для кожної програми лояльності
    for program in loyalty_programs:
        detailed_data[program] = {
            'main_category': {},
            'adjacent_categories': {}
        }
        
        # Основна категорія
        if category:
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_program_hotels = main_category_hotels[main_category_hotels['loyalty_program'] == program]
            
            for style in styles:
                style_filtered = filter_hotels_by_style(main_program_hotels, [style])
                detailed_data[program]['main_category'][style] = len(style_filtered)
        
        # Суміжні категорії
        for adj_cat in adjacent_categories:
            adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_program_hotels = adj_category_hotels[adj_category_hotels['loyalty_program'] == program]
            
            detailed_data[program]['adjacent_categories'][adj_cat] = {}
            for style in styles:
                style_filtered = filter_hotels_by_style(adj_program_hotels, [style])
                detailed_data[program]['adjacent_categories'][adj_cat][style] = len(style_filtered)
    
    return detailed_data

def get_detailed_purpose_data(filtered_by_region, loyalty_programs, category, purposes):
    """
    Отримує детальну інформацію про цілі для кожної категорії
    """
    if not purposes or len(purposes) == 0:
        return {}
    
    # Отримуємо суміжні категорії
    adjacent_categories = get_adjacent_categories(category) if category else []
    
    detailed_data = {}
    
    # Для кожної програми лояльності
    for program in loyalty_programs:
        detailed_data[program] = {
            'main_category': {},
            'adjacent_categories': {}
        }
        
        # Основна категорія
        if category:
            main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
            main_program_hotels = main_category_hotels[main_category_hotels['loyalty_program'] == program]
            
            for purpose in purposes:
                purpose_filtered = filter_hotels_by_purpose(main_program_hotels, [purpose])
                detailed_data[program]['main_category'][purpose] = len(purpose_filtered)
        
        # Суміжні категорії
        for adj_cat in adjacent_categories:
            adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_program_hotels = adj_category_hotels[adj_category_hotels['loyalty_program'] == program]
            
            detailed_data[program]['adjacent_categories'][adj_cat] = {}
            for purpose in purposes:
                purpose_filtered = filter_hotels_by_purpose(adj_program_hotels, [purpose])
                detailed_data[program]['adjacent_categories'][adj_cat][purpose] = len(purpose_filtered)
    
    return detailed_data

def calculate_style_scores_new_logic(filtered_by_region, loyalty_programs, category, styles):
    """
    НОВА ЛОГІКА розрахунку балів за стилем
    """
    if not styles or len(styles) == 0:
        return {program: 0.0 for program in loyalty_programs}, {program: 0 for program in loyalty_programs}
    
    logger.info(f"=== STYLE CALCULATION (NEW LOGIC) ===")
    logger.info(f"Category: {category}, Styles: {styles}")
    
    # Отримуємо суміжні категорії
    adjacent_categories = get_adjacent_categories(category) if category else []
    logger.info(f"Adjacent categories: {adjacent_categories}")
    
    # Розраховуємо готелі за стилем для MAIN категорії
    main_style_counts = {}
    main_style_scores = {}
    
    if category:
        main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
        main_style_filtered = filter_hotels_by_style(main_category_hotels, styles)
        
        for program in loyalty_programs:
            count = len(main_style_filtered[main_style_filtered['loyalty_program'] == program])
            main_style_counts[program] = count
        
        logger.info(f"Main category ({category}) style counts: {main_style_counts}")
        
        # Розраховуємо бали за MAIN категорію (21, 18, 15, 12, 9, 6, 3)
        if sum(main_style_counts.values()) > 0:
            ranked_main = sorted(main_style_counts.items(), key=lambda x: x[1], reverse=True)
            main_score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
            
            for i, (program, count) in enumerate(ranked_main):
                if count > 0 and i < len(main_score_values):
                    main_style_scores[program] = main_score_values[i]
                else:
                    main_style_scores[program] = 0.0
        else:
            main_style_scores = {program: 0.0 for program in loyalty_programs}
    else:
        main_style_counts = {program: 0 for program in loyalty_programs}
        main_style_scores = {program: 0.0 for program in loyalty_programs}
    
    logger.info(f"Main style scores: {main_style_scores}")
    
    # Розраховуємо готелі за стилем для ADJACENT категорій
    adjacent_style_scores = {program: 0.0 for program in loyalty_programs}
    
    if adjacent_categories:
        for adj_cat in adjacent_categories:
            logger.info(f"Processing adjacent category: {adj_cat}")
            
            adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_style_filtered = filter_hotels_by_style(adj_category_hotels, styles)
            
            adj_style_counts = {}
            for program in loyalty_programs:
                count = len(adj_style_filtered[adj_style_filtered['loyalty_program'] == program])
                adj_style_counts[program] = count
            
            logger.info(f"Adjacent category ({adj_cat}) style counts: {adj_style_counts}")
            
            # Розраховуємо бали за цю ADJACENT категорію (7, 6, 5, 4, 3, 2, 1)
            if sum(adj_style_counts.values()) > 0:
                ranked_adj = sorted(adj_style_counts.items(), key=lambda x: x[1], reverse=True)
                adj_score_values = [7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
                
                adj_category_scores = {}
                for i, (program, count) in enumerate(ranked_adj):
                    if count > 0 and i < len(adj_score_values):
                        adj_category_scores[program] = adj_score_values[i]
                    else:
                        adj_category_scores[program] = 0.0
                
                logger.info(f"Adjacent category ({adj_cat}) scores: {adj_category_scores}")
                
                # Для кожної програми беремо МАКСИМУМ з усіх adjacent категорій
                for program in loyalty_programs:
                    current_score = adj_category_scores.get(program, 0.0)
                    adjacent_style_scores[program] = max(adjacent_style_scores[program], current_score)
    
    logger.info(f"Final adjacent style scores: {adjacent_style_scores}")
    
    # Об'єднуємо бали (main + adjacent)
    final_style_scores = {}
    for program in loyalty_programs:
        main_score = main_style_scores.get(program, 0.0)
        adj_score = adjacent_style_scores.get(program, 0.0)
        final_style_scores[program] = main_score + adj_score
    
    # Нормалізуємо, якщо обрано кілька стилів
    if len(styles) > 1:
        normalization_factor = len(styles)
        final_style_scores = {program: score / normalization_factor 
                             for program, score in final_style_scores.items()}
        logger.info(f"Applied normalization factor: {normalization_factor}")
    
    logger.info(f"Final style scores after normalization: {final_style_scores}")
    
    return final_style_scores, main_style_counts

def calculate_purpose_scores_new_logic(filtered_by_region, loyalty_programs, category, purposes):
    """
    НОВА ЛОГІКА розрахунку балів за метою (аналогічна стилю)
    """
    if not purposes or len(purposes) == 0:
        return {program: 0.0 for program in loyalty_programs}, {program: 0 for program in loyalty_programs}
    
    logger.info(f"=== PURPOSE CALCULATION (NEW LOGIC) ===")
    logger.info(f"Category: {category}, Purposes: {purposes}")
    
    # Отримуємо суміжні категорії
    adjacent_categories = get_adjacent_categories(category) if category else []
    logger.info(f"Adjacent categories: {adjacent_categories}")
    
    # Розраховуємо готелі за метою для MAIN категорії
    main_purpose_counts = {}
    main_purpose_scores = {}
    
    if category:
        main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
        main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, purposes)
        
        for program in loyalty_programs:
            count = len(main_purpose_filtered[main_purpose_filtered['loyalty_program'] == program])
            main_purpose_counts[program] = count
        
        logger.info(f"Main category ({category}) purpose counts: {main_purpose_counts}")
        
        # Розраховуємо бали за MAIN категорію (21, 18, 15, 12, 9, 6, 3)
        if sum(main_purpose_counts.values()) > 0:
            ranked_main = sorted(main_purpose_counts.items(), key=lambda x: x[1], reverse=True)
            main_score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
            
            for i, (program, count) in enumerate(ranked_main):
                if count > 0 and i < len(main_score_values):
                    main_purpose_scores[program] = main_score_values[i]
                else:
                    main_purpose_scores[program] = 0.0
        else:
            main_purpose_scores = {program: 0.0 for program in loyalty_programs}
    else:
        main_purpose_counts = {program: 0 for program in loyalty_programs}
        main_purpose_scores = {program: 0.0 for program in loyalty_programs}
    
    logger.info(f"Main purpose scores: {main_purpose_scores}")
    
    # Розраховуємо готелі за метою для ADJACENT категорій
    adjacent_purpose_scores = {program: 0.0 for program in loyalty_programs}
    
    if adjacent_categories:
        for adj_cat in adjacent_categories:
            logger.info(f"Processing adjacent category: {adj_cat}")
            
            adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
            adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, purposes)
            
            adj_purpose_counts = {}
            for program in loyalty_programs:
                count = len(adj_purpose_filtered[adj_purpose_filtered['loyalty_program'] == program])
                adj_purpose_counts[program] = count
            
            logger.info(f"Adjacent category ({adj_cat}) purpose counts: {adj_purpose_counts}")
            
            # Розраховуємо бали за цю ADJACENT категорію (7, 6, 5, 4, 3, 2, 1)
            if sum(adj_purpose_counts.values()) > 0:
                ranked_adj = sorted(adj_purpose_counts.items(), key=lambda x: x[1], reverse=True)
                adj_score_values = [7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
                
                adj_category_scores = {}
                for i, (program, count) in enumerate(ranked_adj):
                    if count > 0 and i < len(adj_score_values):
                        adj_category_scores[program] = adj_score_values[i]
                    else:
                        adj_category_scores[program] = 0.0
                
                logger.info(f"Adjacent category ({adj_cat}) scores: {adj_category_scores}")
                
                # Для кожної програми беремо МАКСИМУМ з усіх adjacent категорій
                for program in loyalty_programs:
                    current_score = adj_category_scores.get(program, 0.0)
                    adjacent_purpose_scores[program] = max(adjacent_purpose_scores[program], current_score)
    
    logger.info(f"Final adjacent purpose scores: {adjacent_purpose_scores}")
    
    # Об'єднуємо бали (main + adjacent)
    final_purpose_scores = {}
    for program in loyalty_programs:
        main_score = main_purpose_scores.get(program, 0.0)
        adj_score = adjacent_purpose_scores.get(program, 0.0)
        final_purpose_scores[program] = main_score + adj_score
    
    # Нормалізуємо, якщо обрано кілька цілей
    if len(purposes) > 1:
        normalization_factor = len(purposes)
        final_purpose_scores = {program: score / normalization_factor 
                               for program, score in final_purpose_scores.items()}
        logger.info(f"Applied normalization factor: {normalization_factor}")
    
    logger.info(f"Final purpose scores after normalization: {final_purpose_scores}")
    
    return final_purpose_scores, main_purpose_counts

def calculate_scores(user_data, hotel_data):
    """
    ОНОВЛЕНА функція розрахунку балів з новою логікою
    """
    logger.info(f"=== STARTING SCORE CALCULATION ===")
    logger.info(f"User data: {user_data}")
    
    # Отримуємо відповіді користувача
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
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
    filtered_by_region = filter_hotels_by_region(hotel_data, regions, countries)
    logger.info(f"Hotels after region filter: {len(filtered_by_region)}")
    
    # Розподіляємо бали за регіонами/країнами
    region_scores = get_region_score(filtered_by_region, regions, countries)
    logger.info(f"Region scores: {region_scores}")
    
    for index, row in scores_df.iterrows():
        program = row['loyalty_program']
        if program in region_scores:
            scores_df.at[index, 'region_score'] = region_scores[program]
        
        # Також заповнюємо region_hotels
        if regions and len(regions) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this region' in filtered_by_region.columns:
                program_data = filtered_by_region[filtered_by_region['loyalty_program'] == program]
                if not program_data.empty:
                    region_hotels = program_data['Total hotels of Corporation / Loyalty Program in this region'].iloc[0]
                    scores_df.at[index, 'region_hotels'] = region_hotels
            else:
                region_counts = filtered_by_region.groupby('loyalty_program').size()
                if program in region_counts:
                    scores_df.at[index, 'region_hotels'] = region_counts[program]
    
    # Крок 2: Розраховуємо бали за категорією (залишається як було)
    if category:
        filtered_by_category = filter_hotels_by_category(filtered_by_region, category)
        category_counts = filtered_by_category.groupby('loyalty_program').size()
        
        if not category_counts.empty:
            category_scores = {}
            ranked_programs = category_counts.sort_values(ascending=False)
            score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
            
            for i, (program, _) in enumerate(ranked_programs.items()):
                if i < len(score_values):
                    category_scores[program] = score_values[i]
                else:
                    category_scores[program] = 0.0
            
            # Додаємо бали за суміжні категорії
            adjacent_categories = get_adjacent_categories(category)
            adjacent_scores = {}
            
            for adj_cat in adjacent_categories:
                adjacent_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
                adjacent_counts = adjacent_filtered.groupby('loyalty_program').size()
                
                if not adjacent_counts.empty:
                    ranked_adjacent = adjacent_counts.sort_values(ascending=False)
                    adjacent_score_values = [7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
                    
                    for i, (program, _) in enumerate(ranked_adjacent.items()):
                        score = adjacent_score_values[i] if i < len(adjacent_score_values) else 0.0
                        adjacent_scores[program] = max(adjacent_scores.get(program, 0.0), score)
            
            # Оновлюємо DataFrame з балами
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                
                main_score = category_scores.get(program, 0.0)
                adj_score = adjacent_scores.get(program, 0.0)
                scores_df.at[index, 'category_score'] = main_score + adj_score
                
                # Записуємо кількість готелів у категорії
                category_mask = filtered_by_category['loyalty_program'] == program
                scores_df.at[index, 'category_hotels'] = category_mask.sum()
    
    # Крок 3: НОВА ЛОГІКА - Розраховуємо бали за стилем
    if styles and len(styles) > 0:
        style_scores, style_counts = calculate_style_scores_new_logic(
            filtered_by_region, loyalty_programs, category, styles
        )
        
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            scores_df.at[index, 'style_score'] = style_scores.get(program, 0.0)
            scores_df.at[index, 'style_hotels'] = style_counts.get(program, 0)
    
    # Крок 4: НОВА ЛОГІКА - Розраховуємо бали за метою
    if purposes and len(purposes) > 0:
        purpose_scores, purpose_counts = calculate_purpose_scores_new_logic(
            filtered_by_region, loyalty_programs, category, purposes
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
    
    logger.info(f"=== FINAL CALCULATION COMPLETE ===")
    for _, row in scores_df.head(3).iterrows():
        logger.info(f"{row['loyalty_program']}: region={row['region_score']:.1f}, "
                   f"category={row['category_score']:.1f}, style={row['style_score']:.1f}, "
                   f"purpose={row['purpose_score']:.1f}, total={row['total_score']:.1f}")
    
    # Сортуємо за загальним рейтингом
    scores_df = scores_df.sort_values('total_score', ascending=False)
    
    return scores_df

def get_detailed_category_scores(filtered_by_region, program, category):
    """Розраховує детальні бали для категорій"""
    scores = {'main': {'hotels': 0, 'points': 0.0}, 'adjacent': {}}
    
    if not category:
        return scores
    
    # Основна категорія
    main_filtered = filter_hotels_by_category(filtered_by_region, category)
    main_program_hotels = len(main_filtered[main_filtered['loyalty_program'] == program])
    scores['main']['hotels'] = main_program_hotels
    
    # Розраховуємо бали для основної категорії
    if main_program_hotels > 0:
        main_counts = main_filtered.groupby('loyalty_program').size()
        ranked_main = main_counts.sort_values(ascending=False)
        main_score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
        
        for i, (prog, _) in enumerate(ranked_main.items()):
            if prog == program and i < len(main_score_values):
                scores['main']['points'] = main_score_values[i]
                break
    
    # Суміжні категорії
    adjacent_categories = get_adjacent_categories(category)
    for adj_cat in adjacent_categories:
        adj_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
        adj_program_hotels = len(adj_filtered[adj_filtered['loyalty_program'] == program])
        
        adj_score = 0.0
        if adj_program_hotels > 0:
            adj_counts = adj_filtered.groupby('loyalty_program').size()
            ranked_adj = adj_counts.sort_values(ascending=False)
            adj_score_values = [7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
            
            for i, (prog, _) in enumerate(ranked_adj.items()):
                if prog == program and i < len(adj_score_values):
                    adj_score = adj_score_values[i]
                    break
        
        scores['adjacent'][adj_cat] = {'hotels': adj_program_hotels, 'points': adj_score}
    
    return scores

def get_detailed_style_scores(filtered_by_region, program, category, styles):
    """Розраховує детальні бали для стилів з правильним розподілом при ties"""
    scores = {'main': {}, 'adjacent': {}}
    
    if not styles or not category:
        return scores
    
    # Основна категорія
    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
    main_style_filtered = filter_hotels_by_style(main_category_hotels, styles)
    main_counts = main_style_filtered.groupby('loyalty_program').size().to_dict()
    
    # Використовуємо нову функцію розподілу балів
    main_score_values = [21, 18, 15, 12, 9, 6, 3]
    main_scores = distribute_scores_with_ties(main_counts, main_score_values)
    main_total_score = main_scores.get(program, 0.0)
    
    # Нормалізуємо, якщо кілька стилів
    if len(styles) > 1:
        main_total_score = main_total_score / len(styles)
    
    for style in styles:
        main_program_hotels = len(filter_hotels_by_style(main_category_hotels[main_category_hotels['loyalty_program'] == program], [style]))
        scores['main'][style] = {'hotels': main_program_hotels, 'points': main_total_score}
    
    # Суміжні категорії
    adjacent_categories = get_adjacent_categories(category)
    max_adj_score = 0.0
    
    for adj_cat in adjacent_categories:
        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
        adj_style_filtered = filter_hotels_by_style(adj_category_hotels, styles)
        adj_counts = adj_style_filtered.groupby('loyalty_program').size().to_dict()
        
        # Використовуємо нову функцію розподілу балів
        adj_score_values = [7, 6, 5, 4, 3, 2, 1]
        adj_scores = distribute_scores_with_ties(adj_counts, adj_score_values)
        adj_score = adj_scores.get(program, 0.0)
        
        # Нормалізуємо, якщо кілька стилів
        if len(styles) > 1:
            adj_score = adj_score / len(styles)
        
        max_adj_score = max(max_adj_score, adj_score)
        
        scores['adjacent'][adj_cat] = {}
        for style in styles:
            adj_program_hotels = len(filter_hotels_by_style(adj_category_hotels[adj_category_hotels['loyalty_program'] == program], [style]))
            scores['adjacent'][adj_cat][style] = {'hotels': adj_program_hotels, 'points': adj_score}
    
    return scores

def get_detailed_purpose_scores(filtered_by_region, program, category, purposes):
    """Розраховує детальні бали для цілей з правильним розподілом при ties"""
    scores = {'main': {}, 'adjacent': {}}
    
    if not purposes or not category:
        return scores
    
    # Основна категорія
    main_category_hotels = filter_hotels_by_category(filtered_by_region, category)
    main_purpose_filtered = filter_hotels_by_purpose(main_category_hotels, purposes)
    main_counts = main_purpose_filtered.groupby('loyalty_program').size().to_dict()
    
    # Використовуємо нову функцію розподілу балів
    main_score_values = [21, 18, 15, 12, 9, 6, 3]
    main_scores = distribute_scores_with_ties(main_counts, main_score_values)
    main_total_score = main_scores.get(program, 0.0)
    
    # Нормалізуємо, якщо кілька цілей
    if len(purposes) > 1:
        main_total_score = main_total_score / len(purposes)
    
    for purpose in purposes:
        main_program_hotels = len(filter_hotels_by_purpose(main_category_hotels[main_category_hotels['loyalty_program'] == program], [purpose]))
        scores['main'][purpose] = {'hotels': main_program_hotels, 'points': main_total_score}
    
    # Суміжні категорії
    adjacent_categories = get_adjacent_categories(category)
    max_adj_score = 0.0
    
    for adj_cat in adjacent_categories:
        adj_category_hotels = filter_hotels_by_category(filtered_by_region, adj_cat)
        adj_purpose_filtered = filter_hotels_by_purpose(adj_category_hotels, purposes)
        adj_counts = adj_purpose_filtered.groupby('loyalty_program').size().to_dict()
        
        # Використовуємо нову функцію розподілу балів
        adj_score_values = [7, 6, 5, 4, 3, 2, 1]
        adj_scores = distribute_scores_with_ties(adj_counts, adj_score_values)
        adj_score = adj_scores.get(program, 0.0)
        
        # Нормалізуємо, якщо кілька цілей
        if len(purposes) > 1:
            adj_score = adj_score / len(purposes)
        
        max_adj_score = max(max_adj_score, adj_score)
        
        scores['adjacent'][adj_cat] = {}
        for purpose in purposes:
            adj_program_hotels = len(filter_hotels_by_purpose(adj_category_hotels[adj_category_hotels['loyalty_program'] == program], [purpose]))
            scores['adjacent'][adj_cat][purpose] = {'hotels': adj_program_hotels, 'points': adj_score}
    
    return scores

def format_detailed_results(user_data, scores_df, lang='en'):
    """Форматує ДЕТАЛЬНІ результати за новим прикладом"""
    results = ""
    
    max_programs = min(5, len(scores_df))
    top_programs = scores_df.head(max_programs)
    
    # Отримуємо детальну інформацію
    regions = user_data.get('regions', []) or []
    countries = user_data.get('countries', []) or []
    category = user_data.get('category')
    styles = user_data.get('styles', []) or []
    purposes = user_data.get('purposes', []) or []
    
    # Фільтруємо дані за регіоном для детального аналізу
    filtered_by_region = filter_hotels_by_region(hotel_data, regions, countries)
    
    for i, (index, row) in enumerate(top_programs.iterrows()):
        program = row['loyalty_program']
        
        if lang == 'uk':
            results += f"🥇 {i+1}. {program}\n"
            results += f"Загальний бал: {row['total_score']:.2f}\n"
            results += "-" * 30 + "\n"
        else:
            results += f"🥇 {i+1}. {program}\n"
            results += f"Total score: {row['total_score']:.2f}\n"
            results += "-" * 30 + "\n"
        
        # РЕГІОН
        if lang == 'uk':
            results += f"📍 REGION: {row['region_score']:.1f} балів\n"
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"   {row['region_hotels']} готелів у {region_str}\n\n"
        else:
            results += f"📍 REGION: {row['region_score']:.1f} points\n"
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"   {row['region_hotels']} hotels in {region_str}\n\n"
        
        # КАТЕГОРІЯ - використовуємо дані з scores_df
        if category:
            # Розраховуємо кількість готелів для основної категорії
            main_filtered = filter_hotels_by_category(filtered_by_region, category)
            main_program_hotels = len(main_filtered[main_filtered['loyalty_program'] == program])
            
            # Розраховуємо кількість готелів для суміжних категорій
            adjacent_categories = get_adjacent_categories(category)
            adjacent_hotels_data = {}
            
            for adj_cat in adjacent_categories:
                adj_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
                adj_program_hotels = len(adj_filtered[adj_filtered['loyalty_program'] == program])
                adjacent_hotels_data[adj_cat] = adj_program_hotels
            
            # Розраховуємо бали для основної категорії (з загального балу категорії)
            total_category_score = row['category_score']
            
            # Розраховуємо суміжні бали
            adjacent_total_score = 0.0
            for adj_cat in adjacent_categories:
                adj_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
                adj_counts = adj_filtered.groupby('loyalty_program').size().to_dict()
                
                if adj_counts:
                    adj_score_values = [7, 6, 5, 4, 3, 2, 1]
                    adj_scores = distribute_scores_with_ties(adj_counts, adj_score_values)
                    adj_score = adj_scores.get(program, 0.0)
                    adjacent_total_score = max(adjacent_total_score, adj_score)
            
            # Основний бал = загальний - суміжний
            main_category_score = total_category_score - adjacent_total_score
            
            if lang == 'uk':
                results += f"🏨 CATEGORY: {row['category_score']:.1f} балів\n"
                results += f"   (основна) {category} – {main_program_hotels} готелів – {main_category_score:.1f} балів\n"
                
                for adj_cat in adjacent_categories:
                    adj_hotels = adjacent_hotels_data[adj_cat]
                    # Розраховуємо бали для цієї суміжної категорії
                    adj_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
                    adj_counts = adj_filtered.groupby('loyalty_program').size().to_dict()
                    adj_score = 0.0
                    if adj_counts:
                        adj_score_values = [7, 6, 5, 4, 3, 2, 1]
                        adj_scores = distribute_scores_with_ties(adj_counts, adj_score_values)
                        adj_score = adj_scores.get(program, 0.0)
                    
                    results += f"   (суміжна) {adj_cat} – {adj_hotels} готелів – {adj_score:.1f} балів\n"
                results += "\n"
            else:
                results += f"🏨 CATEGORY: {row['category_score']:.1f} points\n"
                results += f"   (main) {category} – {main_program_hotels} hotels – {main_category_score:.1f} points\n"
                
                for adj_cat in adjacent_categories:
                    adj_hotels = adjacent_hotels_data[adj_cat]
                    # Розраховуємо бали для цієї суміжної категорії
                    adj_filtered = filter_hotels_by_category(filtered_by_region, adj_cat)
                    adj_counts = adj_filtered.groupby('loyalty_program').size().to_dict()
                    adj_score = 0.0
                    if adj_counts:
                        adj_score_values = [7, 6, 5, 4, 3, 2, 1]
                        adj_scores = distribute_scores_with_ties(adj_counts, adj_score_values)
                        adj_score = adj_scores.get(program, 0.0)
                    
                    results += f"   (adjacent) {adj_cat} – {adj_hotels} hotels – {adj_score:.1f} points\n"
                results += "\n"
        
        # СТИЛЬ
        if styles:
            style_scores = get_detailed_style_scores(filtered_by_region, program, category, styles)
            
            if lang == 'uk':
                results += f"🎨 STYLE: {row['style_score']:.1f} балів\n"
                
                # Основна категорія
                for style in styles:
                    if style in style_scores['main']:
                        data = style_scores['main'][style]
                        results += f"   {style} в {category.lower()} {data['hotels']} готелів – {data['points']:.1f} балів\n"
                
                # Суміжні категорії - показуємо всі, навіть з 0 готелів
                for adj_cat, adj_styles in style_scores['adjacent'].items():
                    for style in styles:
                        if style in adj_styles:
                            data = adj_styles[style]
                            results += f"   {style} в {adj_cat.lower()} (суміжний сегмент) {data['hotels']} готелів – {data['points']:.1f} балів\n"
                results += "\n"
            else:
                results += f"🎨 STYLE: {row['style_score']:.1f} points\n"
                
                # Основна категорія
                for style in styles:
                    if style in style_scores['main']:
                        data = style_scores['main'][style]
                        results += f"   {style} in {category.lower()} {data['hotels']} hotels – {data['points']:.1f} points\n"
                
                # Суміжні категорії - показуємо всі, навіть з 0 готелів
                for adj_cat, adj_styles in style_scores['adjacent'].items():
                    for style in styles:
                        if style in adj_styles:
                            data = adj_styles[style]
                            results += f"   {style} in {adj_cat.lower()} (adjacent segment) {data['hotels']} hotels – {data['points']:.1f} points\n"
                results += "\n"
        
        # МЕТА
        if purposes:
            purpose_scores = get_detailed_purpose_scores(filtered_by_region, program, category, purposes)
            
            if lang == 'uk':
                results += f"🎯 PURPOSE: {row['purpose_score']:.1f} балів\n"
                
                # Основна категорія
                for purpose in purposes:
                    if purpose in purpose_scores['main']:
                        data = purpose_scores['main'][purpose]
                        results += f"   {purpose} в {category.lower()} {data['hotels']} готелів – {data['points']:.1f} балів\n"
                
                # Суміжні категорії - показуємо всі, навіть з 0 готелів
                for adj_cat, adj_purposes in purpose_scores['adjacent'].items():
                    for purpose in purposes:
                        if purpose in adj_purposes:
                            data = adj_purposes[purpose]
                            results += f"   {purpose} в {adj_cat.lower()} (суміжний сегмент) {data['hotels']} готелів – {data['points']:.1f} балів\n"
                results += "\n"
            else:
                results += f"🎯 PURPOSE: {row['purpose_score']:.1f} points\n"
                
                # Основна категорія
                for purpose in purposes:
                    if purpose in purpose_scores['main']:
                        data = purpose_scores['main'][purpose]
                        results += f"   {purpose} in {category.lower()} {data['hotels']} hotels – {data['points']:.1f} points\n"
                
                # Суміжні категорії - показуємо всі, навіть з 0 готелів
                for adj_cat, adj_purposes in purpose_scores['adjacent'].items():
                    for purpose in purposes:
                        if purpose in adj_purposes:
                            data = adj_purposes[purpose]
                            results += f"   {purpose} in {adj_cat.lower()} (adjacent segment) {data['hotels']} hotels – {data['points']:.1f} points\n"
                results += "\n"
        
        # ПІДСУМОК
        if lang == 'uk':
            results += f"➕ ПІДСУМОК:\n"
            results += f"   {row['region_score']:.1f} + {row['category_score']:.1f} + {row['style_score']:.1f} + {row['purpose_score']:.1f} = {row['total_score']:.2f} балів\n"
        else:
            results += f"➕ SUMMARY:\n"
            results += f"   {row['region_score']:.1f} + {row['category_score']:.1f} + {row['style_score']:.1f} + {row['purpose_score']:.1f} = {row['total_score']:.2f} points\n"
        
        if i < max_programs - 1:
            results += "\n" + "="*50 + "\n\n"
    
    return results

async def calculate_and_show_results(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обчислює результати та відображає їх користувачеві з детальним звітом"""
    
    user_id = update.effective_user.id
    user_data = user_data_global[user_id]
    lang = user_data['language']
    
    try:
        logger.info(f"Розрахунок балів для користувача {user_id}")
        
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
        
        # Підраховуємо бали для кожної програми лояльності
        scores_df = calculate_scores(user_data, hotel_data)
        
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
        
        # Форматуємо ДЕТАЛЬНІ результати для відображення
        results = format_detailed_results(user_data, scores_df, lang)
        
        # Відправляємо результати користувачеві частинами (через довгий текст)
        if lang == 'uk':
            intro_text = ("🎉 **Аналіз завершено!** \n\n"
                         "Ось топ-5 програм лояльності готелів з детальним розбором балів:\n\n")
            outro_text = ("\n\n📝 **Пояснення логіки:**\n"
                         "• **Основна категорія**: бали за вибрану категорію (21,18,15,12,9,6,3)\n"
                         "• **Суміжні категорії**: додаткові бали (7,6,5,4,3,2,1)\n"
                         "• **Luxury**: суміжна Comfort\n"
                         "• **Comfort**: суміжні Luxury + Standard\n"
                         "• **Standard**: суміжна Comfort\n\n"
                         "Щоб почати нове опитування, надішліть команду /start.")
        else:
            intro_text = ("🎉 **Analysis completed!** \n\n"
                         "Here are the top 5 hotel loyalty programs with detailed score breakdown:\n\n")
            outro_text = ("\n\n📝 **Logic explanation:**\n"
                         "• **Main category**: points for selected category (21,18,15,12,9,6,3)\n"
                         "• **Adjacent categories**: additional points (7,6,5,4,3,2,1)\n"
                         "• **Luxury**: adjacent Comfort\n"
                         "• **Comfort**: adjacent Luxury + Standard\n"
                         "• **Standard**: adjacent Comfort\n\n"
                         "To start a new survey, send the /start command.")
        
        # Відправляємо повний звіт частинами
        full_message = intro_text + results + outro_text
        await send_long_message_to_chat(context, update.callback_query.message.chat_id, full_message)
    
    except Exception as e:
        logger.error(f"Помилка при обчисленні результатів: {e}")
        
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
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
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
    
    # Створення застосунку
    app = Application.builder().token(token)
    
    # Побудова застосунку
    application = app.build()
    
    # Налаштування обробників
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            LANGUAGE: [CallbackQueryHandler(language_choice)],
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
    
    application.add_handler(conv_handler)
    
    # Використання PORT для webhook
    port = int(os.environ.get("PORT", "10000"))
    
    if webhook_url and webhook_path:
        webhook_info = f"{webhook_url}{webhook_path}"
        logger.info(f"Запуск бота в режимі webhook на {webhook_info}")
        application.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=webhook_path,
            webhook_url=webhook_info,
            allowed_updates=Update.ALL_TYPES
        )
    else:
        logger.info("WEBHOOK_URL не вказано. Запуск бота в режимі polling...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    
    logger.info("Бот запущено")

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
    
    # Запускаємо бота з підтримкою webhook або polling
    main(TOKEN, CSV_PATH, WEBHOOK_URL, 10000, WEBHOOK_PATH)

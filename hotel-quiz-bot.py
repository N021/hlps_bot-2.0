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

# ВИПРАВЛЕНІ функції стилю з чекбоксами
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
    
    # ВИПРАВЛЕННЯ: Перевіряємо, чи це оновлення існуючого повідомлення зі стилями
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
        
        # ВИПРАВЛЕННЯ: Видаляємо клавіатуру, але зберігаємо текст питання 3/4
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
        
        # ВИПРАВЛЕННЯ: Оновлюємо клавіатуру з новим вибором
        return await ask_style(update, context)

# ВИПРАВЛЕНІ функції вибору мети з чекбоксами
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
    
    # ВИПРАВЛЕННЯ: Перевіряємо, чи це оновлення існуючого повідомлення з метою
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
        
        # ВИПРАВЛЕННЯ: Видаляємо клавіатуру, але зберігаємо текст питання 4/4
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
        
        # ВИПРАВЛЕННЯ: Оновлюємо клавіатуру з новим вибором
        return await ask_purpose(update, context)

# Допоміжні функції для фільтрації та зіставлення

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
    
    # Повний словник цілей і брендів
    purpose_mapping = {
        "Бізнес-подорожі / відрядження": [
            "JW Marriott", "Marriott Hotels", "Sheraton", 
            "Conrad Hotels & Resorts", "Hilton Hotels & Resorts", 
            "Crowne Plaza", "InterContinental Hotels & Resorts", 
            "Wyndham", "Wyndham Grand", "Cambria Hotels", 
            "Grand Hyatt", "Hyatt Regency", "Hyatt Place"
        ],
        
        "Відпустка / релакс": [
            "The Ritz-Carlton", "JW Marriott", "Conrad Hotels & Resorts", 
            "Waldorf Astoria Hotels & Resorts", "Kimpton Hotels & Restaurants", 
            "Crowne Plaza", "InterContinental Hotels & Resorts", "Wyndham Grand", 
            "Registry Collection Hotels", "Fairmont Hotels", "Raffles Hotels & Resorts", 
            "Park Hyatt Hotels", "Alila Hotels", "Grand Hyatt"
        ],
        
        "Сімейний відпочинок": [
            "Fairfield Inn & Suites", "Courtyard by Marriott", 
            "DoubleTree by Hilton", "Hampton by Hilton", "Hilton Garden Inn", 
            "Holiday Inn Hotels & Resorts", "Holiday Inn Express", 
            "Wyndham", "Days Inn by Wyndham", "Mercure Hotels", 
            "Novotel Hotels", "Quality Inn Hotels", "Comfort Inn Hotels", 
            "Hyatt Place", "Hyatt House"
        ],
        
        "Довготривале проживання": [
            "Residence Inn", "Courtyard by Marriott", 
            "Homewood Suites by Hilton", "Home2 Suites by Hilton", 
            "Candlewood Suites", "Staybridge Suites", 
            "Hawthorn Suites by Wyndham", "Hyatt House"
        ]
    }
    
    # Додаємо англійські ключі для цілей
    purpose_mapping_en = {
        "Business travel": purpose_mapping["Бізнес-подорожі / відрядження"],
        "Vacation / relaxation": purpose_mapping["Відпустка / релакс"],
        "Family vacation": purpose_mapping["Сімейний відпочинок"],
        "Long-term stay": purpose_mapping["Довготривале проживання"]
    }
    
    # Об'єднуємо словники
    combined_mapping = {**purpose_mapping, **purpose_mapping_en}
    
    result = {}
    for purpose, brands in combined_mapping.items():
        # Більш гнучке порівняння назв брендів
        is_match = False
        for brand in brands:
            brand_lower = brand.lower()
            # Перевіряємо, чи містить бренд готелю назву бренду зі списку
            if brand_lower in hotel_brand:
                is_match = True
                break
        result[purpose] = is_match
    
    return result

def filter_hotels_by_region(df, regions=None, countries=None):
    """
    Фільтрує готелі за регіоном або країною
    
    Args:
        df: DataFrame з даними про готелі
        regions: список обраних регіонів
        countries: список обраних країн
    
    Returns:
        Відфільтрований DataFrame
    """
    # Якщо не вказано ні регіонів, ні країн, повертаємо всі дані
    if (not regions or len(regions) == 0) and (not countries or len(countries) == 0):
        return df
    
    # Фільтрація за регіоном
    if regions and len(regions) > 0:
        # Перетворюємо регіони до нижнього регістру для порівняння
        regions_lower = [region.lower() for region in regions]
        
        # Створюємо маску для регіонів
        region_mask = df['region'].str.lower().apply(lambda x: any(region in x.lower() for region in regions_lower))
        
        # Повертаємо відфільтровані дані
        return df[region_mask]
    
    # Фільтрація за країною
    elif countries and len(countries) > 0:
        # Перетворюємо країни до нижнього регістру для порівняння
        countries_lower = [country.lower() for country in countries]
        
        # Створюємо маску для країн
        country_mask = df['country'].str.lower().apply(lambda x: any(country in x.lower() for country in countries_lower))
        
        # Повертаємо відфільтровані дані
        return df[country_mask]
    
    return df

def filter_hotels_by_category(df, category):
    """
    Фільтрує готелі за категорією
    
    Args:
        df: DataFrame з даними про готелі
        category: обрана категорія
    
    Returns:
        Відфільтрований DataFrame
    """
    if not category:
        return df
    
    # Перевіряємо, чи є колонка 'segment'
    if 'segment' not in df.columns:
        logger.error("Column 'segment' is missing in DataFrame")
        return df
    
    # Перетворюємо категорію і segments до нижнього регістру для порівняння
    category_lower = category.lower()
    
    # Створюємо маску для фільтрації
    if category_lower == 'luxury':
        # Luxury включає 'luxury', 'upscale', 'upper upscale'
        category_mask = df['segment'].str.lower().apply(
            lambda x: any(seg in x.lower() for seg in ['luxury', 'upscale'])
        )
    elif category_lower == 'comfort':
        # Comfort включає 'midscale', 'upper midscale'
        category_mask = df['segment'].str.lower().apply(
            lambda x: any(seg in x.lower() for seg in ['midscale', 'middle', 'comfort'])
        )
    elif category_lower == 'standard':
        # Standard включає 'economy', 'budget'
        category_mask = df['segment'].str.lower().apply(
            lambda x: any(seg in x.lower() for seg in ['standard', 'economy', 'budget'])
        )
    else:
        # Якщо категорія не відповідає відомим, повертаємо всі дані
        return df
    
    # Повертаємо відфільтровані дані
    return df[category_mask]

def filter_hotels_by_adjacent_category(df, category):
    """
    Фільтрує готелі за суміжною категорією
    
    Args:
        df: DataFrame з даними готелів
        category: обрана категорія (Luxury, Comfort, Standard)
    
    Returns:
        Відфільтрований DataFrame
    """
    adjacent_mapping = {
        "Luxury": ["Comfort"],
        "Comfort": ["Luxury", "Standard"],
        "Standard": ["Comfort"],
    }
    
    if category in adjacent_mapping:
        if 'segment' in df.columns:
            adjacent_categories = adjacent_mapping[category]
            masks = []
            
            for adj_cat in adjacent_categories:
                adj_cat_lower = adj_cat.lower()
                if adj_cat_lower == 'luxury':
                    mask = df['segment'].str.lower().apply(
                        lambda x: any(seg in x.lower() for seg in ['luxury', 'upscale'])
                    )
                elif adj_cat_lower == 'comfort':
                    mask = df['segment'].str.lower().apply(
                        lambda x: any(seg in x.lower() for seg in ['midscale', 'middle', 'comfort'])
                    )
                elif adj_cat_lower == 'standard':
                    mask = df['segment'].str.lower().apply(
                        lambda x: any(seg in x.lower() for seg in ['standard', 'economy', 'budget'])
                    )
                else:
                    continue
                masks.append(mask)
            
            if masks:
                # Об'єднуємо всі маски через OR
                combined_mask = masks[0]
                for mask in masks[1:]:
                    combined_mask = combined_mask | mask
                return df[combined_mask]
    
    return df

def filter_hotels_by_style(df, styles):
    """Фільтрує готелі за стилем"""
    if not styles or len(styles) == 0:
        return df
    
    logger.info(f"Filtering by styles: {styles}")
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
    
    return df[style_mask]

def filter_hotels_by_purpose(df, purposes):
    """Фільтрує готелі за метою подорожі"""
    if not purposes or len(purposes) == 0:
        return df
    
    logger.info(f"Filtering by purpose: {purposes}")
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
    
    return df[purpose_mask]

def get_region_score(df, regions=None, countries=None):
    """Розраховує бали для програм лояльності за регіонами/країнами"""
    region_scores = {}
    try:
        if regions and len(regions) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this region' in df.columns:
                region_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this region']]
                region_counts = region_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this region']
            else:
                region_counts = df.groupby('loyalty_program').size()
        elif countries and len(countries) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this country' in df.columns:
                country_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this country']]
                region_counts = country_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this country']
            else:
                region_counts = df.groupby('loyalty_program').size()
        else:
            return {}
        
        region_counts = region_counts.fillna(0).astype(float)
        score_values = [21, 18, 15, 12, 9, 6, 3]
        ranked_programs = region_counts.sort_values(ascending=False)
        
        for i, (program, _) in enumerate(ranked_programs.items()):
            if i < len(score_values):
                region_scores[program] = score_values[i]
            else:
                region_scores[program] = 0.0
    except Exception as e:
        logger.error(f"Error calculating region scores: {e}")
    
    return region_scores

def calculate_scores(user_data, hotel_data):
    """
    Розраховує загальний рейтинг для кожної програми лояльності на основі відповідей користувача
    
    Args:
        user_data: словник з відповідями користувача
        hotel_data: DataFrame з даними готелів
    
    Returns:
        DataFrame з програмами лояльності та їх балами
    """
    # Отримуємо відповіді користувача
    regions = user_data.get('regions', [])
    countries = user_data.get('countries', [])
    category = user_data.get('category')
    styles = user_data.get('styles', [])
    purposes = user_data.get('purposes', [])
    
    # Перевірка на None та перетворення на порожні списки для уникнення помилок
    if regions is None:
        regions = []
    if countries is None:
        countries = []
    if styles

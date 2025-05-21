import logging
import pandas as pd
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, CallbackQueryHandler
import os
import json
import asyncio  # Додайте цей імпорт
from telegram.ext import ApplicationBuilder
import ssl
from aiohttp import web
PORT = int(os.environ.get("PORT", "10000"))

# Налаштування логування
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Етапи розмови
LANGUAGE, REGION, WAITING_REGION_SUBMIT, CATEGORY, WAITING_STYLE_SUBMIT, WAITING_PURPOSE_SUBMIT = range(6)

# Дані користувачів
user_data_global = {}

def analyze_csv_structure(df):
    """
    Аналізує структуру CSV-файлу та логує інформацію
    
    Args:
        df: DataFrame з даними готелів
    """
    logger.info("Аналіз структури CSV-файлу:")
    logger.info(f"Кількість рядків: {len(df)}")
    logger.info(f"Колонки: {list(df.columns)}")
    
    # Перевірка унікальних значень
    if 'loyalty_program' in df.columns:
        logger.info(f"Програми лояльності: {df['loyalty_program'].unique()}")
    
    if 'region' in df.columns:
        logger.info(f"Регіони: {df['region'].unique()}")
    
    if 'segment' in df.columns:
        logger.info(f"Сегменти: {df['segment'].unique()}")
    
    # Перевірка пропущених значень
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"Пропущені значення: {null_counts[null_counts > 0]}")
    
    # Перевірка типів даних
    logger.info(f"Типи даних: {df.dtypes}")

def load_hotel_data(csv_path):
    """Завантаження даних про програми лояльності з CSV файлу"""
    try:
        # Перевіряємо, чи існує файл
        if not os.path.exists(csv_path):
            logger.error(f"Файл не знайдено: {csv_path}")
            return None
            
        df = pd.read_csv(csv_path)
        
        # Аналізуємо структуру CSV
        analyze_csv_structure(df)
        
        # Базова валідація даних - оновлено згідно з очікуваними назвами колонок
        expected_columns = ['loyalty_program', 'region', 'country', 'Hotel Brand', 'segment',
                            'Total hotels of Corporation / Loyalty Program in this region',
                            'Total hotels of Corporation / Loyalty Program in this country']
        
        # Перевіряємо наявність колонок і створюємо відображення для перейменування
        rename_mapping = {}
        
        # Перевіряємо наявність колонки 'Hotel Brand' або 'brand'
        if 'brand' in df.columns and 'Hotel Brand' not in df.columns:
            rename_mapping['brand'] = 'Hotel Brand'
            logger.info("Перейменовано колонку 'brand' в 'Hotel Brand'")
        
        # Перевіряємо наявність колонки 'segment' або 'category'
        if 'category' in df.columns and 'segment' not in df.columns:
            rename_mapping['category'] = 'segment'
            logger.info("Перейменовано колонку 'category' в 'segment'")
        
        # Якщо є колонка з коротшою назвою для регіонів
        if 'region_hotels' in df.columns and 'Total hotels of Corporation / Loyalty Program in this region' not in df.columns:
            rename_mapping['region_hotels'] = 'Total hotels of Corporation / Loyalty Program in this region'
            logger.info("Перейменовано колонку 'region_hotels'")
        
        # Якщо є колонка з коротшою назвою для країн
        if 'country_hotels' in df.columns and 'Total hotels of Corporation / Loyalty Program in this country' not in df.columns:
            rename_mapping['country_hotels'] = 'Total hotels of Corporation / Loyalty Program in this country'
            logger.info("Перейменовано колонку 'country_hotels'")
        
        # Застосовуємо перейменування, якщо потрібно
        if rename_mapping:
            df = df.rename(columns=rename_mapping)
            logger.info(f"Перейменовано колонки: {rename_mapping}")
        
        # Перевіряємо, чи існують обов'язкові колонки після перейменування
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"Після перейменування все ще відсутні колонки: {missing_columns}")
            
            # Створюємо відсутні колонки з порожніми значеннями
            for col in missing_columns:
                df[col] = ''
                logger.warning(f"Створено порожню колонку: {col}")
        
        return df
    except Exception as e:
        logger.error(f"Помилка завантаження CSV: {e}")
        return None

# Функція для початку бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    
    # Завжди очищаємо дані користувача при команді /start
    if user_id in user_data_global:
        del user_data_global[user_id]
    
    # Ініціалізуємо нові дані
    user_data_global[user_id] = {}
    
    # Логуємо початок нової розмови
    logger.info(f"Користувач {user_id} почав нову розмову. Дані очищено.")
    
    # Клавіатура для вибору мови з використанням InlineKeyboardMarkup
    keyboard = [
        [InlineKeyboardButton("Українська (Ukrainian)", callback_data='lang_uk')],
        [InlineKeyboardButton("English (Англійська)", callback_data='lang_en')],
        [InlineKeyboardButton("Other (Інша)", callback_data='lang_other')]
    ]
    
    await update.message.reply_text(
        "Please select your preferred language for our conversation "
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
        return await ask_region(update, context)
    
    elif callback_data == 'lang_en':
        user_data_global[user_id]['language'] = 'en'
        await query.edit_message_text(
            "Thank you! I will continue our conversation in English."
        )
        return await ask_region(update, context)
    
    else:
        user_data_global[user_id]['language'] = 'en'  # За замовчуванням - англійська
        await query.edit_message_text(
            "I'll continue in English. If you need another language, please let me know."
        )
        return await ask_region(update, context)

# Функції для регіону
async def ask_region(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про регіони подорожей з чекбоксами"""
    # Визначаємо, чи це відповідь на callback_query чи новий запит
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
    
    # Ініціалізуємо вибрані регіони, якщо вони ще не вибрані
    if 'selected_regions' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_regions'] = []
    
    # Створюємо InlineKeyboard з чекбоксами
    if lang == 'uk':
        regions = [
            "Європа", "Північна Америка", "Азія",
            "Близький Схід", "Африка", "Південна Америка",
            "Карибський басейн", "Океанія"
        ]
        
        # Створюємо детальний опис з нумерацією для кращого відображення
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
        
        # Створюємо детальний опис з нумерацією для кращого відображення
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
    
    # Групуємо регіони по 2 в рядку з номерами
    for i in range(0, len(regions), 2):
        row = []
        for j in range(2):
            if i + j < len(regions):
                region = regions[i + j]
                region_index = i + j + 1  # Номер регіону (починаючи з 1)
                # Додаємо символ чекбокса в залежності від вибору
                checkbox = "✅ " if region in selected_regions else "☐ "
                row.append(InlineKeyboardButton(
                    f"{checkbox}{region_index}. {region}", 
                    callback_data=f"region_{region}"
                ))
        keyboard.append(row)
    
    # Додаємо кнопку "Відповісти" внизу
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="region_submit")])
    
    # Відправляємо або оновлюємо повідомлення
    if message_id and update.callback_query:
        await context.bot.edit_message_text(
            text=title_text,
            chat_id=chat_id,
            message_id=message_id,
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
    """Обробляє вибір регіонів через чекбокси"""
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
        
        # Зберігаємо вибрані регіони як основні дані
        user_data_global[user_id]['regions'] = selected_regions
        user_data_global[user_id]['countries'] = None  # Виключаємо можливість вказувати країни
        
        # Оновлюємо повідомлення, видаляючи клавіатуру, але залишаючи текст
        regions_description = query.message.text
        try:
            await query.edit_message_text(text=regions_description)
        except Exception as e:
            logger.error(f"Помилка при оновленні повідомлення: {e}")
        
        # Відправляємо нове повідомлення з підтвердженням вибору
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
        
        # Додаємо невелику затримку перед наступним питанням
        import asyncio
        await asyncio.sleep(0.5)
        
        # Переходимо до питання про категорію
        return await ask_category(update, context)
    
    # Інакше це вибір чи скасування вибору регіону
    else:
        # Обробляємо вибір регіону
        region = callback_data.replace("region_", "")
        
        # Перемикаємо стан вибору регіону
        if region in user_data_global[user_id]['selected_regions']:
            user_data_global[user_id]['selected_regions'].remove(region)
        else:
            user_data_global[user_id]['selected_regions'].append(region)
        
        # Оновлюємо клавіатуру
        return await ask_region(update, context)

# Функція завершення розмови
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Скасовує розмову за командою /cancel"""
    user = update.message.from_user
    user_id = user.id
    logger.info(f"Користувач {user_id} скасував розмову.")
    
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
        logger.info(f"Дані користувача {user_id} успішно видалено")
    
    # Очищаємо контекст, якщо він доступний
    if hasattr(context, 'user_data'):
        context.user_data.clear()
    
    return ConversationHandler.END

# Функції для категорії готелів
async def ask_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про категорію готелів"""
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
            [InlineKeyboardButton("Luxury (преміум-клас)", callback_data='category_Luxury')],
            [InlineKeyboardButton("Comfort (середній клас)", callback_data='category_Comfort')],
            [InlineKeyboardButton("Standard (економ-клас)", callback_data='category_Standard')]
        ]
        
        await context.bot.send_message(
            chat_id=chat_id,
            text="Питання 2/4:\nЯку категорію готелів ви зазвичай обираєте?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        keyboard = [
            [InlineKeyboardButton("Luxury (premium class)", callback_data='category_Luxury')],
            [InlineKeyboardButton("Comfort (middle class)", callback_data='category_Comfort')],
            [InlineKeyboardButton("Standard (economy class)", callback_data='category_Standard')]
        ]
        
        await context.bot.send_message(
            chat_id=chat_id,
            text="Question 2/4:\nWhich hotel category do you usually choose?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    return CATEGORY

async def category_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір категорії готелю"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    lang = user_data_global[user_id]['language']
    
    # Визначення обраної категорії
    category = callback_data.replace("category_", "")
    
    # Зберігаємо вибрану категорію
    user_data_global[user_id]['category'] = category
    
    # Оновлюємо повідомлення, видаляючи клавіатуру, але залишаючи текст
    category_text = query.message.text
    await query.edit_message_text(text=category_text, reply_markup=None)
    
    # Відправляємо нове повідомлення з підтвердженням вибору
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
    
    # Переходимо до питання про стиль
    return await ask_style(update, context)

# Функції для стилю готелів з чекбоксами
async def ask_style(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про стиль готелю з чекбоксами та детальними описами"""
    # Визначаємо, чи це відповідь на callback_query
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
    
    # Ініціалізуємо вибрані стилі, якщо вони ще не вибрані
    if 'selected_styles' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_styles'] = []
    
    # Створюємо InlineKeyboard з чекбоксами для стилів
    if lang == 'uk':
        # Основні назви стилів для кнопок
        styles = [
            "Розкішний і вишуканий", 
            "Бутік і унікальний", 
            "Класичний і традиційний", 
            "Сучасний і дизайнерський",
            "Затишний і сімейний", 
            "Практичний і економічний"
        ]
        
        # Детальний опис стилів для відображення в повідомленні
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
        # Основні назви стилів для кнопок
        styles = [
            "Luxurious and refined", 
            "Boutique and unique",
            "Classic and traditional", 
            "Modern and designer",
            "Cozy and family-friendly", 
            "Practical and economical"
        ]
        
        # Детальний опис стилів для відображення в повідомленні
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
        # Додаємо символ чекбокса в залежності від вибору
        checkbox = "✅ " if style in selected_styles else "☐ "
        keyboard.append([InlineKeyboardButton(
            f"{checkbox}{i+1}. {style}", 
            callback_data=f"style_{style}"
        )])
    
    # Додаємо кнопку "Відповісти" внизу
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="style_submit")])
    
    # Відправляємо або оновлюємо повідомлення
    if message_id and update.callback_query:
        await context.bot.edit_message_text(
            text=title_text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"  # Додаємо підтримку форматування Markdown
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=title_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"  # Додаємо підтримку форматування Markdown
        )
    
    return WAITING_STYLE_SUBMIT

async def style_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір стилів через чекбокси"""
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
        
        # Обмежуємо до трьох варіантів
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
            
            # Оновлюємо вибір і клавіатуру
            return await ask_style(update, context)
        
        # Зберігаємо вибрані стилі
        user_data_global[user_id]['styles'] = selected_styles
        
        # Оновлюємо повідомлення, видаляючи клавіатуру, але залишаючи текст
        style_text = query.message.text
        await query.edit_message_text(text=style_text, reply_markup=None, parse_mode="Markdown")
        
        # Відправляємо нове повідомлення з підтвердженням вибору
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
        
        # Переходимо до питання про мету подорожі
        return await ask_purpose(update, context)
    
    # Інакше це вибір чи скасування вибору стилю
    else:
        # Обробляємо вибір стилю
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
        
        # Оновлюємо клавіатуру
        return await ask_style(update, context)

# Функції для мети подорожі з чекбоксами
async def ask_purpose(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про мету подорожі з чекбоксами та детальними описами"""
    # Визначаємо, чи це відповідь на callback_query
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
    
    # Ініціалізуємо вибрані мети, якщо вони ще не вибрані
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
        
        # Детальний опис цілей у основному тексті
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
        
        # Детальний опис цілей у основному тексті
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
        # Додаємо символ чекбокса в залежності від вибору
        checkbox = "✅ " if purpose in selected_purposes else "☐ "
        keyboard.append([InlineKeyboardButton(
            f"{checkbox}{i+1}. {purpose}", 
            callback_data=f"purpose_{purpose}"
        )])
    
    # Додаємо кнопку "Відповісти" внизу
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="purpose_submit")])
    
    # Відправляємо або оновлюємо повідомлення
    if message_id and update.callback_query:
        await context.bot.edit_message_text(
            text=title_text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"  # Додаємо підтримку форматування Markdown
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=title_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"  # Додаємо підтримку форматування Markdown
        )
    
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
        
        # Обмежуємо до двох варіантів
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
            
            # Оновлюємо вибір і клавіатуру
            return await ask_purpose(update, context)
        
        # Зберігаємо вибрані цілі
        user_data_global[user_id]['purposes'] = selected_purposes
        
        # Оновлюємо повідомлення, видаляючи клавіатуру, але залишаючи текст
        purpose_text = query.message.text
        await query.edit_message_text(text=purpose_text, reply_markup=None, parse_mode="Markdown")
        
        # Відправляємо нове повідомлення з підтвердженням вибору
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
        
        # Обчислюємо та відображаємо результати
        return await calculate_and_show_results(update, context)
    
    # Інакше це вибір чи скасування вибору мети
    else:
        # Обробляємо вибір мети
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
        
        # Оновлюємо клавіатуру
        return await ask_purpose(update, context)

# Допоміжні функції для фільтрації і зіставлення

def map_hotel_style(hotel_brand):
    """
    Зіставляє бренд готелю зі стилями
    
    Args:
        hotel_brand: бренд готелю (один рядок, не список)
    
    Returns:
        Словник стилів з відповідними значеннями True/False
    """
    # Переконуємося, що hotel_brand є рядком
    if not isinstance(hotel_brand, str):
        hotel_brand = str(hotel_brand)
    
    hotel_brand = hotel_brand.lower()
    
    # Оновлений повний словник стилів та брендів
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
            # Перевіряємо, чи бренд готелю містить назву бренду зі списку
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
        Словник цілей з відповідними значеннями True/False
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
                                      "Quality Inn Hotels", "ibis Hotels"],
        
        "Відпустка / релакс": ["The Ritz-Carlton", "JW Marriott", "Waldorf Astoria Hotels & Resorts", 
                             "Conrad Hotels & Resorts", "Park Hyatt Hotels", "Fairmont Hotels", 
                             "Raffles Hotels & Resorts", "InterContinental Hotels & Resorts", 
                             "Kimpton Hotels & Restaurants", "Alila Hotels", "Registry Collection Hotels", 
                             "Ascend Hotel Collection"],
        
        "Сімейний відпочинок": ["JW Marriott", "Hyatt Regency", "Sheraton", "Holiday Inn Hotels & Resorts", 
                              "DoubleTree by Hilton", "Wyndham", "Mercure Hotels", "Novotel Hotels", 
                              "Comfort Inn Hotels", "Hampton by Hilton", "Holiday Inn Express", 
                              "Days Inn by Wyndham", "Super 8 by Wyndham"],
        
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

# Функції фільтрації готелів
def filter_hotels_by_region(df, regions, countries=None):
    """
    Фільтрує готелі за регіоном або країною
    
    Args:
        df: DataFrame з даними готелів
        regions: список обраних регіонів
        countries: список обраних країн (якщо вказано)
    
    Returns:
        Відфільтрований DataFrame
    """
    if not regions and not countries:
        return df
    
    # Створюємо копію для уникнення попереджень
    filtered_df = df.copy()
    
    # Фільтрація за регіонами
    if regions and len(regions) > 0:
        region_mask = filtered_df['region'].apply(lambda x: any(region.lower() in str(x).lower() for region in regions))
        filtered_df = filtered_df[region_mask]
    
    # Фільтрація за країнами
    if countries and len(countries) > 0:
        country_mask = filtered_df['country'].apply(lambda x: any(country.lower() in str(x).lower() for country in countries))
        filtered_df = filtered_df[country_mask]
    
    return filtered_df

def filter_hotels_by_category(df, category):
    """
    Фільтрує готелі за категорією
    
    Args:
        df: DataFrame з даними готелів
        category: обрана категорія (Luxury, Comfort, Standard)
    
    Returns:
        Відфільтрований DataFrame
    """
    category_mapping = {
        "Luxury": ["Luxury"],
        "Comfort": ["Comfort"],
        "Standard": ["Standard", "Standart"],  # Обробка обох варіантів написання
    }
    
    if category in category_mapping:
        if 'segment' in df.columns:
            mask = df['segment'].apply(lambda x: any(cat.lower() in str(x).lower() for cat in category_mapping[category]))
            return df[mask]
    
    return df

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
        "Comfort": ["Luxury", "Standard", "Standart"],  # Обробка обох варіантів
        "Standard": ["Comfort"],  # Змінено на Standard для узгодженості
    }
    
    if category in adjacent_mapping:
        if 'segment' in df.columns:
            mask = df['segment'].apply(lambda x: any(cat.lower() in str(x).lower() for cat in adjacent_mapping[category]))
            return df[mask]
    
    return df

def filter_hotels_by_style(df, styles):
    """
    Фільтрує готелі за стилем
    
    Args:
        df: DataFrame з даними готелів
        styles: список обраних стилів
    
    Returns:
        Відфільтрований DataFrame
    """
    if not styles or len(styles) == 0:
        return df
    
    # Логування для відладки
    logger.info(f"Фільтрація за стилями: {styles}")
    logger.info(f"Колонки DataFrame: {df.columns}")
    if 'Hotel Brand' in df.columns:
        logger.info(f"Кількість унікальних брендів: {df['Hotel Brand'].nunique()}")
        logger.info(f"Приклади брендів: {df['Hotel Brand'].unique()[:5]}")
    
    # Спрощуємо стилі для кращого порівняння
    simplified_styles = [style.strip().lower() for style in styles]
    logger.info(f"Спрощені стилі для пошуку: {simplified_styles}")
    
    # Створюємо маску для фільтрації
    style_mask = pd.Series(False, index=df.index)
    
    # Кількість знайдених готелів по стилях для логування
    style_counts = {style: 0 for style in styles}
    
    for idx, row in df.iterrows():
        if 'Hotel Brand' in df.columns and pd.notna(row['Hotel Brand']):
            hotel_brand = row['Hotel Brand']
            
            # Отримуємо відповідність бренду до стилів
            hotel_styles = map_hotel_style(hotel_brand)
            
            # Перевіряємо, чи готель відповідає хоча б одному з обраних стилів
            for style in styles:
                # Перевіряємо як точну відповідність, так і ключові частини
                style_lower = style.lower()
                
                for hotel_style, matches in hotel_styles.items():
                    if matches and (hotel_style.lower() == style_lower or 
                                    style_lower in hotel_style.lower() or
                                    hotel_style.lower() in style_lower):
                        style_mask.loc[idx] = True
                        style_counts[style] += 1
                        break
    
    # Логуємо кількість знайдених готелів для кожного стилю
    for style, count in style_counts.items():
        logger.info(f"Знайдено {count} готелів для стилю '{style}'")
    
    filtered_df = df[style_mask]
    logger.info(f"Загальна кількість готелів після фільтрації: {len(filtered_df)}")
    
    return filtered_df

def filter_hotels_by_purpose(df, purposes):
    """
    Фільтрує готелі за метою подорожі
    
    Args:
        df: DataFrame з даними готелів
        purposes: список обраних цілей
    
    Returns:
        Відфільтрований DataFrame
    """
    if not purposes or len(purposes) == 0:
        return df
    
    # Логування для відладки
    logger.info(f"Фільтрація за метою: {purposes}")
    
    # Створюємо маску для фільтрації
    purpose_mask = pd.Series(False, index=df.index)
    
    # Кількість знайдених готелів по метах для логування
    purpose_counts = {purpose: 0 for purpose in purposes}
    
    for idx, row in df.iterrows():
        if 'Hotel Brand' in df.columns and pd.notna(row['Hotel Brand']):
            hotel_brand = row['Hotel Brand']
            
            # Отримуємо відповідність бренду до цілей
            hotel_purposes = map_hotel_purpose(hotel_brand)
            
            # Перевіряємо, чи готель відповідає хоча б одній з обраних цілей
            for purpose in purposes:
                # Перевіряємо як точну відповідність, так і ключові частини
                purpose_lower = purpose.lower()
                
                for hotel_purpose, matches in hotel_purposes.items():
                    if matches and (hotel_purpose.lower() == purpose_lower or 
                                    purpose_lower in hotel_purpose.lower() or
                                    hotel_purpose.lower() in purpose_lower):
                        purpose_mask.loc[idx] = True
                        purpose_counts[purpose] += 1
                        break
    
    # Логуємо кількість знайдених готелів для кожної мети
    for purpose, count in purpose_counts.items():
        logger.info(f"Знайдено {count} готелів для мети '{purpose}'")
    
    filtered_df = df[purpose_mask]
    logger.info(f"Загальна кількість готелів після фільтрації: {len(filtered_df)}")
    
    return filtered_df

def get_region_score(df, regions=None, countries=None):
    """
    Обчислює бали для програм лояльності за регіонами/країнами
    
    Args:
        df: DataFrame з даними готелів
        regions: список обраних регіонів
        countries: список обраних країн
    
    Returns:
        Dict з програмами лояльності та їх балами
    """
    region_scores = {}
    
    try:
        if regions and len(regions) > 0:
            # Використовуємо колонку для кількості готелів у регіоні
            if 'Total hotels of Corporation / Loyalty Program in this region' in df.columns:
                # Беремо унікальні значення для кожної програми лояльності
                region_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this region']]
                region_counts = region_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this region']
            else:
                # Якщо колонка відсутня, просто рахуємо кількість готелів
                region_counts = df.groupby('loyalty_program').size()
                logger.warning("Колонка 'Total hotels of Corporation / Loyalty Program in this region' відсутня. Використовуємо кількість рядків.")
        
        elif countries and len(countries) > 0:
            # Використовуємо колонку для кількості готелів у країні
            if 'Total hotels of Corporation / Loyalty Program in this country' in df.columns:
                # Беремо унікальні значення для кожної програми лояльності
                country_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this country']]
                region_counts = country_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this country']
            else:
                # Якщо колонка відсутня, просто рахуємо кількість готелів
                region_counts = df.groupby('loyalty_program').size()
                logger.warning("Колонка 'Total hotels of Corporation / Loyalty Program in this country' відсутня. Використовуємо кількість рядків.")
        
        else:
            # Якщо не вибрано ні регіонів, ні країн, повертаємо порожній словник
            return {}
        
        # Переконуємося, що region_counts не містить NaN або None
        region_counts = region_counts.fillna(0).astype(float)
        
        # Розподіляємо бали за рейтингом (21, 18, 15, 12, 9, 6, 3)
        score_values = [21, 18, 15, 12, 9, 6, 3]
        
        # Сортуємо програми за кількістю готелів
        ranked_programs = region_counts.sort_values(ascending=False)
        
        # Нормалізуємо, якщо обрано кілька регіонів/країн
        normalization_factor = 1.0
        if regions and len(regions) > 0:
            normalization_factor = float(len(regions))
        elif countries and len(countries) > 0:
            normalization_factor = float(len(countries))
        
        # Призначаємо бали за рейтингом
        for i, (program, _) in enumerate(ranked_programs.items()):
            if i < len(score_values):
                region_scores[program] = score_values[i] / normalization_factor
            else:
                region_scores[program] = 0.0
                
    except Exception as e:
        logger.error(f"Помилка обчислення балів за регіоном: {e}")
    
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
    if styles is None:
        styles = []
    if purposes is None:
        purposes = []
    
    # Ініціалізуємо DataFrame для зберігання результатів
    loyalty_programs = hotel_data['loyalty_program'].unique()
    scores_df = pd.DataFrame({
        'loyalty_program': loyalty_programs,
        'region_score': 0.0,  # Явно вказуємо тип float для точності
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
    
    # Використовуємо готові значення з колонок для регіонів та країн
    if regions and len(regions) > 0:
        # Перевіряємо наявність колонки "Total hotels of Corporation / Loyalty Program in this region"
        if 'Total hotels of Corporation / Loyalty Program in this region' in filtered_by_region.columns:
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                program_data = filtered_by_region[filtered_by_region['loyalty_program'] == program]
                
                if not program_data.empty:
                    # Використовуємо унікальне значення з колонки
                    region_hotels = program_data['Total hotels of Corporation / Loyalty Program in this region'].iloc[0]
                    scores_df.at[index, 'region_hotels'] = region_hotels
        else:
            # Якщо колонка відсутня, просто рахуємо кількість готелів
            region_counts = filtered_by_region.groupby('loyalty_program').size()
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                if program in region_counts:
                    scores_df.at[index, 'region_hotels'] = region_counts[program]
    
    elif countries and len(countries) > 0:
        # Перевіряємо наявність колонки "Total hotels of Corporation / Loyalty Program in this country"
        if 'Total hotels of Corporation / Loyalty Program in this country' in filtered_by_region.columns:
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                program_data = filtered_by_region[filtered_by_region['loyalty_program'] == program]
                
                if not program_data.empty:
                    # Використовуємо унікальне значення з колонки
                    country_hotels = program_data['Total hotels of Corporation / Loyalty Program in this country'].iloc[0]
                    scores_df.at[index, 'region_hotels'] = country_hotels
        else:
            # Якщо колонка відсутня, просто рахуємо кількість готелів
            country_counts = filtered_by_region.groupby('loyalty_program').size()
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                if program in country_counts:
                    scores_df.at[index, 'region_hotels'] = country_counts[program]
    
    # Розподіляємо бали за регіонами/країнами
    region_scores = get_region_score(filtered_by_region, regions, countries)
    for index, row in scores_df.iterrows():
        program = row['loyalty_program']
        if program in region_scores:
            scores_df.at[index, 'region_score'] = region_scores[program]
    
    # Крок 2: Фільтруємо готелі за категорією в обраному регіоні
    if category:
        filtered_by_category = filter_hotels_by_category(filtered_by_region, category)
        
        category_counts = filtered_by_category.groupby('loyalty_program').size()
        
        # Розподіляємо бали за категорією (21, 18, 15, 12, 9, 6, 3)
        if not category_counts.empty:
            category_scores = {}
            ranked_programs = category_counts.sort_values(ascending=False)
            
            # Бали за рейтингом
            score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
            for i, (program, _) in enumerate(ranked_programs.items()):
                if i < len(score_values):
                    category_scores[program] = score_values[i]
                else:
                    category_scores[program] = 0.0
            
            # Додаємо бали за суміжні категорії
            adjacent_filtered = filter_hotels_by_adjacent_category(filtered_by_region, category)
            adjacent_counts = adjacent_filtered.groupby('loyalty_program').size()
            
            # Бали за суміжною категорією (7, 6, 5, 4, 3, 2, 1)
            adjacent_score_values = [7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
            adjacent_scores = {}
            
            if not adjacent_counts.empty:
                ranked_adjacent = adjacent_counts.sort_values(ascending=False)
                for i, (program, _) in enumerate(ranked_adjacent.items()):
                    if i < len(adjacent_score_values):
                        adjacent_scores[program] = adjacent_score_values[i]
                    else:
                        adjacent_scores[program] = 0.0
            
            # Оновлюємо DataFrame з балами
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                
                # Бали за повну відповідність
                if program in category_scores:
                    scores_df.at[index, 'category_score'] = category_scores[program]
                
                # Додаємо бали за суміжну категорію
                if program in adjacent_scores:
                    scores_df.at[index, 'category_score'] += adjacent_scores[program]
                
                # Записуємо кількість готелів у категорії
                category_mask = filtered_by_category['loyalty_program'] == program
                scores_df.at[index, 'category_hotels'] = category_mask.sum()
        else:
            # Якщо категорія не обрана, використовуємо all hotels
            filtered_by_category = filtered_by_region
    else:
        filtered_by_category = filtered_by_region
    
    # Крок 3: Фільтруємо готелі за стилем у обраній категорії та регіоні
    if styles and len(styles) > 0:
        style_filtered = filter_hotels_by_style(filtered_by_category, styles)
        style_counts_dict = {}
        
        for program in loyalty_programs:
            style_mask = style_filtered['loyalty_program'] == program
            style_counts_dict[program] = style_mask.sum()
            
            # Записуємо кількість готелів за стилем
            scores_df.loc[scores_df['loyalty_program'] == program, 'style_hotels'] = style_mask.sum()
        
        # Розподіляємо бали за стилями (21, 18, 15, 12, 9, 6, 3)
        style_scores = {}
        ranked_programs = sorted(style_counts_dict.items(), key=lambda x: x[1], reverse=True)
        
        # Бали за рейтингом
        score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
        for i, (program, _) in enumerate(ranked_programs):
            if i < len(score_values):
                style_scores[program] = score_values[i]
            else:
                style_scores[program] = 0.0
        
        # Нормалізуємо бали, якщо обрано кілька стилів
        if len(styles) > 1:
            for program in style_scores:
                style_scores[program] /= len(styles)
        
        # Оновлюємо DataFrame з балами
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            if program in style_scores:
                scores_df.at[index, 'style_score'] = style_scores[program]
    
    # Крок 4: Фільтруємо готелі за метою у обраних стилі, категорії та регіоні
    if purposes and len(purposes) > 0:
        purpose_filtered = filter_hotels_by_purpose(filtered_by_category, purposes)
        purpose_counts_dict = {}
        
        for program in loyalty_programs:
            purpose_mask = purpose_filtered['loyalty_program'] == program
            purpose_counts_dict[program] = purpose_mask.sum()
            
            # Записуємо кількість готелів за метою
            scores_df.loc[scores_df['loyalty_program'] == program, 'purpose_hotels'] = purpose_mask.sum()
        
        # Розподіляємо бали за метою (21, 18, 15, 12, 9, 6, 3)
        purpose_scores = {}
        ranked_programs = sorted(purpose_counts_dict.items(), key=lambda x: x[1], reverse=True)
        
        # Бали за рейтингом
        score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
        for i, (program, _) in enumerate(ranked_programs):
            if i < len(score_values):
                purpose_scores[program] = score_values[i]
            else:
                purpose_scores[program] = 0.0
        
        # Нормалізуємо бали, якщо обрано кілька цілей
        if len(purposes) > 1:
            for program in purpose_scores:
                purpose_scores[program] /= len(purposes)
        
        # Оновлюємо DataFrame з балами
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            if program in purpose_scores:
                scores_df.at[index, 'purpose_score'] = purpose_scores[program]
    
    # Додаткове логування для стилів
    if styles and len(styles) > 0:
        logger.info(f"Фільтрація за стилями: {styles}")
        # Вивести кількість готелів для кожного стилю
        for style in styles:
            style_hotels_count = 0
            for program in loyalty_programs:
                program_data = style_filtered[style_filtered['loyalty_program'] == program]
                style_hotels_count += len(program_data)
            logger.info(f"Загальна кількість готелів для стилю '{style}': {style_hotels_count}")
            
            # Перевірити, чи є готелі цього стилю для кожної програми лояльності
            for program in loyalty_programs:
                program_data = style_filtered[style_filtered['loyalty_program'] == program]
                logger.info(f"Програма '{program}' - {len(program_data)} готелів для стилю '{style}'")
    
    # Обчислюємо загальний рейтинг
    scores_df['total_score'] = (
        scores_df['region_score'] + 
        scores_df['category_score'] + 
        scores_df['style_score'] + 
        scores_df['purpose_score']
    )
    
    # Сортуємо за загальним рейтингом
    scores_df = scores_df.sort_values('total_score', ascending=False)
    
    return scores_df

def get_detailed_analysis(user_data, hotel_data, scores_df):
    """
    Генерує детальний аналіз підрахунку балів
    
    Args:
        user_data: словник з відповідями користувача
        hotel_data: DataFrame з даними готелів
        scores_df: DataFrame з підрахованими балами
    
    Returns:
        str: детальний аналіз у текстовому форматі
    """
    analysis = "<detailed_analysis>\n"
    
    # Додаємо узагальнення відповідей користувача
    analysis += "User responses summary:\n"
    if user_data.get('regions'):
        analysis += f"- Selected regions: {', '.join(user_data['regions'])}\n"
    if user_data.get('countries'):
        analysis += f"- Selected countries: {', '.join(user_data['countries'])}\n"
    if user_data.get('category'):
        analysis += f"- Selected hotel category: {user_data['category']}\n"
    if user_data.get('styles'):
        analysis += f"- Selected hotel styles: {', '.join(user_data['styles'])}\n"
    if user_data.get('purposes'):
        analysis += f"- Selected travel purposes: {', '.join(user_data['purposes'])}\n"
    
    analysis += "\nLoyalty program scores calculation:\n"
    
    # Для кожної програми показуємо детальний розрахунок
    for index, row in scores_df.head(5).iterrows():
        program = row['loyalty_program']
        analysis += f"\n{program}:\n"
        analysis += f"- Region score: {row['region_score']:.2f} (hotels in region: {row['region_hotels']})\n"
        analysis += f"- Category score: {row['category_score']:.2f} (hotels in selected category: {row['category_hotels']})\n"
        analysis += f"- Style score: {row['style_score']:.2f} (hotels in selected style(s): {row['style_hotels']})\n"
        analysis += f"- Purpose score: {row['purpose_score']:.2f} (hotels for selected purpose(s): {row['purpose_hotels']})\n"
        analysis += f"- Total score: {row['total_score']:.2f}\n"
    
    analysis += "\nRanking of loyalty programs by total score:\n"
    for i, (index, row) in enumerate(scores_df.head(5).iterrows()):
        analysis += f"{i+1}. {row['loyalty_program']} - {row['total_score']:.2f} points\n"
    
    analysis += "</detailed_analysis>"
    
    return analysis

def format_results(user_data, scores_df, lang='en'):
    """
    Форматує результати для відображення користувачу
    
    Args:
        user_data: словник з відповідями користувача
        scores_df: DataFrame з підрахованими балами
        lang: мова відображення (uk або en)
    
    Returns:
        str: форматовані результати для відображення
    """
    results = ""
    
    # Беремо топ-5 програм або менше, якщо програм менше 5
    max_programs = min(5, len(scores_df))
    top_programs = scores_df.head(max_programs)
    
    for i, (index, row) in enumerate(top_programs.iterrows()):
        program = row['loyalty_program']
        
        # Видаляємо теги <result> та </result> з виведення
        # Початок блоку результату
        
        if lang == 'uk':
            results += f"{program} - посіла {i+1} місце з рейтингом {row['total_score']:.2f}\n"
            
            # Інформація про регіони/країни
            if user_data.get('regions'):
                region_str = ', '.join(user_data['regions'])
                results += f"1) у {region_str} - ({row['region_hotels']} готелів)\n"
            elif user_data.get('countries'):
                country_str = ', '.join(user_data['countries'])
                results += f"1) у {country_str} - ({row['region_hotels']} готелів)\n"
            
            # Інформація про категорію
            if user_data.get('category'):
                results += f"2) у сегменті {user_data['category']} ({row['category_hotels']} готелів)\n"
            
            # Інформація про стиль
            if user_data.get('styles'):
                style_str = ', '.join(user_data['styles'])
                results += f"3) у стилі {style_str} ({row['style_hotels']} готелів у цьому стилі/стилях та у суміжних)\n"
            
            # Інформація про мету
            if user_data.get('purposes'):
                purpose_str = ', '.join(user_data['purposes'])
                results += f"4) для {purpose_str} ({row['purpose_hotels']} готелів)\n"
        else:
            results += f"{program} - ranked {i+1} with a score of {row['total_score']:.2f}\n"
            
            # Інформація про регіони/країни
            if user_data.get('regions'):
                region_str = ', '.join(user_data['regions'])
                results += f"1) in {region_str} - ({row['region_hotels']} hotels)\n"
            elif user_data.get('countries'):
                country_str = ', '.join(user_data['countries'])
                results += f"1) in {country_str} - ({row['region_hotels']} hotels)\n"
            
            # Інформація про категорію
            if user_data.get('category'):
                results += f"2) in the {user_data['category']} segment ({row['category_hotels']} hotels)\n"
            
            # Інформація про стиль
            if user_data.get('styles'):
                style_str = ', '.join(user_data['styles'])
                results += f"3) in the {style_str} style ({row['style_hotels']} hotels in this style(s) and adjacent ones)\n"
            
            # Інформація про мету
            if user_data.get('purposes'):
                purpose_str = ', '.join(user_data['purposes'])
                results += f"4) for {purpose_str} ({row['purpose_hotels']} hotels)\n"
        
        # Додаємо пустий рядок між результатами
        results += "\n"
    
    return results

async def calculate_and_show_results(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обчислює результати та відображає їх користувачеві
    
    Args:
        update: об'єкт Update від Telegram
        context: контекст бота
    
    Returns:
        int: ідентифікатор кінцевого стану розмови
    """
    # Визначаємо, чи це відповідь на callback_query
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.effective_user.id
        chat_id = update.message.chat_id
    
    user_data = user_data_global[user_id]
    lang = user_data['language']
    
    # Виконуємо аналіз та підрахунок балів
    try:
        # Логуємо інформацію для відладки
        logger.info(f"Розрахунок балів для користувача {user_id}")
        logger.info(f"Дані користувача: {user_data}")
        
        # Перевіряємо, чи є дані готелів
        if hotel_data is None or hotel_data.empty:
            logger.error("Дані готелів відсутні або порожні!")
            
            if update.callback_query:
                if lang == 'uk':
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="На жаль, виникла проблема з даними готелів. Спробуйте пізніше."
                    )
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="Unfortunately, there is a problem with the hotel data. Please try again later."
                    )
            else:
                if lang == 'uk':
                    await update.message.reply_text(
                        "На жаль, виникла проблема з даними готелів. Спробуйте пізніше."
                    )
                else:
                    await update.message.reply_text(
                        "Unfortunately, there is a problem with the hotel data. Please try again later."
                    )
            
            return ConversationHandler.END
        
        # Підраховуємо бали для кожної програми лояльності
        scores_df = calculate_scores(user_data, hotel_data)
        
        # Перевіряємо, чи є результати
        if scores_df.empty:
            if update.callback_query:
                if lang == 'uk':
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="На жаль, не вдалося знайти програми лояльності, які відповідають вашим уподобанням. "
                        "Спробуйте змінити параметри пошуку, надіславши команду /start знову."
                    )
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="Unfortunately, I couldn't find any loyalty programs that match your preferences. "
                        "Try changing your search parameters by sending the /start command again."
                    )
            else:
                if lang == 'uk':
                    await update.message.reply_text(
                        "На жаль, не вдалося знайти програми лояльності, які відповідають вашим уподобанням. "
                        "Спробуйте змінити параметри пошуку, надіславши команду /start знову."
                    )
                else:
                    await update.message.reply_text(
                        "Unfortunately, I couldn't find any loyalty programs that match your preferences. "
                        "Try changing your search parameters by sending the /start command again."
                    )
            
            return ConversationHandler.END
        
        # Генеруємо детальний аналіз (не відображається користувачеві)
        detailed_analysis = get_detailed_analysis(user_data, hotel_data, scores_df)
        
        # Логування детального аналізу для розробників
        logger.info(f"Detailed analysis for user {user_id}: {detailed_analysis}")
        
        # Форматуємо результати для відображення
        results = format_results(user_data, scores_df, lang)
        
        # Відправляємо результати користувачеві
        if update.callback_query:
            if lang == 'uk':
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Аналіз завершено! Ось топ-5 програм лояльності готелів, які найкраще відповідають вашим уподобанням:\n\n" + 
                    results + 
                    "\nЩоб почати нове опитування, надішліть команду /start."
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Analysis completed! Here are the top 5 hotel loyalty programs that best match your preferences:\n\n" + 
                    results + 
                    "\nTo start a new survey, send the /start command."
                )
        else:
            if lang == 'uk':
                await update.message.reply_text(
                    "Аналіз завершено! Ось топ-5 програм лояльності готелів, які найкраще відповідають вашим уподобанням:\n\n" + 
                    results + 
                    "\nЩоб почати нове опитування, надішліть команду /start."
                )
            else:
                await update.message.reply_text(
                    "Analysis completed! Here are the top 5 hotel loyalty programs that best match your preferences:\n\n" + 
                    results + 
                    "\nTo start a new survey, send the /start command."
                )
    
    except Exception as e:
        logger.error(f"Помилка при обчисленні результатів: {e}")
        
        if update.callback_query:
            if lang == 'uk':
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Виникла помилка при аналізі ваших відповідей. Будь ласка, спробуйте знову, надіславши команду /start."
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="An error occurred while analyzing your answers. Please try again by sending the /start command."
                )
        else:
            if lang == 'uk':
                await update.message.reply_text(
                    "Виникла помилка при аналізі ваших відповідей. Будь ласка, спробуйте знову, надіславши команду /start."
                )
            else:
                await update.message.reply_text(
                    "An error occurred while analyzing your answers. Please try again by sending the /start command."
                )
    
    # Видаляємо дані користувача
    if user_id in user_data_global:
        del user_data_global[user_id]
    
    return ConversationHandler.END

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє помилки, що виникають під час роботи бота"""
    logger.error(f"Виникла помилка: {context.error}")
    
    # Отримуємо інформацію про користувача
    user_id = None
    if update and update.effective_user:
        user_id = update.effective_user.id
    
    # Логуємо детальну інформацію про помилку
    logger.error(f"Помилка для користувача {user_id}: {context.error}")
    
    # Спроба надіслати повідомлення користувачеві
    if update and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Виникла помилка під час обробки запиту. Спробуйте використати команду /start, щоб почати знову."
        )
    
    # Перевіряємо, чи це помилка через перервану розмову
    if "conversation" in str(context.error).lower():
        # Очищаємо дані користувача, якщо помилка пов'язана з розмовою
        if user_id and user_id in user_data_global:
            del user_data_global[user_id]
            logger.info(f"Очищено дані користувача {user_id} через помилку розмови")

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
            WAITING_PURPOSE_SUBMIT: [CallbackQueryHandler(purpose_choice)],
            ConversationHandler.END: [CommandHandler("start", start)]  # Додано обробник для END стану
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", start)  # /start як fallback
        ],
        name="loyalty_programs_conversation",  # Додано ім'я для кращого логування
        persistent=False  # Переконуємось, що дані не зберігаються між перезапусками
    )
    
    # Додаємо основний обробник розмови
    application.add_handler(conv_handler)
    
    # Додаємо окремий обробник для команди /start з вищим пріоритетом (група 1)
    application.add_handler(CommandHandler("start", start), group=1)
    
    # Додаємо обробник помилок для кращої діагностики
    application.add_error_handler(error_handler)
    
    # Використання PORT для webhook
    port = int(os.environ.get("PORT", "10000"))
    
    # Логуємо готовність бота до запуску
    logger.info("Бот успішно налаштований і готовий до запуску")
    
    # Запуск бота у відповідному режимі
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

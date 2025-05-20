import logging
import pandas as pd
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
import os  # Додано для кращої роботи з шляхами до файлів
# Після існуючих імпортів
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
LANGUAGE, REGION, COUNTRY, CATEGORY, STYLE, PURPOSE = range(6)  # Додано COUNTRY як окремий стан

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
    # Видаляємо перевірку на category, оскільки повинен бути segment
    
    # Перевірка пропущених значень
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"Пропущені значення: {null_counts[null_counts > 0]}")
    
    # Перевірка типів даних
    logger.info(f"Типи даних: {df.dtypes}")

# Функція для завантаження CSV
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
        # Видаляємо створення колонки 'category' як копії 'segment'
        
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
    """Початкова функція при команді /start"""
    user_id = update.effective_user.id
    user_data_global[user_id] = {}
    
    # Клавіатура для вибору мови
    keyboard = [
        ["Українська (Ukrainian)"],
        ["English (Англійська)"],
        ["Other (Інша)"]
    ]
    
    await update.message.reply_text(
        "Please select your preferred language for our conversation "
        "(будь ласка, оберіть мову, якою вам зручніше спілкуватися):",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    )
    
    return LANGUAGE

# Функція обробки вибору мови
async def language_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір мови користувачем"""
    user_id = update.effective_user.id
    text = update.message.text
    
    if "Українська" in text:
        user_data_global[user_id]['language'] = 'uk'
        await update.message.reply_text(
            "Дякую! Я продовжу спілкування українською мовою.",
            reply_markup=ReplyKeyboardRemove()
        )
        return await ask_region(update, context)
    
    elif "English" in text:
        user_data_global[user_id]['language'] = 'en'
        await update.message.reply_text(
            "Thank you! I will continue our conversation in English.",
            reply_markup=ReplyKeyboardRemove()
        )
        return await ask_region(update, context)
    
    else:
        user_data_global[user_id]['language'] = 'en'  # За замовчуванням - англійська
        await update.message.reply_text(
            "I'll continue in English. If you need another language, please let me know.",
            reply_markup=ReplyKeyboardRemove()
        )
        return await ask_region(update, context)

# Функція завершення розмови
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Скасовує розмову за командою /cancel"""
    user = update.message.from_user
    logger.info(f"Користувач {user.id} скасував розмову.")
    
    lang = user_data_global.get(user.id, {}).get('language', 'en')
    
    if lang == 'uk':
        await update.message.reply_text(
            "Розмову завершено. Щоб почати знову, надішліть команду /start.",
            reply_markup=ReplyKeyboardRemove()
        )
    else:
        await update.message.reply_text(
            "Conversation ended. To start again, send the /start command.",
            reply_markup=ReplyKeyboardRemove()
        )
    
    if user.id in user_data_global:
        del user_data_global[user.id]
    
    return ConversationHandler.END

# Функції для регіону та країни
async def ask_region(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про регіони подорожей"""
    user_id = update.effective_user.id
    lang = user_data_global[user_id]['language']
    
    keyboard = []
    
    if lang == 'uk':
        keyboard = [
            ["Європа", "Північна Америка", "Азія"],
            ["Близький Схід", "Африка", "Південна Америка"],
            ["Карибський басейн", "Океанія"],
            ["Мене цікавлять лише конкретні країни"]
        ]
        
        await update.message.reply_text(
            "Питання 1/4:\nУ яких регіонах світу ви плануєте подорожувати?\n"
            "(Оберіть один або кілька варіантів, для вибору кількох варіантів, надішліть їх через кому.)",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    else:
        keyboard = [
            ["Europe", "North America", "Asia"],
            ["Middle East", "Africa", "South America"],
            ["Caribbean", "Oceania"],
            ["I'm only interested in specific countries"]
        ]
        
        await update.message.reply_text(
            "Question 1/4:\nIn which regions of the world are you planning to travel?\n"
            "(Select one or multiple options. For multiple options, send them separated by commas.)",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    
    return REGION

async def region_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє відповідь про регіони"""
    user_id = update.effective_user.id
    text = update.message.text
    lang = user_data_global[user_id]['language']
    
    # Перевіряємо, чи користувач хоче вказати конкретні країни
    if ("конкретні країни" in text.lower()) or ("specific countries" in text.lower()):
        if lang == 'uk':
            await update.message.reply_text(
                "Будь ласка, введіть назви країн, які вас цікавлять (через кому):",
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await update.message.reply_text(
                "Please enter the names of the countries you are interested in (separated by commas):",
                reply_markup=ReplyKeyboardRemove()
            )
        
        # Переходимо до стану COUNTRY замість встановлення флагу
        return COUNTRY
    
    # Обробка звичайного вибору регіонів
    regions = []
    
    # Перевіряємо на множинний вибір (якщо текст містить кому)
    if "," in text:
        regions = [region.strip() for region in text.split(",")]
    else:
        regions = [text.strip()]  # Додаємо один регіон, видаляючи зайві пробіли
    
    # Зберігаємо вибрані регіони
    user_data_global[user_id]['regions'] = regions
    user_data_global[user_id]['countries'] = None
    
    if lang == 'uk':
        await update.message.reply_text(
            f"Дякую! Ви обрали наступні регіони: {', '.join(regions)}.\n"
            "Переходимо до наступного питання."
        )
    else:
        await update.message.reply_text(
            f"Thank you! You have chosen the following regions: {', '.join(regions)}.\n"
            "Moving on to the next question."
        )
    
    return await ask_category(update, context)

async def country_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє введення конкретних країн - тепер це окремий стан"""
    user_id = update.effective_user.id
    text = update.message.text
    lang = user_data_global[user_id]['language']
    
    # Обробка введення країн
    countries = [country.strip() for country in text.split(",")]
    
    # Зберігаємо вибрані країни
    user_data_global[user_id]['regions'] = None
    user_data_global[user_id]['countries'] = countries
    
    if lang == 'uk':
        await update.message.reply_text(
            f"Дякую! Ви обрали наступні країни: {', '.join(countries)}.\n"
            "Переходимо до наступного питання."
        )
    else:
        await update.message.reply_text(
            f"Thank you! You have chosen the following countries: {', '.join(countries)}.\n"
            "Moving on to the next question."
        )
    
    return await ask_category(update, context)

# Функції для категорії та стилю готелів
async def ask_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про категорію готелів"""
    user_id = update.effective_user.id
    lang = user_data_global[user_id]['language']
    
    keyboard = []
    
    if lang == 'uk':
        keyboard = [
            ["Luxury (преміум-клас)"],
            ["Comfort (середній клас)"],
            ["Standard (економ-клас)"]  # Виправлено Standart на Standard
        ]
        
        await update.message.reply_text(
            "Питання 2/4:\nЯку категорію готелів ви зазвичай обираєте?",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    else:
        keyboard = [
            ["Luxury (premium class)"],
            ["Comfort (middle class)"],
            ["Standard (economy class)"]  # Виправлено Standart на Standard
        ]
        
        await update.message.reply_text(
            "Question 2/4:\nWhich hotel category do you usually choose?",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    
    return CATEGORY

async def category_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір категорії готелю"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    lang = user_data_global[user_id]['language']
    
    # Визначення обраної категорії
    category = None
    if "Luxury" in text:
        category = "Luxury"
    elif "Comfort" in text:
        category = "Comfort"
    elif "Standard" in text or "Standart" in text:  # Обробляємо обидва варіанти
        category = "Standard"  # Але зберігаємо уніфіковано як "Standard"
    
    # Зберігаємо вибрану категорію
    user_data_global[user_id]['category'] = category
    
    if lang == 'uk':
        await update.message.reply_text(
            f"Дякую! Ви обрали категорію: {category}.\n"
            "Переходимо до наступного питання."
        )
    else:
        await update.message.reply_text(
            f"Thank you! You have chosen the category: {category}.\n"
            "Moving on to the next question."
        )
    
    return await ask_style(update, context)

async def ask_style(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про стиль готелю"""
    user_id = update.effective_user.id
    lang = user_data_global[user_id]['language']
    category = user_data_global[user_id]['category']
    
    keyboard = []
    
    # Визначаємо варіанти стилів відповідно до обраної категорії
    if category == "Luxury":
        if lang == 'uk':
            keyboard = [
                ["Розкішний та вишуканий (преміум-матеріали, елегантний дизайн, високий рівень сервісу)"],
                ["Бутік та унікальний (оригінальний інтер'єр, творча атмосфера, відчуття ексклюзивності)"],
                ["Класичний та традиційний (перевірений часом стиль, консервативність, історичність)"],
                ["Сучасний та дизайнерський (модні інтер'єри, мінімалізм, технологічність)"]
            ]
        else:
            keyboard = [
                ["Luxurious and refined (premium materials, elegant design, high level of service)"],
                ["Boutique and unique (original interior, creative atmosphere, sense of exclusivity)"],
                ["Classic and traditional (time-tested style, conservatism, historical ambiance)"],
                ["Modern and designer (fashionable interiors, minimalism, technological features)"]
            ]
    elif category == "Comfort":
        if lang == 'uk':
            keyboard = [
                ["Затишний та сімейний (тепла атмосфера, комфорт, дружній до дітей)"],
                ["Класичний та традиційний (перевірений часом стиль, консервативність, історичність)"],
                ["Сучасний та дизайнерський (модні інтер'єри, мінімалізм, технологічність)"]
            ]
        else:
            keyboard = [
                ["Cozy and family-friendly (warm atmosphere, comfort, child-friendly)"],
                ["Classic and traditional (time-tested style, conservatism, historical ambiance)"],
                ["Modern and designer (fashionable interiors, minimalism, technological features)"]
            ]
    else:  # Standard
        if lang == 'uk':
            keyboard = [
                ["Практичний та економічний (без зайвих деталей, функціональний, доступний)"],
                ["Затишний та сімейний (тепла атмосфера, комфорт, дружній до дітей)"]
            ]
        else:
            keyboard = [
                ["Practical and economical (no unnecessary details, functional, affordable)"],
                ["Cozy and family-friendly (warm atmosphere, comfort, child-friendly)"]
            ]
    
    # Додаємо чітку інструкцію про вибір кількох варіантів
    if lang == 'uk':
        await update.message.reply_text(
            "Питання 3/4:\nЯкий стиль готелю ви зазвичай обираєте?\n"
            "(Виберіть до трьох варіантів. Для вибору кількох варіантів, надішліть їх через кому.)",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    else:
        await update.message.reply_text(
            "Question 3/4:\nWhat hotel style do you usually choose?\n"
            "(Choose up to three options. For multiple choices, send them separated by commas.)",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    
    return STYLE

async def style_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір стилю готелю"""
    user_id = update.effective_user.id
    text = update.message.text
    lang = user_data_global[user_id]['language']
    
    # Обробка вибору стилів
    styles = []
    
    # Перевіряємо на множинний вибір (якщо текст містить кому)
    if "," in text:
        styles = [style.strip() for style in text.split(",")]
    else:
        styles = [text.strip()]  # Один стиль
    
    # Обмеження до трьох варіантів з повідомленням
    original_count = len(styles)
    if len(styles) > 3:
        styles = styles[:3]
        
        # Повідомляємо користувача про обмеження
        if lang == 'uk':
            await update.message.reply_text(
                f"Ви обрали {original_count} стилів, але дозволено максимум 3. "
                f"Я врахую тільки перші три: {', '.join(styles)}."
            )
        else:
            await update.message.reply_text(
                f"You selected {original_count} styles, but a maximum of 3 is allowed. "
                f"I will only consider the first three: {', '.join(styles)}."
            )
    
    # Зберігаємо вибрані стилі
    user_data_global[user_id]['styles'] = styles
    
    # Створюємо спрощений список стилів для відображення (без описів у дужках)
    display_styles = []
    for style in styles:
        # Виділяємо основну частину стилю без описів у дужках
        if "(" in style:
            display_style = style.split("(")[0].strip()
            display_styles.append(display_style)
        else:
            display_styles.append(style)
    
    if lang == 'uk':
        await update.message.reply_text(
            f"Дякую! Ви обрали наступні стилі: {', '.join(display_styles)}.\n"
            "Переходимо до наступного питання."
        )
    else:
        await update.message.reply_text(
            f"Thank you! You have chosen the following styles: {', '.join(display_styles)}.\n"
            "Moving on to the next question."
        )
    
    return await ask_purpose(update, context)

# Функції для мети подорожі та фільтрації даних
async def ask_purpose(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про мету подорожі"""
    user_id = update.effective_user.id
    lang = user_data_global[user_id]['language']
    
    keyboard = []
    
    if lang == 'uk':
        keyboard = [
            ["Бізнес-подорожі / відрядження"],
            ["Відпустка / релакс"],
            ["Сімейний відпочинок"],
            ["Довготривале проживання"]
        ]
        
        await update.message.reply_text(
            "Питання 4/4:\nЗ якою метою ви зазвичай зупиняєтесь у готелі?\n"
            "(Оберіть до двох варіантів. Для вибору кількох варіантів, надішліть їх через кому, наприклад: \"Бізнес-подорожі / відрядження, Сімейний відпочинок\")",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    else:
        keyboard = [
            ["Business travel"],
            ["Vacation / relaxation"],
            ["Family vacation"],
            ["Long-term stay"]
        ]
        
        await update.message.reply_text(
            "Question 4/4:\nFor what purpose do you usually stay at a hotel?\n"
            "(Choose up to two options. For multiple choices, send them separated by commas, for example: \"Business travel, Family vacation\")",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    
    return PURPOSE

async def purpose_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір мети подорожі"""
    user_id = update.effective_user.id
    text = update.message.text
    lang = user_data_global[user_id]['language']
    
    # Обробка вибору мети
    purposes = []
    
    # Перевіряємо на множинний вибір (якщо текст містить кому)
    if "," in text:
        purposes = [purpose.strip() for purpose in text.split(",")]
    else:
        purposes = [text.strip()]  # Один варіант
    
    # Обмеження до двох варіантів з повідомленням
    original_count = len(purposes)
    if len(purposes) > 2:
        purposes = purposes[:2]
        
        # Повідомляємо користувача про обмеження
        if lang == 'uk':
            await update.message.reply_text(
                f"Ви обрали {original_count} цілей, але дозволено максимум 2. "
                f"Я врахую тільки перші дві: {', '.join(purposes)}."
            )
        else:
            await update.message.reply_text(
                f"You selected {original_count} purposes, but a maximum of 2 is allowed. "
                f"I will only consider the first two: {', '.join(purposes)}."
            )
    
    # Зберігаємо вибрані мети
    user_data_global[user_id]['purposes'] = purposes
    
    if lang == 'uk':
        await update.message.reply_text(
            f"Дякую! Ви обрали наступні мети: {', '.join(purposes)}.\n"
            "Зачекайте, будь ласка, поки я проаналізую ваші відповіді та підберу найкращі програми лояльності для вас."
        )
    else:
        await update.message.reply_text(
            f"Thank you! You have chosen the following purposes: {', '.join(purposes)}.\n"
            "Please wait while I analyze your answers and select the best loyalty programs for you."
        )
    
    # Обчислюємо та відображаємо результати
    return await calculate_and_show_results(update, context)

# Виправлені функції для роботи з даними
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
    
    style_mapping = {
        "Розкішний та вишуканий (преміум-матеріали, елегантний дизайн, високий рівень сервісу)": ["JW Marriott", "The Ritz-Carlton", "Conrad Hotels & Resorts", 
                                 "Waldorf Astoria Hotels & Resorts", "InterContinental Hotels & Resorts", 
                                 "Fairmont Hotels", "Park Hyatt Hotels"],
        
        "Бутік та унікальний (оригінальний інтер'єр, творча атмосфера, відчуття ексклюзивності)": ["Kimpton Hotels & Restaurants", "Registry Collection Hotels", 
                              "Ascend Hotel Collection", "Alila Hotels"],
        
        "Класичний та традиційний (перевірений часом стиль, консервативність, історичність)": ["The Ritz-Carlton", "Marriott Hotels", "Sheraton", 
                                  "Waldorf Astoria Hotels & Resorts", "Fairmont"],
        
        "Сучасний та дизайнерський (модні інтер'єри, мінімалізм, технологічність)": ["Conrad Hotels & Resorts", "Kimpton Hotels & Restaurants", 
                                   "Wyndham Grand", "Novotel Hotels", "Cambria Hotels"],
        
        "Затишний та сімейний (тепла атмосфера, комфорт, дружній до дітей)": ["DoubleTree by Hilton", "Holiday Inn Hotels & Resorts", 
                              "Mercure Hotels", "Novotel Hotels", "Comfort Inn Hotels", "Hyatt House"],
        
        "Практичний та економічний (без зайвих деталей, функціональний, доступний)": ["Fairfield Inn & Suites", "Courtyard by Marriott", 
                                 "Hampton by Hilton", "Holiday Inn Express", "Ibis Hotels", 
                                 "Super 8 by Wyndham", "Days Inn by Wyndham"]
    }
    
    # Додаємо альтернативні ключі без описів для сумісності зі старими записами
    simplified_mapping = {
        "Розкішний та вишуканий": style_mapping["Розкішний та вишуканий (преміум-матеріали, елегантний дизайн, високий рівень сервісу)"],
        "Бутік та унікальний": style_mapping["Бутік та унікальний (оригінальний інтер'єр, творча атмосфера, відчуття ексклюзивності)"],
        "Класичний та традиційний": style_mapping["Класичний та традиційний (перевірений часом стиль, консервативність, історичність)"],
        "Сучасний та дизайнерський": style_mapping["Сучасний та дизайнерський (модні інтер'єри, мінімалізм, технологічність)"],
        "Затишний та сімейний": style_mapping["Затишний та сімейний (тепла атмосфера, комфорт, дружній до дітей)"],
        "Практичний та економний": style_mapping["Практичний та економічний (без зайвих деталей, функціональний, доступний)"]
    }
    
    # Об'єднуємо обидва словники для обробки як повних, так і скорочених варіантів
    style_mapping.update(simplified_mapping)
    
    # Переклад для англійської мови
    style_mapping_en = {
        "Luxurious and refined (premium materials, elegant design, high level of service)": style_mapping["Розкішний та вишуканий (преміум-матеріали, елегантний дизайн, високий рівень сервісу)"],
        "Boutique and unique (original interior, creative atmosphere, sense of exclusivity)": style_mapping["Бутік та унікальний (оригінальний інтер'єр, творча атмосфера, відчуття ексклюзивності)"],
        "Classic and traditional (time-tested style, conservatism, historical ambiance)": style_mapping["Класичний та традиційний (перевірений часом стиль, консервативність, історичність)"],
        "Modern and designer (fashionable interiors, minimalism, technological features)": style_mapping["Сучасний та дизайнерський (модні інтер'єри, мінімалізм, технологічність)"],
        "Cozy and family-friendly (warm atmosphere, comfort, child-friendly)": style_mapping["Затишний та сімейний (тепла атмосфера, комфорт, дружній до дітей)"],
        "Practical and economical (no unnecessary details, functional, affordable)": style_mapping["Практичний та економічний (без зайвих деталей, функціональний, доступний)"]
    }
    
    # Додаємо короткі англійські варіанти для сумісності
    en_simplified_mapping = {
        "Luxurious and refined": style_mapping_en["Luxurious and refined (premium materials, elegant design, high level of service)"],
        "Boutique and unique": style_mapping_en["Boutique and unique (original interior, creative atmosphere, sense of exclusivity)"],
        "Classic and traditional": style_mapping_en["Classic and traditional (time-tested style, conservatism, historical ambiance)"],
        "Modern and designer": style_mapping_en["Modern and designer (fashionable interiors, minimalism, technological features)"],
        "Cozy and family-friendly": style_mapping_en["Cozy and family-friendly (warm atmosphere, comfort, child-friendly)"],
        "Practical and economical": style_mapping_en["Practical and economical (no unnecessary details, functional, affordable)"]
    }
    
    # Об'єднуємо всі словники
    combined_mapping = {**style_mapping, **style_mapping_en, **en_simplified_mapping}
    
    result = {}
    for style, brands in combined_mapping.items():
        # Перевіряємо, чи бренд відповідає одному зі списку
        result[style] = any(b.lower() in hotel_brand.lower() for b in brands)
    
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
        # Перевіряємо, чи бренд відповідає одному зі списку
        result[purpose] = any(b.lower() in hotel_brand.lower() for b in brands)
    
    return result

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
    
    # Створюємо маску для фільтрації
    style_mask = pd.Series(False, index=df.index)
    
    for idx, row in df.iterrows():
        # Використовуємо колонку 'Hotel Brand'
        if 'Hotel Brand' in row:
            hotel_styles = map_hotel_style(row['Hotel Brand'])
            # Перевіряємо, чи готель відповідає хоча б одному з обраних стилів
            if any(hotel_styles.get(style, False) for style in styles):
                style_mask.loc[idx] = True
    
    return df[style_mask]

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
    
    # Створюємо маску для фільтрації
    purpose_mask = pd.Series(False, index=df.index)
    
    for idx, row in df.iterrows():
        # Використовуємо колонку 'Hotel Brand'
        if 'Hotel Brand' in row:
            hotel_purposes = map_hotel_purpose(row['Hotel Brand'])
            # Перевіряємо, чи готель відповідає хоча б одній з обраних цілей
            if any(hotel_purposes.get(purpose, False) for purpose in purposes):
                purpose_mask.loc[idx] = True
    
    return df[purpose_mask]

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
        
        results += "<result>\n"
        
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
        
        results += "</result>\n\n"
    
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
    user_id = update.effective_user.id
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
            if lang == 'uk':
                await update.message.reply_text(
                    "На жаль, виникла проблема з даними готелів. Спробуйте пізніше.",
                    reply_markup=ReplyKeyboardRemove()
                )
            else:
                await update.message.reply_text(
                    "Unfortunately, there is a problem with the hotel data. Please try again later.",
                    reply_markup=ReplyKeyboardRemove()
                )
            return ConversationHandler.END
        
        # Підраховуємо бали для кожної програми лояльності
        scores_df = calculate_scores(user_data, hotel_data)
        
        # Перевіряємо, чи є результати
        if scores_df.empty:
            if lang == 'uk':
                await update.message.reply_text(
                    "На жаль, не вдалося знайти програми лояльності, які відповідають вашим уподобанням. "
                    "Спробуйте змінити параметри пошуку, надіславши команду /start знову.",
                    reply_markup=ReplyKeyboardRemove()
                )
            else:
                await update.message.reply_text(
                    "Unfortunately, I couldn't find any loyalty programs that match your preferences. "
                    "Try changing your search parameters by sending the /start command again.",
                    reply_markup=ReplyKeyboardRemove()
                )
            
            return ConversationHandler.END
        
        # Генеруємо детальний аналіз (не відображається користувачеві)
        detailed_analysis = get_detailed_analysis(user_data, hotel_data, scores_df)
        
        # Логування детального аналізу для розробників
        logger.info(f"Detailed analysis for user {user_id}: {detailed_analysis}")
        
        # Форматуємо результати для відображення
        results = format_results(user_data, scores_df, lang)
        
        # Відправляємо результати користувачеві
        if lang == 'uk':
            await update.message.reply_text(
                "Аналіз завершено! Ось топ-5 програм лояльності готелів, які найкраще відповідають вашим уподобанням:\n\n" + 
                results + 
                "\nЩоб почати нове опитування, надішліть команду /start.",
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await update.message.reply_text(
                "Analysis completed! Here are the top 5 hotel loyalty programs that best match your preferences:\n\n" + 
                results + 
                "\nTo start a new survey, send the /start command.",
                reply_markup=ReplyKeyboardRemove()
            )
    
    except Exception as e:
        logger.error(f"Помилка при обчисленні результатів: {e}")
        
        if lang == 'uk':
            await update.message.reply_text(
                "Виникла помилка при аналізі ваших відповідей. Будь ласка, спробуйте знову, надіславши команду /start.",
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await update.message.reply_text(
                "An error occurred while analyzing your answers. Please try again by sending the /start command.",
                reply_markup=ReplyKeyboardRemove()
            )
    
    # Видаляємо дані користувача
    if user_id in user_data_global:
        del user_data_global[user_id]
    
    return ConversationHandler.END

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
            LANGUAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, language_choice)],
            REGION: [MessageHandler(filters.TEXT & ~filters.COMMAND, region_choice)],
            COUNTRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, country_choice)],
            CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, category_choice)],
            STYLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, style_choice)],
            PURPOSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, purpose_choice)]
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    
    application.add_handler(conv_handler)
    
...


    # ✅ Використання PORT всередині main
    if webhook_url and webhook_path:
        webhook_info = f"{webhook_url}{webhook_path}"
        logger.info(f"Запуск бота в режимі webhook на {webhook_info}")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
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
WEBHOOK_URL = f"https://{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None

# Перевіряємо наявність токена
if TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
    logger.warning("Токен бота не налаштовано! Встановіть змінну середовища TELEGRAM_BOT_TOKEN або змініть значення в коді.")
    
    # Запускаємо бота з підтримкою webhook або polling
    main(TOKEN, CSV_PATH, WEBHOOK_URL, 10000, WEBHOOK_PATH)
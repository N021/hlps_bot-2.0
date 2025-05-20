import logging
import pandas as pd
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
import os
from telegram.ext import ApplicationBuilder
import ssl
from aiohttp import web
from difflib import get_close_matches
import Levenshtein  # Потрібно встановити: pip install python-Levenshtein
PORT = int(os.environ.get("PORT", "10000"))

# Налаштування логування
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Оновлені стани розмови
LANGUAGE, REGION, COUNTRY, REGION_SELECTION, CATEGORY, STYLE, PURPOSE = range(7)

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

# Словники для перекладу регіонів та країн
region_translation = {
    "Європа": "Europe",
    "Північна Америка": "North America", 
    "Азія": "Asia",
    "Близький Схід": "Middle East",
    "Африка": "Africa",
    "Південна Америка": "South America",
    "Карибський басейн": "Caribbean",
    "Океанія": "Oceania"
}

# Словник для перетворення назв країн (укр -> англ)
country_translation = {
    # Країни з кількома варіантами
    "США": "United States",
    "Сполучені Штати": "United States",
    "Сполучені Штати Америки": "United States",
    "Америка": "United States",
    "USA": "United States",
    
    "Великобританія": "United Kingdom",
    "Сполучене Королівство": "United Kingdom",
    "Англія": "United Kingdom",
    "Британія": "United Kingdom",
    "UK": "United Kingdom",
    
    "Нідерланди": "Netherlands",
    "Голландія": "Netherlands",
    
    "ОАЕ": "United Arab Emirates",
    "Об'єднані Арабські Емірати": "United Arab Emirates",
    "Емірати": "United Arab Emirates",
    
    "Китай": "China",
    "КНР": "China",
    
    "Чехія": "Czech Republic",
    "Чеська Республіка": "Czech Republic",
    
    "Корея": "South Korea",
    "Південна Корея": "South Korea",
    
    "Домінікана": "Dominican Republic",
    
    # Стандартні переклади
    "Албанія": "Albania",
    "Алжир": "Algeria",
    "Андорра": "Andorra",
    "Ангола": "Angola",
    "Аргентина": "Argentina",
    "Вірменія": "Armenia",
    "Аруба": "Aruba",
    "Австралія": "Australia",
    "Австрія": "Austria",
    "Азербайджан": "Azerbaijan",
    "Багамські острови": "Bahamas",
    "Багами": "Bahamas",
    "Бахрейн": "Bahrain",
    "Бангладеш": "Bangladesh",
    "Барбадос": "Barbados",
    "Білорусь": "Belarus",
    "Бельгія": "Belgium",
    "Бенін": "Benin",
    "Бермуди": "Bermuda",
    "Богота": "Bogota",
    "Болівія": "Bolivia",
    "Боснія і Герцеговина": "Bosnia Herzegovina",
    "Боснія": "Bosnia Herzegovina",
    "Ботсвана": "Botswana",
    "Бразилія": "Brazil",
    "Британські Віргінські острови": "British Virgin Islands",
    "Болгарія": "Bulgaria",
    "Камбоджа": "Cambodia",
    "Камерун": "Cameroon",
    "Канада": "Canada",
    "Кабо-Верде": "Cape Verde",
    "Кайманові острови": "Cayman Islands",
    "Чилі": "Chile",
    "Колумбія": "Colombia",
    "Конго - Браззавіль": "Congo - Brazzaville",
    "Конго - Кіншаса": "Congo - Kinshasa",
    "Коста-Ріка": "Costa Rica",
    "Кот-д'Івуар": "Côte d'Ivoire",
    "Хорватія": "Croatia",
    "Кюрасао": "Curacao",
    "Кіпр": "Cyprus",
    "Чеська Республіка": "Czech Republic",
    "Данія": "Denmark",
    "Джибуті": "Djibouti",
    "Домініка": "Dominica",
    "Домініканська Республіка": "Dominican Republic",
    "Еквадор": "Ecuador",
    "Єгипет": "Egypt",
    "Сальвадор": "El Salvador",
    "Екваторіальна Гвінея": "Equatorial Guinea",
    "Естонія": "Estonia",
    "Есватіні": "Eswatini",
    "Ефіопія": "Ethiopia",
    "Фарерські острови": "Faroe Islands",
    "Фіджі": "Fiji",
    "Фінляндія": "Finland",
    "Франція": "France",
    "Французька Гвіана": "French Guiana",
    "Французька Полінезія": "French Polynesia",
    "Грузія": "Georgia",
    "Німеччина": "Germany",
    "Гана": "Ghana",
    "Гібралтар": "Gibraltar",
    "Греція": "Greece",
    "Гуам": "Guam",
    "Гватемала": "Guatemala",
    "Гаяна": "Guyana",
    "Гаїті": "Haiti",
    "Гондурас": "Honduras",
    "Угорщина": "Hungary",
    "Ісландія": "Iceland",
    "Індія": "India",
    "Індонезія": "Indonesia",
    "Ірландія": "Ireland",
    "Ізраїль": "Israel",
    "Італія": "Italy",
    "Ямайка": "Jamaica",
    "Японія": "Japan",
    "Йорданія": "Jordan",
    "Казахстан": "Kazakhstan",
    "Кенія": "Kenya",
    "Кувейт": "Kuwait",
    "Киргизстан": "Kyrgyzstan",
    "Лаос": "Laos",
    "Латвія": "Latvia",
    "Ліван": "Lebanon",
    "Литва": "Lithuania",
    "Люксембург": "Luxembourg",
    "Макао": "Macao",
    "Македонія": "Macedonia",
    "Мадагаскар": "Madagascar",
    "Малайзія": "Malaysia",
    "Мальдіви": "Maldives",
    "Мальта": "Malta",
    "Маврикій": "Mauritius",
    "Майотта": "Mayotte",
    "Мексика": "Mexico",
    "Молдова": "Moldova",
    "Монако": "Monaco",
    "Монголія": "Mongolia",
    "Чорногорія": "Montenegro",
    "Марокко": "Morocco",
    "М'янма": "Myanmar",
    "Намібія": "Namibia",
    "Непал": "Nepal",
    "Нова Каледонія": "New Caledonia",
    "Нова Зеландія": "New Zealand",
    "Нікарагуа": "Nicaragua",
    "Нігерія": "Nigeria",
    "Північна Македонія": "North Macedonia",
    "Північні Маріанські острови": "Northern Mariana Islands",
    "Норвегія": "Norway",
    "Оман": "Oman",
    "Пакистан": "Pakistan",
    "Панама": "Panama",
    "Папуа-Нова Гвінея": "Papua New Guinea",
    "Парагвай": "Paraguay",
    "Перу": "Peru",
    "Філіппіни": "Philippines",
    "Польща": "Poland",
    "Португалія": "Portugal",
    "Пуерто-Ріко": "Puerto Rico",
    "Катар": "Qatar",
    "Румунія": "Romania",
    "Руанда": "Rwanda",
    "Сент-Кітс і Невіс": "Saint Kitts and Nevis",
    "Самоа": "Samoa",
    "Санта-Марта": "Santa Marta",
    "Саудівська Аравія": "Saudi Arabia",
    "Сенегал": "Senegal",
    "Сербія": "Serbia",
    "Сейшельські острови": "Seychelles",
    "Сейшели": "Seychelles",
    "Сінгапур": "Singapore",
    "Сінт-Мартен": "Sint Maarten",
    "Словаччина": "Slovakia",
    "Словенія": "Slovenia",
    "ПАР": "South Africa",
    "Південно-Африканська Республіка": "South Africa",
    "Іспанія": "Spain",
    "Шрі-Ланка": "Sri Lanka",
    "Суринам": "Suriname",
    "Швеція": "Sweden",
    "Швейцарія": "Switzerland",
    "Тайвань": "Taiwan",
    "Таджикистан": "Tajikistan",
    "Танзанія": "Tanzania",
    "Таїланд": "Thailand",
    "Тринідад і Тобаго": "Trinidad and Tobago",
    "Туніс": "Tunisia",
    "Туреччина": "Turkey",
    "Теркс і Кайкос": "Turks and Caicos Islands",
    "Уганда": "Uganda",
    "Україна": "Ukraine",
    "Уругвай": "Uruguay",
    "Узбекистан": "Uzbekistan",
    "Венесуела": "Venezuela",
    "В'єтнам": "Vietnam",
    "Замбія": "Zambia",
    "Зімбабве": "Zimbabwe",
}

# Перевірка на згадування країни-терориста
def is_russia_mentioned(text):
    """
    Перевіряє, чи згадується країна-терорист в тексті
    
    Args:
        text: текст для перевірки
        
    Returns:
        bool: True, якщо згадується, False - інакше
    """
    russia_keywords = ["росія", "россия", "russia"]  # Тільки 3 основні варіанти
    
    # Перетворюємо текст до нижнього регістру для порівняння
    text_lower = text.lower()
    
    # Перевіряємо, чи міститься якесь із ключових слів у тексті
    return any(keyword.lower() in text_lower for keyword in russia_keywords)

# Повідомлення про країну-терориста
def get_russia_message(lang='uk'):
    """
    Повертає повідомлення щодо країни-терориста відповідною мовою
    
    Args:
        lang: мова ('uk' або 'en')
        
    Returns:
        str: форматоване повідомлення
    """
    if lang == 'uk':
        return (
            "Ми не рекомендуємо відвідувати країну-терориста. "
            "В нашій базі даних принципово немає жодного готелю з цієї території.\n\n"
            "Будь ласка, оберіть будь-яку іншу країну для ваших подорожей. "
            "Є так багато чудових місць у світі, які варто відвідати!\n\n"
            "Слава Україні! Героям Слава! 🇺🇦"
        )
    else:
        return (
            "We strongly advise against visiting this terrorist state. "
            "Our database does not include any hotels from this territory.\n\n"
            "Please choose any other country for your journeys. "
            "There are so many wonderful places in the world worth visiting!\n\n"
            "Glory to Ukraine! Glory to the Heroes! 🇺🇦"
        )

# Функції для нечіткого пошуку країн
def find_closest_country(input_name, country_list, cutoff=0.75):
    """
    Знаходить найближчу за написанням країну у словнику.
    
    Args:
        input_name: введена назва країни
        country_list: список правильних назв країн
        cutoff: мінімальна схожість (від 0 до 1)
        
    Returns:
        Найближча правильна назва або None, якщо нічого не знайдено
    """
    matches = get_close_matches(input_name.lower(), [c.lower() for c in country_list], n=1, cutoff=cutoff)
    if matches:
        # Знаходимо оригінальну назву з правильним регістром
        for country in country_list:
            if country.lower() == matches[0]:
                return country
    return None

def find_by_levenshtein(input_name, country_dict, threshold=2):
    """
    Знаходить країну за відстанню Левенштейна.
    
    Args:
        input_name: введена назва країни
        country_dict: словник країн {укр_назва: англ_назва}
        threshold: максимальна допустима відстань
        
    Returns:
        Кортеж (правильна_укр_назва, англ_назва) або (None, None)
    """
    input_lower = input_name.lower()
    min_distance = float('inf')
    closest_match = None
    
    for ukr_name in country_dict.keys():
        distance = Levenshtein.distance(input_lower, ukr_name.lower())
        if distance < min_distance and distance <= threshold:
            min_distance = distance
            closest_match = ukr_name
    
    if closest_match:
        return (closest_match, country_dict[closest_match])
    return (None, None)

def find_closest_country_name(input_name, lang='uk'):
    """
    Знаходить найближчу назву країни з урахуванням можливих помилок написання.
    
    Args:
        input_name: введена назва країни
        lang: мова ('uk' або 'en')
        
    Returns:
        Нормалізована англійська назва країни або None
    """
    if not input_name or len(input_name) < 3:
        return None
        
    input_lower = input_name.lower()
    
    # 1. Спочатку шукаємо точні збіги
    for ukr_name, eng_name in country_translation.items():
        if input_lower == ukr_name.lower():
            return eng_name
    
    # 2. Шукаємо часткові збіги
    for ukr_name, eng_name in country_translation.items():
        if input_lower in ukr_name.lower() or ukr_name.lower() in input_lower:
            return eng_name
    
    # 3. Використовуємо нечітке порівняння
    ukr_country_names = list(country_translation.keys())
    closest = find_closest_country(input_name, ukr_country_names, cutoff=0.75)
    if closest:
        return country_translation[closest]
    
    # 4. Використовуємо відстань Левенштейна для близьких за написанням слів
    ukr_name, eng_name = find_by_levenshtein(input_name, country_translation, threshold=2)
    if ukr_name:
        logger.info(f"Знайдено країну за відстанню Левенштейна: '{input_name}' -> '{ukr_name}'")
        return eng_name
    
    # 5. Пошук за ключовими словами (окремими частинами назви)
    for word in input_lower.split():
        if len(word) > 3:
            for ukr_name, eng_name in country_translation.items():
                ukr_lower = ukr_name.lower()
                # Обчислюємо відстань Левенштейна для кожного слова
                distance = Levenshtein.distance(word, ukr_lower)
                if distance <= 1:  # Допускаємо одну помилку
                    logger.info(f"Знайдено країну за словом з відстанню Левенштейна: '{word}' -> '{ukr_name}'")
                    return eng_name
    
    # Не знайдено відповідності
    logger.warning(f"Не вдалося знайти відповідність для: '{input_name}'")
    return input_name

# Оновлений фільтр готелів за регіоном
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
        # Перетворюємо українські назви на англійські для пошуку
        search_regions = []
        for region in regions:
            if region in region_translation:
                search_regions.append(region_translation[region])
            else:
                search_regions.append(region)
        
        logger.info(f"Оригінальні регіони: {regions}")
        logger.info(f"Регіони для пошуку: {search_regions}")
        
        region_mask = filtered_df['region'].apply(
            lambda x: any(region.lower() in str(x).lower() for region in search_regions)
        )
        filtered_df = filtered_df[region_mask]
    
    # Фільтрація за країнами
    if countries and len(countries) > 0:
        logger.info(f"Країни для пошуку: {countries}")
        
        country_mask = filtered_df['country'].apply(
            lambda x: any(country.lower() in str(x).lower() for country in countries)
        )
        filtered_df = filtered_df[country_mask]
    
    return filtered_df

# Отримання списку країн за регіоном
def get_countries_in_region(region_name):
    """
    Отримує список країн для вказаного регіону з бази даних
    
    Args:
        region_name: назва регіону
        
    Returns:
        list: список назв країн у цьому регіоні
    """
    # Перетворюємо назву регіону в англійську для пошуку в CSV
    region_eng = region_name
    if region_name in region_translation.values():
        region_eng = region_name
    else:
        for ukr, eng in region_translation.items():
            if ukr == region_name:
                region_eng = eng
                break
    
    # Шукаємо країни в обраному регіоні
    countries = []
    
    # Фільтруємо DataFrame за регіоном
    region_filter = hotel_data['region'].apply(
        lambda x: region_eng.lower() in str(x).lower()
    )
    filtered_df = hotel_data[region_filter]
    
    # Отримуємо унікальні країни
    if 'country' in filtered_df.columns:
        unique_countries = filtered_df['country'].dropna().unique()
        
        # Перетворюємо назви країн в українську/англійську залежно від мови інтерфейсу
        for country_eng in unique_countries:
            country_display = country_eng  # За замовчуванням показуємо англійську назву
            
            # Шукаємо українську назву
            for ukr, eng in country_translation.items():
                if eng == country_eng:
                    country_display = ukr
                    break
            
            countries.append(country_display)
    
    return sorted(countries)  # Сортуємо за алфавітом

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

# Обробка відсутності країни
async def handle_missing_country(update: Update, context: ContextTypes.DEFAULT_TYPE, country_name: str) -> int:
    """
    Обробляє випадок, коли країни немає в базі даних
    
    Args:
        update: об'єкт Update від Telegram
        context: контекст бота
        country_name: назва країни, якої немає в базі
        
    Returns:
        int: Новий стан розмови (REGION_SELECTION)
    """
    user_id = update.effective_user.id
    lang = user_data_global[user_id]['language']
    
    # Зберігаємо оригінальний запит користувача
    context.user_data['original_country_query'] = country_name
    
    # Формуємо повідомлення залежно від мови
    if lang == 'uk':
        message = (
            f"На жаль, країна \"{country_name}\" не знайдена в нашій базі даних. "
            f"Можливо, вона записана дещо інакше або входить до складу іншої країни.\n\n"
            f"Для зручності пошуку оберіть один із регіонів світу, і я покажу всі країни "
            f"цього регіону, які представлені в нашій базі:\n\n"
            f"1. Європа\n"
            f"2. Північна Америка\n"
            f"3. Азія\n"
            f"4. Близький Схід\n"
            f"5. Африка\n"
            f"6. Південна Америка\n"
            f"7. Карибський басейн\n"
            f"8. Океанія\n\n"
            f"Будь ласка, надішліть номер регіону, який вас цікавить."
        )
    else:
        message = (
            f"Unfortunately, the country \"{country_name}\" was not found in our database. "
            f"It might be listed under a different name or as part of another country.\n\n"
            f"For easier search, please select one of the world regions, and I will show "
            f"all countries from that region that are represented in our database:\n\n"
            f"1. Europe\n"
            f"2. North America\n"
            f"3. Asia\n"
            f"4. Middle East\n"
            f"5. Africa\n"
            f"6. South America\n"
            f"7. Caribbean\n"
            f"8. Oceania\n\n"
            f"Please send the number of the region you are interested in."
        )
    
    # Створюємо клавіатуру з регіонами
    keyboard = [
        ["1. Європа" if lang == 'uk' else "1. Europe"],
        ["2. Північна Америка" if lang == 'uk' else "2. North America"],
        ["3. Азія" if lang == 'uk' else "3. Asia"],
        ["4. Близький Схід" if lang == 'uk' else "4. Middle East"],
        ["5. Африка" if lang == 'uk' else "5. Africa"],
        ["6. Південна Америка" if lang == 'uk' else "6. South America"],
        ["7. Карибський басейн" if lang == 'uk' else "7. Caribbean"],
        ["8. Океанія" if lang == 'uk' else "8. Oceania"]
    ]
    
    await update.message.reply_text(
        message,
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    )
    
    # Повертаємо новий стан для обробки вибору регіону
    return REGION_SELECTION

# Оновлена функція вибору регіону
async def region_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обробляє вибір регіону для перегляду доступних країн
    
    Args:
        update: об'єкт Update від Telegram
        context: контекст бота
        
    Returns:
        int: Наступний стан розмови (COUNTRY)
    """
    user_id = update.effective_user.id
    text = update.message.text
    lang = user_data_global[user_id]['language']
    
    # Визначаємо обраний регіон за номером або назвою
    selected_region = None
    
    # Словник регіонів
    region_dict = {
        "1": "Europe" if lang == 'en' else "Європа",
        "2": "North America" if lang == 'en' else "Північна Америка",
        "3": "Asia" if lang == 'en' else "Азія",
        "4": "Middle East" if lang == 'en' else "Близький Схід",
        "5": "Africa" if lang == 'en' else "Африка",
        "6": "South America" if lang == 'en' else "Південна Америка",
        "7": "Caribbean" if lang == 'en' else "Карибський басейн",
        "8": "Oceania" if lang == 'en' else "Океанія"
    }
    
    # Витягуємо номер регіону з тексту
    if text.startswith(("1", "2", "3", "4", "5", "6", "7", "8")):
        region_num = text[0]  # Перший символ (цифра)
        selected_region = region_dict[region_num]
    else:
        # Пошук за назвою регіону
        for num, name in region_dict.items():
            if name.lower() in text.lower():
                selected_region = name
                break
    
    if not selected_region:
        # Якщо регіон не розпізнано, повідомляємо і даємо спробувати ще раз
        if lang == 'uk':
            await update.message.reply_text(
                "Вибачте, я не розпізнав обраний регіон. Будь ласка, виберіть один із запропонованих варіантів.",
                reply_markup=ReplyKeyboardMarkup([list(region_dict.values())], one_time_keyboard=True)
            )
        else:
            await update.message.reply_text(
                "Sorry, I couldn't recognize the selected region. Please choose one of the suggested options.",
                reply_markup=ReplyKeyboardMarkup([list(region_dict.values())], one_time_keyboard=True)
            )
        return REGION_SELECTION
    
    # Отримуємо список країн для вибраного регіону
    countries_in_region = get_countries_in_region(selected_region)
    
    if not countries_in_region:
        # Якщо немає країн у цьому регіоні
        if lang == 'uk':
            await update.message.reply_text(
                f"На жаль, у нашій базі даних немає країн для регіону {selected_region}. "
                "Будь ласка, виберіть інший регіон.",
                reply_markup=ReplyKeyboardMarkup([list(region_dict.values())], one_time_keyboard=True)
            )
        else:
            await update.message.reply_text(
                f"Unfortunately, there are no countries in our database for the {selected_region} region. "
                "Please select another region.",
                reply_markup=ReplyKeyboardMarkup([list(region_dict.values())], one_time_keyboard=True)
            )
        return REGION_SELECTION
    
    # Формуємо повідомлення зі списком країн
    if lang == 'uk':
        message = f"Ось всі країни з регіону {selected_region} в нашій базі даних:\n\n"
        message += "\n".join([f"• {country}" for country in countries_in_region])
        message += "\n\nБудь ласка, введіть назву країни з цього списку (або кілька через кому):"
    else:
        message = f"Here are all countries from the {selected_region} region in our database:\n\n"
        message += "\n".join([f"• {country}" for country in countries_in_region])
        message += "\n\nPlease enter a country name from this list (or several countries separated by commas):"
    
    await update.message.reply_text(message, reply_markup=ReplyKeyboardRemove())
    
    # Повертаємося до стану введення країни
    return COUNTRY

# Оновлена функція country_choice
async def country_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє введення конкретних країн"""
    user_id = update.effective_user.id
    text = update.message.text
    lang = user_data_global[user_id]['language']
    
    # Перевірка на згадування країни-терориста
    if is_russia_mentioned(text):
        await update.message.reply_text(get_russia_message(lang))
        return COUNTRY
    
    # Розбиваємо введений текст на окремі країни
    country_names = [name.strip() for name in text.split(",")]
    
    # Нормалізуємо назви країн і конвертуємо в англійську
    normalized_countries = []
    display_results = []  # Результати для відображення користувачеві
    unknown_countries = []  # Країни, яких немає в базі даних
    
    for country in country_names:
        # Спочатку перевіряємо, чи є точне співпадіння
        exact_match = None
        for ukr_name, eng_name in country_translation.items():
            if country.lower() == ukr_name.lower():
                exact_match = (ukr_name, eng_name)
                break
        
        # Якщо є точне співпадіння
        if exact_match:
            normalized_countries.append(exact_match[1])
            # Показуємо правильну форму (з великої літери)
            display_results.append(exact_match[0])
            logger.info(f"Точне співпадіння для країни: '{country}' -> '{exact_match[0]}'")
        else:
            # Якщо немає точного співпадіння, використовуємо нечітке порівняння
            corrected_name = find_closest_country_name(country, lang)
            
            # Перевіряємо, чи знайдено відповідність
            if corrected_name and corrected_name in country_translation.values():
                # Знаходимо оригінальну українську назву для відображення
                ukr_original = None
                for ukr_name, eng_name in country_translation.items():
                    if eng_name == corrected_name:
                        ukr_original = ukr_name
                        break
                
                normalized_countries.append(corrected_name)
                
                # Якщо знайдено відповідність і вона відрізняється від оригіналу
                if ukr_original and country.lower() != ukr_original.lower():
                    display_results.append(f"{country} → {ukr_original}")
                    logger.info(f"Виправлено назву країни: '{country}' -> '{ukr_original}'")
                else:
                    display_results.append(country)
                    logger.info(f"Використано оригінальну назву країни: '{country}'")
            else:
                # Країну не знайдено
                unknown_countries.append(country)
                logger.warning(f"Не знайдено країну: '{country}'")
    
    # Якщо всі країни невідомі, викликаємо обробник невідомих країн
    if not normalized_countries and unknown_countries:
        # Викликаємо обробник для першої невідомої країни
        return await handle_missing_country(update, context, unknown_countries[0])
    
    # Якщо є і відомі, і невідомі країни
    if normalized_countries and unknown_countries:
        # Зберігаємо вибрані країни (нормалізовані)
        user_data_global[user_id]['regions'] = None
        user_data_global[user_id]['countries'] = normalized_countries
        
        # Формуємо повідомлення з попередженням про невідомі країни
        if lang == 'uk':
            message = f"Дякую! Ви обрали наступні країни: {', '.join(display_results)}."
            
            if unknown_countries:
                message += f"\n\nУвага: наступні країни не знайдено в нашій базі: {', '.join(unknown_countries)}."
                message += f"\nМи будемо враховувати тільки знайдені країни."
            
            message += "\nПереходимо до наступного питання."
        else:
            message = f"Thank you! You have chosen the following countries: {', '.join(display_results)}."
            
            if unknown_countries:
                message += f"\n\nNote: the following countries were not found in our database: {', '.join(unknown_countries)}."
                message += f"\nWe will only consider the countries that were found."
            
            message += "\nMoving on to the next question."
        
        await update.message.reply_text(message)
        return await ask_category(update, context)
    
    # Зберігаємо вибрані країни (нормалізовані)
    user_data_global[user_id]['regions'] = None
    user_data_global[user_id]['countries'] = normalized_countries
    
    # Відображаємо результат користувачеві
    if lang == 'uk':
        await update.message.reply_text(
            f"Дякую! Ви обрали наступні країни: {', '.join(display_results)}.\n"
            "Переходимо до наступного питання."
        )
    else:
        await update.message.reply_text(
            f"Thank you! You have chosen the following countries: {', '.join(display_results)}.\n"
            "Moving on to the next question."
        )
    
    return await ask_category(update, context)

# Функції для категорії готелів
async def ask_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про категорію готелів"""
    user_id = update.effective_user.id
    lang = user_data_global[user_id]['language']
    
    keyboard = []
    
    if lang == 'uk':
        keyboard = [
            ["Luxury (преміум-клас)"],
            ["Comfort (середній клас)"],
            ["Standard (економ-клас)"]
        ]
        
        await update.message.reply_text(
            "Питання 2/4:\nЯку категорію готелів ви зазвичай обираєте?",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    else:
        keyboard = [
            ["Luxury (premium class)"],
            ["Comfort (middle class)"],
            ["Standard (economy class)"]
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

# Оновлена функція для вибору стилю готелю
async def ask_style(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про стиль готелю"""
    user_id = update.effective_user.id
    lang = user_data_global[user_id]['language']
    
    if lang == 'uk':
        message = (
            "Питання 3/4:\n"
            "**Який стиль готелю ви зазвичай обираєте?**\n"
            "*(Оберіть до трьох варіантів.)*\n\n"
            "1. **Розкішний і вишуканий** (преміум-матеріали, елегантний дизайн, високий рівень сервісу)\n"
            "2. **Бутік і унікальний** (оригінальний інтер'єр, творча атмосфера, відчуття ексклюзивності)\n"
            "3. **Класичний і традиційний** (перевірений часом стиль, консервативність, історичність)\n"
            "4. **Сучасний і дизайнерський** (модні інтер'єри, мінімалізм, технологічність)\n"
            "5. **Затишний і сімейний** (тепла атмосфера, комфорт, дружній до дітей)\n"
            "6. **Практичний і економічний** (без зайвих деталей, функціональний, доступний)"
        )
        
        keyboard = [
            ["1. Розкішний і вишуканий"],
            ["2. Бутік і унікальний"],
            ["3. Класичний і традиційний"],
            ["4. Сучасний і дизайнерський"],
            ["5. Затишний і сімейний"],
            ["6. Практичний і економічний"]
        ]
    else:
        message = (
            "Question 3/4:\n"
            "**What hotel style do you usually choose?**\n"
            "*(Choose up to three options.)*\n\n"
            "1. **Luxurious and refined** (premium materials, elegant design, high level of service)\n"
            "2. **Boutique and unique** (original interior, creative atmosphere, sense of exclusivity)\n"
            "3. **Classic and traditional** (time-tested style, conservatism, historical ambiance)\n"
            "4. **Modern and designer** (fashionable interiors, minimalism, technological features)\n"
            "5. **Cozy and family-friendly** (warm atmosphere, comfort, child-friendly)\n"
            "6. **Practical and economical** (no unnecessary details, functional, affordable)"
        )
        
        keyboard = [
            ["1. Luxurious and refined"],
            ["2. Boutique and unique"],
            ["3. Classic and traditional"],
            ["4. Modern and designer"],
            ["5. Cozy and family-friendly"],
            ["6. Practical and economical"]
        ]
    
    await update.message.reply_text(
        message,
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    )
    
    return STYLE

# Оновлена функція обробки вибору стилю готелю
async def style_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє вибір стилю готелю"""
    user_id = update.effective_user.id
    text = update.message.text
    lang = user_data_global[user_id]['language']
    
    # Обробка вибору стилів
    styles = []
    
    # Перевіряємо на множинний вибір (якщо текст містить кому)
    if "," in text:
        style_texts = [style.strip() for style in text.split(",")]
    else:
        style_texts = [text.strip()]  # Один стиль
    
    # Мапінг номерів до стилів
    style_mapping_uk = {
        "1": "Розкішний і вишуканий",
        "2": "Бутік і унікальний",
        "3": "Класичний і традиційний",
        "4": "Сучасний і дизайнерський",
        "5": "Затишний і сімейний",
        "6": "Практичний і економічний"
    }
    
    style_mapping_en = {
        "1": "Luxurious and refined",
        "2": "Boutique and unique",
        "3": "Classic and traditional",
        "4": "Modern and designer",
        "5": "Cozy and family-friendly",
        "6": "Practical and economical"
    }
    
    # Визначаємо обрані стилі, обробляючи номери або повні назви
    for style_text in style_texts:
        # Видаляємо крапку після числа, якщо вона є
        if ". " in style_text:
            style_text = style_text.replace(". ", ".")
        
        # Якщо текст починається з цифри (1-6), використовуємо мапінг
        if style_text.startswith(("1", "2", "3", "4", "5", "6")):
            num = style_text[0]  # Перший символ (цифра)
            if lang == 'uk':
                styles.append(style_mapping_uk[num])
            else:
                styles.append(style_mapping_en[num])
        else:
            # Інакше шукаємо відповідність у назвах стилів
            for key, value in (style_mapping_uk.items() if lang == 'uk' else style_mapping_en.items()):
                if value.lower() in style_text.lower():
                    styles.append(value)
                    break
    
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
    
    if lang == 'uk':
        await update.message.reply_text(
            f"Дякую! Ви обрали наступні стилі: {', '.join(styles)}.\n"
            "Переходимо до наступного питання."
        )
    else:
        await update.message.reply_text(
            f"Thank you! You have chosen the following styles: {', '.join(styles)}.\n"
            "Moving on to the next question."
        )
    
    return await ask_purpose(update, context)

# Додаємо функцію для питання про мету подорожі
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

# Функція обробки вибору мети подорожі
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

# Оновлений ConversationHandler
conv_handler = ConversationHandler(
    entry_points=[CommandHandler("start", start)],
    states={
        LANGUAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, language_choice)],
        REGION: [MessageHandler(filters.TEXT & ~filters.COMMAND, region_choice)],
        COUNTRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, country_choice)],
        REGION_SELECTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, region_selection)],  # Новий стан
        CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, category_choice)],
        STYLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, style_choice)],
        PURPOSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, purpose_choice)]
    },
    fallbacks=[
        CommandHandler("cancel", cancel),
        CommandHandler("start", start)
    ]
)

# Додавання обробника до застосунку
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
    
    # Додавання обробника розмови
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


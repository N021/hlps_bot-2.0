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
import aiohttp
from urllib.parse import quote
import openai

# ===============================
# ЧАСТИНА 2: КОНФІГУРАЦІЯ ТА ГЛОБАЛЬНІ ЗМІННІ (ВИПРАВЛЕНА)
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

# ДОДАНО: Посилання на реєстрацію в програмах лояльності
LOYALTY_PROGRAM_REGISTRATION_LINKS = {
    "Marriott Bonvoy": "https://www.marriott.com/loyalty/join/joinPromotion.mi?promotion=FT25",
    "Hilton Honors": "https://www.hilton.com/en/hilton-honors/join/?ocode=JHTNW",
    "IHG One Rewards": "https://www.ihg.com/rewardsclub/us/en/enrollment/join?cm_sp=WEB-_-6C-_-ONEREWARDS-HOME-_-LYMOD1-_-US-EN-_-LOY-_-JOIN-_-FS",
    "Wyndham Rewards": "https://www.wyndhamhotels.com/wyndham-rewards",
    "ALL - Accor Live Limitless": "https://all.accor.com/loyalty-program/reasonstojoin/index.en.shtml",
    "Choice Privileges": "https://www.choicehotels.com/content/choicehotels/apac/au/en/choice-privileges",
    "World of Hyatt": "https://world.hyatt.com/content/gp/en/program-overview.html"
}

# ДОДАНО: Глобальна змінна для зберігання останніх результатів користувача (для команди /more)
user_last_results = {}

# НОВЕ: Google Maps API налаштування
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")
ENABLE_PHOTOS = GOOGLE_MAPS_API_KEY != ""  # Вмикаємо фото тільки якщо є API ключ
MAX_PHOTOS_PER_HOTEL = 3  # Максимум 3 фото на готель

# НОВЕ: Налаштування для Google Maps API
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
PLACES_PHOTOS_URL = "https://maps.googleapis.com/maps/api/place/photo"

# ВИПРАВЛЕНО: OpenAI налаштування з покращеними параметрами
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
ENABLE_OPENAI = OPENAI_API_KEY != ""

# ДОДАНО: Нові константи для OpenAI налаштувань
OPENAI_MAX_TOKENS_SIMPLE = 250     # Для простих запитів (2-3 параметри)
OPENAI_MAX_TOKENS_COMPLEX = 400    # Для складних запитів (4+ параметрів)
OPENAI_TEMPERATURE_CREATIVE = 0.9  # Висока творчість для коротких описів
OPENAI_TEMPERATURE_BALANCED = 0.7  # Збалансована творчість для детальних описів
OPENAI_TIMEOUT = 20                # Збільшений таймаут до 20 секунд
OPENAI_MAX_WORDS = 90              # Максимум слів у згенерованому тексті
OPENAI_MIN_WORDS = 15              # Мінімум слів у згенерованому тексті

# Ініціалізуємо OpenAI клієнт, якщо ключ доступний
if ENABLE_OPENAI:
    openai.api_key = OPENAI_API_KEY

# ===============================
# ВИПРАВЛЕНО: OpenAI Integration для генерації описів готелів
# ===============================

def create_adaptive_prompt(hotel_name: str, hotel_brand: str, selected_styles: list, 
                          selected_purposes: list, lang: str = 'uk') -> tuple:
    """
    Створює адаптивний промт залежно від складності запиту
    
    Returns:
        tuple: (prompt_text, max_tokens, temperature)
    """
    styles_text = ', '.join(selected_styles)
    purposes_text = ', '.join(selected_purposes)
    total_params = len(selected_styles) + len(selected_purposes)
    
    # Визначаємо складність та налаштування
    if total_params <= 3:
        max_tokens = OPENAI_MAX_TOKENS_SIMPLE
        temperature = OPENAI_TEMPERATURE_CREATIVE
        complexity = "simple"
    else:
        max_tokens = OPENAI_MAX_TOKENS_COMPLEX
        temperature = OPENAI_TEMPERATURE_BALANCED
        complexity = "complex"
    
    if lang == 'uk':
        if complexity == "simple":
            prompt = f"""
Створи короткий персоналізований опис готелю бренду {hotel_brand} українською мовою.

Обрані стилі: {styles_text}
Обрані цілі: {purposes_text}

Вимоги:
1. Опис 2-3 речення (60-80 слів)
2. Покажи, як бренд відповідає вибраним критеріям
3. Будь конкретним про особливості {hotel_brand}
4. Не згадуй назву готелю, тільки бренд
5. Пиши природно та переконливо

Формат: [Як бренд відповідає стилям]. [Чому підходить для цілей]. [Унікальна перевага].
"""
        else:
            prompt = f"""
Створи детальний персоналізований опис готелю "{hotel_name}" бренду {hotel_brand} українською мовою.

Обрані стилі: {styles_text}
Обрані цілі подорожі: {purposes_text}

Вимоги:
1. Опис 2-3 речення (70-90 слів)
2. Детально покажи відповідність кожному критерію
3. Використовуй достовірну інформацію про {hotel_brand}
4. Підкресли унікальні особливості бренду
5. Не згадуй назву готелю в описі
6. Пиши захоплююче та персоналізовано

Структура: [Відповідність стилям]. [Підходящість для цілей]. [Ключові переваги бренду].
"""
    else:
        if complexity == "simple":
            prompt = f"""
Create a short personalized description of {hotel_brand} brand hotel in English.

Selected styles: {styles_text}
Selected purposes: {purposes_text}

Requirements:
1. Description 2-3 sentences (60-80 words)
2. Show how the brand matches selected criteria
3. Be specific about {hotel_brand} features
4. Don't mention hotel name, only brand
5. Write naturally and convincingly

Format: [How brand matches styles]. [Why it suits purposes]. [Unique advantage].
"""
        else:
            prompt = f"""
Create a detailed personalized description of hotel "{hotel_name}" from {hotel_brand} brand in English.

Selected styles: {styles_text}
Selected travel purposes: {purposes_text}

Requirements:
1. Description 2-3 sentences (70-90 words)
2. Detail how it matches each criterion
3. Use accurate information about {hotel_brand}
4. Highlight unique brand features
5. Don't mention hotel name in description
6. Write engagingly and personally

Structure: [Style match]. [Purpose suitability]. [Key brand advantages].
"""
    
    return prompt, max_tokens, temperature

def process_ai_generated_text(text: str, hotel_brand: str, styles: list, purposes: list, lang: str) -> str:
    """
    Обробляє та покращує згенерований AI текст
    """
    # Очищаємо текст від зайвих символів
    text = ' '.join(text.split())
    text = text.strip('"\'«»""''')
    
    # Переконуємося, що текст закінчується крапкою
    if not text.endswith('.'):
        text += '.'
    
    # Перевіряємо довжину
    words = text.split()
    word_count = len(words)
    
    # Якщо занадто довго - обрізаємо розумно
    if word_count > OPENAI_MAX_WORDS:
        # Шукаємо останню повну крапку в межах ліміту
        truncated_words = words[:OPENAI_MAX_WORDS-5]  # Залишаємо запас
        truncated_text = ' '.join(truncated_words)
        
        # Шукаємо останню крапку
        last_period = truncated_text.rfind('.')
        if last_period > len(truncated_text) * 0.7:  # Якщо крапка не занадто рано
            text = truncated_text[:last_period + 1]
        else:
            text = truncated_text + '.'
        
        debug_log(f"Обрізано текст з {word_count} до {len(text.split())} слів")
    
    # Якщо занадто коротко - додаємо деталі
    elif word_count < OPENAI_MIN_WORDS:
        if lang == 'uk':
            text += f" Бренд {hotel_brand} забезпечує високу якість сервісу та комфорт."
        else:
            text += f" {hotel_brand} brand ensures high service quality and comfort."
        
        debug_log(f"Розширено короткий текст з {word_count} слів")
    
    return text

def generate_smart_fallback(hotel_brand: str, styles: list, purposes: list, lang: str) -> str:
    """
    Генерує розумний fallback опис без OpenAI
    """
    if lang == 'uk':
        # Аналізуємо стилі
        style_keywords = []
        if any("розкішний" in s.lower() for s in styles):
            style_keywords.append("розкішного")
        elif any("бутік" in s.lower() for s in styles):
            style_keywords.append("унікального")
        elif any("сучасний" in s.lower() for s in styles):
            style_keywords.append("сучасного")
        else:
            style_keywords.append("комфортного")
        
        # Аналізуємо цілі
        purpose_keywords = []
        if any("бізнес" in p.lower() for p in purposes):
            purpose_keywords.append("ділових поїздок")
        elif any("сімейний" in p.lower() for p in purposes):
            purpose_keywords.append("сімейного відпочинку")
        else:
            purpose_keywords.append("відпочинку")
        
        style_desc = style_keywords[0] if style_keywords else "якісного"
        purpose_desc = purpose_keywords[0] if purpose_keywords else "комфортного перебування"
        
        return f"Готелі бренду {hotel_brand} відомі своїм {style_desc} сервісом та ідеально підходять для {purpose_desc}. Цей вибір гарантує незабутнє перебування з усіма необхідними зручностями та високим рівнем обслуговування."
    else:
        # Аналіз для англійської
        style_desc = "luxury" if any("luxury" in s.lower() for s in styles) else "comfortable"
        purpose_desc = "business travel" if any("business" in p.lower() for p in purposes) else "leisure stays"
        
        return f"{hotel_brand} hotels are renowned for their {style_desc} service and perfectly suit {purpose_desc}. This choice guarantees an unforgettable stay with all necessary amenities and exceptional service quality."

# ===============================
# ПРОСТИЙ ПІДХІД: Один розумний промт
# ===============================

# ЗАМІНИТИ тільки функцію generate_hotel_description НА ЦЮ:

async def generate_hotel_description(hotel_name: str, hotel_brand: str, selected_styles: list, 
                                   selected_purposes: list, lang: str = 'uk') -> str:
    """
    Генерує персоналізований опис готелю через один розумний промт
    """
    if not ENABLE_OPENAI:
        if lang == 'uk':
            return f"Цей готель бренду {hotel_brand} чудово підходить для ваших потреб. Відмінний вибір для комфортного перебування."
        else:
            return f"This {hotel_brand} hotel perfectly suits your needs. An excellent choice for a comfortable stay."
    
    try:
        styles_text = ', '.join(selected_styles)
        purposes_text = ', '.join(selected_purposes)
        
        if lang == 'uk':
            prompt = f"""
Ви представник офіційного сервісу з підбору готелів. Створіть персоналізований опис готелю бренду {hotel_brand} для клієнта.

Клієнт обрав:
Стилі: {styles_text}
Цілі подорожі: {purposes_text}

Вимоги до опису:

1. Ввічлива комунікація. Відношення до користувача на "ви".
2. 2-3 речення (70-90 слів)
3. Конкретні факти про {hotel_brand}: послуги, зручності, особливості
4. Пояснити, ЧОМУ саме цей бренд підходить під обрані параметри
5. Уникати штампів: "ідеальний вибір", "неперевершений сервіс"
6. Конкретні приклади замість загальних фраз
7. Заборонені звернення будь які звернення до користувача "Шановний клієнте, Готелі Kimpton Hotels & Restaurants відповідають вашим..."


Приклади конкретності:
- Замість "розкішний сервіс" → "консьєрж працює 24/7, welcome drink при заселенні"
- Замість "сучасний дизайн" → "смарт-телевізори Samsung в номерах, мобільний додаток для керування освітленням"
- Замість "сімейна атмосфера" → "дитяче меню від шеф-кухаря, дитячі халати та тапочки"

Приклад хорошої відповіді:
"Готелі Kimpton Hotels & Restaurants ідеально відповідають Вашим вимогам до бутікової унікальності завдяки авторському дизайну від місцевих художників та лімітованим колекціям арт-об'єктів у кожному номері. Для сімейного відпочинку бренд пропонує безкоштовні ліжечка для дітей, спеціальне дитяче меню та pet-friendly політику без додаткової плати. Програма hosted evening wine hour щовечора створює затишну атмосферу для спілкування між гостями.

Напишіть як експерт, що знає специфіку {hotel_brand} і може пояснити переваги конкретними фактами.
"""
        else:
            prompt = f"""
You represent an official hotel selection service. Create a personalized description of {hotel_brand} hotel for a client.

Client selected:
Styles: {styles_text}
Travel purposes: {purposes_text}

Description requirements:
1. Formal "You" address only (official service style)
2. 2-3 sentences (70-90 words)
3. Specific facts about {hotel_brand}: services, amenities, features
4. Explain WHY this brand matches the selected parameters
5. Avoid clichés: "perfect choice", "unparalleled service"
6. Concrete examples instead of generic phrases

Examples of specificity:
- Instead of "luxury service" → "24/7 concierge, welcome drink upon arrival"
- Instead of "modern design" → "Samsung smart TVs in rooms, mobile app for lighting control"
- Instead of "family atmosphere" → "chef's children menu, kids' bathrobes and slippers"

Write as an expert who knows {hotel_brand} specifics and can explain advantages with concrete facts.
"""
        
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": f"You are a professional hotel consultant representing an official hotel selection service. You write formal, fact-based descriptions in {'Ukrainian' if lang == 'uk' else 'English'} using specific brand knowledge instead of generic praise. Always use formal address."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=400,
            temperature=0.7,  # Знижено для більш фактичного тону
            top_p=0.9,
            frequency_penalty=0.7,  # Збільшено проти штампів
            presence_penalty=0.5,
            timeout=20
        )
        
        generated_text = response.choices[0].message.content.strip()
        
        # Простий cleanup
        generated_text = ' '.join(generated_text.split())
        generated_text = generated_text.strip('"\'«»""''')
        
        if not generated_text.endswith('.'):
            generated_text += '.'
        
        # Перевіряємо довжину
        words = generated_text.split()
        if len(words) > 100:
            generated_text = ' '.join(words[:95]) + '.'
        
        debug_log(f"Згенеровано опис для {hotel_name}: {generated_text}")
        return generated_text
        
    except Exception as e:
        logger.error(f"Помилка генерації опису для {hotel_name}: {e}")
        
        if lang == 'uk':
            return f"Цей готель бренду {hotel_brand} чудово підходить для ваших потреб. Відмінний вибір для комфортного перебування."
        else:
            return f"This {hotel_brand} hotel perfectly suits your needs. An excellent choice for a comfortable stay."

# ВСЕ! Більше нічого не треба змінювати.
# Ніяких нових імпортів, ніяких додаткових функцій.

# ЗАЛИШАЄТЬСЯ БЕЗ ЗМІН
def format_hotel_caption_with_ai_description(hotel_info: dict, ai_description: str, lang: str = 'uk') -> str:
    """
    Форматує підпис до фото готелю з AI-описом
    """
    hotel_name = hotel_info.get('name', 'N/A')
    hotel_brand = hotel_info.get('brand', 'N/A')
    address = hotel_info.get('address', 'N/A')
    
    # Парсимо адресу для отримання міста та країни
    address_parts = address.split(',')
    if len(address_parts) >= 2:
        city = address_parts[-2].strip()
        country = address_parts[-1].strip()
        location = f"{city}, {country}"
    else:
        location = address
    
    # Формуємо фінальний підпис
    caption = f'"{hotel_name}" by {hotel_brand}, {location}\n\n{ai_description}'
    
    return caption

# ЗАЛИШАЮТЬСЯ БЕЗ ЗМІН (всі інші функції debug_log, validate_score_calculation тощо)
def debug_log(message):
    """Логування для дебагу розрахунків"""
    if DEBUG_SCORING:
        logger.info(f"[DEBUG] {message}")

def validate_score_calculation(calculated_total, detailed_breakdown, program_name="Unknown"):
    """
    Перевіряє, чи сума детальних балів дорівнює загальному балу
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

def get_program_rating(program_name):
    """
    Повертає середній рейтинг програми лояльності
    """
    return LOYALTY_PROGRAM_RATINGS.get(program_name, 4.0)

def calculate_rating_coefficient(program_rating):
    """
    Розраховує коефіцієнт на основі рейтингу програми
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
        "Практичний і стриманий": "Practical and understated"
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
    """Показує детальний розбір останніх результатів по 2 програми в повідомленні"""
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
        # Відправляємо вступне повідомлення
        if lang == 'uk':
            intro_text = "🎉 **Детальний аналіз усіх 7 програм лояльності:**"
        else:
            intro_text = "🎉 **Detailed analysis of all 7 loyalty programs:**"
        
        await update.message.reply_text(intro_text, parse_mode="Markdown")
        
        # Групуємо програми по 2 в повідомлення
        all_programs = scores_df.head(7)
        
        # 1-ше повідомлення: програми 1-2
        programs_1_2 = all_programs.iloc[0:2]
        if not programs_1_2.empty:
            detailed_results_1_2 = format_detailed_results_with_ratings(user_data, programs_1_2, lang)
            await send_long_message_to_chat(context, update.message.chat_id, detailed_results_1_2)
            await asyncio.sleep(1)
        
        # 2-ге повідомлення: програми 3-4
        programs_3_4 = all_programs.iloc[2:4]
        if not programs_3_4.empty:
            detailed_results_3_4 = format_detailed_results_with_ratings(user_data, programs_3_4, lang)
            await send_long_message_to_chat(context, update.message.chat_id, detailed_results_3_4)
            await asyncio.sleep(1)
        
        # 3-тє повідомлення: програми 5-6
        programs_5_6 = all_programs.iloc[4:6]
        if not programs_5_6.empty:
            detailed_results_5_6 = format_detailed_results_with_ratings(user_data, programs_5_6, lang)
            await send_long_message_to_chat(context, update.message.chat_id, detailed_results_5_6)
            await asyncio.sleep(1)
        
        # 4-те повідомлення: програма 7
        program_7 = all_programs.iloc[6:7]
        if not program_7.empty:
            detailed_results_7 = format_detailed_results_with_ratings(user_data, program_7, lang)
            await send_long_message_to_chat(context, update.message.chat_id, detailed_results_7)
        
        # Заключне повідомлення
        if lang == 'uk':
            outro_text = "\n\nЩоб почати нове опитування, надішліть команду /start."
        else:
            outro_text = "\n\nTo start a new survey, send the /start command."
        
        await update.message.reply_text(outro_text)
        
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

# ОНОВЛЕНО: Функція для команди /21 (адміністративна) з підтримкою готелів
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
    lang = user_data.get('language', 'uk')
    
    try:
        # Генеруємо ПОВНИЙ адміністративний звіт
        admin_report = format_admin_scoring_report(user_data, scores_df)
        
        # ДОДАНО: Додаємо готелі з зваженими рейтингами в адмін-режимі
        enhanced_admin_report = add_hotels_to_results(admin_report, user_data, scores_df, lang, admin_mode=True)
        
        # Відправляємо адміністративний звіт
        intro_text = "🎉 Звіт по нарахуванню балів!\n\nОсь 7 програм лояльності готелів з повним розбором балів:\n\n"
        full_message = intro_text + enhanced_admin_report
        await send_long_message_to_chat(context, update.message.chat_id, full_message)
        
        # НОВЕ: Додаємо готелі з фото для адмін режиму
        await add_hotels_to_results_with_photos(
            context, 
            update.message.chat_id, 
            user_data, 
            scores_df, 
            lang, 
            admin_mode=True
        )
        
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
            regions_list = '\n'.join(f"- {region};" for region in selected_regions)
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Дякую! Ви обрали наступні регіони:\n{regions_list}"
            )
        else:
            regions_list = '\n'.join(f"- {region};" for region in selected_regions)
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Thank you! You have chosen the following regions:\n{regions_list}"
            )
        
        await asyncio.sleep(2.0)
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
            text=f"Дякую! Ви обрали категорію:\n- {category};"
        )
    else:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"Thank you! You have chosen the category:\n- {category};"
        )

    await asyncio.sleep(2.0)

    return await ask_style(update, context)


# ===============================
# ЧАСТИНА 7: ОБРОБНИКИ СТИЛЮ
# ===============================

def get_styles_for_category(category, lang='uk'):
    """
    Повертає список стилів, які відповідають обраній категорії готелю
    """
    if lang == 'uk':
        if category == "Luxury":
            return [
                "Розкішний і вишуканий",
                "Бутік і унікальний", 
                "Класичний і традиційний",
                "Сучасний і дизайнерський"
            ]
        elif category == "Comfort":
            return [
                "Бутік і унікальний",
                "Класичний і традиційний", 
                "Сучасний і дизайнерський",
                "Затишний і сімейний",
                "Практичний і стриманий"
            ]
        elif category == "Standard":
            return [
                "Класичний і традиційний",
                "Сучасний і дизайнерський",
                "Затишний і сімейний",
                "Практичний і стриманий"
            ]
        else:
            # Fallback - всі стилі
            return [
                "Розкішний і вишуканий", 
                "Бутік і унікальний", 
                "Класичний і традиційний", 
                "Сучасний і дизайнерський",
                "Затишний і сімейний", 
                "Практичний і стриманий"
            ]
    else:  # English
        if category == "Luxury":
            return [
                "Luxurious and refined",
                "Boutique and unique",
                "Classic and traditional",
                "Modern and designer"
            ]
        elif category == "Comfort":
            return [
                "Boutique and unique",
                "Classic and traditional",
                "Modern and designer", 
                "Cozy and family-friendly",
                "Practical and understated"
            ]
        elif category == "Standard":
            return [
                "Classic and traditional",
                "Modern and designer",
                "Cozy and family-friendly",
                "Practical and understated"
            ]
        else:
            # Fallback - всі стилі
            return [
                "Luxurious and refined", 
                "Boutique and unique",
                "Classic and traditional", 
                "Modern and designer",
                "Cozy and family-friendly", 
                "Practical and understated"
            ]

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
    
    # Отримуємо фільтровані стилі відповідно до категорії
    styles = get_styles_for_category(category, lang)
    
    # Очищуємо вибрані стилі, якщо вони більше не доступні для цієї категорії
    user_data_global[user_id]['selected_styles'] = [
        style for style in user_data_global[user_id]['selected_styles'] 
        if style in styles
    ]
    
    # Створюємо InlineKeyboard з чекбоксами для стилів
    if lang == 'uk':
        styles_description = (
            "Питання 3/4:\n"
            "Який стиль готелю ви зазвичай обираєте?\n"
            "*(Оберіть мінімум 2 варіанти)*\n\n"
        )
        
        # Динамічно генеруємо описи тільки для доступних стилів
        style_descriptions = {
            "Розкішний і вишуканий": "**Розкішний і вишуканий** (преміум-матеріали, елегантний дизайн, високий рівень сервісу)",
            "Бутік і унікальний": "**Бутік і унікальний** (оригінальний інтер'єр, творча атмосфера, відчуття ексклюзивності)",
            "Класичний і традиційний": "**Класичний і традиційний** (перевірений часом стиль, консервативність, історичність)",
            "Сучасний і дизайнерський": "**Сучасний і дизайнерський** (модні інтер'єри, мінімалізм, технологічність)",
            "Затишний і сімейний": "**Затишний і сімейний** (тепла атмосфера, комфорт, дружній до дітей)",
            "Практичний і стриманий": "**Практичний і стриманий** (функціональний, комфорт без надлишків, продуманий простір)"
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
            "*(Select at least 2 options)*\n\n"
        )
        
        # Динамічно генеруємо описи тільки для доступних стилів
        style_descriptions = {
            "Luxurious and refined": "**Luxurious and refined** (premium materials, elegant design, high level of service)",
            "Boutique and unique": "**Boutique and unique** (original interior, creative atmosphere, sense of exclusivity)",
            "Classic and traditional": "**Classic and traditional** (time-tested style, conservatism, historical ambiance)",
            "Modern and designer": "**Modern and designer** (fashionable interiors, minimalism, technological features)",
            "Cozy and family-friendly": "**Cozy and family-friendly** (warm atmosphere, comfort, child-friendly)",
            "Practical and understated": "**Practical and understated** (functional, comfort without excess, well-designed space)"
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
    # НЕ викликаємо query.answer() одразу!
    
    user_id = query.from_user.id
    callback_data = query.data
    
    # Якщо користувач натиснув "Відповісти"
    if callback_data == "style_submit":
        selected_styles = user_data_global[user_id]['selected_styles']
        lang = user_data_global[user_id]['language']
        
        # НОВА ПЕРЕВІРКА: мінімум 2 стилі з блокуванням
        if len(selected_styles) < 2:
            if lang == 'uk':
                await query.answer("Будь ласка, оберіть мінімум 2 стилі", show_alert=True)
            else:
                await query.answer("Please select at least 2 styles", show_alert=True)
            return WAITING_STYLE_SUBMIT
        
        # Якщо все ОК - показуємо стандартне повідомлення
        await query.answer()
        
        # Зберігаємо вибрані стилі
        user_data_global[user_id]['styles'] = selected_styles
        
        # Видаляємо клавіатуру, але зберігаємо текст питання 3/4
        try:
            await query.edit_message_text(text=query.message.text, reply_markup=None, parse_mode="Markdown")
        except:
            await query.edit_message_text(text=query.message.text, reply_markup=None)
        
        # Надсилаємо НОВЕ повідомлення з підтвердженням вибору
        if lang == 'uk':
            styles_list = '\n'.join(f"- {style};" for style in selected_styles)
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Дякую! Ви обрали наступні стилі:\n{styles_list}"
            )
        else:
            styles_list = '\n'.join(f"- {style};" for style in selected_styles)
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Thank you! You have chosen the following styles:\n{styles_list}"
            )
        
        # Очищуємо ID повідомлення зі стилем
        if 'style_message_id' in user_data_global[user_id]:
            del user_data_global[user_id]['style_message_id']
        
        await asyncio.sleep(2.0)
        return await ask_purpose(update, context)
    
    # Якщо це вибір або скасування вибору стилю
    else:
        # Для вибору окремих стилів показуємо стандартну відповідь
        await query.answer()
        
        style = callback_data.replace("style_", "")
        
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

import asyncio  # Додаємо імпорт для затримки

def get_purposes_for_category(category, lang='uk'):
    """
    Повертає список цілей подорожі, які відповідають обраній категорії готелю
    """
    if lang == 'uk':
        if category == "Luxury":
            # Для Luxury виключаємо "Довготривале проживання"
            return [
                "Бізнес-подорожі / відрядження",
                "Відпустка / релакс",
                "Сімейний відпочинок"
            ]
        else:
            # Для Comfort та Standard залишаємо всі цілі
            return [
                "Бізнес-подорожі / відрядження",
                "Відпустка / релакс",
                "Сімейний відпочинок",
                "Довготривале проживання"
            ]
    else:  # English
        if category == "Luxury":
            # Для Luxury виключаємо "Long-term stay"
            return [
                "Business travel",
                "Vacation / relaxation",
                "Family vacation"
            ]
        else:
            # Для Comfort та Standard залишаємо всі цілі
            return [
                "Business travel",
                "Vacation / relaxation",
                "Family vacation",
                "Long-term stay"
            ]

async def ask_purpose(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про мету подорожі з чекбоксами та фільтрацією по категорії"""
    
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
    
    # Ініціалізуємо вибрані цілі, якщо їх ще не обрано
    if 'selected_purposes' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_purposes'] = []
    
    # Отримуємо фільтровані цілі відповідно до категорії
    purposes = get_purposes_for_category(category, lang)
    
    # Очищуємо вибрані цілі, якщо вони більше не доступні для цієї категорії
    user_data_global[user_id]['selected_purposes'] = [
        purpose for purpose in user_data_global[user_id]['selected_purposes'] 
        if purpose in purposes
    ]
    
    # Створюємо InlineKeyboard з чекбоксами для цілей
    if lang == 'uk':
        purpose_description = (
            "Питання 4/4:\n"
            "З якою метою ви зазвичай зупиняєтесь у готелі?\n"
            "*(Оберіть мінімум 2 варіанти)*\n\n"
        )
        
        # Динамічно генеруємо описи тільки для доступних цілей
        purpose_descriptions = {
            "Бізнес-подорожі / відрядження": "**Бізнес-подорожі / відрядження** (зручність для роботи, доступ до ділових центрів)",
            "Відпустка / релакс": "**Відпустка / релакс** (комфорт, розваги, відпочинок)",
            "Сімейний відпочинок": "**Сімейний відпочинок** (розваги для дітей, сімейні номери)",
            "Довготривале проживання": "**Довготривале проживання** (відчуття дому, кухня, пральня)"
        }
        
        # Додаємо описи тільки для доступних цілей
        for i, purpose in enumerate(purposes):
            purpose_description += f"{i+1}. {purpose_descriptions[purpose]}\n"
        
        title_text = purpose_description
        submit_text = "Відповісти"
    else:
        purpose_description = (
            "Question 4/4:\n"
            "For what purpose do you usually stay at a hotel?\n"
            "*(Select at least 2 options)*\n\n"
        )
        
        # Динамічно генеруємо описи тільки для доступних цілей
        purpose_descriptions = {
            "Business travel": "**Business travel** (convenience for work, access to business centers)",
            "Vacation / relaxation": "**Vacation / relaxation** (comfort, entertainment, rest)",
            "Family vacation": "**Family vacation** (activities for children, family rooms)",
            "Long-term stay": "**Long-term stay** (home feeling, kitchen, laundry)"
        }
        
        # Додаємо описи тільки для доступних цілей
        for i, purpose in enumerate(purposes):
            purpose_description += f"{i+1}. {purpose_descriptions[purpose]}\n"
        
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
    # НЕ викликаємо query.answer() одразу!
    
    user_id = query.from_user.id
    callback_data = query.data
    
    # Якщо користувач натиснув "Відповісти"
    if callback_data == "purpose_submit":
        selected_purposes = user_data_global[user_id]['selected_purposes']
        lang = user_data_global[user_id]['language']
        
        # НОВА ПЕРЕВІРКА: мінімум 2 цілі з блокуванням та попапом
        if len(selected_purposes) < 2:
            if lang == 'uk':
                await query.answer("Будь ласка, оберіть мінімум 2 мети", show_alert=True)
            else:
                await query.answer("Please select at least 2 purposes", show_alert=True)
            return WAITING_PURPOSE_SUBMIT
        
        # Якщо все ОК - показуємо стандартне повідомлення
        await query.answer()
        
        # Зберігаємо вибрані цілі
        user_data_global[user_id]['purposes'] = selected_purposes
        
        # Видаляємо клавіатуру, але зберігаємо текст питання 4/4
        try:
            await query.edit_message_text(text=query.message.text, reply_markup=None, parse_mode="Markdown")
        except:
            await query.edit_message_text(text=query.message.text, reply_markup=None)
        
        # Надсилаємо НОВЕ повідомлення з підтвердженням вибору
        if lang == 'uk':
            purposes_list = '\n'.join(f"- {purpose};" for purpose in selected_purposes)
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Дякую! Ви обрали наступні цілі:\n{purposes_list}\n\n"
                "Зачекайте, будь ласка, поки я проаналізую ваші відповіді та підберу найкращі програми лояльності для вас."
            )
        else:
            purposes_list = '\n'.join(f"- {purpose};" for purpose in selected_purposes)
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"Thank you! You have chosen the following purposes:\n{purposes_list}\n\n"
                "Please wait while I analyze your answers and select the best loyalty programs for you."
            )
        
        # Очищуємо ID повідомлення з метою
        if 'purpose_message_id' in user_data_global[user_id]:
            del user_data_global[user_id]['purpose_message_id']
        
        # ДОДАЄМО ЗАТРИМКУ 3.0 СЕКУНДИ
        await asyncio.sleep(3.0)
        
        
        # ОНОВЛЕНО: Розрахунок і відображення результатів з рейтингами + збереження для /more
        return await calculate_and_show_results_with_ai(update, context)
    
    # Якщо це вибір або скасування вибору мети
    else:
        # Для вибору окремих цілей показуємо стандартну відповідь
        await query.answer()
        
        purpose = callback_data.replace("purpose_", "")
        
        # Перемикаємо стан вибору мети
        if purpose in user_data_global[user_id]['selected_purposes']:
            user_data_global[user_id]['selected_purposes'].remove(purpose)
        else:
            user_data_global[user_id]['selected_purposes'].append(purpose)
        
        # Оновлюємо клавіатуру з новим вибором
        return await ask_purpose(update, context)

# ===============================
# ЧАСТИНА 9: ФУНКЦІЇ MAPPING ГОТЕЛІВ ЗІ СТИЛЯМИ ТА МЕТОЮ + Google Maps API
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
            "Hyatt Regency", "Grand Hyatt"
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
            "Park Hyatt Hotels", "Grand Hyatt", "Hyatt Place",
            "Hyatt Regency", "Novotel Hotels"
        ],
        
        "Затишний і сімейний": [
            "Fairfield Inn & Suites", "DoubleTree by Hilton", 
            "Hampton by Hilton", "Holiday Inn Hotels & Resorts", 
            "Candlewood Suites", "Wyndham", "Days Inn by Wyndham", 
            "Mercure Hotels", "Novotel Hotels", "Quality Inn Hotels", 
            "Comfort Inn Hotels", "Hyatt House"
        ],
        
        "Практичний і стриманий": [
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
        "Practical and understated": style_mapping["Практичний і стриманий"]
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
                             "Ascend Hotel Collection", "Hilton Hotels & Resorts", "Wyndham Grand", "Grand Hyatt", "Sheraton", "DoubleTree by Hilton", 
                             "Holiday Inn Hotels & Resorts", "Mercure Hotels", "Novotel Hotels"],
        
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
# ЧАСТИНА 9.1:НОВІ ФУНКЦІЇ GOOGLE MAPS API
# ===============================

async def get_hotel_photos_and_link(place_id, api_key, max_photos=3):
    """
    Отримує фото готелю та посилання через Google Places Details API
    
    Args:
        place_id: Place ID готелю з Google Maps
        api_key: Google Maps API ключ
        max_photos: максимальна кількість фото (за замовчуванням 3)
    
    Returns:
        dict: {
            'photos': [список URL фото],
            'maps_link': 'посилання на Google Maps',
            'error': 'опис помилки (якщо є)'
        }
    """
    if not place_id or not api_key:
        return {'photos': [], 'maps_link': '', 'error': 'Missing place_id or API key'}
    
    try:
        # Налаштування для асинхронного HTTP запиту
        timeout = aiohttp.ClientTimeout(total=10)  # 10 секунд таймаут
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Запит деталей готелю
            details_params = {
                'place_id': place_id,
                'fields': 'name,photos,url,formatted_address',
                'key': api_key
            }
            
            debug_log(f"Запит деталей для Place ID: {place_id}")
            
            async with session.get(PLACES_DETAILS_URL, params=details_params) as response:
                if response.status != 200:
                    error_msg = f"HTTP {response.status} при запиті деталей"
                    logger.error(error_msg)
                    return {'photos': [], 'maps_link': '', 'error': error_msg}
                
                data = await response.json()
                
                # Перевіряємо статус відповіді API
                if data.get('status') != 'OK':
                    error_msg = f"API Error: {data.get('status')} - {data.get('error_message', 'Unknown error')}"
                    logger.error(error_msg)
                    return {'photos': [], 'maps_link': '', 'error': error_msg}
                
                result = data.get('result', {})
                
                # Отримуємо посилання на Google Maps
                maps_link = result.get('url', f"https://maps.google.com/?place_id={place_id}")
                
                # Отримуємо список фото
                photos_info = result.get('photos', [])
                photo_urls = []
                
                # Обмежуємо кількість фото
                photos_to_process = photos_info[:max_photos]
                
                for photo_info in photos_to_process:
                    photo_reference = photo_info.get('photo_reference')
                    if photo_reference:
                        # Формуємо URL для отримання фото
                        photo_url = f"{PLACES_PHOTOS_URL}?photo_reference={photo_reference}&maxwidth=800&key={api_key}"
                        photo_urls.append(photo_url)
                
                debug_log(f"Отримано {len(photo_urls)} фото для готелю {result.get('name', 'Unknown')}")
                
                return {
                    'photos': photo_urls,
                    'maps_link': maps_link,
                    'error': None
                }
                
    except asyncio.TimeoutError:
        error_msg = "Таймаут при запиті до Google Maps API"
        logger.error(error_msg)
        return {'photos': [], 'maps_link': '', 'error': error_msg}
    except Exception as e:
        error_msg = f"Помилка при запиті до Google Maps API: {str(e)}"
        logger.error(error_msg)
        return {'photos': [], 'maps_link': '', 'error': error_msg}

async def send_hotel_with_photos(context, chat_id, hotel_info, lang='uk', admin_mode=False):
    """
    Відправляє інформацію про готель з фото як медіагрупу
    
    Args:
        context: Telegram bot context
        chat_id: ID чату для відправлення
        hotel_info: словник з інформацією про готель
        lang: мова інтерфейсу
        admin_mode: чи показувати зважений рейтинг
    """
    try:
        place_id = hotel_info.get('place_id', '')
        hotel_name = hotel_info.get('name', 'N/A')
        hotel_brand = hotel_info.get('brand', 'N/A')
        address = hotel_info.get('address', 'N/A')
        rating = hotel_info.get('rating', 0.0)
        
        debug_log(f"Відправка готелю: {hotel_name} (Place ID: {place_id})")
        
        # Формуємо базовий опис готелю
        if admin_mode:
            # В адмін режимі показуємо зважений рейтинг
            description = f"🏨 {hotel_name}\n🏢 {hotel_brand}\n📍 {address}\n⭐ {rating:.2f} (зважений рейтинг)"
        else:
            # В звичайному режимі без рейтингу
            description = f"🏨 {hotel_name}\n🏢 {hotel_brand}\n📍 {address}"
        
        # Якщо API ключ доступний, намагаємося отримати фото
        if ENABLE_PHOTOS and place_id:
            photos_data = await get_hotel_photos_and_link(place_id, GOOGLE_MAPS_API_KEY, MAX_PHOTOS_PER_HOTEL)
            
            photos = photos_data.get('photos', [])
            maps_link = photos_data.get('maps_link', '')
            error = photos_data.get('error')
            
            if error:
                debug_log(f"Не вдалося отримати фото для {hotel_name}: {error}")
            
            # Якщо є фото, відправляємо як медіагрупу
            if photos:
                media_group = []
                
                for i, photo_url in enumerate(photos):
                    from telegram import InputMediaPhoto
                    if i == 0:
                        # Перше фото з описом
                        media_group.append(InputMediaPhoto(
                            media=photo_url,
                            caption=description
                        ))
                    else:
                        # Решта фото без опису
                        media_group.append(InputMediaPhoto(media=photo_url))
                
                try:
                    # Відправляємо медіагрупу
                    await context.bot.send_media_group(chat_id=chat_id, media=media_group)
                    
                    # Додаємо посилання на Google Maps окремим повідомленням
                    if maps_link:
                        if lang == 'uk':
                            link_text = f"📍 [Переглянути на Google Maps]({maps_link})"
                        else:
                            link_text = f"📍 [View on Google Maps]({maps_link})"
                        
                        await context.bot.send_message(
                            chat_id=chat_id, 
                            text=link_text, 
                            parse_mode="Markdown",
                            disable_web_page_preview=True
                        )
                    
                    debug_log(f"Успішно відправлено {len(photos)} фото для готелю {hotel_name}")
                    return True
                    
                except Exception as e:
                    logger.error(f"Помилка відправлення медіагрупи: {e}")
        
        # Якщо фото немає або сталася помилка, відправляємо текстове повідомлення
        fallback_text = description
        
        # Додаємо посилання на Google Maps, якщо доступне
        if place_id:
            maps_link = f"https://maps.google.com/?place_id={place_id}"
            if lang == 'uk':
                fallback_text += f"\n📍 [Переглянути на Google Maps]({maps_link})"
            else:
                fallback_text += f"\n📍 [View on Google Maps]({maps_link})"
        
        await context.bot.send_message(
            chat_id=chat_id, 
            text=fallback_text, 
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Помилка при відправленні готелю {hotel_info.get('name', 'Unknown')}: {e}")
        return False

def convert_hotel_dataframe_to_dict(hotel_row):
    """
    Конвертує рядок DataFrame готелю в словник для відправлення
    
    Args:
        hotel_row: pandas Series з даними готелю
    
    Returns:
        dict: словник з інформацією про готель
    """
    return {
        'name': str(hotel_row.get('hotel_name', 'N/A')),
        'brand': str(hotel_row.get('Hotel Brand', 'N/A')),
        'address': str(hotel_row.get('address', 'N/A')),
        'place_id': str(hotel_row.get('Place ID', '')),
        'rating': float(hotel_row.get('Weighted rating of each unique hotel', 0.0))
    }

async def send_hotels_for_program(context, chat_id, top_hotels, program_name, lang='uk', admin_mode=False):
    """
    Відправляє готелі для конкретної програми лояльності
    
    Args:
        context: Telegram bot context
        chat_id: ID чату
        top_hotels: DataFrame з готелями
        program_name: назва програми лояльності
        lang: мова
        admin_mode: режим адміністратора
    """
    if top_hotels.empty:
        if lang == 'uk':
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Не знайдено готелів, що відповідають всім вашим критеріям."
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ No hotels found matching all your criteria."
            )
        return
    
    # Відправляємо заголовок
    if lang == 'uk':
        header_text = f"🏆 Приклад готелю в сегменті {category}, що входить до програми {program_name}:"
    else:
        header_text = f"🏆 An example of a hotel in the {category} segment that is part of the {program_name} program:"
    
    await context.bot.send_message(chat_id=chat_id, text=header_text)
    
    # Короткочасна пауза перед відправленням готелів
    await asyncio.sleep(0.5)
    
    # Відправляємо кожен готель окремо з фото
    for i, (index, hotel) in enumerate(top_hotels.iterrows()):
        hotel_dict = convert_hotel_dataframe_to_dict(hotel)
        
        # Додаємо невелику паузу між готелями (хоча тепер тільки 1)
        if i > 0:
            await asyncio.sleep(1)
        
        await send_hotel_with_photos(context, chat_id, hotel_dict, lang, admin_mode)

async def add_hotels_to_results_with_photos(context, chat_id, user_data, scores_df, lang='uk', admin_mode=False):
    """
    Функція більше не потрібна, тому що готелі з фото інтегровані в основний звіт
    через нову функцію send_programs_with_integrated_hotels_and_photos
    """
    # Ця функція тепер порожня, оскільки готелі з фото відправляються разом зі звітом
    pass

# ===============================
# ЧАСТИНА 9.2: ОНОВЛЕНІ ФУНКЦІЇ АНАЛІЗУ ГОТЕЛІВ З FALLBACK-ЛОГІКОЮ
# ===============================

def convert_rating_column_to_numeric(df):
    """
    Конвертує колонку рейтингу в числовий формат (функція залишається без змін)
    
    Args:
        df: DataFrame з готелями
    
    Returns:
        DataFrame з правильним типом колонки рейтингу
    """
    if 'Weighted rating of each unique hotel' in df.columns:
        # Створюємо копію для безпеки
        df = df.copy()
        
        # ВИПРАВЛЕНО: Замінюємо коми на крапки (європейський формат -> американський)
        df['Weighted rating of each unique hotel'] = df['Weighted rating of each unique hotel'].astype(str).str.replace(',', '.')
        
        # Конвертуємо в числовий формат
        df['Weighted rating of each unique hotel'] = pd.to_numeric(
            df['Weighted rating of each unique hotel'], 
            errors='coerce'
        )
        
        # Заповнюємо NaN нулями тільки якщо конверсія не вдалася
        df['Weighted rating of each unique hotel'].fillna(0.0, inplace=True)
        
        debug_log(f"Converted rating column to numeric. Sample values: {df['Weighted rating of each unique hotel'].head().tolist()}")
    
    return df

def select_best_hotel_by_rating(hotels_df, max_count=1):
    """
    НОВА ФУНКЦІЯ: Вибирає найкращі готелі за зваженим рейтингом
    Замінює стару логіку диверсифікації брендів
    
    Args:
        hotels_df: DataFrame з готелями
        max_count: максимальна кількість готелів для вибору (зазвичай 1)
    
    Returns:
        DataFrame з обраними готелями, відсортованими за рейтингом
    """
    if hotels_df.empty:
        debug_log("Пустий DataFrame для вибору готелів")
        return hotels_df
    
    # Переконуємося, що рейтинг в числовому форматі
    if 'Weighted rating of each unique hotel' not in hotels_df.columns:
        debug_log("Відсутня колонка рейтингу, повертаємо перші готелі")
        return hotels_df.head(max_count)
    
    # Сортуємо за рейтингом (найкращі спочатку) 
    sorted_hotels = hotels_df.sort_values('Weighted rating of each unique hotel', ascending=False)
    
    # Беремо топ готелі
    top_hotels = sorted_hotels.head(max_count)
    
    debug_log(f"Обрано {len(top_hotels)} готель(ів) з найвищим рейтингом:")
    for idx, hotel in top_hotels.iterrows():
        hotel_name = hotel.get('hotel_name', 'Unknown')
        rating = hotel.get('Weighted rating of each unique hotel', 0)
        debug_log(f"  - {hotel_name}: {rating:.2f}★")
    
    return top_hotels

# ===============================
# НОВІ ДОПОМІЖНІ ФУНКЦІЇ ДЛЯ FALLBACK-СЦЕНАРІЇВ
# ===============================

def find_hotel_with_style_only(program_hotels, category, styles):
    """
    Fallback 2: Регіон + Сегмент + Стиль (БЕЗ фільтру за метою)
    
    Args:
        program_hotels: готелі програми після фільтрації за регіоном
        category: обрана категорія
        styles: обрані стилі
    
    Returns:
        DataFrame з відфільтрованими готелями
    """
    debug_log(f"FALLBACK 2: Пошук з фільтром тільки за стилем")
    
    # Фільтруємо за категорією
    if category:
        category_filtered = filter_hotels_by_category(program_hotels, category)
        debug_log(f"Після фільтрації за категорією {category}: {len(category_filtered)} готелів")
    else:
        category_filtered = program_hotels
    
    if category_filtered.empty:
        return pd.DataFrame()
    
    # Фільтруємо за стилем (БЕЗ мети)
    if styles:
        style_filtered = filter_hotels_by_style(category_filtered, styles)
        debug_log(f"Після фільтрації за стилями {styles}: {len(style_filtered)} готелів")
        return style_filtered
    else:
        return category_filtered

def find_hotel_with_purpose_only(program_hotels, category, purposes):
    """
    Fallback 3: Регіон + Сегмент + Мета (БЕЗ фільтру за стилем)
    
    Args:
        program_hotels: готелі програми після фільтрації за регіоном
        category: обрана категорія
        purposes: обрані мети
    
    Returns:
        DataFrame з відфільтрованими готелями
    """
    debug_log(f"FALLBACK 3: Пошук з фільтром тільки за метою")
    
    # Фільтруємо за категорією
    if category:
        category_filtered = filter_hotels_by_category(program_hotels, category)
        debug_log(f"Після фільтрації за категорією {category}: {len(category_filtered)} готелів")
    else:
        category_filtered = program_hotels
    
    if category_filtered.empty:
        return pd.DataFrame()
    
    # Фільтруємо за метою (БЕЗ стилю)
    if purposes:
        purpose_filtered = filter_hotels_by_purpose(category_filtered, purposes)
        debug_log(f"Після фільтрації за метами {purposes}: {len(purpose_filtered)} готелів")
        return purpose_filtered
    else:
        return category_filtered

def find_hotel_basic_filter(program_hotels, category):
    """
    Fallback 4: Регіон + Сегмент + Програма (БЕЗ стилю і мети)
    
    Args:
        program_hotels: готелі програми після фільтрації за регіоном
        category: обрана категорія
    
    Returns:
        DataFrame з відфільтрованими готелями
    """
    debug_log(f"FALLBACK 4: Пошук тільки за регіоном, сегментом та програмою")
    
    # Фільтруємо тільки за категорією
    if category:
        category_filtered = filter_hotels_by_category(program_hotels, category)
        debug_log(f"Після фільтрації за категорією {category}: {len(category_filtered)} готелів")
        return category_filtered
    else:
        debug_log(f"Повертаємо всі готелі програми: {len(program_hotels)} готелів")
        return program_hotels

def find_hotel_minimal_filter(filtered_by_region, program_name):
    """
    Fallback 5: Регіон + Програма (крайній випадок)
    
    Args:
        filtered_by_region: готелі після фільтрації за регіоном
        program_name: назва програми лояльності
    
    Returns:
        DataFrame з відфільтрованими готелями
    """
    debug_log(f"FALLBACK 5: Крайній випадок - тільки регіон + програма")
    
    # Фільтруємо тільки за програмою
    program_hotels = filtered_by_region[filtered_by_region['loyalty_program'] == program_name]
    debug_log(f"Після мінімальної фільтрації за програмою {program_name}: {len(program_hotels)} готелів")
    
    return program_hotels

# ===============================
# ГОЛОВНА ФУНКЦІЯ З FALLBACK-ЛОГІКОЮ
# ===============================

def find_top_1_hotel_for_program_strict(program_name, user_data, hotel_data):
    """
    ОНОВЛЕНА ФУНКЦІЯ з 5-рівневою fallback-логікою для знаходження топ-1 готелю
    
    ПРІОРИТЕТИ ПОШУКУ:
    1. Ідеальний збіг: Регіон + Сегмент + Стиль + Мета
    2. Fallback 1: Регіон + Сегмент + Мета (якщо стиль = 0)
    3. Fallback 2: Регіон + Сегмент + Стиль (якщо мета = 0)  
    4. Fallback 3: Регіон + Сегмент (якщо стиль = 0 і мета = 0)
    5. Fallback 4: Регіон + Програма (крайній випадок)
    
    Args:
        program_name: назва програми лояльності
        user_data: дані користувача з відповідями
        hotel_data: повні дані готелів
    
    Returns:
        tuple: (DataFrame з топ-1 готелем, тип вибірки)
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
    
    debug_log(f"=== FALLBACK ПОШУК для програми: {program_name} ===")
    debug_log(f"Критерії: regions={english_regions}, category={category}, styles={english_styles}, purposes={english_purposes}")
    
    # БАЗОВА ФІЛЬТРАЦІЯ (для всіх сценаріїв)
    # 1. Фільтруємо за регіоном
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions, english_countries)
    debug_log(f"Після фільтрації за регіоном: {len(filtered_by_region)} готелів")
    
    # 2. Фільтруємо за програмою лояльності
    program_hotels = filtered_by_region[filtered_by_region['loyalty_program'] == program_name]
    debug_log(f"Після фільтрації за програмою {program_name}: {len(program_hotels)} готелів")
    
    if program_hotels.empty:
        debug_log(f"❌ Немає готелів для програми {program_name} в обраних регіонах")
        return pd.DataFrame(), "no_hotels"
    
    # СЦЕНАРІЙ 1: ІДЕАЛЬНИЙ ЗБІГ - Регіон + Сегмент + Стиль + Мета
    debug_log(f"🎯 СЦЕНАРІЙ 1: Спроба ідеального збігу")
    
    # Фільтруємо за категорією
    if category:
        category_filtered = filter_hotels_by_category(program_hotels, category)
        debug_log(f"Після фільтрації за категорією {category}: {len(category_filtered)} готелів")
    else:
        category_filtered = program_hotels
    
    if not category_filtered.empty:
        # Фільтруємо за стилем
        if english_styles:
            style_filtered = filter_hotels_by_style(category_filtered, english_styles)
            debug_log(f"Після фільтрації за стилями: {len(style_filtered)} готелів")
        else:
            style_filtered = category_filtered
        
        if not style_filtered.empty:
            # Фільтруємо за метою
            if english_purposes:
                purpose_filtered = filter_hotels_by_purpose(style_filtered, english_purposes)
                debug_log(f"Після фільтрації за метами: {len(purpose_filtered)} готелів")
            else:
                purpose_filtered = style_filtered
            
            if not purpose_filtered.empty:
                # ✅ ІДЕАЛЬНИЙ ЗБІГ ЗНАЙДЕНО!
                purpose_filtered = convert_rating_column_to_numeric(purpose_filtered)
                top_1_hotel = select_best_hotel_by_rating(purpose_filtered, 1)
                
                if not top_1_hotel.empty:
                    debug_log(f"✅ СЦЕНАРІЙ 1 УСПІШНИЙ: Ідеальний збіг знайдено!")
                    debug_log(f"Обрано готель: {top_1_hotel.iloc[0].get('hotel_name')} з рейтингом {top_1_hotel.iloc[0].get('Weighted rating of each unique hotel', 0):.2f}")
                    return top_1_hotel, "perfect_match"
    
    # СЦЕНАРІЙ 2: FALLBACK 1 - Регіон + Сегмент + Мета (якщо стиль = 0)
    debug_log(f"🔄 СЦЕНАРІЙ 2: Fallback - пошук БЕЗ фільтру за стилем")
    
    if english_purposes:  # Тільки якщо є мети
        purpose_only_filtered = find_hotel_with_purpose_only(program_hotels, category, english_purposes)
        
        if not purpose_only_filtered.empty:
            purpose_only_filtered = convert_rating_column_to_numeric(purpose_only_filtered)
            top_1_hotel = select_best_hotel_by_rating(purpose_only_filtered, 1)
            
            if not top_1_hotel.empty:
                debug_log(f"✅ СЦЕНАРІЙ 2 УСПІШНИЙ: Збіг без стилю знайдено!")
                debug_log(f"Обрано готель: {top_1_hotel.iloc[0].get('hotel_name')} з рейтингом {top_1_hotel.iloc[0].get('Weighted rating of each unique hotel', 0):.2f}")
                return top_1_hotel, "purpose_only_match"
    
    # СЦЕНАРІЙ 3: FALLBACK 2 - Регіон + Сегмент + Стиль (якщо мета = 0)
    debug_log(f"🔄 СЦЕНАРІЙ 3: Fallback - пошук БЕЗ фільтру за метою")
    
    if english_styles:  # Тільки якщо є стилі
        style_only_filtered = find_hotel_with_style_only(program_hotels, category, english_styles)
        
        if not style_only_filtered.empty:
            style_only_filtered = convert_rating_column_to_numeric(style_only_filtered)
            top_1_hotel = select_best_hotel_by_rating(style_only_filtered, 1)
            
            if not top_1_hotel.empty:
                debug_log(f"✅ СЦЕНАРІЙ 3 УСПІШНИЙ: Збіг без мети знайдено!")
                debug_log(f"Обрано готель: {top_1_hotel.iloc[0].get('hotel_name')} з рейтингом {top_1_hotel.iloc[0].get('Weighted rating of each unique hotel', 0):.2f}")
                return top_1_hotel, "style_only_match"
    
    # СЦЕНАРІЙ 4: FALLBACK 3 - Регіон + Сегмент (БЕЗ стилю і мети)
    debug_log(f"🔄 СЦЕНАРІЙ 4: Fallback - тільки регіон + сегмент + програма")
    
    basic_filtered = find_hotel_basic_filter(program_hotels, category)
    
    if not basic_filtered.empty:
        basic_filtered = convert_rating_column_to_numeric(basic_filtered)
        top_1_hotel = select_best_hotel_by_rating(basic_filtered, 1)
        
        if not top_1_hotel.empty:
            debug_log(f"✅ СЦЕНАРІЙ 4 УСПІШНИЙ: Базовий збіг знайдено!")
            debug_log(f"Обрано готель: {top_1_hotel.iloc[0].get('hotel_name')} з рейтингом {top_1_hotel.iloc[0].get('Weighted rating of each unique hotel', 0):.2f}")
            return top_1_hotel, "basic_match"
    
    # СЦЕНАРІЙ 5: FALLBACK 4 - Регіон + Програма (крайній випадок)
    debug_log(f"🔄 СЦЕНАРІЙ 5: Крайній fallback - тільки регіон + програма")
    
    minimal_filtered = find_hotel_minimal_filter(filtered_by_region, program_name)
    
    if not minimal_filtered.empty:
        minimal_filtered = convert_rating_column_to_numeric(minimal_filtered)
        top_1_hotel = select_best_hotel_by_rating(minimal_filtered, 1)
        
        if not top_1_hotel.empty:
            debug_log(f"✅ СЦЕНАРІЙ 5 УСПІШНИЙ: Мінімальний збіг знайдено!")
            debug_log(f"Обрано готель: {top_1_hotel.iloc[0].get('hotel_name')} з рейтингом {top_1_hotel.iloc[0].get('Weighted rating of each unique hotel', 0):.2f}")
            return top_1_hotel, "minimal_match"
    
    # КРАЙНІЙ ВИПАДОК: Нічого не знайдено
    debug_log(f"❌ ВСІ СЦЕНАРІЇ НЕВДАЛІ: Не знайдено жодного готелю для програми {program_name}")
    return pd.DataFrame(), "no_match"

# ===============================
# ФУНКЦІЇ ФОРМАТУВАННЯ (БЕЗ ЗМІН)
# ===============================

def format_hotel_examples_for_integration(top_hotels, program_name, lang='uk'):
    """
    Форматує інформацію про топ-1 готель для звичайного режиму та /more
    БЕЗ зваженого рейтингу
    
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
        result = f"\n🏆 Ось приклад кращого готелю цієї програми, який відповідає вашому запиту:\n\n"
    else:
        result = f"\n🏆 Here is the best hotel from this program that matches your request:\n\n"
    
    for i, (index, hotel) in enumerate(top_hotels.iterrows()):
        # Простий текст БЕЗ рейтингу в звичайному режимі
        hotel_name = str(hotel.get('hotel_name', 'N/A'))
        hotel_brand = str(hotel.get('Hotel Brand', 'N/A'))
        address = str(hotel.get('address', 'N/A'))
        place_id = str(hotel.get('Place ID', ''))
        
        result += f"{i+1}. {hotel_name}\n"
        result += f"   🏢 {hotel_brand}\n"
        result += f"   📍 {address}\n"
        result += f"   🔗 Place ID: {place_id}\n\n"
    
    return result

def format_hotel_examples_for_admin(top_hotels, program_name, lang='uk'):
    """
    Форматує інформацію про топ-1 готель для АДМІНІСТРАТИВНОГО режиму (/21)
    З відображенням зваженого рейтингу
    
    Args:
        top_hotels: DataFrame з готелями
        program_name: назва програми лояльності
        lang: мова інтерфейсу
    
    Returns:
        str: відформатований текст для адмін-звіту
    """
    if top_hotels.empty:
        if lang == 'uk':
            return "\n❌ Не знайдено готелів, що відповідають всім вашим критеріям."
        else:
            return "\n❌ No hotels found matching all your criteria."
    
    if lang == 'uk':
        result = f"\n🏆 Ось приклад кращого готелю цієї програми, який відповідає вашому запиту:\n\n"
    else:
        result = f"\n🏆 Here is the best hotel from this program that matches your request:\n\n"
    
    for i, (index, hotel) in enumerate(top_hotels.iterrows()):
        hotel_name = str(hotel.get('hotel_name', 'N/A'))
        hotel_brand = str(hotel.get('Hotel Brand', 'N/A'))
        rating = float(hotel.get('Weighted rating of each unique hotel', 0))
        address = str(hotel.get('address', 'N/A'))
        place_id = str(hotel.get('Place ID', ''))
        
        # АДМІН РЕЖИМ: Показуємо зважений рейтинг з 2 знаками після коми
        result += f"{i+1}. {hotel_name} ⭐{rating:.2f} (зважений рейтинг)\n"
        result += f"   🏢 {hotel_brand}\n"
        result += f"   📍 {address}\n"
        result += f"   🔗 Place ID: {place_id}\n\n"
    
    return result

def add_hotels_to_results(detailed_results, user_data, scores_df, lang='uk', admin_mode=False):
    """
    Додає готелі до детального звіту для кожної програми
    ВИПРАВЛЕНО: Роздільники МІЖ програмами, але НЕ перед готелями
    """
    # Розбиваємо звіт на секції для кожної програми
    sections = detailed_results.split("=" * 50)
    
    # В адмін-режимі показуємо всі 7 програм, інакше топ-3
    top_programs = scores_df.head(7) if admin_mode else scores_df.head(3)
    
    result = ""
    
    for i, section in enumerate(sections):
        if i >= len(top_programs):
            break
            
        # Додаємо секцію програми
        result += section
        
        try:
            program_name = top_programs.iloc[i]['loyalty_program']
            
            # ОНОВЛЕНО: Використовуємо нову функцію з fallback-логікою
            top_hotels, selection_type = find_top_1_hotel_for_program_strict(program_name, user_data, hotel_data)
            
            # Використовуємо різні функції форматування залежно від режиму
            if admin_mode:
                hotels_text = format_hotel_examples_for_admin(top_hotels, program_name, lang)
            else:
                hotels_text = format_hotel_examples_for_integration(top_hotels, program_name, lang)
            
            # ВИПРАВЛЕНО: Додаємо готелі БЕЗ роздільника
            result += hotels_text
            
        except Exception as e:
            debug_log(f"Помилка додавання готелів для програми: {e}")
        
        # ВИПРАВЛЕНО: Додаємо роздільник МІЖ програмами (після готелів)
        if i < len(top_programs) - 1:
            result += "\n" + "=" * 50 + "\n"
    
    return result

# ===============================
# ЧАСТИНА 9.3: НОВІ ФУНКЦІЇ ДЛЯ ІНТЕГРАЦІЇ ГОТЕЛІВ З AI-ОПИСАМИ
# ===============================

async def send_hotel_with_ai_description(context, chat_id, hotel_info, user_styles, user_purposes, program_name, lang='uk'):
    """
    ОНОВЛЕНА функція відправлення готелю з AI-описом та посиланням на реєстрацію
    
    Args:
        context: Telegram bot context
        chat_id: ID чату
        hotel_info: словник з інформацією про готель
        user_styles: обрані користувачем стилі
        user_purposes: обрані користувачем цілі
        program_name: назва програми лояльності
        lang: мова
    """
    try:
        hotel_name = hotel_info.get('name', 'N/A')
        hotel_brand = hotel_info.get('brand', 'N/A')
        place_id = hotel_info.get('place_id', '')
        
        debug_log(f"Генерація AI-опису для готелю: {hotel_name}")
        
        # Генеруємо AI-опис
        ai_description = await generate_hotel_description(
            hotel_name, hotel_brand, user_styles, user_purposes, lang
        )
        
        # Формуємо підпис з AI-описом
        caption = format_hotel_caption_with_ai_description(hotel_info, ai_description, lang)
        
        # Якщо API ключ доступний і є Place ID, намагаємося отримати фото
        if ENABLE_PHOTOS and place_id:
            photos_data = await get_hotel_photos_and_link(place_id, GOOGLE_MAPS_API_KEY, MAX_PHOTOS_PER_HOTEL)
            
            photos = photos_data.get('photos', [])
            error = photos_data.get('error')
            
            if error:
                debug_log(f"Не вдалося отримати фото для {hotel_name}: {error}")
            
            # Якщо є фото, відправляємо як медіагрупу з AI-описом
            if photos:
                media_group = []
                
                for i, photo_url in enumerate(photos):
                    from telegram import InputMediaPhoto
                    if i == 0:
                        # Перше фото з AI-описом
                        media_group.append(InputMediaPhoto(
                            media=photo_url,
                            caption=caption
                        ))
                    else:
                        # Решта фото без опису
                        media_group.append(InputMediaPhoto(media=photo_url))
                
                try:
                    # Відправляємо медіагрупу
                    await context.bot.send_media_group(chat_id=chat_id, media=media_group)
                    
                    # ЗАМІНЕНО: Додаємо посилання на реєстрацію замість Google Maps
                    registration_link = LOYALTY_PROGRAM_REGISTRATION_LINKS.get(program_name)
                    if registration_link:
                        if lang == 'uk':
                            link_text = f"Щоб зареєструватися в програмі лояльності {program_name} – [натисніть тут]({registration_link})."
                        else:
                            link_text = f"To register for {program_name} loyalty program – [click here]({registration_link})."
                        
                        await context.bot.send_message(
                            chat_id=chat_id, 
                            text=link_text, 
                            parse_mode="Markdown",
                            disable_web_page_preview=True
                        )
                    
                    debug_log(f"Успішно відправлено {len(photos)} фото з AI-описом для готелю {hotel_name}")
                    return True
                    
                except Exception as e:
                    logger.error(f"Помилка відправлення медіагрупи з AI-описом: {e}")
        
        # Fallback: текстове повідомлення з AI-описом
        fallback_text = caption
        
        # ЗАМІНЕНО: Додаємо посилання на реєстрацію замість Google Maps
        registration_link = LOYALTY_PROGRAM_REGISTRATION_LINKS.get(program_name)
        if registration_link:
            if lang == 'uk':
                fallback_text += f"\n\nЩоб зареєструватися в програмі лояльності {program_name} – [натисніть тут]({registration_link})."
            else:
                fallback_text += f"\n\nTo register for {program_name} loyalty program – [click here]({registration_link})."
        
        await context.bot.send_message(
            chat_id=chat_id, 
            text=fallback_text, 
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Помилка при відправленні готелю з AI-описом {hotel_info.get('name', 'Unknown')}: {e}")
        return False

async def send_individual_hotels_with_ai_descriptions(context, chat_id, top_hotels, user_styles, user_purposes, program_name, lang='uk'):
    """
    ОНОВЛЕНА функція відправлення готелів з AI-описами та назвою програми
    """
    try:
        for i, (index, hotel) in enumerate(top_hotels.iterrows()):
            hotel_dict = convert_hotel_dataframe_to_dict(hotel)
            
            # Відправляємо готель з AI-описом та program_name
            await send_hotel_with_ai_description(
                context, chat_id, hotel_dict, user_styles, user_purposes, program_name, lang
            )
            
            # Пауза між готелями (хоча тепер тільки 1)
            if i < len(top_hotels) - 1:
                await asyncio.sleep(1.5)  # Трохи більша пауза для AI генерації
                
    except Exception as e:
        logger.error(f"Помилка при відправленні готелів з AI-описами: {e}")

async def send_programs_with_ai_integrated_hotels(context, chat_id, user_data, scores_df, lang='uk'):
    """
    ОНОВЛЕНА функція відправлення програм з AI-інтегрованими готелями
    """
    try:
        # Беремо топ-3 програми
        top_programs = scores_df.head(3)
        
        # Отримуємо стилі та цілі користувача для AI
        user_styles = user_data.get('styles', [])
        user_purposes = user_data.get('purposes', [])
        category = user_data.get('category')
        
        for i, (index, row) in enumerate(top_programs.iterrows()):
            program_name = row['loyalty_program']
            
            # Замінюємо назву програми для відображення
            if program_name == "IHG One Rewards":
                display_program_name = "InterContinental Hotels One Rewards"
            else:
                display_program_name = program_name
            
            # 1. Відправляємо звіт про програму
            program_report = format_single_program_report(user_data, row, i, lang)
            await send_long_message_to_chat(context, chat_id, program_report)
            
            # Невелика пауза
            await asyncio.sleep(0.5)
            
            # 2. Відправляємо заголовок готелів
            if lang == 'uk':
                hotels_header = f"🏆 Приклад готелю в сегменті {category}, що входить до програми {display_program_name}:"
            else:
                hotels_header = f"🏆 Example hotel in {category} segment, part of {display_program_name} program:"
            
            await context.bot.send_message(chat_id=chat_id, text=hotels_header)
            
            # Невелика пауза
            await asyncio.sleep(0.5)
            
            # 3. Знаходимо та відправляємо кожен готель з AI-описом
            top_hotels, selection_type = find_top_1_hotel_for_program_strict(program_name, user_data, hotel_data)
            
            if not top_hotels.empty:
                # ОНОВЛЕНО: передаємо program_name для посилань на реєстрацію
                await send_individual_hotels_with_ai_descriptions(
                    context, chat_id, top_hotels, user_styles, user_purposes, program_name, lang
                )
            else:
                if lang == 'uk':
                    await context.bot.send_message(chat_id=chat_id, text="❌ Не знайдено готелів, що відповідають вашим критеріям.")
                else:
                    await context.bot.send_message(chat_id=chat_id, text="❌ No hotels found matching your criteria.")
            
            # Пауза між програмами
            if i < len(top_programs) - 1:
                await asyncio.sleep(2)
                
    except Exception as e:
        logger.error(f"Помилка при відправленні програм з AI-готелями: {e}")

def format_single_program_report(user_data, program_row, position, lang='uk'):
    """
    Форматує звіт для однієї програми
    """
    program = program_row['loyalty_program']
    
    # Замінюємо назву програми для відображення
    if program == "IHG One Rewards":
        display_program_name = "InterContinental Hotels One Rewards"
    else:
        display_program_name = program
    
    # Визначаємо емодзі та назву позиції + ДОДАЄМО ПОЯСНЕННЯ
    if position == 0:
        emoji = "🥇"
        position_text = "Топ 1" if lang == 'uk' else "Top 1"
        explanation = "– містить найбільше збігів з вашими критеріями." if lang == 'uk' else "– contains the most matches with your criteria."
    elif position == 1:
        emoji = "🥈"
        position_text = "Топ 2" if lang == 'uk' else "Top 2"
        explanation = "– друге місце за кількістю збігів з вашими критеріями." if lang == 'uk' else "– second place in matches with your criteria."
    else:
        emoji = "🥉"
        position_text = "Топ 3" if lang == 'uk' else "Top 3"
        explanation = "– третє місце за кількістю збігів з вашими критеріями." if lang == 'uk' else "– third place in matches with your criteria."
    
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
    
    # ВИПРАВЛЕНО: додаємо пояснення з нового рядка
    if lang == 'uk':
        result = f"{emoji} {position_text} – {display_program_name}\n{explanation}\n\n"
        result += f"⭐{program_row['program_rating']:.2f} – середній рейтинг готелів, що входять до програми\n"
        result += f"(на основі відгуків з Google Maps):\n\n"
    else:
        result = f"{emoji} {position_text} – {display_program_name}\n{explanation}\n\n"
        result += f"⭐{program_row['program_rating']:.2f} – average rating of hotels in the program\n"
        result += f"(based on Google Maps reviews):\n\n"
    
    # РЕГІОН - ВИПРАВЛЕНО: змінюємо текст
    if lang == 'uk':
        region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
        result += f"📍 Регіон: {region_str}\n"
        result += f" • {program_row['region_hotels']} готелів, до яких входять такі бренди:\n"  # ЗМІНЕНО
        
        brands_in_region = get_brands_in_region_for_program(program, regions, countries, hotel_data)
        if brands_in_region:
            for brand in brands_in_region:
                result += f"   • {brand}\n"
        else:
            result += "   • Бренди не знайдено\n"
        result += "\n"
    else:
        region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
        result += f"📍 Region: {region_str}\n"
        result += f" • {program_row['region_hotels']} hotels, which include such brands:\n"  # ЗМІНЕНО
        
        brands_in_region = get_brands_in_region_for_program(program, regions, countries, hotel_data)
        if brands_in_region:
            for brand in brands_in_region:
                result += f"   • {brand}\n"
        else:
            result += "   • No brands found\n"
        result += "\n"
    
    # КАТЕГОРІЯ - ВИПРАВЛЕНО: новий формат
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
        
        # ВИПРАВЛЕНО: новий формат з тире
        if lang == 'uk':
            result += f"🏨 Сегмент:\n"
            result += f"– {main_count} готелів {category} (сегмент обраний вами)\n"  # ЗМІНЕНО
            if adjacent_details:
                adj_cats_str = ', '.join(adjacent_details)  # ЗМІНЕНО: кома замість "і"
                result += f"– {adjacent_total} готелів {adj_cats_str} (суміжні до обраного)\n\n"  # ЗМІНЕНО
            else:
                result += "\n"
        else:
            result += f"🏨 Segment:\n"
            result += f"– {main_count} hotels {category} (segment selected by you)\n"  # ЗМІНЕНО
            if adjacent_details:
                adj_cats_str = ', '.join(adjacent_details)  # ЗМІНЕНО: кома замість "and"
                result += f"– {adjacent_total} hotels {adj_cats_str} (adjacent to selected)\n\n"  # ЗМІНЕНО
            else:
                result += "\n"
    
    # СТИЛЬ - залишається без змін
    if styles:
        if lang == 'uk':
            styles_str = '; '.join(styles)
            result += f"🎨 Стиль, позиціонування:\n{styles_str}.\n"
        else:
            styles_str = '; '.join(styles)
            result += f"🎨 Style, positioning:\n{styles_str}.\n"
        
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
            result += f"  - {main_style_total} готелів в обраних стилях, в категорії {category}\n"
            if adjacent_categories_list:
                adj_cats_str = ' і '.join(adjacent_categories_list)
                result += f"  - {adjacent_style_total} готелів в обраних стилях, в суміжних категоріях ({adj_cats_str})\n\n"
            else:
                result += "\n"
        else:
            result += f"  - {main_style_total} hotels in selected styles, in {category} category\n"
            if adjacent_categories_list:
                adj_cats_str = ' and '.join(adjacent_categories_list)
                result += f"  - {adjacent_style_total} hotels in selected styles, in adjacent categories ({adj_cats_str})\n\n"
            else:
                result += "\n"
    
    # МЕТА - ВИПРАВЛЕНО: змінено "Ціль" на "Мета" 
    if purposes:
        if lang == 'uk':
            purposes_str = '; '.join(purposes)
            result += f"🎯 Мета подорожі:\n{purposes_str}:\n"  # ЗМІНЕНО: "Ціль" -> "Мета"
        else:
            purposes_str = '; '.join(purposes)
            result += f"🎯 Travel purpose:\n{purposes_str}:\n"
        
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
            result += f"  - {main_purpose_total} готелів в обраних цілях, в категорії {category}\n"
            if adjacent_categories_list:
                adj_cats_str = ' і '.join(adjacent_categories_list)
                result += f"  - {adjacent_purpose_total} готелів в обраних цілях, в суміжних категоріях ({adj_cats_str})\n"
        else:
            result += f"  - {main_purpose_total} hotels for selected purposes, in {category} category\n"
            if adjacent_categories_list:
                adj_cats_str = ' and '.join(adjacent_categories_list)
                result += f"  - {adjacent_purpose_total} hotels for selected purposes, in adjacent categories ({adj_cats_str})\n"
    
    return result

# ДОДАНО: Функція для команд /more з AI-описами (без готелів, тільки звіт)
async def add_hotels_to_results_with_photos(context, chat_id, user_data, scores_df, lang='uk', admin_mode=False):
    """
    Функція більше не потрібна, тому що готелі з AI-описами інтегровані в основний звіт
    через нову функцію send_programs_with_ai_integrated_hotels
    """
    # Ця функція тепер порожня, оскільки готелі з AI-описами відправляються разом зі звітом
    pass


# ===============================
# ЧАСТИНА 10: ВИПРАВЛЕНІ ФУНКЦІЇ РОЗРАХУНКУ БАЛІВ ТА ГОЛОВНІ ФУНКЦІЇ З AI
# ===============================

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

def get_brands_in_region_for_program(program_name, regions=None, countries=None, hotel_data=None):
    """
    Отримує список унікальних брендів для програми лояльності в обраних регіонах
    
    Args:
        program_name: назва програми лояльності
        regions: список регіонів
        countries: список країн
        hotel_data: дані готелів
    
    Returns:
        list: відсортований список унікальних брендів
    """
    if hotel_data is None:
        return []
    
    # Переводимо регіони на англійську для фільтрації
    english_regions = translate_regions_to_english(regions) if regions else []
    english_countries = translate_regions_to_english(countries) if countries else []
    
    # Фільтруємо за регіоном
    filtered_by_region = filter_hotels_by_region(hotel_data, english_regions, english_countries)
    
    # Фільтруємо за програмою лояльності
    program_hotels = filtered_by_region[filtered_by_region['loyalty_program'] == program_name]
    
    if program_hotels.empty:
        return []
    
    # Отримуємо унікальні бренди та сортуємо їх
    if 'Hotel Brand' in program_hotels.columns:
        unique_brands = program_hotels['Hotel Brand'].dropna().unique().tolist()
        
        # ВИПРАВЛЕНО: Очищуємо від зайвих символів та сортуємо
        cleaned_brands = []
        for brand in unique_brands:
            # Очищуємо від зайвих пробілів та символів
            cleaned_brand = str(brand).strip()
            if cleaned_brand and cleaned_brand not in cleaned_brands:
                cleaned_brands.append(cleaned_brand)
        
        # Сортуємо бренди в алфавітному порядку
        cleaned_brands.sort()
        
        debug_log(f"Знайдено {len(cleaned_brands)} унікальних брендів для програми {program_name} в регіонах {english_regions}")
        return cleaned_brands
    
    return []

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
                # ВИПРАВЛЕНО: Підраховуємо готелі з усіх обраних регіонів для кожної програми
                region_counts = {}
                
                # Отримуємо всі програми лояльності
                all_programs = df['loyalty_program'].unique()
                
                for program in all_programs:
                    total_hotels = 0
                    program_data = df[df['loyalty_program'] == program]
                    
                    # Додаємо готелі з кожного обраного регіону
                    for region in regions:
                        # Фільтруємо за регіоном (регістронезалежний пошук)
                        region_data = program_data[program_data['region'].str.contains(region, case=False, na=False)]
                        
                        if not region_data.empty:
                            # Беремо унікальні записи для цього регіону (щоб уникнути дублювання)
                            unique_region_data = region_data.drop_duplicates(['loyalty_program', 'region'])
                            
                            # Додаємо кількість готелів з цього регіону
                            region_hotels = unique_region_data['Total hotels of Corporation / Loyalty Program in this region'].sum()
                            total_hotels += region_hotels
                            
                            debug_log(f"Програма {program}, регіон {region}: {region_hotels} готелів")
                    
                    region_counts[program] = total_hotels
                    debug_log(f"Програма {program} - загальна кількість з усіх регіонів: {total_hotels}")
                    
            else:
                # Fallback: якщо колонка відсутня, використовуємо кількість рядків
                region_counts = {}
                all_programs = df['loyalty_program'].unique()
                
                for program in all_programs:
                    total_count = 0
                    program_data = df[df['loyalty_program'] == program]
                    
                    for region in regions:
                        region_data = program_data[program_data['region'].str.contains(region, case=False, na=False)]
                        total_count += len(region_data)
                    
                    region_counts[program] = total_count
                
                logger.warning("Колонка 'Total hotels of Corporation / Loyalty Program in this region' відсутня. Використовуємо кількість рядків.")
        
        elif countries and len(countries) > 0:
            if 'Total hotels of Corporation / Loyalty Program in this country' in df.columns:
                # ВИПРАВЛЕНО: Аналогічна логіка для країн
                region_counts = {}
                all_programs = df['loyalty_program'].unique()
                
                for program in all_programs:
                    total_hotels = 0
                    program_data = df[df['loyalty_program'] == program]
                    
                    # Додаємо готелі з кожної обраної країни
                    for country in countries:
                        # Фільтруємо за країною (регістронезалежний пошук)
                        country_data = program_data[program_data['country'].str.contains(country, case=False, na=False)]
                        
                        if not country_data.empty:
                            # Беремо унікальні записи для цієї країни
                            unique_country_data = country_data.drop_duplicates(['loyalty_program', 'country'])
                            
                            # Додаємо кількість готелів з цієї країни
                            country_hotels = unique_country_data['Total hotels of Corporation / Loyalty Program in this country'].sum()
                            total_hotels += country_hotels
                            
                            debug_log(f"Програма {program}, країна {country}: {country_hotels} готелів")
                    
                    region_counts[program] = total_hotels
                    debug_log(f"Програма {program} - загальна кількість з усіх країн: {total_hotels}")
                    
            else:
                # Fallback для країн
                region_counts = {}
                all_programs = df['loyalty_program'].unique()
                
                for program in all_programs:
                    total_count = 0
                    program_data = df[df['loyalty_program'] == program]
                    
                    for country in countries:
                        country_data = program_data[program_data['country'].str.contains(country, case=False, na=False)]
                        total_count += len(country_data)
                    
                    region_counts[program] = total_count
                
                logger.warning("Колонка 'Total hotels of Corporation / Loyalty Program in this country' відсутня. Використовуємо кількість рядків.")
        
        else:
            return {}, {}
        
        # Переводимо в float та заповнюємо NaN як 0
        region_counts = {prog: float(count) if pd.notna(count) else 0.0 for prog, count in region_counts.items()}
        
        # Логування фінальних підрахунків
        debug_log(f"Фінальні підрахунки готелів по регіонах: {region_counts}")
        
        # Використовуємо константи з ЧАСТИНИ 2
        region_scores = distribute_scores_with_ties(region_counts, REGION_POINTS)
        
        # НЕ НОРМАЛІЗУЄМО кількість готелів - тільки бали для множинних регіонів
        normalization_factor = 1.0
        if regions and len(regions) > 1:
            normalization_factor = float(len(regions))
        elif countries and len(countries) > 1:
            normalization_factor = float(len(countries))
        
        if normalization_factor > 1.0:
            # Нормалізуємо тільки БАЛИ, а не кількість готелів
            region_scores = {program: score / normalization_factor for program, score in region_scores.items()}
            debug_log(f"Нормалізація балів (не кількості готелів) на фактор {normalization_factor}")
        
        # ВИПРАВЛЕНО: Повертаємо як бали, так і кількість готелів
        return region_scores, region_counts
                
    except Exception as e:
        logger.error(f"Помилка обчислення балів за регіоном: {e}")
        return {}, {}

def calculate_scores_with_ratings(user_data, hotel_data):
    """
    НОВА функція розрахунку балів з урахуванням рейтингів програм лояльності
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
    base_scores_df['base_score'] = base_scores_df['total_score'].copy()  # Зберігаємо базовий бал
    
    # Застосовуємо рейтинг-коефіцієнти до кожної програми
    for index, row in base_scores_df.iterrows():
        program = row['loyalty_program']
        base_score = row['base_score']
        
        # Отримуємо рейтинг програми
        program_rating = get_program_rating(program)
        rating_coefficient = calculate_rating_coefficient(program_rating)
        
        # Розраховуємо фінальний бал з урахуванням рейтингу
        final_score = base_score * rating_coefficient
        
        # Оновлюємо DataFrame
        base_scores_df.at[index, 'program_rating'] = program_rating
        base_scores_df.at[index, 'rating_coefficient'] = rating_coefficient
        base_scores_df.at[index, 'total_score'] = final_score
        
        debug_log(f"{program}: base={base_score:.2f}, rating={program_rating:.2f}, "
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
    ВИПРАВЛЕНА БАЗОВА функція розрахунку балів БЕЗ рейтингів з правильним підрахунком регіонів
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
    
    # ВИПРАВЛЕНО: Розподіляємо бали за регіонами/країнами з правильним підрахунком
    region_scores, region_hotel_counts = get_region_score(filtered_by_region, english_regions, english_countries)
    debug_log(f"Region scores: {region_scores}")
    debug_log(f"Region hotel counts: {region_hotel_counts}")
    
    for index, row in scores_df.iterrows():
        program = row['loyalty_program']
        scores_df.at[index, 'region_score'] = region_scores.get(program, 0.0)
        # ВИПРАВЛЕНО: Використовуємо правильний підрахунок готелів з region_hotel_counts
        scores_df.at[index, 'region_hotels'] = region_hotel_counts.get(program, 0)
    
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

# ===============================
# ОНОВЛЕНА ГОЛОВНА ФУНКЦІЯ З AI ІНТЕГРАЦІЄЮ
# ===============================

async def calculate_and_show_results_with_ai(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    ОНОВЛЕНА функція обчислення та відображення результатів з AI-описами
    """
    
    user_id = update.effective_user.id
    user_data = user_data_global[user_id]
    lang = user_data['language']
    
    try:
        debug_log(f"Розрахунок балів з AI для користувача {user_id}")
        
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
                         "**Ось топ-3 програми лояльності готелів з персоналізованими рекомендаціями:**")
        else:
            intro_text = ("🎉 **Analysis completed!**\n\n"
                         "**Here are the top 3 hotel loyalty programs with personalized recommendations:**")
        
        await context.bot.send_message(
            chat_id=update.callback_query.message.chat_id,
            text=intro_text,
            parse_mode="Markdown"
        )
        
        # НОВЕ: Відправляємо кожну програму з AI-описами готелів
        await send_programs_with_ai_integrated_hotels(
            context, 
            update.callback_query.message.chat_id, 
            user_data, 
            scores_df, 
            lang
        )
        
        # Відправляємо заключне повідомлення
        if lang == 'uk':
            outro_text = ("💡 Щоб отримати більш детальний звіт \n усіх 7 програм – натисніть /more.\n"
                         "Щоб почати новий пошук — /start.")
        else:
            outro_text = ("💡 To get a more detailed report \n of all 7 programs – click /more.\n"
                         "To start a new search — /start.")
        
        await context.bot.send_message(
            chat_id=update.callback_query.message.chat_id,
            text=outro_text
        )

    except Exception as e:
        logger.error(f"Помилка при обчисленні результатів з AI: {e}")
        
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
            results += f"{emoji} {position_text} – {display_program_name}\n\n"
            results += f"⭐{row['program_rating']:.2f} – середній рейтинг готелів, що входять до програми\n"
            results += f"(на основі відгуків з Google Maps):\n\n"
        else:
            results += f"{emoji} {position_text} – {display_program_name}\n\n"
            results += f"⭐{row['program_rating']:.2f} – average rating of hotels in the program\n"
            results += f"(based on Google Maps reviews):\n\n"
        
        # РЕГІОН - ВИПРАВЛЕНА СЕКЦІЯ З БРЕНДАМИ
        if lang == 'uk':
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"📍 Регіон: {region_str}\n"
            results += f" • {row['region_hotels']} готелів:\n"
            
            # ВИПРАВЛЕНО: Перелік брендів у регіоні з правильним форматуванням
            brands_in_region = get_brands_in_region_for_program(program, regions, countries, hotel_data)
            if brands_in_region:
                for brand in brands_in_region:
                    results += f"   • {brand}\n"
            else:
                results += "   • Бренди не знайдено\n"
            results += "\n"
        else:
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"📍 Region: {region_str}\n"
            results += f" • {row['region_hotels']} hotels:\n"
            
            # ВИПРАВЛЕНО: Перелік брендів у регіоні з правильним форматуванням
            brands_in_region = get_brands_in_region_for_program(program, regions, countries, hotel_data)
            if brands_in_region:
                for brand in brands_in_region:
                    results += f"   • {brand}\n"
            else:
                results += "   • No brands found\n"
            results += "\n"
        
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
                    results += f"Cуміжні – {adj_cats_str} – {adjacent_total} готелів\n\n"
                else:
                    results += "\n"
            else:
                results += f"🏨 Segment:\n"
                results += f"Selected – {category} – {main_count} hotels\n"
                if adjacent_details:
                    adj_cats_str = ' and '.join(adjacent_details)
                    results += f"Adjacent – {adj_cats_str} – {adjacent_total} hotels\n\n"
                else:
                    results += "\n"
        
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
                    results += f"  - {adjacent_style_total} готелів в обраних стилях, в суміжних категоріях ({adj_cats_str})\n\n"
                else:
                    results += "\n"
            else:
                results += f"  - {main_style_total} hotels in selected styles, in {category} category\n"
                if adjacent_style_total > 0 and adjacent_categories_list:
                    adj_cats_str = ' and '.join(adjacent_categories_list)
                    results += f"  - {adjacent_style_total} hotels in selected styles, in adjacent categories ({adj_cats_str})\n\n"
                else:
                    results += "\n"
        
        # МЕТА - НОВИЙ КОМПАКТНИЙ ФОРМАТ
        if purposes:
            if lang == 'uk':
                purposes_str = '; '.join(purposes)
                results += f"🎯 Мета подорожі:\n{purposes_str}:\n"
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
            else:
                results += f"  - {main_purpose_total} hotels for selected purposes, in {category} category\n"
                if adjacent_purpose_total > 0 and adjacent_categories_list:
                    adj_cats_str = ' and '.join(adjacent_categories_list)
                    results += f"  - {adjacent_purpose_total} hotels for selected purposes, in adjacent categories ({adj_cats_str})\n"
        
        if i < 2:  # Додаємо роздільник між програмами (крім останньої)
            results += "\n" + "=" * 50 + "\n"
    
    return results

def format_detailed_results_with_ratings(user_data, scores_df, lang='uk'):
    """
    ОНОВЛЕНА функція детального звіту - працює як з усіма 7 програмами, так і з групами
    """
    results = ""
    
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
    
    for i, (index, row) in enumerate(scores_df.iterrows()):
        program = row['loyalty_program']
        
        # НОВЕ: Визначаємо абсолютну позицію програми (для правильної нумерації)
        absolute_position = index + 1  # index з оригінального DataFrame + 1
        
        # Замінюємо назву програми для відображення
        if program == "IHG One Rewards":
            display_program_name = "InterContinental Hotels One Rewards"
        else:
            display_program_name = program
        
        # НОВЕ: Визначаємо емодзі та пояснення на основі абсолютної позиції
        if absolute_position == 1:
            emoji = "🥇"
            position_text = "Топ 1" if lang == 'uk' else "Top 1"
            explanation = "– містить найбільше збігів з вашими критеріями." if lang == 'uk' else "– contains the most matches with your criteria."
        elif absolute_position == 2:
            emoji = "🥈"
            position_text = "Топ 2" if lang == 'uk' else "Top 2"
            explanation = "– друге місце за кількістю збігів з вашими критеріями." if lang == 'uk' else "– second place in matches with your criteria."
        elif absolute_position == 3:
            emoji = "🥉"
            position_text = "Топ 3" if lang == 'uk' else "Top 3"
            explanation = "– третє місце за кількістю збігів з вашими критеріями." if lang == 'uk' else "– third place in matches with your criteria."
        else:
            emoji = f"{absolute_position}."
            position_text = f"Топ {absolute_position}" if lang == 'uk' else f"Top {absolute_position}"
            explanation = f"– {absolute_position}-е місце за кількістю збігів з вашими критеріями." if lang == 'uk' else f"– {absolute_position} place in matches with your criteria."
        
        # ДОДАНО: Порожній рядок перед кожною програмою (крім першої)
        if i > 0:
            results += "\n"
        
        # ВИПРАВЛЕНО: додаємо пояснення з нового рядка
        if lang == 'uk':
            results += f"{emoji} {position_text} – {display_program_name}\n{explanation}\n\n"
            results += f"⭐{row['program_rating']:.2f} – середній рейтинг готелів, що входять до програми\n"
            results += f"(на основі відгуків з Google Maps):\n\n"
        else:
            results += f"{emoji} {position_text} – {display_program_name}\n{explanation}\n\n"
            results += f"⭐{row['program_rating']:.2f} – average rating of hotels in the program\n"
            results += f"(based on Google Maps reviews):\n\n"
        
        # РЕГІОН - ВИПРАВЛЕНО: змінюємо текст
        if lang == 'uk':
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"📍 Регіон: {region_str}\n"
            results += f" • {row['region_hotels']} готелів, до яких входять такі бренди:\n"  # ЗМІНЕНО
            
            brands_in_region = get_brands_in_region_for_program(program, regions, countries, hotel_data)
            if brands_in_region:
                for brand in brands_in_region:
                    results += f"   • {brand}\n"
            else:
                results += "   • Бренди не знайдено\n"
            results += "\n"
        else:
            region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'
            results += f"📍 Region: {region_str}\n"
            results += f" • {row['region_hotels']} hotels, which include such brands:\n"  # ЗМІНЕНО
            
            brands_in_region = get_brands_in_region_for_program(program, regions, countries, hotel_data)
            if brands_in_region:
                for brand in brands_in_region:
                    results += f"   • {brand}\n"
            else:
                results += "   • No brands found\n"
            results += "\n"
        
        # КАТЕГОРІЯ - ВИПРАВЛЕНО: новий формат
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
            
            # ВИПРАВЛЕНО: новий формат з тире
            if lang == 'uk':
                results += f"🏨 Сегмент:\n"
                results += f"– {main_count} готелів {category} (сегмент обраний вами)\n"  # ЗМІНЕНО
                if adjacent_details:
                    adj_cats_str = ', '.join(adjacent_details)  # ЗМІНЕНО: кома замість "і"
                    results += f"– {adjacent_total} готелів {adj_cats_str} (суміжні до обраного)\n\n"  # ЗМІНЕНО
                else:
                    results += "\n"
            else:
                results += f"🏨 Segment:\n"
                results += f"– {main_count} hotels {category} (segment selected by you)\n"  # ЗМІНЕНО
                if adjacent_details:
                    adj_cats_str = ', '.join(adjacent_details)  # ЗМІНЕНО: кома замість "and"
                    results += f"– {adjacent_total} hotels {adj_cats_str} (adjacent to selected)\n\n"  # ЗМІНЕНО
                else:
                    results += "\n"
        
        # СТИЛЬ - залишається без змін
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
                if adjacent_categories_list:
                    adj_cats_str = ' і '.join(adjacent_categories_list)
                    results += f"  - {adjacent_style_total} готелів в обраних стилях, в суміжних категоріях ({adj_cats_str})\n\n"
                else:
                    results += "\n"
            else:
                results += f"  - {main_style_total} hotels in selected styles, in {category} category\n"
                if adjacent_categories_list:
                    adj_cats_str = ' and '.join(adjacent_categories_list)
                    results += f"  - {adjacent_style_total} hotels in selected styles, in adjacent categories ({adj_cats_str})\n\n"
                else:
                    results += "\n"
        
        # МЕТА - ВИПРАВЛЕНО: змінено "Ціль" на "Мета" 
        if purposes:
            if lang == 'uk':
                purposes_str = '; '.join(purposes)
                results += f"🎯 Мета подорожі:\n{purposes_str}:\n"  # ЗМІНЕНО: "Ціль" -> "Мета"
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
                if adjacent_categories_list:
                    adj_cats_str = ' і '.join(adjacent_categories_list)
                    results += f"  - {adjacent_purpose_total} готелів в обраних цілях, в суміжних категоріях ({adj_cats_str})\n"
            else:
                results += f"  - {main_purpose_total} hotels for selected purposes, in {category} category\n"
                if adjacent_categories_list:
                    adj_cats_str = ' and '.join(adjacent_categories_list)
                    results += f"  - {adjacent_purpose_total} hotels for selected purposes, in adjacent categories ({adj_cats_str})\n"
        
        # Додаємо роздільник між програмами (крім останньої)
        if i < len(scores_df) - 1:
            results += "\n" + "=" * 50 + "\n"
    
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
        
        # РЕГІОН - детальний розбір з брендами
        region_score = row['region_score']
        region_hotels = row['region_hotels']
        region_str = ', '.join(regions) if regions else ', '.join(countries) if countries else 'N/A'

        results += f"📍 Регіон: {region_score:.1f} балів\n"
        results += f"{region_hotels} готелів у {region_str}:\n"

        # ДОДАЄМО: Перелік брендів у регіоні для адмін-звіту
        brands_in_region = get_brands_in_region_for_program(program, regions, countries, hotel_data)
        if brands_in_region:
            for brand in brands_in_region:
                results += f"   * {brand}\n"
        else:
            results += "   * Бренди не знайдено\n"
        results += "\n"
        
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
                adjacent_sum_

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

import logging
import pandas as pd
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, CallbackQueryHandler
import os
import json

# Налаштування логування
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Етапи розмови
LANGUAGE, REGION, WAITING_REGION_SUBMIT = range(3)

# Дані користувачів
user_data_global = {}

def load_hotel_data(csv_path):
    """Завантаження даних про програми лояльності з CSV файлу"""
    try:
        # Перевіряємо, чи існує файл
        if not os.path.exists(csv_path):
            logger.error(f"Файл не знайдено: {csv_path}")
            return None
            
        df = pd.read_csv(csv_path)
        
        # Логуємо інформацію про структуру CSV
        logger.info(f"Завантажено CSV файл. Кількість рядків: {len(df)}")
        logger.info(f"Колонки: {list(df.columns)}")
        
        return df
    except Exception as e:
        logger.error(f"Помилка завантаження CSV: {e}")
        return None

# Функція для початку бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Початкова функція при команді /start"""
    user_id = update.effective_user.id
    user_data_global[user_id] = {}
    
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

# Функція завершення розмови
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Скасовує розмову за командою /cancel"""
    user = update.message.from_user
    logger.info(f"Користувач {user.id} скасував розмову.")
    
    lang = user_data_global.get(user.id, {}).get('language', 'en')
    
    if lang == 'uk':
        await update.message.reply_text(
            "Розмову завершено. Щоб почати знову, надішліть команду /start."
        )
    else:
        await update.message.reply_text(
            "Conversation ended. To start again, send the /start command."
        )
    
    if user.id in user_data_global:
        del user_data_global[user.id]
    
    return ConversationHandler.END

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
        
        title_text = "Питання 1/2:\nУ яких регіонах світу ви плануєте подорожувати?\n(Виберіть один або кілька варіантів і натисніть 'Відповісти')"
        submit_text = "Відповісти"
    else:
        regions = [
            "Europe", "North America", "Asia",
            "Middle East", "Africa", "South America",
            "Caribbean", "Oceania"
        ]
        
        title_text = "Question 1/2:\nIn which regions of the world are you planning to travel?\n(Select one or multiple options and press 'Submit')"
        submit_text = "Submit"
    
    # Створюємо клавіатуру з чекбоксами для регіонів
    keyboard = []
    selected_regions = user_data_global[user_id]['selected_regions']
    
    # Групуємо регіони по 2 в рядку
    for i in range(0, len(regions), 2):
        row = []
        for j in range(2):
            if i + j < len(regions):
                region = regions[i + j]
                # Додаємо символ чекбокса в залежності від вибору
                checkbox = "✅ " if region in selected_regions else "☐ "
                row.append(InlineKeyboardButton(
                    f"{checkbox}{region}", 
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
        
        # Виводимо підтвердження вибору
        if lang == 'uk':
            await query.edit_message_text(
                f"Дякую! Ви обрали наступні регіони: {', '.join(selected_regions)}.\n\n"
                f"Для початку нового опитування, надішліть команду /start."
            )
        else:
            await query.edit_message_text(
                f"Thank you! You have chosen the following regions: {', '.join(selected_regions)}.\n\n"
                f"To start a new survey, send the /start command."
            )
        
        # Виводимо дані користувача для тестування
        logger.info(f"Дані користувача {user_id}: {json.dumps(user_data_global[user_id], ensure_ascii=False)}")
        
        # Завершуємо розмову на цьому етапі для тестового релізу
        return ConversationHandler.END
    
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

def main(token, csv_path):
    """Головна функція запуску бота"""
    # Завантаження даних
    hotel_data = load_hotel_data(csv_path)
    
    if hotel_data is None:
        logger.warning("Не вдалося завантажити дані. Бот буде запущено, але функціональність може бути обмежена.")
    
    # Створення застосунку
    app = Application.builder().token(token).build()
    
    # Налаштування обробників
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            LANGUAGE: [CallbackQueryHandler(language_choice)],
            REGION: [CallbackQueryHandler(ask_region)],
            WAITING_REGION_SUBMIT: [CallbackQueryHandler(region_choice)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    
    app.add_handler(conv_handler)
    
    # Запуск бота
    logger.info("Запуск бота в режимі polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
    
    logger.info("Бот запущено")

if __name__ == "__main__":
    # Використовуємо змінні середовища або значення за замовчуванням
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
    CSV_PATH = os.environ.get("CSV_PATH", "hotel_data.csv")

    # Перевіряємо наявність токена
    if TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logger.warning("Токен бота не налаштовано! Встановіть змінну середовища TELEGRAM_BOT_TOKEN або змініть значення в коді.")
    
    # Запускаємо бота
    main(TOKEN, CSV_PATH)

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

# Port configuration
PORT = int(os.environ.get("PORT", "10000"))

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation stages
LANGUAGE, REGION, WAITING_REGION_SUBMIT, CATEGORY, WAITING_STYLE_SUBMIT, WAITING_PURPOSE_SUBMIT = range(6)

# User data storage
user_data_global = {}

def analyze_csv_structure(df):
    """
    Analyzes CSV file structure and logs information
    
    Args:
        df: DataFrame with hotel data
    """
    logger.info("CSV structure analysis:")
    logger.info(f"Number of rows: {len(df)}")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Check unique values
    if 'loyalty_program' in df.columns:
        logger.info(f"Loyalty programs: {df['loyalty_program'].unique()}")
    
    if 'region' in df.columns:
        logger.info(f"Regions: {df['region'].unique()}")
    
    if 'segment' in df.columns:
        logger.info(f"Segments: {df['segment'].unique()}")
    
    # Check for missing values
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"Missing values: {null_counts[null_counts > 0]}")
    
    # Check data types
    logger.info(f"Data types: {df.dtypes}")

def load_hotel_data(csv_path):
    """Load loyalty program data from CSV file"""
    try:
        # Check if file exists
        if not os.path.exists(csv_path):
            logger.error(f"File not found: {csv_path}")
            return None
            
        df = pd.read_csv(csv_path)
        
        # Analyze CSV structure
        analyze_csv_structure(df)
        
        # Basic data validation - updated with expected column names
        expected_columns = ['loyalty_program', 'region', 'country', 'Hotel Brand', 'segment',
                            'Total hotels of Corporation / Loyalty Program in this region',
                            'Total hotels of Corporation / Loyalty Program in this country']
        
        # Check for columns and create mapping for renaming
        rename_mapping = {}
        
        # Check for 'Hotel Brand' or 'brand' column
        if 'brand' in df.columns and 'Hotel Brand' not in df.columns:
            rename_mapping['brand'] = 'Hotel Brand'
            logger.info("Renamed column 'brand' to 'Hotel Brand'")
        
        # Check for 'segment' or 'category' column
        if 'category' in df.columns and 'segment' not in df.columns:
            rename_mapping['category'] = 'segment'
            logger.info("Renamed column 'category' to 'segment'")
        
        # If there's a column with a shorter name for regions
        if 'region_hotels' in df.columns and 'Total hotels of Corporation / Loyalty Program in this region' not in df.columns:
            rename_mapping['region_hotels'] = 'Total hotels of Corporation / Loyalty Program in this region'
            logger.info("Renamed column 'region_hotels'")
        
        # If there's a column with a shorter name for countries
        if 'country_hotels' in df.columns and 'Total hotels of Corporation / Loyalty Program in this country' not in df.columns:
            rename_mapping['country_hotels'] = 'Total hotels of Corporation / Loyalty Program in this country'
            logger.info("Renamed column 'country_hotels'")
        
        # Apply renaming if needed
        if rename_mapping:
            df = df.rename(columns=rename_mapping)
            logger.info(f"Renamed columns: {rename_mapping}")
        
        # Check if required columns exist after renaming
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"After renaming, still missing columns: {missing_columns}")
            
            # Create missing columns with empty values
            for col in missing_columns:
                df[col] = ''
                logger.warning(f"Created empty column: {col}")
        
        return df
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return None

# Bot start function
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    
    # Always clear user data when the /start command is used
    if user_id in user_data_global:
        del user_data_global[user_id]
    
    # Initialize new data
    user_data_global[user_id] = {}
    
    # Log new conversation start
    logger.info(f"User {user_id} started a new conversation. Data cleared.")
    
    # Keyboard for language selection using InlineKeyboardMarkup
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

# Language choice handling function
async def language_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles user's language choice through InlineKeyboard"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    if callback_data == 'lang_uk':
        user_data_global[user_id]['language'] = 'uk'
        await query.edit_message_text(
            "Дякую! Я продовжу спілкування українською мовою."
        )
        # Wait briefly before asking the next question
        await asyncio.sleep(0.5)
        return await ask_region(update, context)
    
    elif callback_data == 'lang_en':
        user_data_global[user_id]['language'] = 'en'
        await query.edit_message_text(
            "Thank you! I will continue our conversation in English."
        )
        # Wait briefly before asking the next question
        await asyncio.sleep(0.5)
        return await ask_region(update, context)
    
    else:
        user_data_global[user_id]['language'] = 'en'  # Default to English
        await query.edit_message_text(
            "I'll continue in English. If you need another language, please let me know."
        )
        # Wait briefly before asking the next question
        await asyncio.sleep(0.5)
        return await ask_region(update, context)

# Region functions
async def ask_region(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Questions about travel regions with checkboxes"""
    # Determine if this is a response to a callback_query or a new request
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
    
    lang = user_data_global[user_id]['language']
    
    # Initialize selected regions if not already selected
    if 'selected_regions' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_regions'] = []
    
    # Create InlineKeyboard with checkboxes
    if lang == 'uk':
        regions = [
            "Європа", "Північна Америка", "Азія",
            "Близький Схід", "Африка", "Південна Америка",
            "Карибський басейн", "Океанія"
        ]
        
        # Create detailed description with numbering for better display
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
        
        # Create detailed description with numbering for better display
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
    
    # Create keyboard with checkboxes for regions
    keyboard = []
    selected_regions = user_data_global[user_id]['selected_regions']
    
    # Group regions by 2 in a row with numbers
    for i in range(0, len(regions), 2):
        row = []
        for j in range(2):
            if i + j < len(regions):
                region = regions[i + j]
                region_index = i + j + 1  # Region number (starting from 1)
                # Add checkbox symbol depending on selection
                checkbox = "✅ " if region in selected_regions else "☐ "
                row.append(InlineKeyboardButton(
                    f"{checkbox}{region_index}. {region}", 
                    callback_data=f"region_{region}"
                ))
        keyboard.append(row)
    
    # Add "Submit" button at the bottom
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="region_submit")])
    
    # Send a new message with the region question
    await context.bot.send_message(
        chat_id=chat_id,
        text=title_text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return WAITING_REGION_SUBMIT

async def region_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles region selection through checkboxes"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    # If user pressed "Submit"
    if callback_data == "region_submit":
        selected_regions = user_data_global[user_id]['selected_regions']
        lang = user_data_global[user_id]['language']
        
        # Check if at least one region is selected
        if not selected_regions:
            if lang == 'uk':
                await query.answer("Будь ласка, виберіть хоча б один регіон", show_alert=True)
            else:
                await query.answer("Please select at least one region", show_alert=True)
            return WAITING_REGION_SUBMIT
        
        # Save selected regions as main data
        user_data_global[user_id]['regions'] = selected_regions
        user_data_global[user_id]['countries'] = None  # Exclude the possibility to specify countries
        
        # Update the message, removing the keyboard but keeping the text
        regions_description = query.message.text
        await query.edit_message_text(text=regions_description)
        
        # Send a new message confirming the selection
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
        
        # Add a small delay before the next question
        await asyncio.sleep(0.5)
        
        # Move to category question
        return await ask_category(update, context)
    
    # Otherwise, it's a selection or deselection of a region
    else:
        # Process region selection
        region = callback_data.replace("region_", "")
        
        # Toggle region selection state
        if region in user_data_global[user_id]['selected_regions']:
            user_data_global[user_id]['selected_regions'].remove(region)
        else:
            user_data_global[user_id]['selected_regions'].append(region)
        
        # Update keyboard
        return await ask_region(update, context)

# Cancel function
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the conversation with the /cancel command"""
    user = update.message.from_user
    user_id = user.id
    logger.info(f"User {user_id} canceled the conversation.")
    
    lang = user_data_global.get(user_id, {}).get('language', 'en')
    
    # End of conversation message
    if lang == 'uk':
        await update.message.reply_text(
            "Розмову завершено. Щоб почати знову, надішліть команду /start."
        )
    else:
        await update.message.reply_text(
            "Conversation ended. To start again, send the /start command."
        )
    
    # Delete user data
    if user_id in user_data_global:
        del user_data_global[user_id]
        logger.info(f"User data {user_id} successfully deleted")
    
    # Clear context if available
    if hasattr(context, 'user_data'):
        context.user_data.clear()
    
    return ConversationHandler.END

# Category functions
async def ask_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Questions about hotel category"""
    # Determine if this is a response to a callback_query
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
    
    lang = user_data_global[user_id]['language']
    
    # Create InlineKeyboard for category selection
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
    """Handles hotel category selection"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    lang = user_data_global[user_id]['language']
    
    # Define selected category
    category = callback_data.replace("category_", "")
    
    # Save selected category
    user_data_global[user_id]['category'] = category
    
    # Update message, removing keyboard but keeping text
    category_text = query.message.text
    await query.edit_message_text(text=category_text, reply_markup=None)
    
    # Send new message confirming selection
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
    
    # Add a small delay before the next question
    await asyncio.sleep(0.5)
    
    # Move to style question
    return await ask_style(update, context)

# Style functions with checkboxes
async def ask_style(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Question about hotel style with checkboxes and detailed descriptions"""
    # Determine if this is a response to a callback_query
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
    
    lang = user_data_global[user_id]['language']
    
    # Initialize selected styles if not already selected
    if 'selected_styles' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_styles'] = []
    
    # Create InlineKeyboard with checkboxes for styles
    if lang == 'uk':
        # Main style names for buttons
        styles = [
            "Розкішний і вишуканий", 
            "Бутік і унікальний", 
            "Класичний і традиційний", 
            "Сучасний і дизайнерський",
            "Затишний і сімейний", 
            "Практичний і економічний"
        ]
        
        # Detailed description of styles for display in message
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
        # Main style names for buttons
        styles = [
            "Luxurious and refined", 
            "Boutique and unique",
            "Classic and traditional", 
            "Modern and designer",
            "Cozy and family-friendly", 
            "Practical and economical"
        ]
        
        # Detailed description of styles for display in message
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
    
    # Create keyboard with checkboxes for styles
    keyboard = []
    selected_styles = user_data_global[user_id]['selected_styles']
    
    # Add styles with numbers
    for i, style in enumerate(styles):
        # Add checkbox symbol depending on selection
        checkbox = "✅ " if style in selected_styles else "☐ "
        keyboard.append([InlineKeyboardButton(
            f"{checkbox}{i+1}. {style}", 
            callback_data=f"style_{style}"
        )])
    
    # Add "Submit" button at the bottom
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="style_submit")])
    
    # Send a new message with the style question
    await context.bot.send_message(
        chat_id=chat_id,
        text=title_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"  # Add Markdown formatting support
    )
    
    return WAITING_STYLE_SUBMIT

async def style_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles style selection via checkboxes"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    # If user clicked "Submit"
    if callback_data == "style_submit":
        selected_styles = user_data_global[user_id]['selected_styles']
        lang = user_data_global[user_id]['language']
        
        # Check if at least one style is selected
        if not selected_styles:
            if lang == 'uk':
                await query.answer("Будь ласка, виберіть хоча б один стиль", show_alert=True)
            else:
                await query.answer("Please select at least one style", show_alert=True)
            return WAITING_STYLE_SUBMIT
        
        # Limit to three options
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
            
            # Update selection and keyboard
            return await ask_style(update, context)
        
        # Save selected styles
        user_data_global[user_id]['styles'] = selected_styles
        
        # Update message, removing keyboard but keeping text
        style_text = query.message.text
        await query.edit_message_text(text=style_text, reply_markup=None, parse_mode="Markdown")
        
        # Send new message confirming selection
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
        
        # Add a small delay before the next question
        await asyncio.sleep(0.5)
        
        # Move to travel purpose question
        return await ask_purpose(update, context)
    
    # Otherwise, it's a selection or deselection of a style
    else:
        # Process style selection
        style = callback_data.replace("style_", "")
        
        # Check if the maximum number of styles (3) has been exceeded
        if style not in user_data_global[user_id]['selected_styles'] and len(user_data_global[user_id]['selected_styles']) >= 3:
            lang = user_data_global[user_id]['language']
            if lang == 'uk':
                await query.answer("Ви вже обрали максимальну кількість стилів (3)", show_alert=True)
            else:
                await query.answer("You have already selected the maximum number of styles (3)", show_alert=True)
            return WAITING_STYLE_SUBMIT
        
        # Toggle style selection state
        if style in user_data_global[user_id]['selected_styles']:
            user_data_global[user_id]['selected_styles'].remove(style)
        else:
            user_data_global[user_id]['selected_styles'].append(style)
        
        # Update keyboard
        return await ask_style(update, context)

# Purpose functions with checkboxes
async def ask_purpose(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Question about travel purpose with checkboxes and detailed descriptions"""
    # Determine if this is a response to a callback_query
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id
    
    lang = user_data_global[user_id]['language']
    
    # Initialize selected purposes if not already selected
    if 'selected_purposes' not in user_data_global[user_id]:
        user_data_global[user_id]['selected_purposes'] = []
    
    # Create InlineKeyboard with checkboxes for purposes
    if lang == 'uk':
        purposes = [
            "Бізнес-подорожі / відрядження",
            "Відпустка / релакс",
            "Сімейний відпочинок",
            "Довготривале проживання"
        ]
        
        # Detailed description of purposes in main text
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
        
        # Detailed description of purposes in main text
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
    
    # Create keyboard with checkboxes for purposes with numbers
    keyboard = []
    selected_purposes = user_data_global[user_id]['selected_purposes']
    
    # Add purposes with numbers
    for i, purpose in enumerate(purposes):
        # Add checkbox symbol depending on selection
        checkbox = "✅ " if purpose in selected_purposes else "☐ "
        keyboard.append([InlineKeyboardButton(
            f"{checkbox}{i+1}. {purpose}", 
            callback_data=f"purpose_{purpose}"
        )])
    
    # Add "Submit" button at the bottom
    keyboard.append([InlineKeyboardButton(submit_text, callback_data="purpose_submit")])
    
    # Send a new message with the purpose question
    await context.bot.send_message(
        chat_id=chat_id,
        text=title_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"  # Add Markdown formatting support
    )
    
    return WAITING_PURPOSE_SUBMIT

async def purpose_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles purpose selection via checkboxes"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    # If user clicked "Submit"
    if callback_data == "purpose_submit":
        selected_purposes = user_data_global[user_id]['selected_purposes']
        lang = user_data_global[user_id]['language']
        
        # Check if at least one purpose is selected
        if not selected_purposes:
            if lang == 'uk':
                await query.answer("Будь ласка, виберіть хоча б одну мету", show_alert=True)
            else:
                await query.answer("Please select at least one purpose", show_alert=True)
            return WAITING_PURPOSE_SUBMIT
        
        # Limit to two options
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
            
            # Update selection and keyboard
            return await ask_purpose(update, context)
        
        # Save selected purposes
        user_data_global[user_id]['purposes'] = selected_purposes
        
        # Update message, removing keyboard but keeping text
        purpose_text = query.message.text
        await query.edit_message_text(text=purpose_text, reply_markup=None, parse_mode="Markdown")
        
        # Send new message confirming selection
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
        
        # Calculate and display results
        return await calculate_and_show_results(update, context)
    
    # Otherwise, it's a selection or deselection of a purpose
    else:
        # Process purpose selection
        purpose = callback_data.replace("purpose_", "")
        
        # Check if the maximum number of purposes (2) has been exceeded
        if purpose not in user_data_global[user_id]['selected_purposes'] and len(user_data_global[user_id]['selected_purposes']) >= 2:
            lang = user_data_global[user_id]['language']
            if lang == 'uk':
                await query.answer("Ви вже обрали максимальну кількість цілей (2)", show_alert=True)
            else:
                await query.answer("You have already selected the maximum number of purposes (2)", show_alert=True)
            return WAITING_PURPOSE_SUBMIT
        
        # Toggle purpose selection state
        if purpose in user_data_global[user_id]['selected_purposes']:
            user_data_global[user_id]['selected_purposes'].remove(purpose)
        else:
            user_data_global[user_id]['selected_purposes'].append(purpose)
        
        # Update keyboard
        return await ask_purpose(update, context)

# Helper functions for filtering and matching

def map_hotel_style(hotel_brand):
    """
    Maps hotel brand to styles
    
    Args:
        hotel_brand: hotel brand (one string, not a list)
    
    Returns:
        Dictionary of styles with corresponding True/False values
    """
    # Make sure hotel_brand is a string
    if not isinstance(hotel_brand, str):
        hotel_brand = str(hotel_brand)
    
    hotel_brand = hotel_brand.lower()
    
    # Updated complete dictionary of styles and brands
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
    
    # Add English keys for styles
    style_mapping_en = {
        "Luxurious and refined": style_mapping["Розкішний і вишуканий"],
        "Boutique and unique": style_mapping["Бутік і унікальний"],
        "Classic and traditional": style_mapping["Класичний і традиційний"],
        "Modern and designer": style_mapping["Сучасний і дизайнерський"],
        "Cozy and family-friendly": style_mapping["Затишний і сімейний"],
        "Practical and economical": style_mapping["Практичний і економічний"]
    }
    
    # Combine dictionaries
    combined_mapping = {**style_mapping, **style_mapping_en}
    
    result = {}
    for style, brands in combined_mapping.items():
        # More flexible brand name comparison
        is_match = False
        for brand in brands:
            brand_lower = brand.lower()
            # Check if hotel brand contains the brand name from the list
            if brand_lower in hotel_brand:
                is_match = True
                break
        result[style] = is_match
    
    return result

def filter_hotels_by_style(df, styles):
    """
    Filters hotels by style
    
    Args:
        df: DataFrame with hotel data
        styles: list of selected styles
    
    Returns:
        Filtered DataFrame
    """
    if not styles or len(styles) == 0:
        return df
    
    # Logging for debugging
    logger.info(f"Filtering by styles: {styles}")
    logger.info(f"DataFrame columns: {df.columns}")
    if 'Hotel Brand' in df.columns:
        logger.info(f"Number of unique brands: {df['Hotel Brand'].nunique()}")
        logger.info(f"Brand examples: {df['Hotel Brand'].unique()[:5]}")
    
    # Simplify styles for better comparison
    simplified_styles = [style.strip().lower() for style in styles]
    logger.info(f"Simplified styles for search: {simplified_styles}")
    
    # Create mask for filtering
    style_mask = pd.Series(False, index=df.index)
    
    # Count of hotels found by style for logging
    style_counts = {style: 0 for style in styles}
    
    for idx, row in df.iterrows():
        if 'Hotel Brand' in df.columns and pd.notna(row['Hotel Brand']):
            hotel_brand = row['Hotel Brand']
            
            # Get brand's style correspondence
            hotel_styles = map_hotel_style(hotel_brand)
            
            # Check if the hotel matches at least one of the selected styles
            for style in styles:
                # Check both exact match and key parts
                style_lower = style.lower()
                
                for hotel_style, matches in hotel_styles.items():
                    if matches and (hotel_style.lower() == style_lower or 
                                    style_lower in hotel_style.lower() or
                                    hotel_style.lower() in style_lower):
                        style_mask.loc[idx] = True
                        style_counts[style] += 1
                        break
    
    # Log number of hotels found for each style
    for style, count in style_counts.items():
        logger.info(f"Found {count} hotels for style '{style}'")
    
    filtered_df = df[style_mask]
    logger.info(f"Total number of hotels after filtering: {len(filtered_df)}")
    
    return filtered_df

def filter_hotels_by_purpose(df, purposes):
    """
    Filters hotels by travel purpose
    
    Args:
        df: DataFrame with hotel data
        purposes: list of selected purposes
    
    Returns:
        Filtered DataFrame
    """
    if not purposes or len(purposes) == 0:
        return df
    
    # Logging for debugging
    logger.info(f"Filtering by purpose: {purposes}")
    
    # Create mask for filtering
    purpose_mask = pd.Series(False, index=df.index)
    
    # Count of hotels found by purpose for logging
    purpose_counts = {purpose: 0 for purpose in purposes}
    
    for idx, row in df.iterrows():
        if 'Hotel Brand' in df.columns and pd.notna(row['Hotel Brand']):
            hotel_brand = row['Hotel Brand']
            
            # Get brand's purpose correspondence
            hotel_purposes = map_hotel_purpose(hotel_brand)
            
            # Check if the hotel matches at least one of the selected purposes
            for purpose in purposes:
                # Check both exact match and key parts
                purpose_lower = purpose.lower()
                
                for hotel_purpose, matches in hotel_purposes.items():
                    if matches and (hotel_purpose.lower() == purpose_lower or 
                                    purpose_lower in hotel_purpose.lower() or
                                    hotel_purpose.lower() in purpose_lower):
                        purpose_mask.loc[idx] = True
                        purpose_counts[purpose] += 1
                        break
    
    # Log number of hotels found for each purpose
    for purpose, count in purpose_counts.items():
        logger.info(f"Found {count} hotels for purpose '{purpose}'")
    
    filtered_df = df[purpose_mask]
    logger.info(f"Total number of hotels after filtering: {len(filtered_df)}")
    
    return filtered_df

def get_region_score(df, regions=None, countries=None):
    """
    Calculates scores for loyalty programs by regions/countries
    
    Args:
        df: DataFrame with hotel data
        regions: list of selected regions
        countries: list of selected countries
    
    Returns:
        Dict with loyalty programs and their scores
    """
    region_scores = {}
    
    try:
        if regions and len(regions) > 0:
            # Use column for region hotel count
            if 'Total hotels of Corporation / Loyalty Program in this region' in df.columns:
                # Take unique values for each loyalty program
                region_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this region']]
                region_counts = region_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this region']
            else:
                # If column missing, just count the number of hotels
                region_counts = df.groupby('loyalty_program').size()
                logger.warning("Column 'Total hotels of Corporation / Loyalty Program in this region' missing. Using row count.")
        
        elif countries and len(countries) > 0:
            # Use column for country hotel count
            if 'Total hotels of Corporation / Loyalty Program in this country' in df.columns:
                # Take unique values for each loyalty program
                country_data = df.drop_duplicates('loyalty_program')[['loyalty_program', 'Total hotels of Corporation / Loyalty Program in this country']]
                region_counts = country_data.set_index('loyalty_program')['Total hotels of Corporation / Loyalty Program in this country']
            else:
                # If column missing, just count the number of hotels
                region_counts = df.groupby('loyalty_program').size()
                logger.warning("Column 'Total hotels of Corporation / Loyalty Program in this country' missing. Using row count.")
        
        else:
            # If neither regions nor countries selected, return empty dict
            return {}
        
        # Make sure region_counts doesn't contain NaN or None
        region_counts = region_counts.fillna(0).astype(float)
        
        # Distribute scores by ranking (21, 18, 15, 12, 9, 6, 3)
        score_values = [21, 18, 15, 12, 9, 6, 3]
        
        # Sort programs by hotel count
        ranked_programs = region_counts.sort_values(ascending=False)
        
        # Normalize if multiple regions/countries selected
        normalization_factor = 1.0
        if regions and len(regions) > 0:
            normalization_factor = float(len(regions))
        elif countries and len(countries) > 0:
            normalization_factor = float(len(countries))
        
        # Assign scores by ranking
        for i, (program, _) in enumerate(ranked_programs.items()):
            if i < len(score_values):
                region_scores[program] = score_values[i] / normalization_factor
            else:
                region_scores[program] = 0.0
                
    except Exception as e:
        logger.error(f"Error calculating region scores: {e}")
    
    return region_scores

def calculate_scores(user_data, hotel_data):
    """
    Calculates overall rating for each loyalty program based on user responses
    
    Args:
        user_data: dictionary with user responses
        hotel_data: DataFrame with hotel data
    
    Returns:
        DataFrame with loyalty programs and their scores
    """
    # Get user responses
    regions = user_data.get('regions', [])
    countries = user_data.get('countries', [])
    category = user_data.get('category')
    styles = user_data.get('styles', [])
    purposes = user_data.get('purposes', [])
    
    # Check for None and convert to empty lists to avoid errors
    if regions is None:
        regions = []
    if countries is None:
        countries = []
    if styles is None:
        styles = []
    if purposes is None:
        purposes = []
    
    # Initialize DataFrame to store results
    loyalty_programs = hotel_data['loyalty_program'].unique()
    scores_df = pd.DataFrame({
        'loyalty_program': loyalty_programs,
        'region_score': 0.0,  # Explicitly specify float type for accuracy
        'category_score': 0.0,
        'style_score': 0.0,
        'purpose_score': 0.0,
        'total_score': 0.0,
        'region_hotels': 0,
        'category_hotels': 0,
        'style_hotels': 0,
        'purpose_hotels': 0
    })
    
    # Step 1: Filter hotels by region
    filtered_by_region = filter_hotels_by_region(hotel_data, regions, countries)
    
    # Use ready values from columns for regions and countries
    if regions and len(regions) > 0:
        # Check for "Total hotels of Corporation / Loyalty Program in this region" column
        if 'Total hotels of Corporation / Loyalty Program in this region' in filtered_by_region.columns:
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                program_data = filtered_by_region[filtered_by_region['loyalty_program'] == program]
                
                if not program_data.empty:
                    # Use unique value from column
                    region_hotels = program_data['Total hotels of Corporation / Loyalty Program in this region'].iloc[0]
                    scores_df.at[index, 'region_hotels'] = region_hotels
        else:
            # If column missing, just count hotels
            region_counts = filtered_by_region.groupby('loyalty_program').size()
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                if program in region_counts:
                    scores_df.at[index, 'region_hotels'] = region_counts[program]
    
    elif countries and len(countries) > 0:
        # Check for "Total hotels of Corporation / Loyalty Program in this country" column
        if 'Total hotels of Corporation / Loyalty Program in this country' in filtered_by_region.columns:
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                program_data = filtered_by_region[filtered_by_region['loyalty_program'] == program]
                
                if not program_data.empty:
                    # Use unique value from column
                    country_hotels = program_data['Total hotels of Corporation / Loyalty Program in this country'].iloc[0]
                    scores_df.at[index, 'region_hotels'] = country_hotels
        else:
            # If column missing, just count hotels
            country_counts = filtered_by_region.groupby('loyalty_program').size()
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                if program in country_counts:
                    scores_df.at[index, 'region_hotels'] = country_counts[program]
    
    # Distribute scores by regions/countries
    region_scores = get_region_score(filtered_by_region, regions, countries)
    for index, row in scores_df.iterrows():
        program = row['loyalty_program']
        if program in region_scores:
            scores_df.at[index, 'region_score'] = region_scores[program]
    
    # Step 2: Filter hotels by category in selected region
    if category:
        filtered_by_category = filter_hotels_by_category(filtered_by_region, category)
        
        category_counts = filtered_by_category.groupby('loyalty_program').size()
        
        # Distribute scores by category (21, 18, 15, 12, 9, 6, 3)
        if not category_counts.empty:
            category_scores = {}
            ranked_programs = category_counts.sort_values(ascending=False)
            
            # Ranking scores
            score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
            for i, (program, _) in enumerate(ranked_programs.items()):
                if i < len(score_values):
                    category_scores[program] = score_values[i]
                else:
                    category_scores[program] = 0.0
            
            # Add scores for adjacent categories
            adjacent_filtered = filter_hotels_by_adjacent_category(filtered_by_region, category)
            adjacent_counts = adjacent_filtered.groupby('loyalty_program').size()
            
            # Scores for adjacent category (7, 6, 5, 4, 3, 2, 1)
            adjacent_score_values = [7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
            adjacent_scores = {}
            
            if not adjacent_counts.empty:
                ranked_adjacent = adjacent_counts.sort_values(ascending=False)
                for i, (program, _) in enumerate(ranked_adjacent.items()):
                    if i < len(adjacent_score_values):
                        adjacent_scores[program] = adjacent_score_values[i]
                    else:
                        adjacent_scores[program] = 0.0
            
            # Update DataFrame with scores
            for index, row in scores_df.iterrows():
                program = row['loyalty_program']
                
                # Full match scores
                if program in category_scores:
                    scores_df.at[index, 'category_score'] = category_scores[program]
                
                # Add adjacent category scores
                if program in adjacent_scores:
                    scores_df.at[index, 'category_score'] += adjacent_scores[program]
                
                # Record number of hotels in category
                category_mask = filtered_by_category['loyalty_program'] == program
                scores_df.at[index, 'category_hotels'] = category_mask.sum()
        else:
            # If category not selected, use all hotels
            filtered_by_category = filtered_by_region
    else:
        filtered_by_category = filtered_by_region
    
    # Step 3: Filter hotels by style in selected category and region
    if styles and len(styles) > 0:
        style_filtered = filter_hotels_by_style(filtered_by_category, styles)
        style_counts_dict = {}
        
        for program in loyalty_programs:
            style_mask = style_filtered['loyalty_program'] == program
            style_counts_dict[program] = style_mask.sum()
            
            # Record number of hotels by style
            scores_df.loc[scores_df['loyalty_program'] == program, 'style_hotels'] = style_mask.sum()
        
        # Distribute scores by styles (21, 18, 15, 12, 9, 6, 3)
        style_scores = {}
        ranked_programs = sorted(style_counts_dict.items(), key=lambda x: x[1], reverse=True)
        
        # Ranking scores
        score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
        for i, (program, _) in enumerate(ranked_programs):
            if i < len(score_values):
                style_scores[program] = score_values[i]
            else:
                style_scores[program] = 0.0
        
        # Normalize scores if multiple styles selected
        if len(styles) > 1:
            for program in style_scores:
                style_scores[program] /= len(styles)
        
        # Update DataFrame with scores
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            if program in style_scores:
                scores_df.at[index, 'style_score'] = style_scores[program]
    
    # Step 4: Filter hotels by purpose in selected style, category, and region
    if purposes and len(purposes) > 0:
        purpose_filtered = filter_hotels_by_purpose(filtered_by_category, purposes)
        purpose_counts_dict = {}
        
        for program in loyalty_programs:
            purpose_mask = purpose_filtered['loyalty_program'] == program
            purpose_counts_dict[program] = purpose_mask.sum()
            
            # Record number of hotels by purpose
            scores_df.loc[scores_df['loyalty_program'] == program, 'purpose_hotels'] = purpose_mask.sum()
        
        # Distribute scores by purpose (21, 18, 15, 12, 9, 6, 3)
        purpose_scores = {}
        ranked_programs = sorted(purpose_counts_dict.items(), key=lambda x: x[1], reverse=True)
        
        # Ranking scores
        score_values = [21.0, 18.0, 15.0, 12.0, 9.0, 6.0, 3.0]
        for i, (program, _) in enumerate(ranked_programs):
            if i < len(score_values):
                purpose_scores[program] = score_values[i]
            else:
                purpose_scores[program] = 0.0
        
        # Normalize scores if multiple purposes selected
        if len(purposes) > 1:
            for program in purpose_scores:
                purpose_scores[program] /= len(purposes)
        
        # Update DataFrame with scores
        for index, row in scores_df.iterrows():
            program = row['loyalty_program']
            if program in purpose_scores:
                scores_df.at[index, 'purpose_score'] = purpose_scores[program]
    
    # Additional logging for styles
    if styles and len(styles) > 0:
        logger.info(f"Filtering by styles: {styles}")
        # Print number of hotels for each style
        for style in styles:
            style_hotels_count = 0
            for program in loyalty_programs:
                program_data = style_filtered[style_filtered['loyalty_program'] == program]
                style_hotels_count += len(program_data)
            logger.info(f"Total number of hotels for style '{style}': {style_hotels_count}")
            
            # Check if there are hotels of this style for each loyalty program
            for program in loyalty_programs:
                program_data = style_filtered[style_filtered['loyalty_program'] == program]
                logger.info(f"Program '{program}' - {len(program_data)} hotels for style '{style}'")
    
    # Calculate overall rating
    scores_df['total_score'] = (
        scores_df['region_score'] + 
        scores_df['category_score'] + 
        scores_df['style_score'] + 
        scores_df['purpose_score']
    )
    
    # Sort by overall rating
    scores_df = scores_df.sort_values('total_score', ascending=False)
    
    return scores_df

def get_detailed_analysis(user_data, hotel_data, scores_df):
    """
    Generates detailed score calculation analysis
    
    Args:
        user_data: dictionary with user responses
        hotel_data: DataFrame with hotel data
        scores_df: DataFrame with calculated scores
    
    Returns:
        str: detailed analysis in text format
    """
    analysis = "<detailed_analysis>\n"
    
    # Add user response summary
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
    
    # For each program show detailed calculation
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
    Formats results for user display
    
    Args:
        user_data: dictionary with user responses
        scores_df: DataFrame with calculated scores
        lang: display language (uk or en)
    
    Returns:
        str: formatted results for display
    """
    results = ""
    
    # Take top-5 programs or fewer if there are less than 5
    max_programs = min(5, len(scores_df))
    top_programs = scores_df.head(max_programs)
    
    for i, (index, row) in enumerate(top_programs.iterrows()):
        program = row['loyalty_program']
        
        if lang == 'uk':
            results += f"{program} - посіла {i+1} місце з рейтингом {row['total_score']:.2f}\n"
            
            # Region/country information
            if user_data.get('regions'):
                region_str = ', '.join(user_data['regions'])
                results += f"1) у {region_str} - ({row['region_hotels']} готелів)\n"
            elif user_data.get('countries'):
                country_str = ', '.join(user_data['countries'])
                results += f"1) у {country_str} - ({row['region_hotels']} готелів)\n"
            
            # Category information
            if user_data.get('category'):
                results += f"2) у сегменті {user_data['category']} ({row['category_hotels']} готелів)\n"
            
            # Style information
            if user_data.get('styles'):
                style_str = ', '.join(user_data['styles'])
                results += f"3) у стилі {style_str} ({row['style_hotels']} готелів у цьому стилі/стилях та у суміжних)\n"
            
            # Purpose information
            if user_data.get('purposes'):
                purpose_str = ', '.join(user_data['purposes'])
                results += f"4) для {purpose_str} ({row['purpose_hotels']} готелів)\n"
        else:
            results += f"{program} - ranked {i+1} with a score of {row['total_score']:.2f}\n"
            
            # Region/country information
            if user_data.get('regions'):
                region_str = ', '.join(user_data['regions'])
                results += f"1) in {region_str} - ({row['region_hotels']} hotels)\n"
            elif user_data.get('countries'):
                country_str = ', '.join(user_data['countries'])
                results += f"1) in {country_str} - ({row['region_hotels']} hotels)\n"
            
            # Category information
            if user_data.get('category'):
                results += f"2) in the {user_data['category']} segment ({row['category_hotels']} hotels)\n"
            
            # Style information
            if user_data.get('styles'):
                style_str = ', '.join(user_data['styles'])
                results += f"3) in the {style_str} style ({row['style_hotels']} hotels in this style(s) and adjacent ones)\n"
            
            # Purpose information
            if user_data.get('purposes'):
                purpose_str = ', '.join(user_data['purposes'])
                results += f"4) for {purpose_str} ({row['purpose_hotels']} hotels)\n"
        
        # Add empty line between results
        results += "\n"
    
    return results

async def calculate_and_show_results(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Calculates results and displays them to the user
    
    Args:
        update: Update object from Telegram
        context: bot context
    
    Returns:
        int: identifier of final conversation state
    """
    # Determine if this is a response to a callback_query
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.effective_user.id
        chat_id = update.message.chat_id
    
    user_data = user_data_global[user_id]
    lang = user_data['language']
    
    # Perform analysis and score calculation
    try:
        # Log debug information
        logger.info(f"Calculating scores for user {user_id}")
        logger.info(f"User data: {user_data}")
        
        # Check if hotel data exists
        if hotel_data is None or hotel_data.empty:
            logger.error("Hotel data missing or empty!")
            
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
            
            return ConversationHandler.END
        
        # Calculate scores for each loyalty program
        scores_df = calculate_scores(user_data, hotel_data)
        
        # Check if there are results
        if scores_df.empty:
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
            
            return ConversationHandler.END
        
        # Generate detailed analysis (not displayed to the user)
        detailed_analysis = get_detailed_analysis(user_data, hotel_data, scores_df)
        
        # Log detailed analysis for developers
        logger.info(f"Detailed analysis for user {user_id}: {detailed_analysis}")
        
        # Format results for display
        results = format_results(user_data, scores_df, lang)
        
        # Send results to the user
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
    
    except Exception as e:
        logger.error(f"Error calculating results: {e}")
        
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
    
    # Delete user data
    if user_id in user_data_global:
        del user_data_global[user_id]
    
    return ConversationHandler.END

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles errors occurring during bot operation"""
    logger.error(f"Error occurred: {context.error}")
    
    # Get user information
    user_id = None
    if update and update.effective_user:
        user_id = update.effective_user.id
    
    # Log detailed error information
    logger.error(f"Error for user {user_id}: {context.error}")
    
    # Try to send message to user
    if update and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="An error occurred while processing your request. Try using the /start command to begin again."
        )
    
    # Check if error is due to interrupted conversation
    if "conversation" in str(context.error).lower():
        # Clear user data if error is related to conversation
        if user_id and user_id in user_data_global:
            del user_data_global[user_id]
            logger.info(f"Cleared user data {user_id} due to conversation error")

def main(token, csv_path, webhook_url=None, webhook_port=None, webhook_path=None):
    """Main function to launch the bot with webhook support"""
    # Data loading
    global hotel_data
    hotel_data = load_hotel_data(csv_path)
    
    if hotel_data is None:
        logger.error("Failed to load data. Bot not started.")
        return
    
    # Additional check for required columns
    required_columns = ['loyalty_program', 'region', 'country', 'Hotel Brand']
    missing_required = [col for col in required_columns if col not in hotel_data.columns]
    
    if missing_required:
        logger.error(f"Missing critical columns: {missing_required}. Bot not started.")
        return
    
    # Ensure 'segment' column exists
    if 'segment' not in hotel_data.columns:
        logger.error("Missing 'segment' column. Bot not started.")
        return
    
    # Create application
    app = Application.builder().token(token)
    
    # Build application
    application = app.build()
    
    # Set up handlers
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            LANGUAGE: [CallbackQueryHandler(language_choice)],
            WAITING_REGION_SUBMIT: [CallbackQueryHandler(region_choice)],
            CATEGORY: [CallbackQueryHandler(category_choice)],
            WAITING_STYLE_SUBMIT: [CallbackQueryHandler(style_choice)],
            WAITING_PURPOSE_SUBMIT: [CallbackQueryHandler(purpose_choice)],
            ConversationHandler.END: [CommandHandler("start", start)]  # Added handler for END state
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", start)  # /start as fallback
        ],
        name="loyalty_programs_conversation",  # Added name for better logging
        persistent=False  # Ensure data isn't stored between restarts
    )
    
    # Add main conversation handler
    application.add_handler(conv_handler)
    
    # Add error handler for better diagnostics
    application.add_error_handler(error_handler)
    
    # Use PORT for webhook
    port = int(os.environ.get("PORT", "10000"))
    
    # Log bot readiness
    logger.info("Bot successfully configured and ready to launch")
    
    # Launch bot in appropriate mode
    if webhook_url and webhook_path:
        webhook_info = f"{webhook_url}{webhook_path}"
        logger.info(f"Starting bot in webhook mode at {webhook_info}")
        application.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=webhook_path,
            webhook_url=webhook_info,
            allowed_updates=Update.ALL_TYPES
        )
    else:
        logger.info("WEBHOOK_URL not specified. Starting bot in polling mode...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    
    logger.info("Bot launched")

if __name__ == "__main__":
    # Use environment variables or default values
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
    CSV_PATH = os.environ.get("CSV_PATH", "hotel_data.csv")

    if not CSV_PATH:
        logger.error("CSV_PATH not set. Terminating launch.")
        exit(1)
    logger.info(f"Using CSV path: {CSV_PATH}")
    
    # Webhook parameters (optional)
    WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST", "").replace("https://", "")  # Clear https://, if present
    WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", f"/webhook/{TOKEN}")
    
    # Form complete URL for webhook if WEBHOOK_HOST is specified
    WEBHOOK_URL = f"https://{WEBHOOK_HOST}" if WEBHOOK_HOST else None
    
    # Check for token presence
    if TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logger.warning("Bot token not configured! Set the TELEGRAM_BOT_TOKEN environment variable or change the value in the code.")
    
    # Launch bot with webhook or polling support
    main(TOKEN, CSV_PATH, WEBHOOK_URL, 10000, WEBHOOK_PATH)

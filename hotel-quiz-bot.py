async def ask_region(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Питання про регіони подорожей"""
    user_id = update.effective_user.id
    lang = user_data_global[user_id]['language']
    
    keyboard = []
    
    if lang == 'uk':
        keyboard = [
            ["1. Європа"], ["2. Північна Америка"], ["3. Азія"],
            ["4. Близький Схід"], ["5. Африка"], ["6. Південна Америка"],
            ["7. Карибський басейн"], ["8. Океанія"],
            ["Мене цікавлять лише конкретні країни"]
        ]
        
        await update.message.reply_text(
            "**Питання 1/4**\n"
            "У яких регіонах світу ви плануєте подорожувати?\n"
            "*(можете обрати декілька варіантів)*\n"
            "1. Європа\n"
            "2. Північна Америка\n"
            "3. Азія\n"
            "4. Близький Схід\n"
            "5. Африка\n"
            "6. Південна Америка\n"
            "7. Карибський басейн\n"
            "8. Океанія",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    else:
        keyboard = [
            ["1. Europe"], ["2. North America"], ["3. Asia"],
            ["4. Middle East"], ["5. Africa"], ["6. South America"],
            ["7. Caribbean"], ["8. Oceania"],
            ["I'm only interested in specific countries"]
        ]
        
        await update.message.reply_text(
            "**Question 1/4**\n"
            "In which regions of the world are you planning to travel?\n"
            "*(you can choose multiple options)*\n"
            "1. Europe\n"
            "2. North America\n"
            "3. Asia\n"
            "4. Middle East\n"
            "5. Africa\n"
            "6. South America\n"
            "7. Caribbean\n"
            "8. Oceania",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
    
    return REGION

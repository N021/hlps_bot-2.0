def format_detailed_results_with_ratings(user_data, scores_df, lang='uk', start_position=0):
    """
    ВИПРАВЛЕНА функція детального звіту з правильною нумерацією
    
    Args:
        user_data: дані користувача
        scores_df: DataFrame з програмами
        lang: мова
        start_position: стартова позиція для нумерації (0, 2, 4, 6)
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
        
        # ВИПРАВЛЕНО: Використовуємо start_position + i для правильної нумерації
        absolute_position = start_position + i + 1
        
        # Замінюємо назву програми для відображення
        if program == "IHG One Rewards":
            display_program_name = "InterContinental Hotels One Rewards"
        else:
            display_program_name = program
        
        # Визначаємо емодзі та пояснення на основі правильної позиції
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

# ТАКОЖ ОНОВЛЮЄМО ФУНКЦІЮ /more з правильними start_position:

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
        
        # 1-ше повідомлення: програми 1-2 (start_position=0)
        programs_1_2 = all_programs.iloc[0:2]
        if not programs_1_2.empty:
            detailed_results_1_2 = format_detailed_results_with_ratings(user_data, programs_1_2, lang, start_position=0)
            await send_long_message_to_chat(context, update.message.chat_id, detailed_results_1_2)
            await asyncio.sleep(1)
        
        # 2-ге повідомлення: програми 3-4 (start_position=2)
        programs_3_4 = all_programs.iloc[2:4]
        if not programs_3_4.empty:
            detailed_results_3_4 = format_detailed_results_with_ratings(user_data, programs_3_4, lang, start_position=2)
            await send_long_message_to_chat(context, update.message.chat_id, detailed_results_3_4)
            await asyncio.sleep(1)
        
        # 3-тє повідомлення: програми 5-6 (start_position=4)
        programs_5_6 = all_programs.iloc[4:6]
        if not programs_5_6.empty:
            detailed_results_5_6 = format_detailed_results_with_ratings(user_data, programs_5_6, lang, start_position=4)
            await send_long_message_to_chat(context, update.message.chat_id, detailed_results_5_6)
            await asyncio.sleep(1)
        
        # 4-те повідомлення: програма 7 (start_position=6)
        program_7 = all_programs.iloc[6:7]
        if not program_7.empty:
            detailed_results_7 = format_detailed_results_with_ratings(user_data, program_7, lang, start_position=6)
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

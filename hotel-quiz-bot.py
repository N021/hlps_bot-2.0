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
            mask = df['segment'].apply(lambda x: any(cat.lower() in str(x).lower() for cat in adjacent_mapping[category]))
            return df[mask]
    
    return df

def format_results(user_data, scores_df, lang='en'):
    """
    Форматує результати для відображення користувачу у стилі "новий код 2.0"
    
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
        
        if lang == 'uk':
            results += f"{program} - посіла {i+1} місце з рейтингом {row['total_score']:.2f}\n"
            
            # Інформація про регіони/країни
            if user_data.get('regions'):
                region_str = ', '.join(user_data['regions'])
                results += f"1) у {region_str} - ({int(row['region_hotels'])} готелів)\n"
            elif user_data.get('countries'):
                country_str = ', '.join(user_data['countries'])
                results += f"1) у {country_str} - ({int(row['region_hotels'])} готелів)\n"
            
            # Інформація про категорію
            if user_data.get('category'):
                results += f"2) у сегменті {user_data['category']} ({int(row['category_hotels'])} готелів)\n"
            
            # Інформація про стиль
            if user_data.get('styles'):
                style_str = ', '.join(user_data['styles'])
                results += f"3) у стилі {style_str} ({int(row['style_hotels'])} готелів у цьому стилі/стилях та у суміжних)\n"
            
            # Інформація про мету
            if user_data.get('purposes'):
                purpose_str = ', '.join(user_data['purposes'])
                results += f"4) для {purpose_str} ({int(row['purpose_hotels'])} готелів)\n"
        else:
            results += f"{program} - ranked {i+1} with a score of {row['total_score']:.2f}\n"
            
            # Інформація про регіони/країни
            if user_data.get('regions'):
                region_str = ', '.join(user_data['regions'])
                results += f"1) in {region_str} - ({int(row['region_hotels'])} hotels)\n"
            elif user_data.get('countries'):
                country_str = ', '.join(user_data['countries'])
                results += f"1) in {country_str} - ({int(row['region_hotels'])} hotels)\n"
            
            # Інформація про категорію
            if user_data.get('category'):
                results += f"2) in the {user_data['category']} segment ({int(row['category_hotels'])} hotels)\n"
            
            # Інформація про стиль
            if user_data.get('styles'):
                style_str = ', '.join(user_data['styles'])
                results += f"3) in the {style_str} style ({int(row['style_hotels'])} hotels in this style(s) and adjacent ones)\n"
            
            # Інформація про мету
            if user_data.get('purposes'):
                purpose_str = ', '.join(user_data['purposes'])
                results += f"4) for {purpose_str} ({int(row['purpose_hotels'])} hotels)\n"
        
        results += "\n"
    
    return results

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
    analysis = "Detailed analysis:\n"
    
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
        analysis += f"- Region score: {row['region_score']:.2f} (hotels in region: {int(row['region_hotels'])})\n"
        analysis += f"- Category score: {row['category_score']:.2f} (hotels in selected category: {int(row['category_hotels'])})\n"
        analysis += f"- Style score: {row['style_score']:.2f} (hotels in selected style(s): {int(row['style_hotels'])})\n"
        analysis += f"- Purpose score: {row['purpose_score']:.2f} (hotels for selected purpose(s): {int(row['purpose_hotels'])})\n"
        analysis += f"- Total score: {row['total_score']:.2f}\n"
    
    analysis += "\nRanking of loyalty programs by total score:\n"
    for i, (index, row) in enumerate(scores_df.head(5).iterrows()):
        analysis += f"{i+1}. {row['loyalty_program']} - {row['total_score']:.2f} points\n"
    
    return analysis

async def calculate_and_show_results(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обчислює результати та відображає їх користувачеві
    
    Args:
        update: об'єкт Update від Telegram
        context: контекст бота
    
    Returns:
        int: ідентифікатор кінцевого стану розмови
    """
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        user_id = update.effective_user.id
        chat_id = update.message.chat_id
    
    user_data = user_data_global[user_id]
    lang = user_data['language']
    
    try:
        logger.info(f"Calculating scores for user {user_id}")
        
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
        
        scores_df = calculate_scores(user_data, hotel_data)
        
        if scores_df.empty:
            if lang == 'uk':
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="На жаль, не вдалося знайти програми лояльності, які відповідають вашим уподобанням."
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Unfortunately, I couldn't find any loyalty programs that match your preferences."
                )
            
            return ConversationHandler.END
        
        # Генеруємо детальний аналіз для логування
        detailed_analysis = get_detailed_analysis(user_data, hotel_data, scores_df)
        logger.info(f"Detailed analysis for user {user_id}: {detailed_analysis}")
        
        results = format_results(user_data, scores_df, lang)
        
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
                text="Виникла помилка при аналізі ваших відповідей. Спробуйте знову."
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="An error occurred while analyzing your answers. Please try again."
            )
    
    if user_id in user_data_global:
        del user_data_global[user_id]
    
    return ConversationHandler.END

# Оновлена функція зіставлення бренду готелю зі стилями
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

# Оновлена функція зіставлення бренду готелю з метою подорожі
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

# Оновлена функція фільтрації за стилем
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

# Оновлена функція фільтрації за метою
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

# Додаємо функції для оновлення коду після всіх інших функцій

# Функція зіставлення стилю з категорією
def style_matches_category(style, category):
    """
    Перевіряє, чи стиль відповідає категорії
    
    Args:
        style: назва стилю
        category: категорія (Luxury, Comfort, Standard)
    
    Returns:
        bool: True, якщо стиль підходить для категорії
    """
    category_style_mapping = {
        "Luxury": ["Розкішний і вишуканий", "Бутік і унікальний", 
                  "Luxurious and refined", "Boutique and unique"],
        
        "Comfort": ["Класичний і традиційний", "Сучасний і дизайнерський", "Затишний і сімейний",
                   "Classic and traditional", "Modern and designer", "Cozy and family-friendly"],
        
        "Standard": ["Практичний і економічний", "Practical and economical"]
    }
    
    if category in category_style_mapping:
        return style in category_style_mapping[category]
    
    return False

# Функція для виведення категорій для кожної програми лояльності
def get_segment_distribution(df):
    """
    Отримує розподіл категорій для програм лояльності
    
    Args:
        df: DataFrame з даними готелів
    
    Returns:
        Словник {програма: {категорія: кількість}}
    """
    result = {}
    
    if 'loyalty_program' in df.columns and 'segment' in df.columns:
        # Для кожної програми лояльності
        for program in df['loyalty_program'].unique():
            program_data = df[df['loyalty_program'] == program]
            
            # Підрахунок категорій
            segment_counts = program_data['segment'].value_counts()
            
            result[program] = {}
            for segment in ["Luxury", "Comfort", "Standard"]:
                if segment in segment_counts:
                    result[program][segment] = segment_counts[segment]
                else:
                    result[program][segment] = 0
    
    return result

# Функція для генерації рекомендацій щодо програм лояльності
def generate_recommendations(scores_df, top_n=3):
    """
    Генерує текстові рекомендації щодо програм лояльності
    
    Args:
        scores_df: DataFrame з підрахованими балами
        top_n: кількість програм для рекомендацій
    
    Returns:
        str: текст з рекомендаціями
    """
    recommendations = []
    
    top_programs = scores_df.head(top_n)
    
    for i, (index, row) in enumerate(top_programs.iterrows()):
        program = row['loyalty_program']
        score = row['total_score']
        
        # Базова рекомендація
        recommendation = f"{i+1}. {program} (загальний бал: {score:.2f})"
        
        # Додаємо примітки щодо сильних сторін
        strengths = []
        if row['region_score'] > 15.0:
            strengths.append("велика кількість готелів у вибраному регіоні")
        if row['category_score'] > 15.0:
            strengths.append("відмінна відповідність вибраній категорії")
        if row['style_score'] > 15.0:
            strengths.append("ідеальний стиль для ваших уподобань")
        if row['purpose_score'] > 15.0:
            strengths.append("оптимальний вибір для вашої мети подорожі")
        
        if strengths:
            recommendation += f" - сильні сторони: {', '.join(strengths)}"
        
        recommendations.append(recommendation)
    
    return "\n".join(recommendations)

# Додаткові функції для розширеного аналізу та відображення

# Функція для порівняння програм лояльності
def compare_loyalty_programs(df, selected_programs, user_preferences=None):
    """
    Порівнює обрані програми лояльності за різними критеріями
    
    Args:
        df: DataFrame з даними готелів
        selected_programs: список програм для порівняння
        user_preferences: словник із перевагами користувача (опціонально)
    
    Returns:
        DataFrame з порівнянням
    """
    comparison = {}
    
    # Рахуємо загальну кількість готелів
    for program in selected_programs:
        program_data = df[df['loyalty_program'] == program]
        comparison[program] = {
            'total_hotels': len(program_data),
        }
        
        # Розподіл за категоріями
        for category in ['Luxury', 'Comfort', 'Standard']:
            category_data = program_data[program_data['segment'] == category]
            comparison[program][f'{category}_hotels'] = len(category_data)
        
        # Середній рейтинг (якщо є)
        if 'rating' in program_data.columns:
            comparison[program]['avg_rating'] = program_data['rating'].mean()
        
        # Розподіл по регіонах
        regions = program_data['region'].value_counts()
        for region, count in regions.items():
            comparison[program][f'region_{region}'] = count
    
    # Перетворюємо на DataFrame
    comparison_df = pd.DataFrame.from_dict(comparison, orient='index')
    
    # Додаємо метрику відповідності перевагам користувача
    if user_preferences:
        comparison_df['preference_match'] = 0.0
        
        for program in selected_programs:
            match_score = 0.0
            
            # Враховуємо категорію
            if 'category' in user_preferences and user_preferences['category']:
                category = user_preferences['category']
                match_score += comparison_df.loc[program, f'{category}_hotels'] / comparison_df.loc[program, 'total_hotels'] * 100
            
            # Враховуємо регіон
            if 'regions' in user_preferences and user_preferences['regions']:
                for region in user_preferences['regions']:
                    if f'region_{region}' in comparison_df.columns:
                        match_score += comparison_df.loc[program, f'region_{region}'] / comparison_df.loc[program, 'total_hotels'] * 100
            
            comparison_df.loc[program, 'preference_match'] = match_score
    
    return comparison_df

# Функція для визначення найкращої категорії для наявної кількості поїнтів
def recommend_category_for_points(program, points, hotel_data):
    """
    Рекомендує найкращу категорію готелю для наявної кількості поїнтів
    
    Args:
        program: назва програми лояльності
        points: кількість поїнтів
        hotel_data: DataFrame з даними готелів
    
    Returns:
        dict: рекомендації для кожної категорії
    """
    # Орієнтовні значення поїнтів для ночі в різних категоріях
    points_per_night = {
        "Luxury": 50000,
        "Comfort": 25000,
        "Standard": 15000
    }
    
    # Результати для кожної категорії
    results = {}
    
    # Для кожної категорії
    for category, avg_points in points_per_night.items():
        # Кількість ночей, яку можна забронювати
        nights = points // avg_points
        
        # Залишок поїнтів
        remaining = points % avg_points
        
        # Рекомендація
        recommendation = {
            "nights": nights,
            "points_per_night": avg_points,
            "remaining_points": remaining,
            "percentage_used": (points - remaining) / points * 100 if points > 0 else 0
        }
        
        results[category] = recommendation
    
    return results

# Функція для аналізу географічного розподілу готелів програми лояльності
def analyze_geographic_distribution(program, hotel_data):
    """
    Аналізує географічний розподіл готелів програми лояльності
    
    Args:
        program: назва програми лояльності
        hotel_data: DataFrame з даними готелів
    
    Returns:
        dict: розподіл готелів по регіонах та країнах
    """
    program_data = hotel_data[hotel_data['loyalty_program'] == program].copy()
    
    # Розподіл по регіонах
    region_distribution = program_data['region'].value_counts()
    
    # Розподіл по країнах
    country_distribution = program_data['country'].value_counts()
    
    # Топ-5 країн
    top_countries = dict(country_distribution.head(5))
    
    # Додаємо відсотки
    total_hotels = len(program_data)
    region_percentages = {region: count / total_hotels * 100 for region, count in region_distribution.items()}
    country_percentages = {country: count / total_hotels * 100 for country, count in top_countries.items()}
    
    result = {
        "total_hotels": total_hotels,
        "region_distribution": {
            "counts": dict(region_distribution),
            "percentages": region_percentages
        },
        "top_countries": {
            "counts": top_countries,
            "percentages": country_percentages
        }
    }
    
    return result

# Функція для оцінки співвідношення якості до ціни
def estimate_value_for_money(program, category, hotel_data):
    """
    Оцінює співвідношення якості до ціни для програми лояльності у заданій категорії
    
    Args:
        program: назва програми лояльності
        category: категорія (Luxury, Comfort, Standard)
        hotel_data: DataFrame з даними готелів
    
    Returns:
        dict: оцінка співвідношення якості до ціни
    """
    # Фільтруємо дані
    filtered_data = hotel_data[(hotel_data['loyalty_program'] == program) & 
                             (hotel_data['segment'] == category)]
    
    # Середня кількість готелів у категорії для всіх програм
    all_programs_count = hotel_data[hotel_data['segment'] == category].groupby('loyalty_program').size().mean()
    
    # Кількість готелів у програмі та категорії
    program_count = len(filtered_data)
    
    # Відносна доступність (порівняно з середньою)
    availability_score = program_count / all_programs_count if all_programs_count > 0 else 0
    
    # Оцінка співвідношення якості до ціни (приблизна логіка)
    if category == "Luxury":
        value_rating = min(10, availability_score * 8)  # Максимум 10 балів
    elif category == "Comfort":
        value_rating = min(10, availability_score * 10)  # Максимум 10 балів
    else:  # Standard
        value_rating = min(10, availability_score * 12)  # Максимум 10 балів
    
    result = {
        "program": program,
        "category": category,
        "hotels_count": program_count,
        "average_count_all_programs": all_programs_count,
        "availability_score": availability_score,
        "value_for_money_rating": value_rating
    }
    
    return result

# Щоб уникнути кругових імпортів і помилок, переконайтеся, що всі необхідні функції визначені
# Перевірте відсутність залежностей, які можуть викликати помилки при запуску

# Додайте ці функції до вашого файлу hotel-quiz-bot.py
# Також переконайтеся, що ви імпортували всі необхідні бібліотеки на початку файлу

# Кінець додаткових функцій

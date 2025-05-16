main_counts = main_df.groupby('Loyalty Program')['Hotel Brand'].count().to_dict()
            
            # Виводимо кількість готелів для цього стилю
            rank = 1
            for program, count in sorted(main_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0 and rank <= 5:  # Обмежуємо до топ-5
                    result_text += f"{rank}. {program}: {count} готелів\n"
                    rank += 1
            
            result_text += "\n"
        
        await update.message.reply_text(result_text)
        
        # Переходимо до наступного питання
        return await self.ask_question_4(update, context)

    async def ask_question_4(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Задає четверте питання про мету проживання в готелі."""
        message_text = """
**Питання 4/4**
З якою метою ви зазвичай зупиняєтеся в готелі?
(Оберіть до двох варіантів.)
1. Бізнес-подорожі / відрядження (конференції, ділові зустрічі, робочі простори)
2. Відпустка / релакс (відпочинок наодинці або з партнером, спа, пляж, тиша)
3. Сімейний відпочинок (подорож із дітьми, сімейні номери, дитячі зони)
4. Довготривале проживання (кухня в номері, домашній комфорт, самообслуговування)
"""
        # Надсилаємо повідомлення
        await update.message.reply_text(message_text)
        return QUESTION_4

    async def process_question_4(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обробляє відповідь на четверте питання."""
        user_id = update.effective_user.id
        dev_mode = self.dev_mode.get(user_id, False)
        answer = update.message.text
        
        # Мапування цілей проживання та відповідних брендів готелів
        purpose_brands = {
            "Бізнес-подорожі": ["Marriott Hotels", "InterContinental Hotels & Resorts", "Crowne Plaza", 
                             "Hyatt Regency", "Grand Hyatt", "Courtyard by Marriott", "Hilton Garden Inn", 
                             "Sheraton", "DoubleTree by Hilton", "Novotel Hotels", "Cambria Hotels", 
                             "Fairfield Inn & Suites", "Holiday Inn Express", "Wingate by Wyndham", 
                             "Quality Inn Hotels", "ibis Hotels"],
            "Відпустка / релакс": ["The Ritz-Carlton", "JW Marriott", "Waldorf Astoria Hotels & Resorts", 
                                "Conrad Hotels & Resorts", "Park Hyatt Hotels", "Fairmont Hotels", 
                                "Raffles Hotels & Resorts", "InterContinental Hotels & Resorts", 
                                "Kimpton Hotels & Restaurants", "Alila Hotels", 
                                "Registry Collection Hotels", "Ascend Hotel Collection"],
            "Сімейний відпочинок": ["JW Marriott", "Hyatt Regency", "Sheraton", "Holiday Inn Hotels & Resorts", 
                                "DoubleTree by Hilton", "Wyndham", "Mercure Hotels", "Novotel Hotels", 
                                "Comfort Inn Hotels", "Hampton by Hilton", "Holiday Inn Express", 
                                "Days Inn by Wyndham", "Super 8 by Wyndham"],
            "Довготривале проживання": ["Hyatt House", "Candlewood Suites", "ibis Styles"]
        }
        
        # Парсимо відповідь користувача
        selected_purposes = []
        
        if "1" in answer:
            selected_purposes.append("Бізнес-подорожі")
        if "2" in answer:
            selected_purposes.append("Відпустка / релакс")
        if "3" in answer:
            selected_purposes.append("Сімейний відпочинок")
        if "4" in answer:
            selected_purposes.append("Довготривале проживання")
        
        # Обмежуємо кількість обраних цілей до 2
        selected_purposes = selected_purposes[:2]
        
        # Зберігаємо вибір користувача
        context.user_data['purposes'] = selected_purposes
        
        if not selected_purposes:
            await update.message.reply_text("Будь ласка, оберіть принаймні одну мету проживання в готелі.")
            return QUESTION_4
        
        # Фільтрація готелів за регіоном/країною
        filtered_df = self.df
        
        if context.user_data.get('regions'):
            filtered_df = filtered_df[filtered_df['Region'].isin(context.user_data['regions'])]
        
        if context.user_data.get('countries'):
            filtered_df = filtered_df[filtered_df['Country'].isin(context.user_data['countries'])]
        
        # Обчислення кількості готелів і балів для кожної обраної цілі
        purpose_scores = {}
        
        for purpose in selected_purposes:
            # Отримуємо бренди для цієї цілі
            brands = purpose_brands.get(purpose, [])
            
            # Фільтруємо дані за брендами
            purpose_df = filtered_df[filtered_df['Hotel Brand'].isin(brands)]
            
            # Підраховуємо кількість готелів для кожної програми лояльності
            purpose_counts = purpose_df.groupby('Loyalty Program')['Hotel Brand'].count().to_dict()
            
            # Розрахунок балів
            purpose_program_scores = {}
            rank = 1
            for program, count in sorted(purpose_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    if rank == 1:
                        purpose_program_scores[program] = 21
                    elif rank == 2:
                        purpose_program_scores[program] = 18
                    elif rank == 3:
                        purpose_program_scores[program] = 15
                    elif rank == 4:
                        purpose_program_scores[program] = 12
                    elif rank == 5:
                        purpose_program_scores[program] = 9
                    elif rank == 6:
                        purpose_program_scores[program] = 6
                    elif rank == 7:
                        purpose_program_scores[program] = 3
                    else:
                        purpose_program_scores[program] = 0
                    rank += 1
                else:
                    purpose_program_scores[program] = 0
            
            # Додаємо бали за цю ціль
            for program, score in purpose_program_scores.items():
                if program in purpose_scores:
                    purpose_scores[program] += score
                else:
                    purpose_scores[program] = score
            
            if dev_mode:
                await update.message.reply_text(f"Розрахунки для цілі '{purpose}':")
                
                # Виводимо кількість готелів
                counts_text = f"Кількість готелів для цілі '{purpose}':\n\n"
                for program, count in sorted(purpose_counts.items(), key=lambda x: x[1], reverse=True):
                    counts_text += f"{program}: {count} готелів\n"
                
                await update.message.reply_text(counts_text)
                
                # Виводимо нараховані бали
                scores_text = f"Бали за ціль '{purpose}':\n\n"
                for program, score in sorted(purpose_program_scores.items(), key=lambda x: x[1], reverse=True):
                    scores_text += f"{program}: {score} балів\n"
                
                await update.message.reply_text(scores_text)
        
        # Нормалізуємо бали за кількістю обраних цілей
        normalized_purpose_scores = {}
        for program, score in purpose_scores.items():
            normalized_purpose_scores[program] = score / len(selected_purposes)
        
        # Зберігаємо результати
        context.user_data['purpose_scores'] = normalized_purpose_scores
        
        # Виводимо результат для користувача
        result_text = f"Ось як оцінені програми лояльності за обраними цілями проживання:\n\n"
        
        for purpose in selected_purposes:
            result_text += f"Ціль: {purpose}\n"
            
            # Отримуємо бренди для цієї цілі
            brands = purpose_brands.get(purpose, [])
            
            # Фільтруємо дані за брендами
            purpose_df = filtered_df[filtered_df['Hotel Brand'].isin(brands)]
            
            # Підраховуємо кількість готелів для кожної програми лояльності
            purpose_counts = purpose_df.groupby('Loyalty Program')['Hotel Brand'].count().to_dict()
            
            # Виводимо кількість готелів для цієї цілі
            rank = 1
            for program, count in sorted(purpose_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0 and rank <= 5:  # Обмежуємо до топ-5
                    result_text += f"{rank}. {program}: {count} готелів\n"
                    rank += 1
            
            result_text += "\n"
        
        await update.message.reply_text(result_text)
        
        # Обчислюємо загальний результат
        return await self.calculate_final_results(update, context)
        
    async def calculate_final_results(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обчислює та виводить загальний результат."""
        user_id = update.effective_user.id
        dev_mode = self.dev_mode.get(user_id, False)
        
        # Збираємо всі бали
        region_scores = context.user_data.get('region_scores', {})
        category_scores = context.user_data.get('category_scores', {})
        style_scores = context.user_data.get('style_scores', {})
        purpose_scores = context.user_data.get('purpose_scores', {})
        
        # Об'єднуємо всі програми лояльності
        all_programs = set()
        for scores_dict in [region_scores, category_scores, style_scores, purpose_scores]:
            all_programs.update(scores_dict.keys())
        
        # Обчислюємо загальний бал для кожної програми
        total_scores = {}
        for program in all_programs:
            region_score = region_scores.get(program, 0)
            category_score = category_scores.get(program, 0)
            style_score = style_scores.get(program, 0)
            purpose_score = purpose_scores.get(program, 0)
            
            total_scores[program] = region_score + category_score + style_score + purpose_score
        
        if dev_mode:
            await update.message.reply_text("Деталі розрахунку загального балу:")
            
            for program in sorted(total_scores, key=lambda p: total_scores[p], reverse=True):
                scores_text = f"{program}:\n"
                scores_text += f"- Бали за регіон: {region_scores.get(program, 0):.1f}\n"
                scores_text += f"- Бали за категорію: {category_scores.get(program, 0):.1f}\n"
                scores_text += f"- Бали за стиль: {style_scores.get(program, 0):.1f}\n"
                scores_text += f"- Бали за мету проживання: {purpose_scores.get(program, 0):.1f}\n"
                scores_text += f"- Загальний бал: {total_scores[program]:.1f}\n"
                
                await update.message.reply_text(scores_text)
        
        # Сортуємо програми за загальним балом
        sorted_programs = sorted(total_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Обираємо топ-5 програм
        top_programs = sorted_programs[:5]
        
        # Готуємо текст з результатами
        result_text = "**Топ-5 програм лояльності для вас:**\n\n"
        
        for rank, (program, score) in enumerate(top_programs, 1):
            result_text += f"{rank}. **{program}** - {score:.1f} балів\n"
            
            # Додаємо інформацію про бали за кожною категорією
            result_text += f"   Регіон: {region_scores.get(program, 0):.1f} | "
            result_text += f"Категорія: {category_scores.get(program, 0):.1f} | "
            result_text += f"Стиль: {style_scores.get(program, 0):.1f} | "
            result_text += f"Мета: {purpose_scores.get(program, 0):.1f}\n"
            
            # Додаємо найбільш підходящі бренди для цієї програми
            # Це потрібно реалізувати на основі обраних параметрів користувача
            result_text += "   Рекомендовані бренди: "
            
            # Отримуємо бренди для цієї програми з CSV файлу
            program_df = self.df[self.df['Loyalty Program'] == program]
            
            # Фільтруємо за обраними параметрами користувача
            if context.user_data.get('regions'):
                program_df = program_df[program_df['Region'].isin(context.user_data['regions'])]
            
            if context.user_data.get('countries'):
                program_df = program_df[program_df['Country'].isin(context.user_data['countries'])]
            
            # Отримуємо унікальні бренди
            program_brands = program_df['Hotel Brand'].unique()
            
            # Додаємо перші 3 бренди (або менше, якщо їх менше 3)
            brands_text = ", ".join(program_brands[:3])
            result_text += f"{brands_text}\n\n"
        
        # Відправляємо результат
        await update.message.reply_text(result_text)
        
        # Завершуємо розмову
        await update.message.reply_text("Дякую за використання нашого бота для вибору програми лояльності! Якщо хочете почати знову, просто введіть /start.")
        
        return ConversationHandler.END

    def register_handlers(self, application):
        """Реєструє обробники команд та повідомлень."""
        # Створюємо ConversationHandler
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', self.start)],
            states={
                QUESTION_1: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_question_1)],
                QUESTION_2: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_question_2)],
                QUESTION_3: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_question_3)],
                QUESTION_4: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_question_4)],
            },
            fallbacks=[CommandHandler('start', self.start)],
        )
        
        # Додаємо ConversationHandler до application
        application.add_handler(conv_handler)
        
        # Додаємо обробник команди /test
        application.add_handler(CommandHandler('test', self.test_command))

def main():
    """Основна функція для запуску бота."""
    # Створюємо об'єкт бота
    bot = HotelQuizBot()
    
    # Отримуємо токен бота з середовища
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("Не вказано токен бота. Встановіть змінну середовища TELEGRAM_BOT_TOKEN.")
        return
    
    # Створюємо application
    application = Application.builder().token(token).build()
    
    # Реєструємо обробники
    bot.register_handlers(application)
    
    # Запускаємо бота
    application.run_polling()

if __name__ == '__main__':
    main()
FROM python:3.11-slim

WORKDIR /app

# Копіюємо requirements
COPY requirements.txt .

# Встановлюємо залежності
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо код додатку
COPY . .

# Створюємо директорію для даних
RUN mkdir -p /app/data

# Відкриваємо порт
EXPOSE 5000

# Запускаємо Flask
CMD ["python", "app.py"]
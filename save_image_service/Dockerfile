FROM python:3.9-slim

WORKDIR /app

# Системные зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Исходный код
COPY app/ .

# Python path
ENV PYTHONPATH=/app

# Порт, на котором реально работает gunicorn
EXPOSE 9000

CMD ["gunicorn", "-b", "0.0.0.0:9000", "-k", "uvicorn.workers.UvicornWorker", "main:app"]


# FROM python:3.9-slim

# WORKDIR /app

# # Установка зависимостей
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends gcc python3-dev curl && \
#     rm -rf /var/lib/apt/lists/*

# # Копируем зависимости
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # Копируем исходный код
# COPY app/ .
# COPY .env.template .
# COPY requirements.txt .
# COPY etc/secrets/authorized_key-5.json etc/secrets/authorized_key-5.json

# # Создаем директории для секретов
# RUN mkdir -p /etc/secrets && \
#     chmod 700 /etc/secrets

# # Копируем файл ключа
# COPY etc/secrets/authorized_key-5.json /etc/secrets/authorized_key-5.json
# RUN chmod 400 /etc/secrets/authorized_key-5.json

# # Создаем .env из шаблона
# RUN cp .env.template .env && \
#     chmod 600 .env

# EXPOSE 8080

# # Указываем Python, что нужно искать модули в /app
# ENV PYTHONPATH=/app

# CMD ["gunicorn", "-b", "0.0.0.0:9000", "-k", "uvicorn.workers.UvicornWorker", "main:app"]
# Python 3.11 slim
FROM python:3.11-slim

WORKDIR /scripts

# Установим зависимости
RUN pip install --no-cache-dir pandas pymongo tqdm

# Копируем скрипты и данные
COPY ./scripts /scripts
COPY ./data /data

# Точка входа
CMD ["python", "/scripts/setup_import.py"]

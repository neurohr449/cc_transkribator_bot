FROM python:3.12-slim-bookworm

# Установка зависимостей для мультимедиа и базовых утилит
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libavcodec-dev \
    libavformat-dev \
    libswscale-dev \
    libavutil-dev \
    libopus-dev \
    libvpx-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала копируем requirements.txt для лучшего кэширования слоев
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем остальные файлы
COPY . .

CMD ["python", "main.py"]
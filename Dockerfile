# Use the official Python image as the base image
FROM python:3.12-slim-bookworm

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Install system dependencies (добавлены ffmpeg и аудио-библиотеки)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    curl \
    ffmpeg \                    # Основной пакет для обработки аудио
    libavcodec-dev \             # Доп. кодеки
    libavformat-dev \            # Поддержка форматов
    libswscale-dev \             # Обработка медиа
    && rm -rf /var/lib/apt/lists/*

# Явно указываем пути к ffmpeg (опционально, но рекомендуется)
ENV PATH="/usr/bin/ffmpeg:${PATH}"

# Set the working directory
WORKDIR /app

# Копируем зависимости отдельно для кэширования
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем остальные файлы
COPY . .

# Expose the port the app runs on
EXPOSE 8443

# Фикс для pydub (явно указываем пути к бинарникам)
RUN echo "AudioSegment.converter = \"/usr/bin/ffmpeg\"" >> /usr/local/lib/python3.12/site-packages/pydub/__init__.py && \
    echo "AudioSegment.ffprobe = \"/usr/bin/ffprobe\"" >> /usr/local/lib/python3.12/site-packages/pydub/__init__.py

# Set the default command
CMD ["python", "main.py"]
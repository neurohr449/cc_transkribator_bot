# Use the official Python image as the base image
FROM python:3.12-slim-bookworm

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Install system dependencies (исправленное форматирование)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    curl \
    ffmpeg \
    libavcodec-dev \
    libavformat-dev \
    libswscale-dev \
    && rm -rf /var/lib/apt/lists/*

# Явно указываем пути к ffmpeg
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

# Фикс для pydub (без модификации системных файлов)
RUN echo -e "from pydub import AudioSegment\nAudioSegment.converter = \"/usr/bin/ffmpeg\"\nAudioSegment.ffprobe = \"/usr/bin/ffprobe\"" > /app/ffmpeg_fix.py

# Set the default command
CMD ["python", "main.py"]
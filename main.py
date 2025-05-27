import asyncio
import logging
import sys
import os
import json
from datetime import datetime, timedelta
import aiohttp
from aiogram import types
from aiogram import Bot, Dispatcher, html, Router, BaseMiddleware, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.filters.state import StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from openai import OpenAI
from pydub import AudioSegment  
import tempfile
import aiofiles
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import uuid
import math
from aiogram.client.session.aiohttp import AiohttpSession

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MAX_FILE_SIZE = 24 * 1024 * 1024  # 24 МБ с запасом от лимита в 25 МБ
CHUNK_DURATION = 300  # Длительность чанка в секундах (5 минут)
DOWNLOAD_TIMEOUT = 300  # 5 минут для скачивания
MAX_RETRIES = 3  # Количество попыток скачивания

# Настройка сессии с увеличенным таймаутом
session = AiohttpSession(timeout=aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT))
bot = Bot(token=BOT_TOKEN, session=session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
router = Router()
dp = Dispatcher(storage=storage)

class UserState(StatesGroup):
    audio = State()
    ass_token = State()
    company_name = State()

class StateMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        state = data['state']
        current_state = await state.get_state()
        data['current_state'] = current_state
        return await handler(event, data)

async def safe_download(file: types.File, destination: str) -> bool:
    """Безопасное скачивание файла с повторными попытками"""
    for attempt in range(MAX_RETRIES):
        try:
            await bot.download(file, destination=destination)
            return True
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            if attempt == MAX_RETRIES - 1:
                raise
            await asyncio.sleep(2 * (attempt + 1))
            continue
        except Exception as e:
            raise
    return False

async def convert_audio(input_path: str) -> str:
    """Конвертирует аудио в WAV с уникальным именем файла"""
    unique_id = uuid.uuid4().hex
    output_path = os.path.join(tempfile.gettempdir(), f"converted_{unique_id}.wav")
    
    try:
        audio = AudioSegment.from_file(input_path)
        audio = audio.set_channels(1)  # Моно
        audio = audio.set_frame_rate(8000)  # 8 kHz
        
        audio.export(
            output_path,
            format="wav",
            codec="pcm_s16le",
            bitrate="64k"
        )
        
        if os.path.getsize(output_path) > MAX_FILE_SIZE:
            os.remove(output_path)
            raise ValueError("Файл слишком большой после конвертации")
            
        return output_path
    except Exception as e:
        logging.error(f"Ошибка конвертации: {e}")
        if 'output_path' in locals() and os.path.exists(output_path):
            os.remove(output_path)
        return None

async def process_large_audio(file_path: str) -> str:
    """Разбивает и обрабатывает большой аудиофайл"""
    try:
        audio = AudioSegment.from_file(file_path)
        duration_sec = len(audio) / 1000
        num_chunks = math.ceil(duration_sec / CHUNK_DURATION)
        all_texts = []
        
        for i in range(num_chunks):
            start = i * CHUNK_DURATION * 1000
            end = (i + 1) * CHUNK_DURATION * 1000
            chunk = audio[start:end]
            
            chunk_path = f"{file_path}_chunk_{i}.wav"
            try:
                chunk.export(chunk_path, format="wav")
                
                with open(chunk_path, "rb") as f:
                    transcript = client.audio.transcriptions.create(
                        file=f,
                        model="whisper-1", 
                        language="ru"
                    )
                    all_texts.append(transcript.text)
            finally:
                if os.path.exists(chunk_path):
                    os.remove(chunk_path)
        
        return "\n\n".join(f"[Часть {i+1}/{num_chunks}]\n{text}" for i, text in enumerate(all_texts))
    except Exception as e:
        logging.error(f"Ошибка обработки большого файла: {e}")
        raise

async def process_audio_file(file_path: str, file_name: str, message: types.Message, state: FSMContext) -> int:
    """Обрабатывает аудиофайл с автоматической разбивкой при необходимости"""
    try:
        file_size = os.path.getsize(file_path)
        
        if file_size <= MAX_FILE_SIZE:
            with open(file_path, "rb") as audio_file:
                transcript = client.audio.transcriptions.create(
                    file=audio_file,
                    model="whisper-1",
                    language="ru"
                )
            transcription_text = transcript.text
        else:
            await message.reply("🔪 Разбиваю большой файл на части...")
            transcription_text = await process_large_audio(file_path)
        
        state_data = await state.get_data()
        assistant_id = state_data.get('ass_token')
        
        thread = client.beta.threads.create()
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=transcription_text
        )
        
        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=assistant_id
        )
        
        while True:
            run_status = client.beta.threads.runs.retrieve(
                thread_id=thread.id,
                run_id=run.id
            )
            if run_status.status == "completed":
                break
            await asyncio.sleep(1)
        
        messages = client.beta.threads.messages.list(thread_id=thread.id)
        response_text = messages.data[0].content[0].text.value
        
        username = message.from_user.username or str(message.from_user.id)
        return await write_to_google_sheets(
            transcription_text=transcription_text,
            ai_response=response_text,
            file_name=file_name,
            username=username,
            state=state
        )
    except Exception as e:
        logging.error(f"Ошибка обработки файла: {e}")
        raise

@router.message(CommandStart())
async def command_start_handler(message: Message, state: FSMContext) -> None:
    await state.set_state(UserState.ass_token)
    await message.answer("👋 Добро пожаловать в наш чат-бот! Для начала нужен токен ассистента")

@router.message(StateFilter(UserState.ass_token))
async def company_name(message: Message, state: FSMContext):
    await state.update_data(ass_token=message.text)
    await state.set_state(UserState.company_name)
    await message.answer("Напиши название компании")

@router.message(StateFilter(UserState.company_name))
async def ass_token(message: Message, state: FSMContext):
    await state.update_data(company_name=message.text)
    await state.set_state(UserState.audio)
    await message.answer("Присылай аудио для оценки")

@router.message(F.voice | F.audio | F.document, StateFilter(UserState.audio))
async def handle_audio(message: types.Message, state: FSMContext):
    unique_id = uuid.uuid4().hex
    input_path = None
    output_path = None
    
    try:
        # Определение типа файла
        if message.voice:
            file = await bot.get_file(message.voice.file_id)
            ext = "ogg"
            file_name = "Голосовое сообщение"
        elif message.audio:
            file = await bot.get_file(message.audio.file_id)
            ext = "mp3"
            file_name = message.audio.file_name or "Аудиофайл"
        else:
            if not message.document.mime_type.startswith('audio/'):
                await message.reply("❌ Пожалуйста, отправьте аудиофайл")
                return
            file = await bot.get_file(message.document.file_id)
            ext = os.path.splitext(message.document.file_name)[1][1:] or "mp3"
            file_name = message.document.file_name
        
        input_path = f"temp_{unique_id}.{ext}"
        
        # Скачивание с обработкой ошибок
        try:
            if not await safe_download(file, input_path):
                await message.reply("❌ Не удалось скачать файл после нескольких попыток")
                return
        except Exception as e:
            await message.reply(f"❌ Ошибка скачивания файла: {str(e)}")
            return
        
        # Проверка размера файла
        if os.path.getsize(input_path) > 100 * 1024 * 1024:
            os.remove(input_path)
            await message.reply("❌ Файл слишком большой. Максимальный размер: 100MB")
            return

        await message.reply("🔍 Начинаю обработку аудио...")
        
        # Конвертация
        output_path = await convert_audio(input_path)
        if not output_path:
            await message.reply("❌ Ошибка конвертации аудио")
            return

        # Обработка файла
        try:
            row_number = await process_audio_file(output_path, file_name, message, state)
            await message.reply(f"✅ Результат записан в строку {row_number}")
        except Exception as e:
            await message.reply(f"❌ Ошибка обработки: {str(e)}")
            
    except Exception as e:
        logging.exception("Ошибка в handle_audio")
        await message.reply("❌ Произошла непредвиденная ошибка при обработке файла")
    finally:
        # Гарантированная очистка временных файлов
        for path in [input_path, output_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    logging.error(f"Ошибка удаления файла {path}: {e}")

async def write_to_google_sheets(transcription_text: str, ai_response: str, file_name: str, username: str, state: FSMContext) -> int:
    """Записывает данные в Google Sheets и возвращает номер строки"""
    try:
        user_data = await state.get_data()
        service_account_info = {
            "type": os.getenv("GS_TYPE"),
            "project_id": os.getenv("GS_PROJECT_ID"),
            "private_key_id": os.getenv("GS_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GS_PRIVATE_KEY").replace('\\n', '\n'),
            "client_email": os.getenv("GS_CLIENT_EMAIL"),
            "client_id": os.getenv("GS_CLIENT_ID"),
            "auth_uri": os.getenv("GS_AUTH_URI"),
            "token_uri": os.getenv("GS_TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("GS_AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("GS_CLIENT_X509_CERT_URL"),
            "universe_domain": os.getenv("UNIVERSE_DOMAIN", "googleapis.com")
        }

        scope = ['https://www.googleapis.com/auth/spreadsheets',
               'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        gc = gspread.authorize(creds)

        spreadsheet = gc.open_by_key(os.getenv("GSHEETS_SPREADSHEET_ID"))
        worksheet = spreadsheet.worksheet(os.getenv("GSHEETS_SHEET_NAME", "Sheet1"))

        row_data = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(transcription_text),
            str(ai_response),
            str(file_name),
            f"@{username}",
            f"https://t.me/{username}",
            user_data.get('company_name'),
            user_data.get('ass_token')
        ]

        worksheet.append_row(row_data)
        return len(worksheet.col_values(1))
    
    except Exception as e:
        logging.error(f"Ошибка записи в Google Sheets: {str(e)}")
        raise Exception(f"Ошибка записи в таблицу: {str(e)}")

async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout
    )
    dp.include_router(router)
    dp.message.middleware(StateMiddleware())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
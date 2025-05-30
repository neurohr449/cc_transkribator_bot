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
from urllib.parse import urlparse, parse_qs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
from typing import List
import time

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MAX_FILE_SIZE = 24 * 1024 * 1024  
CHUNK_DURATION = 300  
DOWNLOAD_TIMEOUT = 1200 
MAX_RETRIES = 3  
MAX_FILES_PER_FOLDER = 100  # Максимальное количество файлов для обработки из одной папки

# Настройки Google Drive
GOOGLE_DRIVE_CREDS = {
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

# Инициализация бота
session = AiohttpSession(timeout=aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT))
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    session_timeout=DOWNLOAD_TIMEOUT  
)
storage = MemoryStorage()
router = Router()
dp = Dispatcher(storage=storage)

# Состояния бота
class UserState(StatesGroup):
    ass_token = State()
    company_name = State()
    audio = State()
    folder_processing = State()

# Middleware для отслеживания состояния
class StateMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        state = data['state']
        current_state = await state.get_state()
        data['current_state'] = current_state
        return await handler(event, data)

# Сервис для работы с Google Drive
async def get_google_drive_service():
    """Создает сервис для работы с Google Drive"""
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_DRIVE_CREDS,
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

# Функции для работы с Google Drive
def extract_file_id_from_url(url: str) -> str:
    """Извлекает ID файла или папки из URL Google Drive"""
    parsed = urlparse(url)
    
    # Обработка разных форматов URL
    if 'drive.google.com' in parsed.netloc:
        if '/file/d/' in parsed.path:
            return parsed.path.split('/')[3]  # Формат: https://drive.google.com/file/d/FILE_ID/view
        elif '/drive/folders/' in parsed.path:
            return parsed.path.split('/')[4]  # Формат: https://drive.google.com/drive/folders/FOLDER_ID
        elif 'id=' in parsed.query:
            return parse_qs(parsed.query)['id'][0]  # Формат: https://drive.google.com/open?id=FILE_ID
    
    return None

async def download_from_google_drive(file_id: str, destination: str) -> bool:
    """Скачивает файл из Google Drive"""
    try:
        service = await get_google_drive_service()
        request = service.files().get_media(fileId=file_id)
        
        fh = io.FileIO(destination, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logging.info(f"Download {int(status.progress() * 100)}%.")
        
        return True
    except Exception as e:
        logging.error(f"Ошибка загрузки из Google Drive: {e}")
        return False

async def list_files_in_folder(folder_id: str) -> List[dict]:
    """Возвращает список аудиофайлов в указанной папке Google Drive"""
    service = await get_google_drive_service()
    results = []
    page_token = None
    
    try:
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents and (mimeType contains 'audio/' or mimeType contains 'application/octet-stream')",
                spaces='drive',
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                pageSize=MAX_FILES_PER_FOLDER
            ).execute()
            
            results.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            
            if page_token is None or len(results) >= MAX_FILES_PER_FOLDER:
                break
                
        return results[:MAX_FILES_PER_FOLDER]  # Ограничиваем максимальное количество файлов
    except Exception as e:
        logging.error(f"Ошибка при получении списка файлов: {e}")
        raise

# Функции обработки аудио
async def safe_download_file(url: str, destination: str) -> bool:
    """Безопасное скачивание файла по URL"""
    for attempt in range(MAX_RETRIES):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        async with aiofiles.open(destination, 'wb') as f:
                            await f.write(await response.read())
                        return True
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            if attempt == MAX_RETRIES - 1:
                raise
            await asyncio.sleep(2 * (attempt + 1))
            continue
    return False

async def convert_audio(input_path: str) -> str:
    """Конвертирует аудио в WAV формать"""
    unique_id = uuid.uuid4().hex
    output_path = os.path.join(tempfile.gettempdir(), f"converted_{unique_id}.wav")
    
    try:
        audio = AudioSegment.from_file(input_path)
        audio = audio.set_channels(1).set_frame_rate(8000)
        
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
    """Разбивает большой аудиофайл на части и обрабатывает их"""
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
    """Обрабатывает аудиофайл и возвращает номер строки в Google Sheets"""
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

async def process_folder(folder_url: str, message: types.Message, state: FSMContext):
    """Обрабатывает все аудиофайлы в указанной папке"""
    folder_id = extract_file_id_from_url(folder_url)
    if not folder_id:
        await message.reply("❌ Не удалось определить ID папки из ссылки")
        return False
    
    try:
        files = await list_files_in_folder(folder_id)
        if not files:
            await message.reply("🔍 В папке не найдено аудиофайлов")
            return False
        
        await state.set_state(UserState.folder_processing)
        await state.update_data(current_folder=folder_id, files_to_process=files)
        
        total_files = len(files)
        await message.reply(f"🔍 Найдено {total_files} аудиофайлов. Начинаю обработку...")
        
        processed_count = 0
        failed_count = 0
        results = []
        
        for file in files:
            file_id = file['id']
            file_name = file['name']
            
            try:
                unique_id = uuid.uuid4().hex
                ext = file_name.split('.')[-1] if '.' in file_name else 'mp3'
                input_path = f"temp_{unique_id}.{ext}"
                
                # Скачиваем файл
                if not await download_from_google_drive(file_id, input_path):
                    failed_count += 1
                    results.append(f"❌ {file_name} - ошибка скачивания")
                    continue
                
                # Конвертируем и обрабатываем
                output_path = await convert_audio(input_path)
                if not output_path:
                    failed_count += 1
                    results.append(f"❌ {file_name} - ошибка конвертации")
                    continue
                
                try:
                    row_number = await process_audio_file(output_path, file_name, message, state)
                    results.append(f"✅ {file_name} - строка {row_number}")
                    processed_count += 1
                except Exception as e:
                    logging.error(f"Ошибка обработки файла {file_name}: {e}")
                    results.append(f"❌ {file_name} - ошибка обработки")
                    failed_count += 1
                
                # Задержка между файлами
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Ошибка при обработке файла {file_name}: {e}")
                failed_count += 1
                results.append(f"❌ {file_name} - непредвиденная ошибка")
            finally:
                # Удаляем временные файлы
                for path in [input_path, output_path]:
                    if path and os.path.exists(path):
                        try:
                            os.remove(path)
                        except:
                            pass
        
        # Формируем итоговый отчет
        report = [
            f"📊 Итоговый отчет:",
            f"Всего файлов: {total_files}",
            f"Успешно обработано: {processed_count}",
            f"Не удалось обработать: {failed_count}",
            "",
            "Результаты по файлам:"
        ]
        
        # Разбиваем результаты на части, если их слишком много
        chunk_size = 40
        for i in range(0, len(results), chunk_size):
            chunk = results[i:i + chunk_size]
            report_chunk = "\n".join([*report[:5], *chunk]) if i == 0 else "\n".join(chunk)
            await message.reply(report_chunk)
        
        return True
    
    except Exception as e:
        logging.error(f"Ошибка при обработке папки: {e}")
        await message.reply(f"❌ Произошла ошибка при обработке папки: {e}")
        return False

# Функция записи в Google Sheets
async def write_to_google_sheets(transcription_text: str, ai_response: str, file_name: str, username: str, state: FSMContext) -> int:
    """Записывает данные в Google Sheets и возвращает номер строки"""
    try:
        user_data = await state.get_data()
        
        scope = ['https://www.googleapis.com/auth/spreadsheets',
               'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(GOOGLE_DRIVE_CREDS, scope)
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

# Обработчики команд
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
    await message.answer("Присылай ссылку на аудиофайл или папку в Google Drive для оценки")

@router.message(F.text, StateFilter(UserState.audio))
async def handle_audio_link(message: types.Message, state: FSMContext):
    """Обработчик ссылок на аудиофайлы и папки"""
    url = message.text.strip()
    
    # Проверяем, является ли ссылка на Google Drive
    if 'drive.google.com' not in url and 'docs.google.com' not in url:
        await message.reply("❌ Пожалуйста, отправьте корректную ссылку на Google Drive")
        return
    
    # Проверяем, это ссылка на папку или файл
    if 'folder' in url or 'drive.google.com/drive/folders' in url:
        # Это папка - обрабатываем все файлы
        await process_folder(url, message, state)
    else:
        # Это отдельный файл - обрабатываем как раньше
        file_id = extract_file_id_from_url(url)
        if not file_id:
            await message.reply("❌ Не удалось извлечь ID файла из ссылки")
            return
        
        unique_id = uuid.uuid4().hex
        input_path = None
        output_path = None
        
        try:
            ext = "mp3"  # Будем пытаться определить расширение после скачивания
            input_path = f"temp_{unique_id}.{ext}"
            
            
            
            if not await download_from_google_drive(file_id, input_path):
                await message.reply("❌ Не удалось скачать файл из Google Drive")
                return
            
            if os.path.getsize(input_path) > 100 * 1024 * 1024:
                os.remove(input_path)
                await message.reply("❌ Файл слишком большой. Максимальный размер: 100MB")
                return

            
            
            output_path = await convert_audio(input_path)
            if not output_path:
                await message.reply("❌ Ошибка конвертации аудио")
                return
            
            try:
                file_name = f"Google_Drive_file_{file_id[:8]}"
                row_number = await process_audio_file(output_path, file_name, message, state)
                await message.reply(f"✅ Результат записан в строку {row_number}")
            except Exception as e:
                await message.reply(f"❌ Ошибка обработки: {str(e)}")
                
        except Exception as e:
            logging.exception("Ошибка в handle_audio_link")
            await message.reply("❌ Произошла непредвиденная ошибка при обработке файла")
        finally:
            for path in [input_path, output_path]:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception as e:
                        logging.error(f"Ошибка удаления файла {path}: {e}")

# Запуск бота
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
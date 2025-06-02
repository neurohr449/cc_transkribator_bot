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
from openai import OpenAI, AsyncOpenAI
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
import ffmpeg


# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
client2 = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
BFL_TOKEN = os.getenv("")
MAX_FILE_SIZE = 24 * 1024 * 1024  
CHUNK_DURATION = 180 
DOWNLOAD_TIMEOUT = 1200 
MAX_RETRIES = 3  
MAX_FILES_PER_FOLDER = 1000  # Максимальное количество файлов для обработки из одной папки
IMG = "AgACAgIAAxkBAAO4aD2DBZsntEbv4pCVKjSi-Rg8JUkAAvPzMRuH3OlJMKrGXBeky5IBAAMCAAN4AAM2BA"


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
    sheet_id_token = State()
    audio_link = State()
class ImageUploadState(StatesGroup):
    waiting_for_image = State()
    
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

    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

    return build('drive', 'v3', credentials=creds)

async def get_chatgpt_response(prompt: str) -> str:
    try:
        response = await client2.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7  
        )
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return "Извините, не удалось обработать запрос"  

def extract_file_id_from_url(url: str) -> str:
    """Извлекает ID файла или папки из URL Google Drive с учетом всех форматов"""
    try:
        # Удаляем возможные параметры после ?
        clean_url = url.split('?')[0]
        
        # Форматы ссылок:
        # 1. https://drive.google.com/drive/folders/{folder_id}
        # 2. https://drive.google.com/open?id={file_id}
        # 3. https://drive.google.com/file/d/{file_id}/view
        # 4. https://docs.google.com/document/d/{file_id}/edit
        
        if 'drive.google.com' in clean_url:
            if '/folders/' in clean_url:
                # Ссылка на папку
                parts = clean_url.split('/folders/')
                if len(parts) > 1:
                    folder_id = parts[1].split('/')[0].split('?')[0]
                    if len(folder_id) > 5:  # Минимальная длина ID
                        return folder_id
            
            elif '/file/d/' in clean_url:
                # Ссылка на файл
                parts = clean_url.split('/file/d/')
                if len(parts) > 1:
                    file_id = parts[1].split('/')[0].split('?')[0]
                    if len(file_id) > 5:
                        return file_id
            
            elif 'id=' in url:
                # Ссылка с параметром id
                from urllib.parse import parse_qs, urlparse
                query = urlparse(url).query
                params = parse_qs(query)
                return params.get('id', [''])[0]
        
        return None
    except Exception as e:
        logging.error(f"Error extracting ID from URL: {e}")
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
    """Возвращает список аудио и видео файлов"""
    service = await get_google_drive_service()
    response = service.files().list(
        q=f"'{folder_id}' in parents and (mimeType contains 'audio/' or mimeType contains 'video/' or mimeType contains 'application/octet-stream')",
        fields="files(id, name, mimeType)",
        pageSize=MAX_FILES_PER_FOLDER
    ).execute()
    return response.get('files', [])

# Функции обработки аудио
async def safe_download_file(file: types.File, destination: str) -> bool:
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
    """Конвертирует аудио в MP3"""
    unique_id = uuid.uuid4().hex
    output_path = os.path.join(tempfile.gettempdir(), f"converted_{unique_id}.mp3")  # Изменили на MP3
    
    try:
        audio = AudioSegment.from_file(input_path)
        
        # Оптимальные параметры для уменьшения размера
        audio = audio.set_channels(1)  # Моно
        audio = audio.set_frame_rate(16000)  # 16 kHz
        
        audio.export(
            output_path,
            format="mp3",  
            bitrate="64k"  
        )
        
        if os.path.getsize(output_path) > MAX_FILE_SIZE:
            raise ValueError("Файл слишком большой после конвертации")
            
        return output_path
    except Exception as e:
        logging.error(f"Ошибка конвертации: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        return None

async def process_large_audio(file_path: str) -> str:
    """Разбивает большой файл на чанки в MP3"""
    try:
        audio = AudioSegment.from_file(file_path)
        all_texts = []
        
        # Рассчитываем максимальную длительность чанка (MP3 ~64kbps)
        max_chunk_duration_sec = (MAX_FILE_SIZE * 8) / 64000  # 64kbps в битах
        chunk_duration_ms = int(max_chunk_duration_sec * 1000)
        
        num_chunks = math.ceil(len(audio) / chunk_duration_ms)
        
        for i in range(num_chunks):
            start = i * chunk_duration_ms
            end = min((i + 1) * chunk_duration_ms, len(audio))
            chunk = audio[start:end]
            
            chunk_path = f"{file_path}_chunk_{i}.mp3"  # Изменили на MP3
            try:
                chunk.export(
                    chunk_path,
                    format="mp3",  # Экспорт в MP3
                    bitrate="64k",
                    parameters=["-ar", "16000"]  # Частота 16kHz
                )
                
                # Проверка размера
                if os.path.getsize(chunk_path) > MAX_FILE_SIZE:
                    raise ValueError(f"Чанк {i+1} превысил лимит размера")
                
                # Обработка
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
        
        return "\n\n".join(f"🔹 Часть {i+1}/{num_chunks}:\n{text}" for i, text in enumerate(all_texts))
    except Exception as e:
        logging.error(f"Ошибка обработки большого файла: {e}")
        raise

async def extract_audio_from_video(video_path: str) -> str:
    """Извлекает аудио из видео в MP3 формат"""
    audio_path = os.path.join(tempfile.gettempdir(), f"audio_{uuid.uuid4().hex}.mp3")
    try:
        video = AudioSegment.from_file(video_path)
        video.set_channels(1).set_frame_rate(16000).export(
            audio_path,
            format="mp3",
            bitrate="64k"
        )
        return audio_path
    except Exception as e:
        logging.error(f"Ошибка извлечения аудио: {e}")
        if os.path.exists(audio_path):
            os.remove(audio_path)
        return None

async def process_audio_file(file_path: str, file_name: str, message: types.Message, state: FSMContext) -> int:
    """Обрабатывает аудиофайл и возвращает номер строки в Google Sheets"""
    try:
        audio = AudioSegment.from_file(file_path)
        file_size = os.path.getsize(file_path)
        file_len = round(len(audio) / 1000)  
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
        personal_sheet_row = await write_to_google_sheets(
            transcription_text=transcription_text,
            ai_response=response_text,
            file_name=file_name,
            username=username,
            state=state,
            sheet_n=1,
            file_len=str(file_len)
        )
        return await write_to_google_sheets(
            transcription_text=transcription_text,
            ai_response=response_text,
            file_name=file_name,
            username=username,
            state=state,
            sheet_n=2,
            file_len=str(file_len)
        )
    except Exception as e:
        logging.error(f"Ошибка обработки файла: {e}")
        raise

async def process_folder(folder_url: str, message: types.Message, state: FSMContext):
    """Обрабатывает все аудиофайлы в указанной папке с параллельным выполнением"""
    folder_id = extract_file_id_from_url(folder_url)
    if not folder_id:
        await message.reply("❌ Не удалось определить ID папки из ссылки")
        return False
    
    try:
        files = await list_files_in_folder(folder_id)
        if not files:
            await message.reply("🔍 В папке не найдено аудиофайлов")
            return False
        
        
        await state.update_data(current_folder=folder_id, files_to_process=files)
        
        total_files = len(files)
        await message.reply(f"🔍 Найдено {total_files} файлов. Начинаю обработку...")

        # Создаем семафор для ограничения одновременных задач (3-5 в зависимости от сервера)
        concurrency_limit = asyncio.Semaphore(3)
        results = []

        async def process_single_file_wrapper(file: dict):
            async with concurrency_limit:
                file_id = file['id']
                file_name = file['name']
                input_path = f"temp_{uuid.uuid4().hex}_{file_name}"
                
                try:
                    # Скачивание
                    if not await download_from_google_drive(file_id, input_path):
                        return f"❌ {file_name} - ошибка скачивания"
                    audio = AudioSegment.from_file(input_path)
                    if len(audio) < 3000:
                        return f"⚠️ {file_name} - слишком короткое аудио (меньше 3 сек)"
                    # Если это видео - извлекаем аудио
                    if file['mimeType'].startswith('video/'):
                        audio_path = await extract_audio_from_video(input_path)
                        if not audio_path:
                            return f"❌ {file_name} - ошибка извлечения аудио"
                        processing_path = audio_path
                    else:
                        # Для аудио - конвертируем в MP3 если нужно
                        processing_path = await convert_audio(input_path) if not input_path.endswith('.mp3') else input_path
                        if not processing_path:
                            return f"❌ {file_name} - ошибка конвертации"

                    # Обработка
                    row_number = await process_audio_file(processing_path, file_name, message, state)
                    return f"✅ {file_name} - строка {row_number}"

                except Exception as e:
                    logging.error(f"Ошибка обработки {file_name}: {e}")
                    
                    return f"❌ {file_name} - ошибка: {str(e)}"
                finally:
                    # Удаляем все временные файлы
                    for path in [input_path, processing_path if 'processing_path' in locals() else None]:
                        if path and os.path.exists(path) and path != input_path:
                            try:
                                os.remove(path)
                            except:
                                pass

        # Запускаем все задачи параллельно
        tasks = [process_single_file_wrapper(file) for file in files]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Анализ результатов
        successful = sum(1 for r in results if isinstance(r, str) and r.startswith("✅"))
        failed = len(results) - successful

        # Формируем отчет
        report = [
            f"📊 Итоговый отчет:",
            f"Всего файлов: {total_files}",
            f"Успешно обработано: {successful}",
            f"Не удалось обработать: {failed}",
            "",
            "Результаты по файлам:"
        ]

        # Разбиваем результаты на части для отправки
        chunk_size = 40
        for i in range(0, len(results), chunk_size):
            chunk = results[i:i + chunk_size]
            report_chunk = "\n".join([*report[:5], *chunk]) if i == 0 else "\n".join(chunk)
            await message.reply(report_chunk)

        return True

    except Exception as e:
        logging.error(f"Ошибка при обработке папки: {e}")
        await message.reply(f"❌ Произошла критическая ошибка при обработке папки: {e}")
        return False

# Функция записи в Google Sheets
async def write_to_google_sheets(transcription_text: str, ai_response: str, file_name: str, username: str, sheet_n: int, file_len: str, state: FSMContext) -> int:
    """Записывает данные в Google Sheets и возвращает номер строки"""
    try:
        user_data = await state.get_data()
        
        scope = ['https://www.googleapis.com/auth/spreadsheets',
               'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(GOOGLE_DRIVE_CREDS, scope)
        gc = gspread.authorize(creds)
        if sheet_n == 1:
            spreadsheet = gc.open_by_key(os.getenv("GSHEETS_SPREADSHEET_ID"))
        else:
            spreadsheet = gc.open_by_key(user_data.get("sheet_id_token"))
        worksheet = spreadsheet.worksheet(os.getenv("GSHEETS_SHEET_NAME", "Sheet1"))

        promt = f"Твоя задача проанализировать название файла и написать ответ строго в заданном формате, если данных недостаточно вместо отсутствующих данных напиши Empty, сохраняя формат сообщения. Дополнительно для выдачи номера телефона используй следующие данные: Номер телефона всегда должен начинатся на +7 (если в названии файла это 8 или 7 замени на +7). Формат для выдачи номера телефона: +7 999 999-99-99  Название файла для анализа{file_name} Ответ дай строго в формате: День/Месяц/Год/Номер телефона"
        raw_response = await get_chatgpt_response(promt)
        print(raw_response)
        day, month, year, phone = raw_response.split('/')


        row_data = [
            (datetime.now() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"),
            str(transcription_text),
            str(ai_response),
            str(file_name),
            f"@{username}",
            f"https://t.me/{username}",
#           user_data.get('company_name'),
            user_data.get('ass_token'),
            file_len,
            phone,
            day,
            month,
            year
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
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="БФЛ", callback_data="bfl")],[InlineKeyboardButton(text="Другое", callback_data="other")]])
    await message.answer(text="👋 Добро пожаловать в наш чат-бот! Ваша компания занимаеться БФЛ или у вас другая сфера?", reply_markup=keyboard)

@router.callback_query(StateFilter(UserState.ass_token))
async def company_name(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.data == "bfl":
        ass_token = os.getenv("BFL_TOKEN")
        
    else:
        ass_token = os.getenv("OTHER_TOKEN")
    await state.update_data(ass_token=ass_token)
    await state.set_state(UserState.sheet_id_token)
    await callback_query.message.answer_photo(photo=IMG, caption="Скопируйте данную таблицу. В ней будут отображаться результаты обработки аудио.\nhttps://docs.google.com/spreadsheets/d/1YiruDfMBpp075KMTmUG_dV2vomGZus5-82pkXPMu64k/edit?gid=0#gid=0\n\nОткройте настройки доступа, выберите в пункте \"Доступ пользователям, у которых есть ссылка\" режим \"Редактор\" и нажмите \"Готово\"\n\nИ пришлите ID таблицы в этот чат.\n\nГде найти ID таблицы, смотрите на картинке", disable_web_page_preview=True)

# @router.message(StateFilter(UserState.company_name))
# async def ass_token(message: Message, state: FSMContext):
#     await state.update_data(company_name=message.text)
#     await state.set_state(UserState.sheet_id_token)
#     await message.answer("Скопируйте данную таблицу. В ней будут отображаться записанные на собеседование кандидаты.\nhttps://docs.google.com/spreadsheets/d/1YiruDfMBpp075KMTmUG_dV2vomGZus5-82pkXPMu64k/edit?gid=0#gid=0\n\nОткройте настройки доступа, выберите в пункте \"Доступ пользователям, у которых есть ссылка\" режим \"Редактор\" и нажмите \"Готово\"\n\nИ пришлите ID таблицы в этот чат.\n\nГде найти ID таблицы, смотрите на картинке")

@router.message(StateFilter(UserState.sheet_id_token))
async def ass_token(message: Message, state: FSMContext):
    await state.update_data(sheet_id_token=message.text)
    await state.set_state(UserState.audio_link)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Файлами в этот чат", callback_data="tg_audio")],[InlineKeyboardButton(text="Сcылка на файлы Google Drive", callback_data="gdrive_link")],[InlineKeyboardButton(text="Сcылка на папку Google Drive", callback_data="gdrive_folder")]])
    await message.answer(text="Выбери формат для загрузки", reply_markup=keyboard)


@router.callback_query(StateFilter(UserState.audio_link))
async def ass_token(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(UserState.audio)
    if callback_query.data == "tg_audio":
        await callback_query.message.answer("Присылай файлы")
    elif callback_query.data == "gdrive_link":
        await callback_query.message.answer("Присылай ссылки на файлы Google Drive по одной")
    elif callback_query.data == "gdrive_folder":
        await callback_query.message.answer("Присылай ссылку на папку в Google Drive для оценки")

@router.message(F.text, StateFilter(UserState.audio))
async def handle_audio_link(message: types.Message, state: FSMContext):
    url = message.text.strip()
    
    if not any(x in url for x in ['drive.google.com', 'docs.google.com']):
        await message.reply("❌ Пожалуйста, отправьте ссылку на Google Drive")
        return
    
    if 'folder' in url or '/folders/' in url:
        await process_folder(url, message, state)
        return
    
    file_id = extract_file_id_from_url(url)
    if not file_id:
        await message.reply("❌ Не удалось извлечь ID файла")
        return
    
    temp_path = f"temp_{uuid.uuid4().hex}"
    try:
        # Скачивание
        await message.reply("⏳ Скачиваю файл...")
        if not await download_from_google_drive(file_id, temp_path):
            await message.reply("❌ Ошибка скачивания")
            return

        # Определяем тип файла
        is_video = any(temp_path.endswith(ext) for ext in ['.mp4', '.mov', '.avi'])
        
        # Обработка
        await message.reply("🔍 Извлекаю аудио..." if is_video else "🔍 Обрабатываю аудио...")
        audio_path = await extract_audio_from_video(temp_path) if is_video else await convert_audio(temp_path)
        
        if not audio_path:
            await message.reply("❌ Ошибка обработки аудио")
            return
            
        row_number = await process_audio_file(audio_path, "Видеофайл" if is_video else "Аудиофайл", message, state)
        await message.reply(f"✅ Результат записан в строку {row_number}")
        
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        await message.reply(f"❌ Ошибка: {str(e)}")
    finally:
        for path in [temp_path, audio_path if 'audio_path' in locals() else None]:
            if path and os.path.exists(path):
                try: os.remove(path)
                except: pass




@router.message(F.voice | F.audio | F.document, StateFilter(UserState.audio))
async def handle_tg_audio(message: types.Message, state: FSMContext):
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
        elif message.video:
            file = await bot.get_file(message.audio.file_id)
            ext = "mp4"
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
            if not await safe_download_file(file, input_path):
                await message.reply("❌ Не удалось скачать файл после нескольких попыток")
                return
        except Exception as e:
            await message.reply(f"❌ Ошибка скачивания файла: {str(e)}")
            return
        
        try:
            # Определяем тип файла
            is_video = any(input_path.endswith(ext) for ext in ['.mp4', '.mov', '.avi'])
        
            if is_video:
                audio_path = await extract_audio_from_video(input_path)  
                input_path = audio_path
        except Exception as e:
            await message.reply(f"❌ Ошибка извлечения: {str(e)}")
            return
        # Проверка размера файла
        if os.path.getsize(input_path) > 100 * 1024 * 1024:
            os.remove(input_path)
            await message.reply("❌ Файл слишком большой. Максимальный размер: 100MB")
            return

                
        if ext != "mp3":
            output_path = await convert_audio(input_path)
            if not output_path:
                await message.reply("❌ Ошибка конвертации аудио")
                return
        
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

# @router.message(Command("upload_image"))
# async def upload_image_command(message: types.Message, state: FSMContext):
#     await state.set_state(ImageUploadState.waiting_for_image)
#     await message.answer("Please send me an image, and I'll give you its file_id.")

# @router.message(ImageUploadState.waiting_for_image, lambda message: message.photo)
# async def handle_image_upload(message: types.Message, state: FSMContext):
#     file_id = message.photo[-1].file_id

#     await message.answer(f"Here is the file_id of your image:\n\n<code>{file_id}</code>\n\n"
#                          "You can use this file_id to send the image in your bot.")

#     await state.clear()


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
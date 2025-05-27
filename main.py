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
from datetime import datetime
import uuid

BOT_TOKEN = os.getenv("BOT_TOKEN")
client  = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(
    parse_mode=ParseMode.HTML))
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


@router.message(CommandStart())
async def command_start_handler(message: Message, state: FSMContext) -> None:
    await state.set_state(UserState.ass_token)
    text = "👋 Добро пожаловать в наш чат-бот! Для начала нужен токен ассистента"
    await message.answer(f"{text}")

@router.message(StateFilter(UserState.ass_token))
async def company_name(message: Message, state: FSMContext):
    ass_token = message.text
    await state.update_data(ass_token=ass_token)
    await state.set_state(UserState.company_name)
    text = "Напиши название компании"
    await message.answer(f"{text}")

@router.message(StateFilter(UserState.company_name))
async def ass_token(message: Message, state: FSMContext):
    company_name = message.text
    await state.update_data(company_name=company_name)
    await state.set_state(UserState.audio)
    text = "Присылай аудио для оценки"
    await message.answer(f"{text}")



async def convert_audio(input_path: str) -> str:
    """Конвертирует аудио в WAV с уникальным именем файла"""
    unique_id = uuid.uuid4().hex
    output_path = os.path.join(tempfile.gettempdir(), f"converted_{unique_id}.wav")
    
    try:
        audio = AudioSegment.from_file(input_path)
        audio.export(
            output_path,
            format="wav",
            codec="pcm_s16le",
            bitrate="128k",
            parameters=["-ac", "1", "-ar", "16000"]
        )
        return output_path
    except Exception as e:
        print(f"Ошибка конвертации: {e}")
        return None



@router.message(F.voice | F.audio | F.document, StateFilter(UserState.audio))
async def handle_audio(message: types.Message, state: FSMContext):
    unique_id = uuid.uuid4().hex
    await message.reply("🔍 Начинаю обработку...")
    
    try:
        # Определяем тип файла
        if message.voice:
            file = await bot.get_file(message.voice.file_id)
            ext = "ogg"
        elif message.audio:
            file = await bot.get_file(message.audio.file_id)
            ext = "mp3"
        else:
            if not message.document.mime_type.startswith('audio/'):
                return await message.reply("❌ Пожалуйста, отправьте аудиофайл")
            file = await bot.get_file(message.document.file_id)
            ext = os.path.splitext(message.document.file_name)[1][1:] or "mp3"
            file_name = message.document.file_name

        # Скачиваем файл с уникальным именем
        input_path = f"temp_{unique_id}.{ext}"
        await bot.download(file, destination=input_path)

        # Конвертируем в WAV
        output_path = await convert_audio(input_path)
        if not output_path:
            return await message.reply("❌ Ошибка конвертации аудио")

        # Транскрибируем
        with open(output_path, "rb") as audio_file:
            transcript = client.audio.transcriptions.create(
                file=audio_file,
                model="whisper-1",
                language="ru"
            )
            await message.reply("🎙️ Аудио расшифровано")

        # Получаем ответ ассистента
        state_data = await state.get_data()
        assistant_id = state_data.get('ass_token')
        
        thread = client.beta.threads.create()
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=transcript.text
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
        
        # Записываем в Google Sheets
        row_number = await write_to_google_sheets(transcript.text, response_text, file_name, message.from_user.id)
        await message.reply(f"📊 Результат записан в строку {row_number}")

    except Exception as e:
        await message.reply(f"❌ Ошибка: {str(e)}")
    finally:
        # Гарантированная очистка временных файлов
        for path in [input_path, output_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass




async def write_to_google_sheets(transcription_text: str, ai_response: str, file_name: str, username: str, state: FSMContext) -> int:
    """Записывает данные в Google Sheets и возвращает номер строки"""
    user_data = await state.get_data()
    ass_token = user_data.get('ass_token')
    company_name = user_data.get('company_name')
    try:
        # Формируем данные для авторизации (используем сырые переменные из .env)
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

        # Авторизация
        scope = ['https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        gc = gspread.authorize(creds)

        # Открываем таблицу
        spreadsheet = gc.open_by_key(os.getenv("GSHEETS_SPREADSHEET_ID"))
        worksheet = spreadsheet.worksheet(os.getenv("GSHEETS_SHEET_NAME", "Sheet1"))

        # Подготавливаем данные для записи
        row_data = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Колонка A: Дата
            str(transcription_text),                        # Колонка B: Транскрипция (явное преобразование в str)
            str(ai_response),                                # Колонка C: Ответ
            file_name,
            f"@username",
            f"https://t.me/{username}",
            company_name,
            ass_token

        ]

        # Добавляем строку
        worksheet.append_row(row_data)

        # Получаем номер последней строки
        return len(worksheet.col_values(1))

    except Exception as e:
        error_msg = f"Ошибка записи в Google Sheets: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    dp.include_router(router)
    dp.message.middleware(StateMiddleware())
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(
        parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
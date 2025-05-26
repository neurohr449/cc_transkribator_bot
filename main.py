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

BOT_TOKEN = os.getenv("BOT_TOKEN")
GPT_TOKEN = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(
    parse_mode=ParseMode.HTML))
storage = MemoryStorage()
router = Router()
dp = Dispatcher(storage=storage)

class UserState(StatesGroup):
    audio = State()
    ass_token = State()
    

class StateMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        state = data['state']
        current_state = await state.get_state()
        data['current_state'] = current_state
        return await handler(event, data)



async def transcribe_audio(file_path: str) -> str:
    """Транскрибация аудио через OpenAI Whisper"""
    try:
        with open(file_path, "rb") as audio_file:
            transcript = GPT_TOKEN.audio.transcriptions.create(
                file=audio_file,
                model="whisper-1",
                language="ru"
            )
            return transcript.text
    except Exception as e:
        print(f"Ошибка транскрибации: {e}")
        return None






@router.message(CommandStart())
async def command_start_handler(message: Message, state: FSMContext) -> None:
    await state.set_state(UserState.ass_token)
    text = "👋 Добро пожаловать в наш чат-бот! Для начала нужен токен ассистента"
    await message.answer(f"{text}")

@router.message(StateFilter(UserState.ass_token))
async def ass_token(message: Message, state: FSMContext):
    ass_token = message.text
    await state.update_data(ass_token=ass_token)
    await state.set_state(UserState.audio)
    text = "Присылай аудио для оценки"
    await message.answer(f"{text}")



@router.message(F.voice | F.audio | F.document, (StateFilter(UserState.audio)))
async def handle_audio(message: types.Message):
    if message.voice:
        file = await bot.get_file(message.voice.file_id)
        ext = "ogg"  # Голосовые сообщения в Telegram всегда .ogg
    elif message.audio:
        file = await bot.get_file(message.audio.file_id)
        ext = message.audio.mime_type.split("/")[-1]  # "audio/mp3" → "mp3"
    elif message.document:
        file = await bot.get_file(message.document.file_id)
        ext = os.path.splitext(message.document.file_name)[1][1:]  # ".mp3" → "mp3"
    else:
        await message.reply("❌ Формат не поддерживается")
        return

     # Скачиваем файл
    input_path = f"temp_audio.{ext}"
    await bot.download_file(file.file_path, destination=input_path)

    # Конвертируем в WAV (если нужно)
    output_path = "temp_audio.wav"
    try:
        audio = AudioSegment.from_file(input_path, format=ext)
        audio.export(output_path, format="wav")
    except Exception as e:
        await message.reply(f"❌ Ошибка конвертации: {e}")
        if os.path.exists(input_path):
            os.remove(input_path)
        return

    # Транскрибируем
    transcription = await transcribe_audio(output_path)

    # Удаляем временные файлы
    if os.path.exists(input_path):
        os.remove(input_path)
    if os.path.exists(output_path):
        os.remove(output_path)

    # Отправляем результат
    if transcription:
        await message.reply(f"Текст распознан")
        print(transcription)
    else:
        await message.reply("❌ Не удалось распознать речь")
    

async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    dp.include_router(router)
    dp.message.middleware(StateMiddleware())
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(
        parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import logging
import sys
import os
import json
from datetime import datetime, timedelta
import aiohttp
from aiogram import Bot, Dispatcher, html, Router, BaseMiddleware, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.filters.state import StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
import shelve

BOT_TOKEN = os.getenv("BOT_TOKEN")
SBER_TOKEN = os.getenv("SBER_TOKEN")
SBER_SPEECH_API_URL="https://smartspeech.sber.ru/rest/v1/speech:recognize"

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(
    parse_mode=ParseMode.HTML))
storage = MemoryStorage()
router = Router()
dp = Dispatcher(storage=storage)

class UserState(StatesGroup):
    welcome = State()
    ass_token = State()
    get = State()

class StateMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        state = data['state']
        current_state = await state.get_state()
        data['current_state'] = current_state
        return await handler(event, data)




async def transcribe_audio(audio_data: bytes) -> str | None:
    api_url = SBER_SPEECH_API_URL
    api_key = SBER_TOKEN
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "audio/mpeg"  
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url,
                headers=headers,
                data=audio_data
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    return result.get("result")
                else:
                    print(f"Ошибка API: {response.status}")
                    return None
    except Exception as e:
        print(f"Ошибка запроса: {e}")
        return None






@router.message(CommandStart())
async def command_start_handler(message: Message, state: FSMContext) -> None:
    await state.set_state(UserState.welcome)
    text = "👋 Добро пожаловать в наш чат-бот! Для начала нужен токен ассистента"
    await message.answer(f"{text}")

@router.message(StateFilter(UserState.ass_token))
async def ass_token(message: Message, state: FSMContext):
    ass_token = message.text
    await state.update_data(ass_token=ass_token)
    text = "Присылай аудио для оценки"
    await message.answer(f"{text}")

@router.message(F.voice | F.audio | F.document)
async def handle_audio(message: Message):
    file = await bot.get_file(
        message.voice.file_id if message.voice else (
            message.audio.file_id if message.audio else message.document.file_id
        )
    )
    audio_data = await bot.download_file(file.file_path)
    transcription = await transcribe_audio(audio_data)
    
    if transcription:
        await message.reply(f"Текст распознан")
        print(transcription)
    else:
        await message.reply("❌ Не удалось распознать речь.")
    

async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    dp.include_router(router)
    dp.message.middleware(StateMiddleware())
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(
        parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
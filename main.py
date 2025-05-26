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
async def ass_token(message: Message, state: FSMContext):
    ass_token = message.text
    await state.update_data(ass_token=ass_token)
    await state.set_state(UserState.audio)
    text = "Присылай аудио для оценки"
    await message.answer(f"{text}")



async def convert_audio(input_path: str) -> str:
    """Конвертирует аудио в WAV с явными параметрами"""
    output_path = os.path.join(tempfile.gettempdir(), "converted_audio.wav")
    
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
    await message.reply("Анализирую")
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

        # Скачиваем файл
        input_path = f"temp_audio.{ext}"
        await bot.download(file, destination=input_path)

        # Конвертируем в WAV (если нужно)
        output_path = "temp_audio.wav"
        AudioSegment.from_file(input_path).export(output_path, format="wav")

        # Транскрибируем
        with open(output_path, "rb") as audio_file:
            transcript = client.audio.transcriptions.create(
                file=audio_file,
                model="whisper-1",
                language="ru"
            )
            await message.reply(f"Аудио обработано")
        
            state_data = await state.get_data()
            assistant_id = state_data.get('ass_token')
            
            # Создаем тред и добавляем сообщение
            thread = client.beta.threads.create()
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=transcript.text
            )
            
            # Запускаем ассистента
            run = client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=assistant_id
            )
            
            # Ожидаем завершения
            while True:
                run_status = client.beta.threads.runs.retrieve(
                    thread_id=thread.id,
                    run_id=run.id
                )
                if run_status.status == "completed":
                    break
                await asyncio.sleep(1)
            
            # Получаем ответ
            messages = client.beta.threads.messages.list(thread_id=thread.id)
            response_text = messages.data[0].content[0].text.value
            
            await message.reply(f"🤖 Ответ ассистента:\n\n{response_text}")
        
        # Удаляем временные файлы
        os.remove(input_path)
        os.remove(output_path)

    except Exception as e:
        await message.reply(f"❌ Ошибка: {str(e)}")
        # Удаляем файлы при ошибке (если они созданы)
        for path in [input_path, output_path]:
            if path and os.path.exists(path):
                os.remove(path)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    dp.include_router(router)
    dp.message.middleware(StateMiddleware())
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(
        parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
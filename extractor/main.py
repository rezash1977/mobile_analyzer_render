import os
import asyncio
import json
import redis
from telethon.sync import TelegramClient
from dotenv import load_dotenv

load_dotenv()

# --- Configs ---
API_ID = os.environ.get('TELEGRAM_API_ID')
API_HASH = os.environ.get('TELEGRAM_API_HASH')
SESSION_NAME = 'telegram_session'
TARGET_CHAT_ID = os.environ.get('TELEGRAM_TARGET_CHAT_ID') # بهتر است به صورت int باشد اگر عددی است
REDIS_URL = os.environ.get('REDIS_URL')
DOWNLOAD_PATH = '/tmp/downloads' # در محیط‌های کانتینری از /tmp استفاده کنید

# اتصال به Redis
r = redis.from_url(REDIS_URL, decode_responses=True)

async def main():
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)

    print("Extractor Service starting...")
    # Telethon session file will be created in the current directory of the container
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        print("Telegram client connected.")
        target_entity = await client.get_entity(int(TARGET_CHAT_ID) if TARGET_CHAT_ID.lstrip('-').isdigit() else TARGET_CHAT_ID)
        
        print(f"Listening for new messages in '{target_entity.title}'...")
        
        @client.on(events.NewMessage(chats=target_entity))
        async def handler(event):
            message = event.message
            message_content = message.text or ""
            media_path = None
            
            if message.photo:
                print("Photo detected, downloading...")
                media_path = await message.download_media(file=DOWNLOAD_PATH)
            
            # ساخت پیام برای ارسال به صف
            task = {
                "text": message_content,
                "photo_path": media_path,
                "date": message.date.isoformat()
            }
            
            # ارسال پیام به صف پردازش در Redis
            r.rpush('parsing_queue', json.dumps(task))
            print(f"Message sent to parsing queue. Queue size: {r.llen('parsing_queue')}")

        await client.run_until_disconnected()

if __name__ == "__main__":
    # در اولین اجرا، نیاز به لاگین دستی در محیط لوکال برای ساخت فایل session دارید
    # سپس فایل .session را در کنار پروژه قرار دهید تا در داکر کپی شود
    asyncio.run(main())
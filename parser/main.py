import os
import json
import time
import redis
import psycopg2
from PIL import Image
import pytesseract
from dotenv import load_dotenv

load_dotenv()

# --- Configs ---
REDIS_URL = os.environ.get('REDIS_URL')
DATABASE_URL = os.environ.get('DATABASE_URL')

# کد توابع تحلیل متن (extract_price, extract_model, etc.) را از پروژه قبلی اینجا کپی کنید
# ... (کدهای data_parser.py اینجا قرار می‌گیرند) ...
def clean_text(text): # نمونه
    # ...
    return text

def extract_price(text): # نمونه
    # ...
    return 1000

# توابع دیگر ...

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def create_table_if_not_exists():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS mobiles (
            id SERIAL PRIMARY KEY,
            brand TEXT,
            model TEXT,
            color TEXT,
            storage TEXT,
            price BIGINT,
            message_date TIMESTAMPTZ,
            source_text TEXT UNIQUE
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()
    print("Table 'mobiles' is ready.")

def insert_data(item):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO mobiles (brand, model, color, storage, price, message_date, source_text) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (item['brand'], item['model'], item['color'], item['storage'], item['price'], item['message_date'], item['source_text'])
        )
        conn.commit()
        print(f"Successfully inserted model: {item['model']}")
    except psycopg2.IntegrityError:
        conn.rollback()
        print("Skipping duplicate record.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred during insertion: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    print("Parser Service starting...")
    create_table_if_not_exists()
    
    r = redis.from_url(REDIS_URL, decode_responses=True)
    print("Connected to Redis, waiting for tasks...")

    while True:
        try:
            # منتظر ماندن برای دریافت پیام از صف به صورت Blocking
            _, task_json = r.blpop('parsing_queue')
            task = json.loads(task_json)
            
            print("New task received!")
            
            full_text = task['text']
            if task.get('photo_path'):
                try:
                    ocr_text = pytesseract.image_to_string(Image.open(task['photo_path']), lang='fas+eng')
                    full_text += "\n" + ocr_text
                    # پس از پردازش، فایل موقت را پاک کنید
                    os.remove(task['photo_path'])
                except Exception as e:
                    print(f"OCR failed: {e}")

            if not full_text.strip():
                continue

            # اینجا تابع parse_message_content را صدا بزنید که از پروژه قبلی کپی کرده‌اید
            # parsed_item = parse_message_content(full_text, task['date'])
            # برای مثال یک دیتای ساختگی می‌سازیم:
            parsed_item = {
                "brand": "Apple", "model": "iPhone 15", "color": "Blue", "storage": "256GB",
                "price": extract_price(full_text), "message_date": task['date'], "source_text": full_text[:500]
            }

            if parsed_item and parsed_item.get('price'):
                insert_data(parsed_item)

        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
            time.sleep(5) # در صورت بروز خطا، کمی صبر کنید

if __name__ == "__main__":
    main()
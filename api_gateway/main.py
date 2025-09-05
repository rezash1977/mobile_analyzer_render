import os
from flask import Flask, jsonify, request
import psycopg2
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

@app.route('/')
def index():
    return "Mobile Price Analyzer API is running!"

@app.route('/prices/min', methods=['GET'])
def get_min_price():
    model_query = request.args.get('model')
    if not model_query:
        return jsonify({"error": "Model query parameter is required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    
    # استفاده از ILIKE برای جستجوی غیرحساس به حروف
    cur.execute(
        "SELECT * FROM mobiles WHERE model ILIKE %s ORDER BY price ASC LIMIT 1",
        (f'%{model_query}%',)
    )
    result = cur.fetchone()
    cur.close()
    conn.close()

    if result:
        # نام ستون‌ها را برای ساخت دیکشنری استخراج کنید
        columns = [desc[0] for desc in cur.description]
        data = dict(zip(columns, result))
        return jsonify(data)
    else:
        return jsonify({"message": f"No records found for model: {model_query}"}), 404

if __name__ == '__main__':
    # این بخش فقط برای اجرای لوکال است. رندر از Gunicorn استفاده می‌کند.
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
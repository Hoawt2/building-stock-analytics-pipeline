import os
import time
import requests
import asyncio
import aiohttp
from flask import Flask, send_file, jsonify, request
from flask_socketio import SocketIO

# === Flask Setup ===
app = Flask(__name__, static_folder='.', static_url_path='/asset')
socketio = SocketIO(app, cors_allowed_origins="*")

# === Config ===
API_KEY = "d19f6b9r01qmm7tuqbk0d19f6b9r01qmm7tuqbkg"
NEWS_API_KEY = "071e2a569c784e639f72de8f095d702c"
STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "NVDA", "TSLA"]
INDEX_ETF_SYMBOLS = {
    "S&P 500": "SPY",
    "Dow Jones": "DIA",
    "Nasdaq": "QQQ"
}

def build_combined_symbol_list():
    return [(s, s, "stock") for s in STOCK_SYMBOLS] + [(n, s, "etf") for n, s in INDEX_ETF_SYMBOLS.items()]

@app.route("/")
def index():
    # Trả về index.html từ thư mục gốc
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return send_file(os.path.join(root_dir, "index.html"))

@app.route("/api/news")
def get_all_news():
    result = {}
    for symbol in STOCK_SYMBOLS:
        articles = get_news_for_stock(symbol)
        print(f"[INFO] /api/news - {symbol}: {len(articles)} articles")
        result[symbol] = articles
    return jsonify(result)

@app.route("/api/quotes")
def get_combined_quotes():
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(fetch_all_quotes_combined())
        loop.close()

        print("[INFO] /api/quotes - Fetched combined quotes:")
        for item in results:
            print(f"  {item['symbol']} - data: {item['data']}")

        output = []
        for item in results:
            data = item['data']
            if data and 'c' in data and data['c'] is not None:
                current_price = float(data['c'])
                prev_close = float(data.get('pc', 0))
                change_percent = ((current_price - prev_close) / prev_close) * 100 if prev_close != 0 else 0.0
                info = {
                    "symbol": item["symbol"],
                    "name": item["name"],
                    "price": round(current_price, 2),
                    "change": round(change_percent, 2),
                    "open": round(float(data.get('o', 0)), 2),
                    "high": round(float(data.get('h', 0)), 2),
                    "low": round(float(data.get('l', 0)), 2),
                    "prev_close": round(prev_close, 2),
                    "type": item["type"]
                }
                output.append(info)
        return jsonify(output)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_news_for_stock(symbol, max_articles=3):
    company_names = {
        "AAPL": "Apple",
        "GOOGL": "Google Alphabet",
        "MSFT": "Microsoft",
        "NVDA": "NVIDIA",
        "TSLA": "Tesla"
    }
    search_term = company_names.get(symbol, symbol)
    url = f"https://newsapi.org/v2/everything"
    params = {
        'q': f'"{search_term}" OR "{symbol}"',
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': max_articles,
        'apiKey': NEWS_API_KEY,
        'domains': 'reuters.com,bloomberg.com,cnbc.com,marketwatch.com,yahoo.com'
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        print(f"[INFO] NewsAPI response for {symbol}: {data}")

        if data.get('status') == 'ok':
            articles = data.get('articles', [])[:max_articles]
            return [{
                'title': a.get('title', ''),
                'description': a.get('description', ''),
                'url': a.get('url', ''),
                'publishedAt': a.get('publishedAt', ''),
                'source': a.get('source', {}).get('name', 'Unknown')
            } for a in articles if a.get('title') and a.get('url')]
    except:
        return []
    

    return []

async def fetch_quote_snapshot(session, name, symbol, data_type):
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}"
    try:
        async with session.get(url) as response:
            data = await response.json()
            return {"symbol": symbol, "name": name, "type": data_type, "data": data}
    except:
        return {"symbol": symbol, "name": name, "type": data_type, "data": None}

async def fetch_all_quotes_combined():
    combined = build_combined_symbol_list()
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_quote_snapshot(session, n, s, t) for (n, s, t) in combined]
        return await asyncio.gather(*tasks)

def emit_stock_data():
    while True:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            all_results = loop.run_until_complete(fetch_all_quotes_combined())
            loop.close()

            print("[INFO] [Socket] Fetched data from API:")
            for item in all_results:
                print(f"  {item['type'].upper()} {item['symbol']}: {item['data']}")

            stock_data = []
            etf_data = []
            for item in all_results:
                data = item['data']
                if data and 'c' in data and data['c'] is not None:
                    current_price = float(data['c'])
                    prev_close = float(data.get('pc', 0))
                    change_percent = ((current_price - prev_close) / prev_close) * 100 if prev_close != 0 else 0.0
                    info = {
                        "symbol": item["symbol"],
                        "name": item["name"],
                        "price": round(current_price, 2),
                        "change": round(change_percent, 2),
                        "open": round(float(data.get('o', 0)), 2),
                        "high": round(float(data.get('h', 0)), 2),
                        "low": round(float(data.get('l', 0)), 2),
                        "prev_close": round(prev_close, 2),
                        "type": item["type"]
                    }
                    (stock_data if item["type"] == "stock" else etf_data).append(info)

            if stock_data:
                socketio.emit("stock_data", stock_data)
            if etf_data:
                socketio.emit("etf_data", etf_data)

        except Exception as e:
            print(f"[ERROR] emit_stock_data failed: {e}")
        time.sleep(5)

@socketio.on('connect')
def handle_connect():
    print("[INFO] Client connected")

@socketio.on('disconnect')
def handle_disconnect():
    print("[INFO] Client disconnected")

if __name__ == "__main__":
    socketio.start_background_task(emit_stock_data)
    socketio.run(app, debug=True, host="0.0.0.0", port=5000)

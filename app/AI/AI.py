from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
from sqlalchemy import create_engine, text
import google.generativeai as genai
from datetime import datetime
import logging
import re

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Khởi tạo Flask app
app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app, origins=["*"])

# Cấu hình Gemini API
genai.configure(api_key="AIzaSyCFjDLfku7fUp5R_FD0T-Ss6f69lvP5eTw")
model = genai.GenerativeModel("models/gemini-2.0-flash")

# Kết nối MySQL
try:
    engine = create_engine("mysql+pymysql://root:hoang29102004@127.0.0.1/stock_bi",
                          echo=False, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Database connected successfully")
except Exception as e:
    logger.error(f"Database connection failed: {e}")
    engine = None

class FinancialDataService:
    """Service để xử lý dữ liệu tài chính"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def check_tickers_exist(self, tickers):
        """Kiểm tra các mã chứng khoán có trong database"""
        if not tickers or not self.engine:
            return []
        
        tickers_str = ','.join([f"'{t.upper()}'" for t in tickers])
        query = f"SELECT DISTINCT ticker FROM dim_stock WHERE ticker IN ({tickers_str})"
        
        try:
            df = pd.read_sql(query, self.engine)
            valid_tickers = df['ticker'].tolist()
            logger.info(f"Valid tickers found: {valid_tickers}")
            return valid_tickers
        except Exception as e:
            logger.error(f"Error checking tickers: {e}")
            return []
    
    def get_financial_data(self, tickers):
        """Lấy dữ liệu tài chính cho các mã hợp lệ"""
        if not tickers or not self.engine:
            return pd.DataFrame()
        
        tickers_str = ','.join([f"'{t}'" for t in tickers])
        query = f"""
        SELECT
            s.ticker,
            s.company_name,
            f.fiscal_year,
            f.fiscal_quarter,
            f.revenue,
            f.net_income,
            f.total_assets,
            f.total_liabilities,
            ROUND(f.net_income / NULLIF(f.revenue, 0) * 100, 2) as net_margin,
            ROUND(f.total_liabilities / NULLIF(f.total_assets, 0) * 100, 2) as debt_ratio
        FROM fact_company_financials_quarterly f
        JOIN dim_stock s ON f.stock_id = s.stock_id
        WHERE s.ticker IN ({tickers_str})
        AND f.fiscal_year = 2024
        ORDER BY s.ticker, f.fiscal_quarter DESC
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            logger.info(f"Retrieved {len(df)} financial records")
            return df
        except Exception as e:
            logger.error(f"Error getting financial data: {e}")
            return pd.DataFrame()

class AIFinancialAnalyst:
    """AI Chuyên gia Tài chính - Nhập tâm hoàn toàn"""
    
    def __init__(self, model):
        self.model = model
    
    def extract_tickers_from_text(self, text):
        """Trích xuất mã chứng khoán từ văn bản"""
        potential_tickers = re.findall(r'\b[A-Z]{1,5}\b', text.upper())
        return list(set(potential_tickers))
    
    def create_analysis_prompt(self, user_question, valid_tickers, financial_data):
        """Tạo prompt với nhân cách chuyên gia tài chính"""
        if not valid_tickers:
            return f"""
Bạn là một CHUYÊN GIA TÀI CHÍNH kỳ cựu với 20 năm kinh nghiệm phân tích thị trường chứng khoán Mỹ.

Khách hàng hỏi: "{user_question}"

🚫 **PHẢN HỒI CHUYÊN NGHIỆP**:

Tôi hiểu bạn muốn tìm hiểu về các mã chứng khoán, tuy nhiên các mã bạn đề cập không có trong cơ sở dữ liệu hiện tại của tôi.

**Với kinh nghiệm của mình, tôi khuyên bạn:**

💡 **Các mã phổ biến tôi thường phân tích:**
- **Tech Giants**: AAPL (Apple), GOOGL (Google), MSFT (Microsoft)
- **EV & Innovation**: TSLA (Tesla), NVDA (Nvidia)
- **Financial**: JPM (JPMorgan), BAC (Bank of America)

📋 **Cách đặt câu hỏi hiệu quả:**
- "Anh phân tích AAPL giúp em"
- "So sánh AAPL và MSFT"
- "TSLA có nên mua không anh?"

Hãy cho tôi biết mã cụ thể, tôi sẽ phân tích chi tiết dựa trên kinh nghiệm và dữ liệu thực tế!
"""
        
        tickers_str = ', '.join(valid_tickers)
        
        if not financial_data.empty:
            financial_md = financial_data.to_markdown(index=False)
            data_status = "📊 **Dữ liệu thực từ hệ thống**"
        else:
            financial_md = "Dữ liệu tài chính hạn chế"
            data_status = "⚠️ **Dữ liệu hạn chế**"
        
        prompt = f"""
Bạn là một CHUYÊN GIA TÀI CHÍNH kỳ cựu với 20 năm kinh nghiệm đầu tư chứng khoán Mỹ. 
Bạn có tính cách thân thiện, chuyên nghiệp và luôn đưa ra lời khuyên thực tế.

{data_status}

**Khách hàng hỏi**: {user_question}
**Mã đang phân tích**: {tickers_str}

### **📊 Dữ liệu tài chính:**
{financial_md}

**NHIỆM VỤ**: Hãy phân tích với vai trò chuyên gia tài chính thực thụ:

### **🎯 ĐÁNH GIÁ CHUYÊN NGHIỆP**
- **Khuyến nghị đầu tư**: [MUA/GIỮ/BÁN] với lý do cụ thể
- **Mức độ rủi ro**: [Thấp/Trung bình/Cao]
- **Khung thời gian**: Ngắn hạn vs Dài hạn

### **📈 PHÂN TÍCH KỸ THUẬT**
- **Điểm mạnh**: Những yếu tố tích cực
- **Điểm yếu**: Rủi ro cần cảnh báo
- **Triển vọng**: Dự báo xu hướng

### **💰 LỜI KHUYÊN ĐẦU TƯ**
- **Giá mục tiêu**: Dự báo giá hợp lý
- **Thời điểm vào lệnh**: Khi nào nên mua/bán
- **Quản lý rủi ro**: Cách bảo vệ vốn

## **PHONG CÁCH TRẢ LỜI**:
- Nói chuyện như một chuyên gia thực thụ
- Sử dụng thuật ngữ tài chính chuyên nghiệp nhưng dễ hiểu
- Đưa ra lời khuyên cụ thể, thực tế
- Thể hiện kinh nghiệm qua cách phân tích

Hãy trả lời với tư cách một chuyên gia tài chính thực thụ:
"""
        return prompt
    
    def create_general_chat_prompt(self, user_question):
        """Tạo prompt cho chat tổng quan với nhân cách chuyên gia"""
        prompt = f"""
Bạn là một CHUYÊN GIA TÀI CHÍNH kỳ cựu với 20 năm kinh nghiệm trong lĩnh vực đầu tư chứng khoán Mỹ.
Bạn có tính cách thân thiện, am hiểu sâu sắc về thị trường và luôn sẵn sàng chia sẻ kiến thức.

**Khách hàng hỏi**: {user_question}

**VAI TRÒ CỦA BẠN**: Chuyên gia tài chính thực thụ

## **CÁCH TRẢ LỜI**:

### **📊 Nếu hỏi về cổ phiếu cụ thể**:
- "Bạn có thể cho tôi biết mã cụ thể không? Ví dụ AAPL, GOOGL, MSFT..."
- "Với kinh nghiệm của mình, tôi cần mã chính xác để phân tích chính xác nhất"

### **💡 Nếu hỏi về kiến thức tài chính**:
- Chia sẻ kinh nghiệm thực tế từ 20 năm làm nghề
- Đưa ra lời khuyên cụ thể, thực tế
- Sử dụng ví dụ từ thị trường thực

### **🔍 Nếu hỏi về thị trường**:
- Phân tích xu hướng dựa trên kinh nghiệm
- Đưa ra góc nhìn chuyên nghiệp
- Cảnh báo rủi ro một cách thực tế

## **PHONG CÁCH**:
- **Ngôn ngữ**: Tiếng Việt thân thiện, chuyên nghiệp
- **Tính cách**: Như một chuyên gia thực thụ, có kinh nghiệm
- **Nội dung**: Thực tế, hữu ích, dựa trên kinh nghiệm
- **Độ dài**: Vừa phải, dễ đọc

Hãy trả lời với tư cách chuyên gia tài chính 20 năm kinh nghiệm:
"""
        return prompt

# Khởi tạo services
data_service = FinancialDataService(engine)
ai_analyst = AIFinancialAnalyst(model)

# Routes
@app.route("/")
def index():
    """Trang chủ với chatbot widget"""
    return render_template("index2.html")

@app.route("/api/chat", methods=["POST"])
def chat_with_ai():
    """Chat với AI Chuyên gia Tài chính"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Không có dữ liệu"}), 400
        
        user_question = data.get("question", "").strip()
        if not user_question:
            return jsonify({"error": "Vui lòng nhập câu hỏi"}), 400
        
        logger.info(f"User question: {user_question}")
        
        # Trích xuất mã chứng khoán từ câu hỏi
        potential_tickers = ai_analyst.extract_tickers_from_text(user_question)
        valid_tickers = data_service.check_tickers_exist(potential_tickers)
        
        if valid_tickers:
            # Có mã hợp lệ - phân tích cụ thể
            financial_data = data_service.get_financial_data(valid_tickers)
            prompt = ai_analyst.create_analysis_prompt(user_question, valid_tickers, financial_data)
        else:
            # Không có mã cụ thể - chat tổng quan
            prompt = ai_analyst.create_general_chat_prompt(user_question)
        
        # Gửi đến Gemini AI
        response = model.generate_content(prompt)
        
        return jsonify({
            "response": response.text,
            "valid_tickers": valid_tickers,
            "has_analysis": bool(valid_tickers),
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Chat error: {e}")
        return jsonify({"error": f"Lỗi xử lý: {str(e)}"}), 500

if __name__ == "__main__":
    logger.info("Starting AI Financial Expert Chatbot...")
    logger.info("Server running on: http://127.0.0.1:5000")
    app.run(debug=True, host="127.0.0.1", port=5000)

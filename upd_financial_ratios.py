from utils_ratio_quarter import fetch_fmp_ratios_ttm, fetch_alpha_income_statement, get_stock_list, get_time_id, check_existing_record, insert_financial_ratios
import datetime
from config import API_KEY_APV, API_KEY_FMP


def calculate_ebitda_margin(total_revenue, ebitda):
    if total_revenue and total_revenue != 0:
        return ebitda / total_revenue
    return None

def update_financial_ratios():
    stocks = get_stock_list()
    today = datetime.date.today()

    for stock in stocks:
        ticker = stock['ticker']
        stock_id = stock['stock_id']

        # API call
        fmp_data = fetch_fmp_ratios_ttm(ticker, API_KEY_FMP)
        alpha_data = fetch_alpha_income_statement(ticker, API_KEY_APV)

        # Extract FMP fields
        ratios = fmp_data.get('ratiosTTM', {})
        if not ratios:
            continue

        # Extract Alpha fields
        total_revenue = None
        ebitda = None
        try:
            annual_reports = alpha_data.get('annualReports', [])
            if annual_reports:
                latest_report = annual_reports[0]
                total_revenue = float(latest_report.get('totalRevenue', 0))
                ebitda = float(latest_report.get('ebitda', 0))
        except Exception:
            pass

        ebitda_margin = calculate_ebitda_margin(total_revenue, ebitda)

        # Gán ngày hôm nay cho time_id
        time_id = get_time_id(today)
        if not time_id:
            continue

        # Check nếu đã tồn tại record
        if check_existing_record(stock_id, time_id):
            continue

        record = {
            'stock_id': stock_id,
            'time_id': time_id,
            'current_ratio': ratios.get('currentRatioTTM'),
            'quick_ratio': ratios.get('quickRatioTTM'),
            'roa': ratios.get('returnOnAssetsTTM'),
            'roe': ratios.get('returnOnEquityTTM'),
            'debt_to_equity': ratios.get('debtEquityRatioTTM'),
            'pe_ratio': ratios.get('peRatioTTM'),
            'pb_ratio': ratios.get('pbRatioTTM'),
            'dividend_yield': ratios.get('dividendYieldTTM'),
            'interest_coverage_ratio': ratios.get('interestCoverageTTM'),
            'ebitda_margin': ebitda_margin
        }

        insert_financial_ratios(record)
        print(f"Inserted financial ratios for {ticker} on {today}")

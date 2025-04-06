from dim_stock import main as stock_main
from dim_index import main as index_main
from upd_sto_5y import update_stock_history_for_all_stocks
from upd_sto_yesterday import update_yesterday
from upd_idx_5y import update_index_history_for_all_indices
from upd_fundamental import update_fundamental_for_all_stocks
from upd_financial_ratios import update_financial_ratios

if __name__ == "__main__":
    stock_main()

    index_main()

    update_stock_history_for_all_stocks()

    update_index_history_for_all_indices()

    # 2 hàm này nên chạy 1 lần duy nhất thôi
    update_fundamental_for_all_stocks()
    update_financial_ratios()

    # Cập nhật dữ liệu ngày hôm qua (chạy hàng ngày) chưa cần dùng lắmlắm
    # update_yesterday()

# Creating stock_list.csv with the requested tickers and company names.
import pandas as pd

rows = [
    ("AAPL", "Apple"),
    ("MSFT", "Microsoft"),
    ("GOOGL", "Alphabet (Google)"),
    ("AMZN", "Amazon"),
    ("META", "Meta Platforms"),
    ("TSLA", "Tesla"),
    ("NVDA", "NVIDIA"),
    ("JPM", "JPMorgan Chase"),
    ("V", "Visa"),
    ("JNJ", "Johnson & Johnson"),
    ("AMD", "Advanced Micro Devices"),
    ("PFE", "Pfizer"),
    ("INTC", "Intel"),
    ("KO", "Coca-Cola"),
    ("PEP", "PepsiCo"),
    ("NFLX", "Netflix"),
    ("DIS", "Walt Disney"),
    ("MA", "Mastercard"),
    ("SPOT", "Spotify"),
    ("NKE", "Nike")
]

df = pd.DataFrame(rows, columns=["symbol", "company"])

# Save to CSV in /mnt/data so user can download
path = "stock_list.csv"
df.to_csv(path, index=False)



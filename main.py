# main.py
from telesales.pipeline import run_mock_hot_only

if __name__ == "__main__":
    results = run_mock_hot_only()
    for k, v in results.items():
        print(f"{k}: {v}")

from __future__ import annotations

import argparse
import json

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    args = parser.parse_args()

    payload = {"job": "financial_analysis", "ticker_symbol": "MSFT", "year": "2024"}
    response = httpx.post(f"{args.base_url}/jobs", json=payload, timeout=10.0)
    print(json.dumps({"status_code": response.status_code, "body": response.text}, indent=2))


if __name__ == "__main__":
    main()


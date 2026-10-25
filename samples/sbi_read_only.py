"""Read-only SBI smoke check. Opt-in AI configuration and credentials are required."""

import argparse
import json
import os

from finance_client.sbi.sbi_util.sbi_util.env import load_sbi_env
from finance_client.sbi.sbi_util.sbi_util.read_recovery import ReadRecovery, ReadRecoveryError
from finance_client.sbi.sbi_util.sbi_util.rpa import STOCK


def main():
    parser = argparse.ArgumentParser(description="SBIの買付余力・保有銘柄・注文一覧を読み取り専用で確認します")
    parser.parse_args()
    load_sbi_env()
    if os.getenv("SBI_AI_HEALING") != "1":
        parser.error("Set SBI_AI_HEALING=1, SBI_AI_MODEL and OPENAI_API_KEY first")
    if not all(os.getenv(name) for name in ("sbi_id", "sbi_password", "SBI_AI_MODEL", "OPENAI_API_KEY")):
        parser.error("Missing configuration: sbi_id, sbi_password, SBI_AI_MODEL or OPENAI_API_KEY")
    recovery = ReadRecovery.from_env()
    stock = STOCK(os.environ["sbi_id"], os.environ["sbi_password"], read_recovery=recovery, read_only=True)
    try:
        result = {
            "budget": stock.get_available_budget(),
            "positions": stock.get_positions(),
            "orders": stock.get_orders(),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except ReadRecoveryError as error:
        parser.exit(1, f"読み取りを停止しました: {error}\n")
    finally:
        stock.driver.quit()
        del stock.driver


if __name__ == "__main__":
    main()

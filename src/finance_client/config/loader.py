import json
import os

import yaml

from .schema import AccountRiskConfig, SymbolRiskConfig


def load_account_risk_config(file_path: str) -> AccountRiskConfig:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Account risk config file not found: {file_path}")
    if file_path.endswith(".json"):
        with open(file_path, "r") as f:
            data = json.load(f)
    elif file_path.endswith(".yaml") or file_path.endswith(".yml"):
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
    else:
        raise ValueError("Unsupported file format. Only JSON and YAML are supported.")
    
    account_data = data["account"] if "account" in data else data
    
    return AccountRiskConfig(**account_data)

def load_symbol_risk_config(file_path: str) -> dict[str, SymbolRiskConfig]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Symbol risk config file not found: {file_path}")
    if file_path.endswith(".json"):
        with open(file_path, "r") as f:
            data = json.load(f)
    elif file_path.endswith(".yaml") or file_path.endswith(".yml"):
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
    else:
        raise ValueError("Unsupported file format. Only JSON and YAML are supported.")
    
    symbols_data = data["symbols"] if "symbols" in data else {}
    provider_default = data.get("provider", {})
    
    return {symbol: SymbolRiskConfig(**config, **provider_default) for symbol, config in symbols_data.items()}
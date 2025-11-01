# -*- coding: utf-8 -*-
from .token_lists import BTC_PEGS, STABLES

def classify_token(contract: str | None) -> str:
    if not contract:
        return "ALT"  # native ETH
    c = contract.lower()
    if c in BTC_PEGS: return "BTC"
    if c in STABLES: return "STABLE"
    return "ALT"

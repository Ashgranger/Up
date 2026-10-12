from __future__ import annotations


# Enable direct file discovery on 9p filesystems
import sys, os, importlib.util, importlib.abc

class _DirectFileFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        mod_name = fullname.split('.')[-1]
        search_paths = path if path is not None else sys.path
        for p in search_paths:
            if not p:
                p = os.getcwd()
            candidate = os.path.join(p, mod_name + '.py')
            if os.path.isfile(candidate):
                return importlib.util.spec_from_file_location(fullname, candidate)
            cand_pkg = os.path.join(p, mod_name, '__init__.py')
            if os.path.isfile(cand_pkg):
                return importlib.util.spec_from_file_location(fullname, cand_pkg)
        return None

if not any(isinstance(finder, _DirectFileFinder) for finder in sys.meta_path):
    sys.meta_path.insert(0, _DirectFileFinder())

"""Small shared helpers (decimal maths, canonical JSON, logging)."""

import json
import logging
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any

BPS = Decimal("10000")
ZERO = Decimal("0")
ONE = Decimal("1")
BUY, SELL = "BUY", "SELL"


class Fatal(Exception):
    """Unrecoverable config / market problem - do not reconnect."""


def q_down(x: Decimal, unit: Decimal) -> Decimal:
    return (x / unit).to_integral_value(rounding=ROUND_DOWN) * unit


def q_up(x: Decimal, unit: Decimal) -> Decimal:
    return (x / unit).to_integral_value(rounding=ROUND_UP) * unit


def to_int(value: Decimal, unit: Decimal) -> int:
    """Exact decimal -> integer ticks/quantums (the signed payload needs exactness)."""
    n = value / unit
    rounded = round(n)
    if abs(n - rounded) < Decimal("0.00001"):
        return int(rounded)
    if n != n.to_integral_value():
        raise ValueError(f"{value} is not a multiple of {unit}")
    return int(n)


def fmt(d: Decimal) -> str:
    return format(d.normalize(), "f")


def canonical(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def clamp(x: Decimal, lo: Decimal, hi: Decimal) -> Decimal:
    return max(lo, min(hi, x))


def bps_diff(a: Decimal, b: Decimal) -> Decimal:
    """(a - b) / b in basis points."""
    return (a - b) / b * BPS if b else ZERO


def setup_logging(level: str = "INFO") -> None:
    """Dated timestamps (multi-day runs) + optional size-rotated file via LOG_FILE / LOG_MAX_MB / LOG_BACKUPS."""
    import os
    from logging.handlers import RotatingFileHandler
    handlers = [logging.StreamHandler()]
    path = os.getenv("LOG_FILE", "").strip()
    if path:
        mb = float(os.getenv("LOG_MAX_MB", "20"))
        handlers.append(RotatingFileHandler(path, maxBytes=int(mb * 1024 * 1024),
                                            backupCount=int(os.getenv("LOG_BACKUPS", "10")), encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )

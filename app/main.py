import asyncio
import hashlib
import hmac
import json
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_TESTNET_FAPI = "https://demo-fapi.binance.com"
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_LIVE_TRADING_CONFIRM = os.getenv("BINANCE_LIVE_TRADING_CONFIRM", "")
ADMIN_ACTION_KEY = os.getenv("ADMIN_ACTION_KEY", "")
DINGTALK_WEBHOOK = os.getenv(
    "DINGTALK_WEBHOOK",
    "",
)
DINGTALK_KEYWORD = "异动"
DISPLAY_TZ = timezone(timedelta(hours=8))
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


class MonitorConfig(BaseModel):
    monitor_all: bool = True
    top_symbols: int = Field(default=120, ge=5, le=500)
    min_24h_quote_volume: float = Field(default=0, ge=0)
    oi_5m_threshold: float = Field(default=3.0, ge=0)
    volume_multiple_threshold: float = Field(default=1.5, ge=0)
    signal_strength_threshold: int = Field(default=50, ge=0, le=100)
    max_data_age_seconds: int = Field(default=90, ge=30, le=300)
    seed_top_symbols: int = Field(default=160, ge=0, le=500)
    kline_top_symbols: int = Field(default=120, ge=0, le=500)
    refresh_seconds: int = Field(default=45, ge=15, le=180)
    paper_enabled: bool = True
    paper_start_balance: float = Field(default=10_000, ge=100)
    paper_risk_pct: float = Field(default=1.0, ge=0.1, le=10)
    paper_stop_loss_pct: float = Field(default=2.0, ge=0.1, le=20)
    paper_take_profit_pct: float = Field(default=4.0, ge=0.1, le=50)
    paper_max_hold_minutes: int = Field(default=45, ge=1, le=240)
    paper_reentry_cooldown_minutes: int = Field(default=15, ge=0, le=120)
    paper_roll_window_minutes: int = Field(default=10, ge=1, le=60)
    paper_max_roll_entries: int = Field(default=2, ge=0, le=5)
    paper_pullback_roll_risk_factor: float = Field(default=0.5, ge=0.1, le=1)
    paper_momentum_roll_risk_factor: float = Field(default=0.3, ge=0.1, le=1)
    paper_roll_stop_loss_pct: float = Field(default=1.5, ge=0.1, le=10)
    paper_pullback_min_pct: float = Field(default=0.5, ge=0.1, le=10)
    paper_pullback_max_pct: float = Field(default=1.2, ge=0.1, le=10)
    paper_max_open_positions: int = Field(default=5, ge=1, le=30)
    paper_max_leverage: float = Field(default=3.0, ge=1, le=20)
    paper_fee_rate_pct: float = Field(default=0.05, ge=0, le=1)
    paper_breakeven_trigger_pct: float = Field(default=2.0, ge=0.1, le=20)
    paper_trailing_trigger_pct: float = Field(default=3.0, ge=0.1, le=30)
    paper_trailing_protect_ratio: float = Field(default=0.5, ge=0.1, le=0.95)
    paper_daily_loss_limit_pct: float = Field(default=10.0, ge=0.1, le=50)
    paper_max_consecutive_losses: int = Field(default=5, ge=1, le=20)
    paper_loss_pause_minutes: int = Field(default=240, ge=1, le=1440)
    api_trading_enabled: bool = False
    api_trading_testnet: bool = True
    api_trading_long_enabled: bool = True
    api_trading_short_enabled: bool = False
    api_max_notional_per_trade: float = Field(default=20.0, ge=5, le=10_000)
    api_max_open_positions: int = Field(default=1, ge=1, le=10)
    api_leverage: int = Field(default=1, ge=1, le=20)


@dataclass
class SymbolState:
    oi_history: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=128))
    price_history: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=128))
    trigger_times: deque[float] = field(default_factory=lambda: deque(maxlen=64))
    last_signal_at: float = 0
    last_oi_at: float = 0
    seeded: bool = False
    seeding: bool = False
    row: dict[str, Any] = field(default_factory=dict)


class BinanceMonitor:
    def __init__(self) -> None:
        self.config = MonitorConfig()
        self.states: dict[str, SymbolState] = defaultdict(SymbolState)
        self.rows: list[dict[str, Any]] = []
        self.signal_events: deque[dict[str, Any]] = deque(maxlen=500)
        self.alert_events: deque[dict[str, Any]] = deque(maxlen=200)
        self.symbol_specs: dict[str, dict[str, Any]] = {}
        self.paper_balance = self.config.paper_start_balance
        self.paper_positions: dict[str, dict[str, Any]] = {}
        self.paper_trades: deque[dict[str, Any]] = deque(maxlen=500)
        self.paper_cooldowns: dict[str, float] = {}
        self.paper_roll_setups: dict[str, dict[str, Any]] = {}
        self.paper_equity_high = self.config.paper_start_balance
        self.paper_max_drawdown_pct = 0.0
        self.paper_day = local_day()
        self.paper_day_start_balance = self.config.paper_start_balance
        self.paper_consecutive_losses = 0
        self.paper_pause_until = 0.0
        self.api_positions: dict[str, dict[str, Any]] = {}
        self.api_trades: deque[dict[str, Any]] = deque(maxlen=200)
        self.api_cooldowns: dict[str, float] = {}
        self.api_roll_setups: dict[str, dict[str, Any]] = {}
        self.api_status: dict[str, Any] = {
            "enabled": False,
            "ready": False,
            "mode": "testnet",
            "message": "未开启",
            "updatedAt": None,
        }
        self.status: dict[str, Any] = {
            "ok": False,
            "message": "warming up",
            "updatedAt": None,
            "tracked": 0,
            "staleRows": 0,
        }
        self._seed_semaphore = asyncio.Semaphore(6)
        self._backoff_until: float = 0
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run_loop())

    async def update_config(self, config: MonitorConfig) -> None:
        async with self._lock:
            self.config = config
        await self.poll_once()

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "config": self.config.model_dump(),
                "status": self.status,
                "rows": self.rows,
                "signals": list(self.signal_events),
                "alerts": list(self.alert_events),
                "paper": self._paper_snapshot(),
                "api": self._api_snapshot(),
            }

    async def _run_loop(self) -> None:
        while True:
            started = time.time()
            await self.poll_once()
            async with self._lock:
                wait_for = self.config.refresh_seconds
            elapsed = time.time() - started
            await asyncio.sleep(max(1, wait_for - elapsed))

    async def poll_once(self) -> None:
        try:
            now = time.time()
            if now < self._backoff_until:
                wait_seconds = int(self._backoff_until - now)
                async with self._lock:
                    self.status = {
                        "ok": False,
                        "message": f"Binance限流退避中 {wait_seconds}s",
                        "updatedAt": iso_now(),
                        "tracked": len(self.rows),
                        "staleRows": sum(1 for row in self.rows if row.get("isStale")),
                    }
                return
            timeout = httpx.Timeout(25.0, connect=8.0)
            limits = httpx.Limits(max_connections=24, max_keepalive_connections=12)
            async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
                symbols = await self._select_symbols(client)
                async with self._lock:
                    self.status = {
                        "ok": False,
                        "message": f"scanning 0/{len(symbols)}",
                        "updatedAt": iso_now(),
                        "tracked": len(symbols),
                        "staleRows": sum(1 for row in self.rows if row.get("isStale")),
                    }
                premium = await self._get_premium_index(client)
                rows = await self._collect_rows(client, symbols, premium)

            now_iso = iso_now()
            self._update_paper_trading(rows)
            await self._update_api_trading(rows)
            async with self._lock:
                self.rows = sorted(
                    rows,
                    key=lambda row: (
                        not row["isStrongSignal"],
                        -row["triggerCount1h"],
                        not row["isHighlighted"],
                        -row["signalStrength"],
                        -safe_num(row["oiChange5m"]),
                    ),
                )
                self.status = {
                    "ok": True,
                    "message": "live",
                    "updatedAt": now_iso,
                    "tracked": len(symbols),
                    "staleRows": sum(1 for row in rows if row["isStale"]),
                }
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {418, 429}:
                self._backoff_until = time.time() + 300
            async with self._lock:
                self.status = {
                    "ok": False,
                    "message": f"Binance限流 {exc.response.status_code}: 退避5分钟",
                    "updatedAt": iso_now(),
                    "tracked": len(self.rows),
                    "staleRows": sum(1 for row in self.rows if row.get("isStale")),
                }
        except Exception as exc:
            async with self._lock:
                self.status = {
                    "ok": False,
                    "message": f"{type(exc).__name__}: {exc}",
                    "updatedAt": iso_now(),
                    "tracked": len(self.rows),
                    "staleRows": sum(1 for row in self.rows if row.get("isStale")),
                }

    async def _select_symbols(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        exchange_info, tickers = await asyncio.gather(
            get_json(client, "/fapi/v1/exchangeInfo"),
            get_json(client, "/fapi/v1/ticker/24hr"),
        )
        allowed = {
            item["symbol"]
            for item in exchange_info.get("symbols", [])
            if item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
            and item.get("status") == "TRADING"
        }
        self.symbol_specs = {
            item["symbol"]: symbol_spec_from_exchange_info(item)
            for item in exchange_info.get("symbols", [])
            if item.get("symbol") in allowed
        }
        async with self._lock:
            config = self.config
        candidates = []
        for ticker in tickers:
            symbol = ticker.get("symbol")
            quote_volume = float(ticker.get("quoteVolume") or 0)
            if symbol in allowed and quote_volume >= config.min_24h_quote_volume:
                candidates.append(
                    {
                        "symbol": symbol,
                        "lastPrice": float(ticker.get("lastPrice") or 0),
                        "quoteVolume": quote_volume,
                        "priceChangePercent": float(ticker.get("priceChangePercent") or 0),
                    }
                )
        candidates.sort(key=lambda item: item["quoteVolume"], reverse=True)
        for index, item in enumerate(candidates, start=1):
            item["rank"] = index
        if config.monitor_all:
            return candidates
        return candidates[: config.top_symbols]

    async def _get_premium_index(self, client: httpx.AsyncClient) -> dict[str, dict[str, Any]]:
        payload = await get_json(client, "/fapi/v1/premiumIndex")
        return {item["symbol"]: item for item in payload if "symbol" in item}

    async def _collect_rows(
        self,
        client: httpx.AsyncClient,
        symbols: list[dict[str, Any]],
        premium: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        semaphore = asyncio.Semaphore(24)
        collected: list[dict[str, Any]] = []

        async def collect(symbol_info: dict[str, Any]) -> dict[str, Any] | None:
            async with semaphore:
                symbol = symbol_info["symbol"]
                try:
                    state = self.states[symbol]
                    self._schedule_seed_if_needed(symbol, symbol_info, state)
                    async with self._lock:
                        config = self.config
                    if symbol_info.get("rank", 999999) <= config.kline_top_symbols:
                        oi_payload, klines = await asyncio.gather(
                            get_json(client, "/fapi/v1/openInterest", {"symbol": symbol}),
                            get_json(
                                client,
                                "/fapi/v1/klines",
                                {"symbol": symbol, "interval": "1m", "limit": 12},
                            ),
                        )
                    else:
                        oi_payload = await get_json(client, "/fapi/v1/openInterest", {"symbol": symbol})
                        klines = None
                    return self._build_row(symbol_info, oi_payload, klines, premium.get(symbol, {}))
                except Exception:
                    return None

        tasks = [asyncio.create_task(collect(item)) for item in symbols]
        for index, task in enumerate(asyncio.as_completed(tasks), start=1):
            row = await task
            if row is not None:
                collected.append(row)
            if index % 25 == 0 or index == len(tasks):
                await self._publish_partial_rows(collected, index, len(tasks))
        return collected

    def _schedule_seed_if_needed(
        self,
        symbol: str,
        symbol_info: dict[str, Any],
        state: SymbolState,
    ) -> None:
        if state.seeded or state.seeding:
            return
        config = self.config
        if symbol_info.get("rank", 999999) > config.seed_top_symbols:
            return
        state.seeding = True
        asyncio.create_task(self._seed_symbol_history(symbol, symbol_info, state))

    async def _seed_symbol_history(
        self,
        symbol: str,
        symbol_info: dict[str, Any],
        state: SymbolState,
    ) -> None:
        try:
            async with self._seed_semaphore:
                timeout = httpx.Timeout(20.0, connect=8.0)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    kline_task = get_json(
                        client,
                        "/fapi/v1/klines",
                        {"symbol": symbol, "interval": "1m", "limit": 12},
                    )
                    oi_hist_task = get_json(
                        client,
                        "/futures/data/openInterestHist",
                        {"symbol": symbol, "period": "5m", "limit": 3},
                    )
                    results = await asyncio.gather(kline_task, oi_hist_task, return_exceptions=True)
            klines = results[0] if not isinstance(results[0], Exception) else None
            oi_hist = results[1] if not isinstance(results[1], Exception) else None
            self._apply_seed_payloads(state, oi_hist)
        finally:
            state.seeding = False

    def _apply_seed_payloads(self, state: SymbolState, oi_hist: list[dict[str, Any]] | None) -> None:
        if oi_hist:
            self._seed_oi_history(state, oi_hist)
            state.seeded = True

    def _seed_oi_history(self, state: SymbolState, oi_hist: list[dict[str, Any]]) -> None:
        for item in oi_hist:
            timestamp = float(item.get("timestamp") or 0) / 1000
            value = item.get("sumOpenInterest")
            if timestamp and value is not None:
                append_unique_sample(state.oi_history, (timestamp, float(value)))

    async def _publish_partial_rows(self, rows: list[dict[str, Any]], scanned: int, total: int) -> None:
        seen_symbols = {row["symbol"] for row in rows}
        merged_rows = rows + [
            self._row_with_current_age(symbol, state)
            for symbol, state in self.states.items()
            if state.row and symbol not in seen_symbols
        ]
        sorted_rows = sorted(
            merged_rows,
            key=lambda row: (
                not row["isStrongSignal"],
                -row["triggerCount1h"],
                not row["isHighlighted"],
                -row["signalStrength"],
                -safe_num(row["oiChange5m"]),
            ),
        )
        async with self._lock:
            self.rows = sorted_rows
            self.status = {
                "ok": bool(sorted_rows),
                "message": f"scanning {scanned}/{total}",
                "updatedAt": iso_now(),
                "tracked": total,
                "staleRows": sum(1 for row in sorted_rows if row["isStale"]),
            }

    def _row_with_current_age(self, symbol: str, state: SymbolState) -> dict[str, Any]:
        row = dict(state.row)
        async_config = self.config
        data_age_seconds = int(time.time() - state.last_oi_at) if state.last_oi_at else 999999
        is_stale = data_age_seconds > async_config.max_data_age_seconds
        row["dataAgeSeconds"] = data_age_seconds
        row["isStale"] = is_stale
        if is_stale:
            row["isHighlighted"] = False
            row["isStrongSignal"] = False
        row["updatedAt"] = iso_at(state.last_oi_at) if state.last_oi_at else row.get("updatedAt")
        return row

    def _build_row(
        self,
        symbol_info: dict[str, Any],
        oi_payload: dict[str, Any],
        klines: list[list[Any]] | None,
        premium: dict[str, Any],
    ) -> dict[str, Any]:
        symbol = symbol_info["symbol"]
        now = time.time()
        latest_price = float(symbol_info["lastPrice"])
        open_interest = float(oi_payload.get("openInterest") or 0)
        state = self.states[symbol]
        state.oi_history.append((now, open_interest))
        state.price_history.append((now, latest_price))
        state.last_oi_at = now

        oi_1m = pct_change_from_history(state.oi_history, now, 60)
        oi_3m = pct_change_from_history(state.oi_history, now, 180)
        oi_5m = pct_change_from_history(state.oi_history, now, 300)
        price_5m = price_change_from_klines(klines, latest_price, now) if klines else None
        volume_multiple = volume_multiple_from_klines(klines, now) if klines else None
        funding_rate = float(premium.get("lastFundingRate") or 0) * 100
        data_age_seconds = int(now - state.last_oi_at)

        async_config = self.config
        direction = classify_signal(oi_5m, price_5m, funding_rate, async_config.oi_5m_threshold)
        strength = signal_strength(oi_5m, price_5m, volume_multiple, symbol_info["quoteVolume"])
        is_stale = data_age_seconds > async_config.max_data_age_seconds
        highlighted = (
            oi_5m is not None
            and volume_multiple is not None
            and not is_stale
            and safe_num(oi_5m) >= async_config.oi_5m_threshold
            and safe_num(volume_multiple) >= async_config.volume_multiple_threshold
        )
        is_strong_signal = highlighted and strength >= async_config.signal_strength_threshold
        should_record_signal = direction != "观察" and not is_stale
        self._prune_triggers(state, now)
        recorded_signal = False
        if should_record_signal and now - state.last_signal_at >= 180:
            state.last_signal_at = now
            state.trigger_times.append(now)
            recorded_signal = True

        row = {
            "symbol": symbol,
            "latestPrice": latest_price,
            "oiChange1m": oi_1m,
            "oiChange3m": oi_3m,
            "oiChange5m": oi_5m,
            "priceChange5m": price_5m,
            "volumeMultiple5m": volume_multiple,
            "fundingRate": funding_rate,
            "quoteVolume24h": symbol_info["quoteVolume"],
            "signalDirection": direction,
            "signalStrength": strength,
            "triggerCount1h": len(state.trigger_times),
            "lastSignalAt": iso_at(state.last_signal_at) if state.last_signal_at else None,
            "repeatSignalLevel": repeat_signal_level(len(state.trigger_times)),
            "dataAgeSeconds": data_age_seconds,
            "isStale": is_stale,
            "updatedAt": iso_now(),
            "isHighlighted": highlighted,
            "isStrongSignal": is_strong_signal,
        }
        if recorded_signal:
            event = {
                "symbol": symbol,
                "signalDirection": direction,
                "signalStrength": strength,
                "isStrongSignal": is_strong_signal,
                "triggerCount1h": len(state.trigger_times),
                "oiChange5m": oi_5m,
                "priceChange5m": price_5m,
                "volumeMultiple5m": volume_multiple,
                "fundingRate": funding_rate,
                "quoteVolume24h": symbol_info["quoteVolume"],
                "latestPrice": latest_price,
                "createdAt": iso_at(now),
            }
            self.signal_events.appendleft(event)
            schedule_background_task(self._send_dingtalk_alert(event))
        state.row = row
        return row

    def _update_paper_trading(self, rows: list[dict[str, Any]]) -> None:
        config = self.config
        if not config.paper_enabled:
            return
        row_map = {row["symbol"]: row for row in rows}
        self._refresh_paper_day(self._paper_equity(row_map))
        for symbol, position in list(self.paper_positions.items()):
            row = row_map.get(symbol)
            if row is None:
                continue
            self._maybe_close_paper_position(position, row)
        for row in rows:
            self._maybe_open_paper_position(row)
        equity = self._paper_equity(row_map)
        self.paper_equity_high = max(self.paper_equity_high, equity)
        if self.paper_equity_high > 0:
            drawdown = ((self.paper_equity_high - equity) / self.paper_equity_high) * 100
            self.paper_max_drawdown_pct = max(self.paper_max_drawdown_pct, drawdown)

    def _maybe_open_paper_position(self, row: dict[str, Any]) -> None:
        config = self.config
        symbol = row["symbol"]
        side = paper_side_from_signal(row["signalDirection"])
        if side is None:
            return
        if symbol in self.paper_positions:
            return
        if self._paper_opening_paused():
            return
        cooldown_until = self.paper_cooldowns.get(symbol, 0)
        if time.time() < cooldown_until:
            return
        if len(self.paper_positions) >= config.paper_max_open_positions:
            return
        if not row["isStrongSignal"] or row["isStale"]:
            return
        latest_price = float(row["latestPrice"])
        if latest_price <= 0:
            return
        had_roll_setup = symbol in self.paper_roll_setups
        roll_setup = self._paper_roll_setup_for(row, side, latest_price)
        if had_roll_setup and not roll_setup and symbol in self.paper_roll_setups:
            return
        risk_factor = roll_setup["riskFactor"] if roll_setup else 1.0
        stop_loss_pct_value = config.paper_roll_stop_loss_pct if roll_setup else config.paper_stop_loss_pct
        take_profit_pct_value = config.paper_take_profit_pct
        risk_amount = self.paper_balance * (config.paper_risk_pct / 100) * risk_factor
        stop_loss_pct = stop_loss_pct_value / 100
        notional_by_risk = risk_amount / stop_loss_pct
        max_total_notional = self.paper_balance * config.paper_max_leverage
        used_notional = sum(position["notional"] for position in self.paper_positions.values())
        remaining_notional = max(0.0, max_total_notional - used_notional)
        notional = min(notional_by_risk, remaining_notional)
        if notional <= 0:
            return
        qty = notional / latest_price
        if side == "long":
            stop_price = latest_price * (1 - stop_loss_pct_value / 100)
            take_profit_price = latest_price * (1 + take_profit_pct_value / 100)
        else:
            stop_price = latest_price * (1 + stop_loss_pct_value / 100)
            take_profit_price = latest_price * (1 - take_profit_pct_value / 100)
        self.paper_positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "entryPrice": latest_price,
            "qty": qty,
            "notional": notional,
            "stopPrice": stop_price,
            "initialStopPrice": stop_price,
            "takeProfitPrice": take_profit_price,
            "entryAt": time.time(),
            "entryTime": iso_now(),
            "signalDirection": row["signalDirection"],
            "signalStrength": row["signalStrength"],
            "entryType": roll_setup["mode"] if roll_setup else "首仓",
            "rollCount": roll_setup["nextRollCount"] if roll_setup else 0,
            "stopLossPct": stop_loss_pct_value,
            "takeProfitPct": take_profit_pct_value,
            "bestReturnPct": 0.0,
            "trailingStopActive": False,
        }
        if roll_setup:
            self.paper_roll_setups.pop(symbol, None)
        schedule_background_task(self._send_dingtalk_trade_alert("模拟开仓", self.paper_positions[symbol]))

    def _paper_roll_setup_for(self, row: dict[str, Any], side: str, latest_price: float) -> dict[str, Any] | None:
        config = self.config
        setup = self.paper_roll_setups.get(row["symbol"])
        if not setup:
            return None
        now = time.time()
        if now - setup["createdAt"] > config.paper_roll_window_minutes * 60:
            self.paper_roll_setups.pop(row["symbol"], None)
            return None
        if setup["side"] != side or setup["rollCount"] >= config.paper_max_roll_entries:
            self.paper_roll_setups.pop(row["symbol"], None)
            return None
        if row["oiChange5m"] is None or row["volumeMultiple5m"] is None:
            return None
        if row["oiChange5m"] < config.oi_5m_threshold or row["volumeMultiple5m"] < config.volume_multiple_threshold:
            return None
        price_5m = float(row["priceChange5m"] or 0)
        exit_price = float(setup["exitPrice"])
        if side == "long":
            pullback_pct = ((exit_price - latest_price) / exit_price) * 100
            momentum_ok = latest_price > exit_price and price_5m > 0
            pullback_ok = config.paper_pullback_min_pct <= pullback_pct <= config.paper_pullback_max_pct and price_5m > 0
        else:
            pullback_pct = ((latest_price - exit_price) / exit_price) * 100
            momentum_ok = latest_price < exit_price and price_5m < 0
            pullback_ok = config.paper_pullback_min_pct <= pullback_pct <= config.paper_pullback_max_pct and price_5m < 0
        if pullback_ok:
            return {
                "mode": "回踩滚仓",
                "riskFactor": config.paper_pullback_roll_risk_factor,
                "nextRollCount": setup["rollCount"] + 1,
            }
        if momentum_ok:
            return {
                "mode": "动量滚仓",
                "riskFactor": config.paper_momentum_roll_risk_factor,
                "nextRollCount": setup["rollCount"] + 1,
            }
        return None

    def _maybe_close_paper_position(self, position: dict[str, Any], row: dict[str, Any]) -> None:
        config = self.config
        latest_price = float(row["latestPrice"])
        side_mult = 1 if position["side"] == "long" else -1
        return_pct = ((latest_price - position["entryPrice"]) / position["entryPrice"]) * side_mult * 100
        self._update_paper_trailing_stop(position, latest_price, return_pct)
        age_minutes = (time.time() - position["entryAt"]) / 60
        stop_loss_pct = float(position.get("stopLossPct", config.paper_stop_loss_pct))
        take_profit_pct = float(position.get("takeProfitPct", config.paper_take_profit_pct))
        reason = None
        if self._paper_stop_hit(position, latest_price):
            reason = "移动止损" if position.get("trailingStopActive") else "止损"
        elif return_pct <= -stop_loss_pct:
            reason = "止损"
        elif return_pct >= take_profit_pct:
            reason = "止盈"
        elif age_minutes >= config.paper_max_hold_minutes:
            reason = "超时"
        else:
            new_side = paper_side_from_signal(row["signalDirection"])
            if new_side and new_side != position["side"] and row["isStrongSignal"]:
                reason = "反向信号"
        if reason:
            self._close_paper_position(position, latest_price, reason)

    def _close_paper_position(self, position: dict[str, Any], exit_price: float, reason: str) -> None:
        side_mult = 1 if position["side"] == "long" else -1
        gross_pnl = position["notional"] * ((exit_price - position["entryPrice"]) / position["entryPrice"]) * side_mult
        fee = position["notional"] * (self.config.paper_fee_rate_pct / 100) * 2
        net_pnl = gross_pnl - fee
        self.paper_balance += net_pnl
        trade = {
            **position,
            "exitPrice": exit_price,
            "exitTime": iso_now(),
            "exitReason": reason,
            "grossPnl": gross_pnl,
            "fee": fee,
            "pnl": net_pnl,
            "pnlPct": (net_pnl / position["notional"]) * 100 if position["notional"] else 0,
        }
        self.paper_trades.appendleft(trade)
        self.paper_positions.pop(position["symbol"], None)
        self._update_paper_risk_state(net_pnl)
        self._update_paper_reentry_state(position, exit_price, reason)
        schedule_background_task(self._send_dingtalk_trade_alert("模拟平仓", trade))

    def _update_paper_trailing_stop(self, position: dict[str, Any], latest_price: float, return_pct: float) -> None:
        config = self.config
        position["bestReturnPct"] = max(float(position.get("bestReturnPct", 0)), return_pct)
        if return_pct < config.paper_breakeven_trigger_pct:
            return
        side = position["side"]
        entry_price = float(position["entryPrice"])
        if side == "long":
            breakeven_stop = entry_price
            if breakeven_stop > float(position["stopPrice"]):
                position["stopPrice"] = breakeven_stop
                position["trailingStopActive"] = True
        else:
            breakeven_stop = entry_price
            if breakeven_stop < float(position["stopPrice"]):
                position["stopPrice"] = breakeven_stop
                position["trailingStopActive"] = True
        if return_pct < config.paper_trailing_trigger_pct:
            return
        protected_return = max(0.0, position["bestReturnPct"] * config.paper_trailing_protect_ratio)
        if side == "long":
            trailing_stop = entry_price * (1 + protected_return / 100)
            if trailing_stop > float(position["stopPrice"]):
                position["stopPrice"] = trailing_stop
                position["trailingStopActive"] = True
        else:
            trailing_stop = entry_price * (1 - protected_return / 100)
            if trailing_stop < float(position["stopPrice"]):
                position["stopPrice"] = trailing_stop
                position["trailingStopActive"] = True

    def _paper_stop_hit(self, position: dict[str, Any], latest_price: float) -> bool:
        if position["side"] == "long":
            return latest_price <= float(position["stopPrice"])
        return latest_price >= float(position["stopPrice"])

    def _refresh_paper_day(self, equity: float) -> None:
        today = local_day()
        if self.paper_day == today:
            return
        self.paper_day = today
        self.paper_day_start_balance = equity
        self.paper_consecutive_losses = 0
        self.paper_pause_until = 0.0

    def _paper_opening_paused(self) -> bool:
        if time.time() < self.paper_pause_until:
            return True
        daily_pnl_pct = self._paper_daily_pnl_pct()
        return daily_pnl_pct <= -self.config.paper_daily_loss_limit_pct

    def _paper_daily_pnl(self) -> float:
        return self.paper_balance - self.paper_day_start_balance

    def _paper_daily_pnl_pct(self) -> float:
        if self.paper_day_start_balance <= 0:
            return 0.0
        return (self._paper_daily_pnl() / self.paper_day_start_balance) * 100

    def _update_paper_risk_state(self, net_pnl: float) -> None:
        if net_pnl < 0:
            self.paper_consecutive_losses += 1
            if self.paper_consecutive_losses >= self.config.paper_max_consecutive_losses:
                self.paper_pause_until = time.time() + self.config.paper_loss_pause_minutes * 60
        else:
            self.paper_consecutive_losses = 0

    def _update_paper_reentry_state(self, position: dict[str, Any], exit_price: float, reason: str) -> None:
        symbol = position["symbol"]
        self.paper_roll_setups.pop(symbol, None)
        if reason == "止盈" and position.get("rollCount", 0) < self.config.paper_max_roll_entries:
            self.paper_cooldowns.pop(symbol, None)
            self.paper_roll_setups[symbol] = {
                "symbol": symbol,
                "side": position["side"],
                "exitPrice": exit_price,
                "createdAt": time.time(),
                "rollCount": int(position.get("rollCount", 0)),
            }
            return
        cooldown_seconds = self.config.paper_reentry_cooldown_minutes * 60
        if cooldown_seconds > 0:
            self.paper_cooldowns[symbol] = time.time() + cooldown_seconds

    def _paper_snapshot(self) -> dict[str, Any]:
        row_map = {row["symbol"]: row for row in self.rows}
        equity = self._paper_equity(row_map)
        closed = list(self.paper_trades)
        wins = sum(1 for trade in closed if trade["pnl"] > 0)
        total = len(closed)
        opening_paused = self._paper_opening_paused()
        return {
            "enabled": self.config.paper_enabled,
            "balance": self.paper_balance,
            "equity": equity,
            "totalPnl": equity - self.config.paper_start_balance,
            "totalPnlPct": ((equity - self.config.paper_start_balance) / self.config.paper_start_balance) * 100,
            "openCount": len(self.paper_positions),
            "closedCount": total,
            "winRate": (wins / total) * 100 if total else 0,
            "maxDrawdownPct": self.paper_max_drawdown_pct,
            "dailyPnl": self._paper_daily_pnl(),
            "dailyPnlPct": self._paper_daily_pnl_pct(),
            "consecutiveLosses": self.paper_consecutive_losses,
            "openingPaused": opening_paused,
            "pauseReason": self._paper_pause_reason() if opening_paused else "正常",
            "pauseUntil": iso_at(self.paper_pause_until) if time.time() < self.paper_pause_until else "-",
            "statsBySignal": self._paper_group_stats("signalDirection"),
            "statsByEntryType": self._paper_group_stats("entryType"),
            "positions": [self._paper_position_view(position, row_map.get(symbol)) for symbol, position in self.paper_positions.items()],
            "trades": closed[:100],
        }

    def _paper_pause_reason(self) -> str:
        if time.time() < self.paper_pause_until:
            return f"连亏暂停，至 {iso_at(self.paper_pause_until)}"
        if self._paper_daily_pnl_pct() <= -self.config.paper_daily_loss_limit_pct:
            return "今日亏损达到上限"
        return "正常"

    def _paper_group_stats(self, key: str) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for trade in self.paper_trades:
            label = str(trade.get(key) or "-")
            group = groups.setdefault(label, {"name": label, "total": 0, "wins": 0, "pnl": 0.0, "grossWin": 0.0, "grossLoss": 0.0})
            pnl = float(trade.get("pnl") or 0)
            group["total"] += 1
            group["pnl"] += pnl
            if pnl > 0:
                group["wins"] += 1
                group["grossWin"] += pnl
            elif pnl < 0:
                group["grossLoss"] += abs(pnl)
        stats = []
        for group in groups.values():
            total = group["total"]
            losses = total - group["wins"]
            stats.append(
                {
                    "name": group["name"],
                    "total": total,
                    "wins": group["wins"],
                    "losses": losses,
                    "winRate": (group["wins"] / total) * 100 if total else 0,
                    "pnl": group["pnl"],
                    "avgPnl": group["pnl"] / total if total else 0,
                    "profitFactor": (group["grossWin"] / group["grossLoss"]) if group["grossLoss"] > 0 else None,
                }
            )
        return sorted(stats, key=lambda item: item["pnl"], reverse=True)

    def _paper_equity(self, row_map: dict[str, dict[str, Any]]) -> float:
        unrealized = 0.0
        for symbol, position in self.paper_positions.items():
            row = row_map.get(symbol)
            if row:
                unrealized += self._paper_unrealized_pnl(position, float(row["latestPrice"]))
        return self.paper_balance + unrealized

    def _paper_position_view(
        self,
        position: dict[str, Any],
        row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        latest_price = float(row["latestPrice"]) if row else position["entryPrice"]
        unrealized = self._paper_unrealized_pnl(position, latest_price)
        return {
            **position,
            "latestPrice": latest_price,
            "unrealizedPnl": unrealized,
            "unrealizedPnlPct": (unrealized / position["notional"]) * 100 if position["notional"] else 0,
            "ageMinutes": (time.time() - position["entryAt"]) / 60,
        }

    def _paper_unrealized_pnl(self, position: dict[str, Any], latest_price: float) -> float:
        side_mult = 1 if position["side"] == "long" else -1
        gross_pnl = position["notional"] * ((latest_price - position["entryPrice"]) / position["entryPrice"]) * side_mult
        exit_fee = position["notional"] * (self.config.paper_fee_rate_pct / 100)
        return gross_pnl - exit_fee

    def reset_paper(self) -> None:
        self.paper_balance = self.config.paper_start_balance
        self.paper_positions.clear()
        self.paper_trades.clear()
        self.paper_cooldowns.clear()
        self.paper_roll_setups.clear()
        self.paper_equity_high = self.config.paper_start_balance
        self.paper_max_drawdown_pct = 0.0
        self.paper_day = local_day()
        self.paper_day_start_balance = self.config.paper_start_balance
        self.paper_consecutive_losses = 0
        self.paper_pause_until = 0.0

    async def _update_api_trading(self, rows: list[dict[str, Any]]) -> None:
        config = self.config
        if not config.api_trading_enabled:
            self.api_status = self._api_status("未开启，不会下单", enabled=False)
            return
        ready_error = self._api_ready_error()
        if ready_error:
            self.api_status = self._api_status(ready_error, ready=False)
            return
        row_map = {row["symbol"]: row for row in rows}
        for symbol, position in list(self.api_positions.items()):
            row = row_map.get(symbol)
            if row is None:
                continue
            await self._maybe_close_api_position(position, row)
        for row in rows:
            await self._maybe_open_api_position(row)
        self.api_status = self._api_status("运行中", ready=True)

    def _api_ready_error(self) -> str | None:
        if not BINANCE_API_KEY or not BINANCE_API_SECRET:
            return "缺少 BINANCE_API_KEY / BINANCE_API_SECRET"
        if not self.config.api_trading_testnet and BINANCE_LIVE_TRADING_CONFIRM != "I_UNDERSTAND_REAL_MONEY":
            return "主网交易未确认"
        return None

    def _api_status(self, message: str, enabled: bool | None = None, ready: bool = False) -> dict[str, Any]:
        return {
            "enabled": self.config.api_trading_enabled if enabled is None else enabled,
            "ready": ready,
            "mode": "testnet" if self.config.api_trading_testnet else "live",
            "message": message,
            "updatedAt": iso_now(),
        }

    async def _maybe_open_api_position(self, row: dict[str, Any]) -> None:
        config = self.config
        symbol = row["symbol"]
        side = paper_side_from_signal(row["signalDirection"])
        if side is None:
            return
        if side == "long" and not config.api_trading_long_enabled:
            return
        if side == "short" and not config.api_trading_short_enabled:
            return
        if symbol in self.api_positions:
            return
        if self._paper_opening_paused():
            return
        if time.time() < self.api_cooldowns.get(symbol, 0):
            return
        if len(self.api_positions) >= config.api_max_open_positions:
            return
        if not row["isStrongSignal"] or row["isStale"]:
            return
        latest_price = float(row["latestPrice"])
        if latest_price <= 0:
            return
        had_roll_setup = symbol in self.api_roll_setups
        roll_setup = self._api_roll_setup_for(row, side, latest_price)
        if had_roll_setup and not roll_setup and symbol in self.api_roll_setups:
            return
        risk_factor = roll_setup["riskFactor"] if roll_setup else 1.0
        stop_loss_pct_value = config.paper_roll_stop_loss_pct if roll_setup else config.paper_stop_loss_pct
        take_profit_pct_value = config.paper_take_profit_pct
        notional = config.api_max_notional_per_trade * risk_factor
        qty_text = self._api_quantity(symbol, notional / latest_price)
        if qty_text is None:
            self.api_status = self._api_status(f"{symbol} 数量低于交易所最小值", ready=True)
            return
        order_side = "BUY" if side == "long" else "SELL"
        try:
            await self._api_set_leverage(symbol)
            order = await self._api_signed_request(
                "POST",
                "/fapi/v1/order",
                {
                    "symbol": symbol,
                    "side": order_side,
                    "type": "MARKET",
                    "quantity": qty_text,
                    "newOrderRespType": "RESULT",
                },
            )
        except Exception as exc:
            self.api_status = self._api_status(f"{symbol} 开仓失败: {type(exc).__name__}: {exc}", ready=True)
            return
        entry_price = api_order_price(order, latest_price)
        qty = float(qty_text)
        if side == "long":
            stop_price = entry_price * (1 - stop_loss_pct_value / 100)
            take_profit_price = entry_price * (1 + take_profit_pct_value / 100)
        else:
            stop_price = entry_price * (1 + stop_loss_pct_value / 100)
            take_profit_price = entry_price * (1 - take_profit_pct_value / 100)
        position = {
            "mode": "API交易-testnet" if config.api_trading_testnet else "API交易-主网",
            "symbol": symbol,
            "side": side,
            "entryPrice": entry_price,
            "qty": qty,
            "notional": qty * entry_price,
            "stopPrice": stop_price,
            "initialStopPrice": stop_price,
            "takeProfitPrice": take_profit_price,
            "entryAt": time.time(),
            "entryTime": iso_now(),
            "signalDirection": row["signalDirection"],
            "signalStrength": row["signalStrength"],
            "entryType": roll_setup["mode"] if roll_setup else "首仓",
            "rollCount": roll_setup["nextRollCount"] if roll_setup else 0,
            "stopLossPct": stop_loss_pct_value,
            "takeProfitPct": take_profit_pct_value,
            "bestReturnPct": 0.0,
            "trailingStopActive": False,
            "order": order,
        }
        self.api_positions[symbol] = position
        if roll_setup:
            self.api_roll_setups.pop(symbol, None)
        schedule_background_task(self._send_dingtalk_trade_alert("API开仓", position))

    def _api_roll_setup_for(self, row: dict[str, Any], side: str, latest_price: float) -> dict[str, Any] | None:
        config = self.config
        setup = self.api_roll_setups.get(row["symbol"])
        if not setup:
            return None
        now = time.time()
        if now - setup["createdAt"] > config.paper_roll_window_minutes * 60:
            self.api_roll_setups.pop(row["symbol"], None)
            return None
        if setup["side"] != side or setup["rollCount"] >= config.paper_max_roll_entries:
            self.api_roll_setups.pop(row["symbol"], None)
            return None
        if row["oiChange5m"] is None or row["volumeMultiple5m"] is None:
            return None
        if row["oiChange5m"] < config.oi_5m_threshold or row["volumeMultiple5m"] < config.volume_multiple_threshold:
            return None
        price_5m = float(row["priceChange5m"] or 0)
        exit_price = float(setup["exitPrice"])
        if side == "long":
            pullback_pct = ((exit_price - latest_price) / exit_price) * 100
            momentum_ok = latest_price > exit_price and price_5m > 0
            pullback_ok = config.paper_pullback_min_pct <= pullback_pct <= config.paper_pullback_max_pct and price_5m > 0
        else:
            pullback_pct = ((latest_price - exit_price) / exit_price) * 100
            momentum_ok = latest_price < exit_price and price_5m < 0
            pullback_ok = config.paper_pullback_min_pct <= pullback_pct <= config.paper_pullback_max_pct and price_5m < 0
        if pullback_ok:
            return {"mode": "回踩滚仓", "riskFactor": config.paper_pullback_roll_risk_factor, "nextRollCount": setup["rollCount"] + 1}
        if momentum_ok:
            return {"mode": "动量滚仓", "riskFactor": config.paper_momentum_roll_risk_factor, "nextRollCount": setup["rollCount"] + 1}
        return None

    async def _maybe_close_api_position(self, position: dict[str, Any], row: dict[str, Any]) -> None:
        latest_price = float(row["latestPrice"])
        side_mult = 1 if position["side"] == "long" else -1
        return_pct = ((latest_price - position["entryPrice"]) / position["entryPrice"]) * side_mult * 100
        self._update_paper_trailing_stop(position, latest_price, return_pct)
        age_minutes = (time.time() - position["entryAt"]) / 60
        stop_loss_pct = float(position.get("stopLossPct", self.config.paper_stop_loss_pct))
        take_profit_pct = float(position.get("takeProfitPct", self.config.paper_take_profit_pct))
        reason = None
        if self._paper_stop_hit(position, latest_price):
            reason = "移动止损" if position.get("trailingStopActive") else "止损"
        elif return_pct <= -stop_loss_pct:
            reason = "止损"
        elif return_pct >= take_profit_pct:
            reason = "止盈"
        elif age_minutes >= self.config.paper_max_hold_minutes:
            reason = "超时"
        else:
            new_side = paper_side_from_signal(row["signalDirection"])
            if new_side and new_side != position["side"] and row["isStrongSignal"]:
                reason = "反向信号"
        if reason:
            await self._close_api_position(position, latest_price, reason)

    async def _close_api_position(self, position: dict[str, Any], latest_price: float, reason: str) -> None:
        symbol = position["symbol"]
        order_side = "SELL" if position["side"] == "long" else "BUY"
        qty_text = self._api_quantity(symbol, float(position["qty"]))
        if qty_text is None:
            self.api_status = self._api_status(f"{symbol} 平仓数量无效", ready=True)
            return
        try:
            order = await self._api_signed_request(
                "POST",
                "/fapi/v1/order",
                {
                    "symbol": symbol,
                    "side": order_side,
                    "type": "MARKET",
                    "quantity": qty_text,
                    "reduceOnly": "true",
                    "newOrderRespType": "RESULT",
                },
            )
        except Exception as exc:
            self.api_status = self._api_status(f"{symbol} 平仓失败: {type(exc).__name__}: {exc}", ready=True)
            return
        exit_price = api_order_price(order, latest_price)
        side_mult = 1 if position["side"] == "long" else -1
        gross_pnl = position["notional"] * ((exit_price - position["entryPrice"]) / position["entryPrice"]) * side_mult
        trade = {
            **position,
            "exitPrice": exit_price,
            "exitTime": iso_now(),
            "exitReason": reason,
            "grossPnl": gross_pnl,
            "fee": 0.0,
            "pnl": gross_pnl,
            "pnlPct": (gross_pnl / position["notional"]) * 100 if position["notional"] else 0,
            "closeOrder": order,
        }
        self.api_trades.appendleft(trade)
        self.api_positions.pop(symbol, None)
        self._update_api_reentry_state(position, exit_price, reason)
        schedule_background_task(self._send_dingtalk_trade_alert("API平仓", trade))

    def _update_api_reentry_state(self, position: dict[str, Any], exit_price: float, reason: str) -> None:
        symbol = position["symbol"]
        self.api_roll_setups.pop(symbol, None)
        if reason == "止盈" and position.get("rollCount", 0) < self.config.paper_max_roll_entries:
            self.api_cooldowns.pop(symbol, None)
            self.api_roll_setups[symbol] = {
                "symbol": symbol,
                "side": position["side"],
                "exitPrice": exit_price,
                "createdAt": time.time(),
                "rollCount": int(position.get("rollCount", 0)),
            }
            return
        cooldown_seconds = self.config.paper_reentry_cooldown_minutes * 60
        if cooldown_seconds > 0:
            self.api_cooldowns[symbol] = time.time() + cooldown_seconds

    async def _api_set_leverage(self, symbol: str) -> None:
        await self._api_signed_request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": self.config.api_leverage})

    async def _api_signed_request(self, method: str, path: str, params: dict[str, Any]) -> Any:
        base_url = BINANCE_TESTNET_FAPI if self.config.api_trading_testnet else BINANCE_FAPI
        signed_params = {**params, "timestamp": int(time.time() * 1000), "recvWindow": 5000}
        query = urlencode(signed_params)
        signature = hmac.new(BINANCE_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY, "Content-Type": "application/x-www-form-urlencoded"}
        timeout = httpx.Timeout(12.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            if method == "POST":
                response = await client.post(f"{base_url}{path}", content=f"{query}&signature={signature}")
            else:
                response = await client.get(f"{base_url}{path}?{query}&signature={signature}")
            response.raise_for_status()
            return response.json()

    def _api_quantity(self, symbol: str, qty: float) -> str | None:
        spec = self.symbol_specs.get(symbol, {})
        step_size = Decimal(str(spec.get("stepSize") or "0.001"))
        min_qty = Decimal(str(spec.get("minQty") or "0"))
        raw_qty = Decimal(str(qty))
        if step_size <= 0:
            return str(raw_qty)
        steps = (raw_qty / step_size).to_integral_value(rounding=ROUND_DOWN)
        final_qty = steps * step_size
        if final_qty <= 0 or final_qty < min_qty:
            return None
        return format(final_qty.normalize(), "f")

    def _api_snapshot(self) -> dict[str, Any]:
        return {
            **self.api_status,
            "hasKeys": bool(BINANCE_API_KEY and BINANCE_API_SECRET),
            "liveConfirmed": BINANCE_LIVE_TRADING_CONFIRM == "I_UNDERSTAND_REAL_MONEY",
            "adminKeyProtected": bool(ADMIN_ACTION_KEY),
            "openCount": len(self.api_positions),
            "closedCount": len(self.api_trades),
            "positions": list(self.api_positions.values()),
            "trades": list(self.api_trades)[:100],
        }

    async def _send_dingtalk_alert(self, event: dict[str, Any]) -> None:
        if not DINGTALK_WEBHOOK:
            return
        payload = build_dingtalk_payload(event)
        await self._post_dingtalk_payload(payload, event["symbol"])

    async def _send_dingtalk_trade_alert(self, action: str, trade: dict[str, Any]) -> None:
        if not DINGTALK_WEBHOOK:
            return
        payload = build_dingtalk_trade_payload(action, trade)
        await self._post_dingtalk_payload(payload, trade["symbol"])

    async def _post_dingtalk_payload(self, payload: dict[str, Any], symbol: str) -> None:
        alert_record = {
            "symbol": symbol,
            "createdAt": iso_now(),
            "ok": False,
            "message": "sending",
        }
        try:
            timeout = httpx.Timeout(10.0, connect=5.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(DINGTALK_WEBHOOK, json=payload)
                response.raise_for_status()
                result = response.json()
            ok = result.get("errcode") == 0
            alert_record["ok"] = ok
            alert_record["message"] = result.get("errmsg", "ok" if ok else "failed")
        except Exception as exc:
            alert_record["message"] = f"{type(exc).__name__}: {exc}"
        finally:
            self.alert_events.appendleft(alert_record)

    def _prune_triggers(self, state: SymbolState, now: float) -> None:
        cutoff = now - 3600
        while state.trigger_times and state.trigger_times[0] < cutoff:
            state.trigger_times.popleft()


async def get_json(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any] | None = None,
) -> Any:
    response = await client.get(f"{BINANCE_FAPI}{path}", params=params)
    response.raise_for_status()
    return response.json()


def pct_change_from_history(history: deque[tuple[float, float]], now: float, seconds: int) -> float | None:
    if len(history) < 2:
        return None
    target = now - seconds
    baseline = history[0][1]
    for ts, value in history:
        if ts <= target:
            baseline = value
        else:
            break
    latest = history[-1][1]
    if baseline <= 0:
        return None
    return ((latest - baseline) / baseline) * 100


def price_change_from_klines(
    klines: list[list[Any]],
    latest_price: float,
    now: float,
) -> float | None:
    closed = closed_klines(klines, now)
    if len(closed) < 5:
        return None
    baseline_open = float(closed[-5][1])
    if baseline_open <= 0:
        return None
    return ((latest_price - baseline_open) / baseline_open) * 100


def volume_multiple_from_klines(klines: list[list[Any]], now: float) -> float | None:
    closed = closed_klines(klines, now)
    if len(closed) < 10:
        return None
    volumes = [float(item[7]) for item in closed]
    recent_5m = sum(volumes[-5:])
    previous_5m = sum(volumes[-10:-5])
    if previous_5m <= 0:
        return None
    return recent_5m / previous_5m


def closed_klines(klines: list[list[Any]], now: float) -> list[list[Any]]:
    now_ms = now * 1000
    return [item for item in klines if float(item[6]) <= now_ms]


def append_unique_sample(history: deque[tuple[float, float]], sample: tuple[float, float]) -> None:
    ts, value = sample
    if not history or abs(history[-1][0] - ts) > 0.001:
        history.append((ts, value))
    else:
        history[-1] = (ts, value)


def classify_signal(oi_5m: float | None, price_5m: float | None, funding: float, oi_threshold: float) -> str:
    oi = safe_num(oi_5m)
    price = safe_num(price_5m)
    if oi < oi_threshold:
        return "观察"
    if price >= 1.2 and funding >= 0.01:
        return "挤空"
    if price <= -1.2 and funding <= -0.01:
        return "挤多"
    if price >= 0.25:
        return "多头增仓"
    if price <= -0.25:
        return "空头增仓"
    return "仅OI增长"


def paper_side_from_signal(signal_direction: str) -> str | None:
    if signal_direction in {"多头增仓", "挤空"}:
        return "long"
    if signal_direction in {"空头增仓", "挤多"}:
        return "short"
    return None


def signal_strength(
    oi_5m: float | None,
    price_5m: float | None,
    volume_multiple: float | None,
    quote_volume: float,
) -> int:
    oi_score = min(max(safe_num(oi_5m), 0) * 9, 45)
    volume_score = min(max(safe_num(volume_multiple) - 1, 0) * 16, 25)
    price_score = min(abs(safe_num(price_5m)) * 6, 18)
    liquidity_score = min(math.log10(max(quote_volume, 1)) * 1.5, 12)
    return int(round(min(100, oi_score + volume_score + price_score + liquidity_score)))


def build_dingtalk_payload(event: dict[str, Any]) -> dict[str, Any]:
    title = f"{DINGTALK_KEYWORD} {event['symbol']} {event['signalDirection']}"
    text = "\n".join(
        [
            f"## {title}",
            f"- 方向：{event['signalDirection']}",
            f"- 强度：{event['signalStrength']}",
            f"- 强信号：{'是' if event.get('isStrongSignal') else '否'}",
            f"- 1小时触发：{event['triggerCount1h']} 次",
            f"- 最新价：{format_price(event['latestPrice'])}",
            f"- 5m OI：{format_pct(event['oiChange5m'])}",
            f"- 5m价格：{format_pct(event['priceChange5m'])}",
            f"- 5m量倍数：{format_multiple(event['volumeMultiple5m'])}",
            f"- 资金费率：{format_pct(event['fundingRate'])}",
            f"- 24h成交额：{format_money(event['quoteVolume24h'])}",
            f"- 时间：{event['createdAt']}",
        ]
    )
    return {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": text,
        },
    }


def build_dingtalk_trade_payload(action: str, trade: dict[str, Any]) -> dict[str, Any]:
    side_text = "做多" if trade["side"] == "long" else "做空"
    title = f"{DINGTALK_KEYWORD} {action} {trade['symbol']} {side_text}"
    lines = [
        f"## {title}",
        f"- 模式：{trade.get('mode', '模拟交易')}",
        f"- 入场类型：{trade.get('entryType', '首仓')}",
        f"- 方向：{side_text}",
        f"- 入场价：{format_price(trade['entryPrice'])}",
        f"- 名义金额：{format_money(trade['notional'])} USDT",
        f"- 数量：{trade['qty']:.6f}",
        f"- 止损：{format_price(trade['stopPrice'])}",
        f"- 止盈：{format_price(trade['takeProfitPrice'])}",
        f"- 入场时间：{trade['entryTime']}",
        f"- 信号：{trade['signalDirection']} / 强度 {trade['signalStrength']}",
    ]
    if action.endswith("平仓"):
        lines.extend(
            [
                f"- 出场价：{format_price(trade['exitPrice'])}",
                f"- 平仓原因：{trade['exitReason']}",
                f"- 手续费：{trade['fee']:.2f} USDT",
                f"- 净盈亏：{trade['pnl']:.2f} USDT ({trade['pnlPct']:.2f}%)",
                f"- 出场时间：{trade['exitTime']}",
            ]
        )
    return {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": "\n".join(lines),
        },
    }


def format_pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}%"


def format_multiple(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}x"


def format_price(value: float | None) -> str:
    if value is None:
        return "-"
    if value >= 100:
        return f"{value:.2f}"
    if value >= 1:
        return f"{value:.4f}"
    return f"{value:.8f}".rstrip("0").rstrip(".")


def format_money(value: float | None) -> str:
    if value is None:
        return "-"
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    return f"{value:.0f}"


def symbol_spec_from_exchange_info(item: dict[str, Any]) -> dict[str, Any]:
    filters = {flt.get("filterType"): flt for flt in item.get("filters", [])}
    lot_filter = filters.get("MARKET_LOT_SIZE") or filters.get("LOT_SIZE") or {}
    return {
        "stepSize": lot_filter.get("stepSize", "0.001"),
        "minQty": lot_filter.get("minQty", "0"),
    }


def api_order_price(order: dict[str, Any], fallback_price: float) -> float:
    for key in ("avgPrice", "price"):
        value = float(order.get(key) or 0)
        if value > 0:
            return value
    return fallback_price


def api_config_changed(old_config: MonitorConfig, new_config: MonitorConfig) -> bool:
    api_fields = {
        "api_trading_enabled",
        "api_trading_testnet",
        "api_trading_long_enabled",
        "api_trading_short_enabled",
        "api_max_notional_per_trade",
        "api_max_open_positions",
        "api_leverage",
    }
    return any(getattr(old_config, field) != getattr(new_config, field) for field in api_fields)


def admin_key_valid(admin_key: str) -> bool:
    if not ADMIN_ACTION_KEY:
        return True
    return hmac.compare_digest(admin_key, ADMIN_ACTION_KEY)


def safe_num(value: float | None) -> float:
    if value is None or math.isnan(value):
        return 0.0
    return value


def iso_now() -> str:
    return iso_at(time.time())


def iso_at(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).astimezone(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S")


def local_day() -> str:
    return datetime.now(UTC).astimezone(DISPLAY_TZ).strftime("%Y-%m-%d")


def repeat_signal_level(count: int) -> str:
    if count >= 3:
        return "多次触发"
    if count == 2:
        return "二次触发"
    if count == 1:
        return "首次触发"
    return "-"


def schedule_background_task(coro: Any) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        coro.close()
        return
    loop.create_task(coro)


monitor = BinanceMonitor()
app = FastAPI(title="Binance USDT Futures OI Spike Monitor")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
async def startup() -> None:
    await monitor.start()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/snapshot")
async def api_snapshot() -> dict[str, Any]:
    return await monitor.snapshot()


@app.get("/api/config")
async def api_get_config() -> dict[str, Any]:
    snapshot = await monitor.snapshot()
    return snapshot["config"]


@app.post("/api/config")
async def api_set_config(payload: dict[str, Any]) -> dict[str, Any]:
    admin_key = str(payload.pop("admin_key", ""))
    config = MonitorConfig(**payload)
    if api_config_changed(monitor.config, config) and not admin_key_valid(admin_key):
        raise HTTPException(status_code=403, detail="修改 API 真实交易配置需要输入正确操作密钥")
    await monitor.update_config(config)
    return await monitor.snapshot()


@app.post("/api/paper/reset")
async def api_reset_paper() -> dict[str, Any]:
    monitor.reset_paper()
    return await monitor.snapshot()


@app.get("/events")
async def events() -> StreamingResponse:
    async def stream():
        while True:
            payload = await monitor.snapshot()
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            await asyncio.sleep(3)

    return StreamingResponse(stream(), media_type="text/event-stream")

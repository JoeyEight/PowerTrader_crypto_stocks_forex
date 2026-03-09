from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Tuple

from app.api_endpoint_validation import normalize_endpoint_url
from app.backoff_policy import BackoffPolicy
from app.http_utils import retry_after_from_urllib_http_error


def _retry_after_seconds_from_http_error(exc: urllib.error.HTTPError) -> float:
    return retry_after_from_urllib_http_error(exc, max_wait_s=300.0)


class AlpacaBrokerClient:
    def __init__(self, api_key_id: str, secret_key: str, base_url: str, data_url: str = "https://data.alpaca.markets") -> None:
        self.api_key_id = str(api_key_id or "").strip()
        self.secret_key = str(secret_key or "").strip()
        norm_base, base_ok, _ = normalize_endpoint_url(base_url, default="https://paper-api.alpaca.markets")
        norm_data, data_ok, _ = normalize_endpoint_url(data_url, default="https://data.alpaca.markets")
        self.base_url = str((norm_base if base_ok else "https://paper-api.alpaca.markets") or "").strip().rstrip("/")
        self.data_url = str((norm_data if data_ok else "https://data.alpaca.markets") or "").strip().rstrip("/")

    def configured(self) -> bool:
        return bool(self.api_key_id and self.secret_key and self.base_url)

    def _headers(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
        }

    @staticmethod
    def _retryable_http(code: int) -> bool:
        return int(code or 0) in {408, 425, 429, 500, 502, 503, 504}

    @staticmethod
    def _feed_candidates(feed: str) -> List[str]:
        f = str(feed or "iex").strip().lower() or "iex"
        out: List[str] = []
        for candidate in (f, "iex"):
            name = str(candidate or "").strip().lower()
            if name and name not in out:
                out.append(name)
        return out or ["iex"]

    @staticmethod
    def _is_feed_permission_error(exc: urllib.error.HTTPError) -> bool:
        try:
            code = int(getattr(exc, "code", 0) or 0)
        except Exception:
            code = 0
        if code not in {400, 401, 403, 422}:
            return False
        parts = [str(exc or "")]
        try:
            reader = getattr(exc, "read", None)
            if callable(reader):
                raw = reader()
                if isinstance(raw, bytes):
                    parts.append(raw.decode("utf-8", errors="ignore")[:2000])
                elif isinstance(raw, str):
                    parts.append(raw[:2000])
        except Exception:
            pass
        txt = " ".join(parts).lower()
        needles = ("subscription", "entitlement", "forbidden", "not allowed", "unauthorized", "permission", "feed")
        return any(tok in txt for tok in needles)

    def _open_with_retry(self, req: urllib.request.Request, timeout: float, max_attempts: int = 3) -> str:
        attempts = max(1, int(max_attempts or 1))
        backoff = BackoffPolicy(base_delay_s=0.2, max_delay_s=8.0, jitter_s=0.25, max_retry_after_s=300.0)
        last_exc: Exception | None = None
        for att in range(1, attempts + 1):
            retry_after_s = 0.0
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    body = resp.read()
                    if isinstance(body, bytes):
                        return body.decode("utf-8")
                    return str(body or "")
            except urllib.error.HTTPError as exc:
                last_exc = exc
                retry_after_s = _retry_after_seconds_from_http_error(exc)
                if (not self._retryable_http(int(getattr(exc, "code", 0) or 0))) or att >= attempts:
                    raise
            except urllib.error.URLError as exc:
                last_exc = exc
                if att >= attempts:
                    raise
            except Exception as exc:
                last_exc = exc
                if att >= attempts:
                    raise
            wait_s = backoff.wait_seconds(att, retry_after_s=retry_after_s)
            time.sleep(wait_s)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Alpaca request failed")

    def _request_json(self, path: str, timeout: float = 8.0, max_attempts: int = 3) -> Any:
        req = urllib.request.Request(f"{self.base_url}{path}", headers=self._headers())
        raw = self._open_with_retry(req, timeout=timeout, max_attempts=max_attempts)
        if not raw:
            return {}
        return json.loads(raw)

    def _request(self, path: str, method: str = "GET", payload: Any = None, timeout: float = 8.0, max_attempts: int = 1) -> Any:
        body = None
        headers = self._headers()
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(f"{self.base_url}{path}", data=body, headers=headers, method=method.upper())
        raw = self._open_with_retry(req, timeout=timeout, max_attempts=max_attempts)
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def _request_data_json(self, path: str, timeout: float = 8.0, max_attempts: int = 3) -> Dict[str, Any]:
        req = urllib.request.Request(f"{self.data_url}{path}", headers=self._headers())
        raw = self._open_with_retry(req, timeout=timeout, max_attempts=max_attempts)
        payload = json.loads(raw or "{}")
        return payload if isinstance(payload, dict) else {}

    def _latest_quotes(self, symbols: List[str], feed: str = "iex") -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        norm = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
        if not norm:
            return out
        for feed_name in self._feed_candidates(feed):
            try:
                params = urllib.parse.urlencode({"symbols": ",".join(norm), "feed": feed_name})
                payload = self._request_data_json(f"/v2/stocks/quotes/latest?{params}", timeout=10.0)
                rows = payload.get("quotes", {}) or {}
                if isinstance(rows, dict):
                    for sym, row in rows.items():
                        key = str(sym or "").strip().upper()
                        if key and isinstance(row, dict):
                            out[key] = row
                if out:
                    return out
            except urllib.error.HTTPError as exc:
                if feed_name != "iex" and self._is_feed_permission_error(exc):
                    continue
            except Exception:
                continue
        return out

    def _latest_trades(self, symbols: List[str], feed: str = "iex") -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        norm = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
        if not norm:
            return out
        for feed_name in self._feed_candidates(feed):
            try:
                params = urllib.parse.urlencode({"symbols": ",".join(norm), "feed": feed_name})
                payload = self._request_data_json(f"/v2/stocks/trades/latest?{params}", timeout=10.0)
                rows = payload.get("trades", {}) or {}
                if isinstance(rows, dict):
                    for sym, row in rows.items():
                        key = str(sym or "").strip().upper()
                        if key and isinstance(row, dict):
                            out[key] = row
                if out:
                    return out
            except urllib.error.HTTPError as exc:
                if feed_name != "iex" and self._is_feed_permission_error(exc):
                    continue
            except Exception:
                continue
        return out

    def test_connection(self) -> Tuple[bool, str]:
        if not self.configured():
            return False, "Alpaca credentials missing"
        try:
            acct = self.get_account_summary()
            status = str(acct.get("status", "ok") or "ok")
            buying_power = acct.get("buying_power", "N/A")
            return True, f"Connected | status={status} | buying_power={buying_power}"
        except urllib.error.HTTPError as exc:
            return False, f"HTTP {exc.code}: {exc.reason}"
        except urllib.error.URLError as exc:
            return False, f"Network error: {exc.reason}"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    def get_account_summary(self) -> Dict[str, Any]:
        try:
            payload = self._request_json("/v2/account")
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def list_positions(self) -> List[Dict[str, Any]]:
        try:
            rows = self._request("/v2/positions", method="GET", max_attempts=3)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        except Exception:
            pass
        return []

    def list_tradable_assets(self) -> List[Dict[str, Any]]:
        try:
            rows = self._request(
                "/v2/assets?status=active&asset_class=us_equity",
                method="GET",
                timeout=15.0,
                max_attempts=3,
            )
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        except Exception:
            pass
        return []

    def get_snapshot_details(self, symbols: List[str], feed: str = "iex") -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        norm = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
        if not norm:
            return out
        latest_quotes: Dict[str, Dict[str, Any]] = {}
        latest_trades: Dict[str, Dict[str, Any]] = {}
        try:
            payload: Dict[str, Any] = {}
            for feed_name in self._feed_candidates(feed):
                try:
                    params = urllib.parse.urlencode({"symbols": ",".join(norm), "feed": feed_name})
                    payload = self._request_data_json(f"/v2/stocks/snapshots?{params}", timeout=12.0)
                    break
                except urllib.error.HTTPError as exc:
                    if feed_name != "iex" and self._is_feed_permission_error(exc):
                        continue
                    raise
            snaps = payload.get("snapshots", {}) or {}
            if not isinstance(snaps, dict):
                snaps = {}
            for sym in norm:
                row = snaps.get(sym, {}) or {}
                quote = row.get("latestQuote", {}) or {}
                trade = row.get("latestTrade", {}) or {}
                daily = row.get("dailyBar", {}) or {}
                try:
                    bid = float(quote.get("bp", 0.0) or 0.0)
                except Exception:
                    bid = 0.0
                try:
                    ask = float(quote.get("ap", 0.0) or 0.0)
                except Exception:
                    ask = 0.0
                try:
                    last = float(trade.get("p", 0.0) or 0.0)
                except Exception:
                    last = 0.0
                if (bid <= 0.0 and ask <= 0.0) or last <= 0.0:
                    if not latest_quotes:
                        latest_quotes = self._latest_quotes(norm, feed=feed)
                    if not latest_trades:
                        latest_trades = self._latest_trades(norm, feed=feed)
                    q2 = latest_quotes.get(sym, {}) or {}
                    t2 = latest_trades.get(sym, {}) or {}
                    if bid <= 0.0:
                        try:
                            bid = float(q2.get("bp", 0.0) or 0.0)
                        except Exception:
                            bid = bid
                    if ask <= 0.0:
                        try:
                            ask = float(q2.get("ap", 0.0) or 0.0)
                        except Exception:
                            ask = ask
                    if last <= 0.0:
                        try:
                            last = float(t2.get("p", 0.0) or 0.0)
                        except Exception:
                            last = last
                if bid > 0 and ask > 0:
                    mid = (bid + ask) * 0.5
                elif last > 0:
                    mid = last
                else:
                    mid = max(ask, bid, 0.0)
                spread_bps = 0.0
                if bid > 0 and ask > 0 and mid > 0:
                    spread_bps = ((ask - bid) / mid) * 10000.0
                try:
                    vol = float(daily.get("v", 0.0) or 0.0)
                except Exception:
                    vol = 0.0
                dollar_vol = vol * mid if mid > 0 else 0.0
                out[sym] = {
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "last": last,
                    "spread_bps": spread_bps,
                    "volume": vol,
                    "dollar_vol": dollar_vol,
                }
        except Exception:
            pass
        return out

    def get_stock_bars(
        self,
        symbol: str,
        timeframe: str = "1Hour",
        limit: int = 120,
        feed: str = "iex",
        start_iso: str = "",
        end_iso: str = "",
    ) -> List[Dict[str, Any]]:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return []
        tf = str(timeframe or "1Hour").strip()
        lim = max(10, min(1000, int(limit or 120)))
        s = str(start_iso or "").strip()
        e = str(end_iso or "").strip()
        for feed_name in self._feed_candidates(feed):
            try:
                params_dict: Dict[str, str] = {
                    "symbols": sym,
                    "timeframe": tf,
                    "limit": str(lim),
                    "adjustment": "raw",
                    "feed": feed_name,
                    # Prefer latest bars for scanning; we normalize to chronological below.
                    "sort": "desc",
                }
                if s:
                    params_dict["start"] = s
                if e:
                    params_dict["end"] = e
                params = urllib.parse.urlencode(params_dict)
                payload = self._request_data_json(f"/v2/stocks/bars?{params}", timeout=12.0)
                bars = ((payload.get("bars", {}) or {}).get(sym, []) or [])
                out = [row for row in bars if isinstance(row, dict)]
                out.sort(key=lambda row: str(row.get("t", "") or ""))
                if out:
                    return out
            except urllib.error.HTTPError as exc:
                if feed_name != "iex" and self._is_feed_permission_error(exc):
                    continue
                return []
            except Exception:
                continue
        return []

    def fetch_snapshot(self) -> Dict[str, Any]:
        if not self.configured():
            return {
                "state": "NOT CONFIGURED",
                "ai_state": "Credentials missing",
                "trader_state": "Idle",
                "msg": "Add Alpaca paper keys in Settings",
                "buying_power": "Pending account link",
                "open_positions": "0",
                "realized_pnl": "N/A",
                "positions_preview": [],
                "raw_positions": [],
                "equity": "N/A",
                "pdt_note": "Pattern Day Trader rules may restrict live same-day round trips under $25k.",
            }

        try:
            acct = self.get_account_summary()
            raw_positions = self.list_positions()

            preview: List[str] = []
            for pos in raw_positions[:8]:
                sym = str(pos.get("symbol", "") or "").strip()
                qty = str(pos.get("qty", "") or "").strip()
                market_val = str(pos.get("market_value", "") or "").strip()
                unreal = str(pos.get("unrealized_pl", "") or "").strip()
                preview.append(f"{sym} | qty {qty} | mv {market_val} | uPnL {unreal}")

            return {
                "state": "READY",
                "ai_state": "Broker linked",
                "trader_state": "Paper mode ready",
                "msg": f"Account status={str(acct.get('status', 'ok') or 'ok')}",
                "buying_power": str(acct.get("buying_power", "N/A") or "N/A"),
                "open_positions": str(len(raw_positions)),
                "realized_pnl": str(acct.get("equity", "N/A") or "N/A"),
                "positions_preview": preview,
                "raw_positions": raw_positions,
                "equity": str(acct.get("equity", "N/A") or "N/A"),
                "pdt_note": "Paper mode can still simulate PDT protections; keep live stock day-trading rules in mind.",
            }
        except urllib.error.HTTPError as exc:
            return {
                "state": "ERROR",
                "ai_state": "Broker error",
                "trader_state": "Idle",
                "msg": f"HTTP {exc.code}: {exc.reason}",
                "buying_power": "N/A",
                "open_positions": "0",
                "realized_pnl": "N/A",
                "positions_preview": [],
                "raw_positions": [],
                "equity": "N/A",
                "pdt_note": "Pattern Day Trader protections still apply in live stock trading.",
            }
        except urllib.error.URLError as exc:
            return {
                "state": "ERROR",
                "ai_state": "Network error",
                "trader_state": "Idle",
                "msg": f"Network error: {exc.reason}",
                "buying_power": "N/A",
                "open_positions": "0",
                "realized_pnl": "N/A",
                "positions_preview": [],
                "raw_positions": [],
                "equity": "N/A",
                "pdt_note": "Pattern Day Trader protections still apply in live stock trading.",
            }
        except Exception as exc:
            return {
                "state": "ERROR",
                "ai_state": "Load failed",
                "trader_state": "Idle",
                "msg": f"{type(exc).__name__}: {exc}",
                "buying_power": "N/A",
                "open_positions": "0",
                "realized_pnl": "N/A",
                "positions_preview": [],
                "raw_positions": [],
                "equity": "N/A",
                "pdt_note": "Pattern Day Trader protections still apply in live stock trading.",
            }

    def get_mid_prices(self, symbols: List[str], feed: str = "iex") -> Dict[str, float]:
        out: Dict[str, float] = {}
        norm = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
        if not norm:
            return out
        for feed_name in self._feed_candidates(feed):
            try:
                params = urllib.parse.urlencode({"symbols": ",".join(norm), "feed": feed_name})
                payload = self._request_data_json(f"/v2/stocks/snapshots?{params}", timeout=10.0)
                snaps = payload.get("snapshots", {}) or {}
                if not isinstance(snaps, dict):
                    snaps = {}
                for sym in norm:
                    row = snaps.get(sym, {}) or {}
                    trade = row.get("latestTrade", {}) or {}
                    quote = row.get("latestQuote", {}) or {}
                    px = 0.0
                    try:
                        px = float(trade.get("p", 0.0) or 0.0)
                    except Exception:
                        px = 0.0
                    if px <= 0:
                        try:
                            bid = float(quote.get("bp", 0.0) or 0.0)
                        except Exception:
                            bid = 0.0
                        try:
                            ask = float(quote.get("ap", 0.0) or 0.0)
                        except Exception:
                            ask = 0.0
                        if bid > 0 and ask > 0:
                            px = (bid + ask) * 0.5
                        elif ask > 0:
                            px = ask
                        elif bid > 0:
                            px = bid
                    if px > 0:
                        out[sym] = px
                if out:
                    return out
            except urllib.error.HTTPError as exc:
                if feed_name != "iex" and self._is_feed_permission_error(exc):
                    continue
                break
            except Exception:
                continue
        return out

    def place_market_order(
        self,
        symbol: str,
        side: str,
        notional: float,
        client_order_id: str = "",
        max_retries: int = 2,
        retry_delay_s: float = 0.35,
        max_retry_after_s: float = 300.0,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        sym = str(symbol or "").strip().upper()
        order_side = str(side or "buy").strip().lower()
        amount = float(notional or 0.0)
        if not sym or order_side not in {"buy", "sell"} or amount <= 0:
            return False, "Invalid order parameters", {}
        payload = {
            "symbol": sym,
            "side": order_side,
            "type": "market",
            "time_in_force": "day",
            "notional": round(amount, 2),
        }
        if str(client_order_id or "").strip():
            payload["client_order_id"] = str(client_order_id).strip()[:48]
        attempts = max(1, int(max_retries))
        backoff = BackoffPolicy(
            base_delay_s=max(0.05, float(retry_delay_s)),
            max_delay_s=8.0,
            jitter_s=0.35,
            max_retry_after_s=max(1.0, float(max_retry_after_s or 300.0)),
        )
        last_msg = "order failed"
        for att in range(1, attempts + 1):
            retry_after_s = 0.0
            try:
                out = self._request("/v2/orders", method="POST", payload=payload)
                if isinstance(out, dict):
                    oid = str(out.get("id", "") or "").strip()
                    return True, (f"order_id={oid}" if oid else "order accepted"), out
                return True, "order accepted", {}
            except urllib.error.HTTPError as exc:
                retry_after_s = _retry_after_seconds_from_http_error(exc)
                try:
                    body = exc.read().decode("utf-8", errors="ignore")
                except Exception:
                    body = ""
                last_msg = f"HTTP {exc.code}: {exc.reason} {body}".strip()
                # 4xx is usually terminal; do not retry except 429.
                if int(exc.code) != 429:
                    break
            except urllib.error.URLError as exc:
                last_msg = f"Network error: {exc.reason}"
            except Exception as exc:
                last_msg = f"{type(exc).__name__}: {exc}"
            if att < attempts:
                try:
                    import time as _t
                    wait_s = backoff.wait_seconds(att, retry_after_s=retry_after_s)
                    if retry_after_s > 0.0:
                        last_msg = f"{last_msg} | retry_after={wait_s:.2f}s".strip()
                    _t.sleep(wait_s)
                except Exception:
                    pass
        return False, last_msg, {}

    def close_position(self, symbol: str) -> Tuple[bool, str, Dict[str, Any]]:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return False, "Missing symbol", {}
        try:
            out = self._request(f"/v2/positions/{sym}", method="DELETE")
            if isinstance(out, dict):
                oid = str(out.get("id", "") or "").strip()
                return True, (f"close_order_id={oid}" if oid else "close accepted"), out
            return True, "close accepted", {}
        except urllib.error.HTTPError as exc:
            try:
                body = exc.read().decode("utf-8", errors="ignore")
            except Exception:
                body = ""
            return False, f"HTTP {exc.code}: {exc.reason} {body}".strip(), {}
        except urllib.error.URLError as exc:
            return False, f"Network error: {exc.reason}", {}
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}", {}

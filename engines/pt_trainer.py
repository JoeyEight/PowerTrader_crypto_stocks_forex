import requests
try:
	from kucoin.client import Market  # type: ignore
except Exception:
	Market = None  # type: ignore


class _KucoinMarketFallback:
	def __init__(self, url: str = "https://api.kucoin.com"):
		self.url = str(url or "https://api.kucoin.com").rstrip("/")
		self._session = requests.Session()

	def get_kline(self, symbol: str, kline_type: str, startAt=None, endAt=None):
		params = {"symbol": str(symbol or ""), "type": str(kline_type or "")}
		if startAt is not None:
			params["startAt"] = int(float(startAt))
		if endAt is not None:
			params["endAt"] = int(float(endAt))
		resp = self._session.get(f"{self.url}/api/v1/market/candles", params=params, timeout=12)
		resp.raise_for_status()
		payload = resp.json() if resp.content else {}
		if not isinstance(payload, dict):
			return []
		code = str(payload.get("code", "") or "").strip()
		if code and code != "200000":
			msg = str(payload.get("msg", "") or "").strip()
			raise RuntimeError(f"KuCoin error {code}: {msg}")
		data = payload.get("data", [])
		return data if isinstance(data, list) else []


if Market is not None:
	market = Market(url='https://api.kucoin.com')
else:
	market = _KucoinMarketFallback(url='https://api.kucoin.com')
import time
"""
<------------
newest oldest
------------>
oldest newest
"""
avg50 = []
import sys
import datetime
import traceback
import linecache
import base64
import calendar
import hashlib
import hmac
from datetime import datetime
sells_count = 0
prediction_prices_avg_list = []
pt_server = 'server'
try:
	import psutil  # type: ignore
except Exception:
	psutil = None  # type: ignore
import logging
list_len = 0
restarting = 'no'
in_trade = 'no'
updowncount = 0
updowncount1 = 0
updowncount1_2 = 0
updowncount1_3 = 0
updowncount1_4 = 0
high_var2 = 0.0
low_var2 = 0.0
last_flipped = 'no'
starting_amounth02 = 100.0
starting_amounth05 = 100.0
starting_amounth10 = 100.0
starting_amounth20 = 100.0
starting_amounth50 = 100.0
starting_amount = 100.0
starting_amount1 = 100.0
starting_amount1_2 = 100.0
starting_amount1_3 = 100.0
starting_amount1_4 = 100.0
starting_amount2 = 100.0
starting_amount2_2 = 100.0
starting_amount2_3 = 100.0
starting_amount2_4 = 100.0
starting_amount3 = 100.0
starting_amount3_2 = 100.0
starting_amount3_3 = 100.0
starting_amount3_4 = 100.0
starting_amount4 = 100.0
starting_amount4_2 = 100.0
starting_amount4_3 = 100.0
starting_amount4_4 = 100.0
profit_list = []
profit_list1 = []
profit_list1_2 = []
profit_list1_3 = []
profit_list1_4 = []
profit_list2 = []
profit_list2_2 = []
profit_list2_3 = []
profit_list2_4 = []
profit_list3 = []
profit_list3_2 = []
profit_list3_3 = []
profit_list4 = []
profit_list4_2 = []
good_hits = []
good_preds = []
good_preds2 = []
good_preds3 = []
good_preds4 = []
good_preds5 = []
good_preds6 = []
big_good_preds = []
big_good_preds2 = []
big_good_preds3 = []
big_good_preds4 = []
big_good_preds5 = []
big_good_preds6 = []
big_good_hits = []
upordown = []
upordown1 = []
upordown1_2 = []
upordown1_3 = []
upordown1_4 = []
upordown2 = []
upordown2_2 = []
upordown2_3 = []
upordown2_4 = []
upordown3 = []
upordown3_2 = []
upordown3_3 = []
upordown3_4 = []
upordown4 = []
upordown4_2 = []
upordown4_3 = []
upordown4_4 = []
upordown5 = []
import json
import uuid
import os
from app.path_utils import resolve_runtime_paths, resolve_settings_path, read_settings_file, log_once

# ---- speed knobs ----
VERBOSE = False  # set True if you want the old high-volume prints
def vprint(*args, **kwargs):
	if VERBOSE:
		print(*args, **kwargs)

# Cache memory/weights in RAM (avoid re-reading and re-writing every loop)
_memory_cache = {}  # tf_choice -> dict(memory_list, weight_list, high_weight_list, low_weight_list, dirty)
_last_threshold_written = {}  # tf_choice -> float
BASE_DIR, _GUI_SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "pt_trainer")
TRAINER_STATUS_PATH = os.path.join(BASE_DIR, "trainer_status.json")
TRAINER_STATUS_TMP_PATH = TRAINER_STATUS_PATH + ".tmp"
TRAINER_LAST_START_PATH = os.path.join(BASE_DIR, "trainer_last_start_time.txt")
TRAINER_LAST_TRAINING_PATH = os.path.join(BASE_DIR, "trainer_last_training_time.txt")

def _read_text(path):
	with open(path, "r", encoding="utf-8", errors="ignore") as f:
		return f.read()

def load_memory(tf_choice):
	"""Load memories/weights for a timeframe once and keep them in RAM."""
	if tf_choice in _memory_cache:
		return _memory_cache[tf_choice]
	data = {
		"memory_list": [],
		"weight_list": [],
		"high_weight_list": [],
		"low_weight_list": [],
		"dirty": False,
	}
	try:
		data["memory_list"] = _read_text(f"memories_{tf_choice}.txt").replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split('~')
	except:
		data["memory_list"] = []
	try:
		data["weight_list"] = _read_text(f"memory_weights_{tf_choice}.txt").replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split(' ')
	except:
		data["weight_list"] = []
	try:
		data["high_weight_list"] = _read_text(f"memory_weights_high_{tf_choice}.txt").replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split(' ')
	except:
		data["high_weight_list"] = []
	try:
		data["low_weight_list"] = _read_text(f"memory_weights_low_{tf_choice}.txt").replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split(' ')
	except:
		data["low_weight_list"] = []
	_memory_cache[tf_choice] = data
	return data

def flush_memory(tf_choice, force=False):
	"""Write memories/weights back to disk only when they changed (batch IO)."""
	data = _memory_cache.get(tf_choice)
	if not data:
		return
	if (not data.get("dirty")) and (not force):
		return
	try:
		with open(f"memories_{tf_choice}.txt", "w+", encoding="utf-8") as f:
			f.write("~".join([x for x in data["memory_list"] if str(x).strip() != ""]))
	except:
		pass
	try:
		with open(f"memory_weights_{tf_choice}.txt", "w+", encoding="utf-8") as f:
			f.write(" ".join([str(x) for x in data["weight_list"] if str(x).strip() != ""]))
	except:
		pass
	try:
		with open(f"memory_weights_high_{tf_choice}.txt", "w+", encoding="utf-8") as f:
			f.write(" ".join([str(x) for x in data["high_weight_list"] if str(x).strip() != ""]))
	except:
		pass
	try:
		with open(f"memory_weights_low_{tf_choice}.txt", "w+", encoding="utf-8") as f:
			f.write(" ".join([str(x) for x in data["low_weight_list"] if str(x).strip() != ""]))
	except:
		pass
	data["dirty"] = False

def write_threshold_sometimes(tf_choice, perfect_threshold, loop_i, every=200):
	"""Avoid writing neural_perfect_threshold_* every single loop."""
	last = _last_threshold_written.get(tf_choice)
	# write occasionally, or if it changed meaningfully
	if (loop_i % every != 0) and (last is not None) and (abs(perfect_threshold - last) < 0.05):
		return
	try:
		with open(f"neural_perfect_threshold_{tf_choice}.txt", "w+", encoding="utf-8") as f:
			f.write(str(perfect_threshold))
		_last_threshold_written[tf_choice] = perfect_threshold
	except:
		pass

def should_stop_training(loop_i, every=50):
	"""Check killer.txt less often (still responsive, way less IO)."""
	if loop_i % every != 0:
		return False
	try:
		with open("killer.txt", "r", encoding="utf-8", errors="ignore") as f:
			return f.read().strip().lower() == "yes"
	except:
		return False

def PrintException():
	exc_type, exc_obj, tb = sys.exc_info()

	# IMPORTANT: don't swallow clean exits (sys.exit()) or Ctrl+C
	if isinstance(exc_obj, (SystemExit, KeyboardInterrupt)):
		raise

	# Safety: sometimes tb can be None
	if tb is None:
		print(f"EXCEPTION: {exc_obj}")
		return

	f = tb.tb_frame
	lineno = tb.tb_lineno
	filename = f.f_code.co_filename
	linecache.checkcache(filename)
	line = linecache.getline(filename, lineno, f.f_globals)
	print('EXCEPTION IN (LINE {} "{}"): {}'.format(lineno, line.strip(), exc_obj))

def _load_history_cache_settings():
	cache_enabled = True
	retention_days = 365
	try:
		settings_path = resolve_settings_path(BASE_DIR) or _GUI_SETTINGS_PATH or os.path.join(BASE_DIR, "gui_settings.json")
		if os.path.isfile(settings_path):
			data = read_settings_file(settings_path, module_name="pt_trainer")
			cache_enabled = bool(data.get("history_cache_enabled", True))
			try:
				retention_days = int(float(data.get("history_retention_days", 365) or 365))
			except Exception:
				retention_days = 365
	except Exception:
		cache_enabled = True
		retention_days = 365
	if retention_days <= 0:
		retention_days = 365
	cache_root = os.path.join(HUB_DATA_DIR, "history_cache")
	return cache_root, cache_enabled, retention_days

def _normalize_history_row(row):
	try:
		if isinstance(row, (list, tuple)):
			out = list(row)
		else:
			return None
		if len(out) < 5:
			return None
		ts = int(float(out[0]))
		if ts < 1000000000000:
			ts *= 1000
		out[0] = ts
		return out
	except Exception:
		return None

def _merge_dedupe_by_ts(old_rows, new_rows):
	by_ts = {}
	for src in (old_rows or []):
		row = _normalize_history_row(src)
		if row is not None:
			by_ts[int(row[0])] = row
	for src in (new_rows or []):
		row = _normalize_history_row(src)
		if row is not None:
			by_ts[int(row[0])] = row
	rows = [by_ts[k] for k in sorted(by_ts.keys())]
	return rows

def _purge_old(rows, min_ts_ms):
	out = []
	for src in (rows or []):
		row = _normalize_history_row(src)
		if row is None:
			continue
		if int(row[0]) >= int(min_ts_ms):
			out.append(row)
	return out

def _history_rows_sorted_unique(rows):
	last_ts = None
	for src in (rows or []):
		row = _normalize_history_row(src)
		if row is None:
			return False
		cur_ts = int(row[0])
		if last_ts is not None and cur_ts <= last_ts:
			return False
		last_ts = cur_ts
	return True

def _history_cache_file(coin_symbol, timeframe):
	cache_root, _, _ = _load_history_cache_settings()
	base_coin = str(coin_symbol).split("-")[0].strip().upper()
	tf = str(timeframe).replace("/", "_").replace(" ", "_")
	return os.path.join(cache_root, base_coin, f"{tf}.json")

def _history_cache_file_legacy(coin_symbol, timeframe):
	cache_root, _, _ = _load_history_cache_settings()
	base_coin = str(coin_symbol).split("-")[0].strip().upper()
	tf = str(timeframe).replace("/", "_").replace(" ", "_")
	return os.path.join(cache_root, f"{base_coin}_{tf}.json")

def _load_cached_history(coin_symbol, timeframe):
	paths = [_history_cache_file(coin_symbol, timeframe), _history_cache_file_legacy(coin_symbol, timeframe)]
	for path in paths:
		try:
			if not os.path.isfile(path):
				continue
			with open(path, "r", encoding="utf-8", errors="ignore") as f:
				data = json.load(f)
			if not isinstance(data, list):
				return []
			return _merge_dedupe_by_ts([], data)
		except (FileNotFoundError, json.JSONDecodeError):
			return []
		except Exception:
			return []
	return []

def _save_cached_history(coin_symbol, timeframe, rows):
	path = _history_cache_file(coin_symbol, timeframe)
	cache_dir = os.path.dirname(path)
	os.makedirs(cache_dir, exist_ok=True)
	tmp_path = path + ".tmp"
	with open(tmp_path, "w", encoding="utf-8") as f:
		json.dump(rows, f)
	os.replace(tmp_path, path)

def _fetch_kucoin_candles_range(coin_symbol, timeframe, timeframe_minutes, start_ts_ms, end_ts_ms, progress_cb=None, progress_min=1, progress_max=30):
	interval_s = max(60, int(float(timeframe_minutes) * 60))
	start_s = int(int(start_ts_ms) / 1000)
	end_s = int(int(end_ts_ms) / 1000)
	if end_s < start_s:
		return []
	out = []
	cursor_end = end_s
	backoff_s = 1.5
	total_expected = max(1, int(((end_s - start_s) / float(interval_s))) + 1)
	while True:
		cursor_start = max(start_s, int(cursor_end - (1500 * interval_s)))
		try:
			history = market.get_kline(coin_symbol, timeframe, startAt=cursor_start, endAt=cursor_end)
			backoff_s = 1.5
		except Exception:
			PrintException()
			time.sleep(backoff_s)
			backoff_s = min(backoff_s * 2.0, 8.0)
			if backoff_s >= 8.0:
				break
			continue
		normalized = []
		if isinstance(history, list):
			for row in history:
				norm = _normalize_history_row(row)
				if norm is not None:
					normalized.append(norm)
		if normalized:
			out = _merge_dedupe_by_ts(out, normalized)
			if progress_cb:
				try:
					loaded_ratio = min(1.0, float(len(out)) / float(total_expected))
					pct_now = int(round(float(progress_min) + ((float(progress_max) - float(progress_min)) * loaded_ratio)))
					progress_cb(pct_now, "history", f"{timeframe} {len(out)}/{total_expected}")
				except Exception:
					pass
		if cursor_start <= start_s:
			break
		if not normalized:
			break
		cursor_end = cursor_start - interval_s
		time.sleep(0.15)
	return out

def _legacy_download_history(coin_symbol, timeframe, timeframe_minutes, last_start_time=0.0):
	history_list = []
	list_len_local = 0
	start_time_local = int(time.time())
	end_time_local = int(start_time_local - ((1500 * timeframe_minutes) * 60))
	while True:
		time.sleep(.5)
		try:
			history = market.get_kline(coin_symbol, timeframe, startAt=end_time_local, endAt=start_time_local)
		except Exception:
			PrintException()
			time.sleep(3.5)
			continue
		current_change = 0
		if isinstance(history, list):
			for row in history:
				norm = _normalize_history_row(row)
				if norm is not None:
					history_list.append(norm)
			current_change = len(history_list) - list_len_local
		print('gathering history')
		try:
			print('\n\n\n\n')
			print(current_change)
			if current_change < 1000:
				break
		except Exception:
			PrintException()
		list_len_local = len(history_list)
		start_time_local = end_time_local
		end_time_local = int(start_time_local - ((1500 * timeframe_minutes) * 60))
		print(last_start_time)
		print(start_time_local)
		print(end_time_local)
		print('\n')
		if start_time_local <= last_start_time:
			break
	return history_list

def _load_training_history_with_cache(coin_symbol, timeframe, timeframe_minutes, last_start_time=0.0):
	cache_root, cache_enabled, retention_days = _load_history_cache_settings()
	now_ms = int(time.time() * 1000)
	min_ts_ms = int(now_ms - (int(retention_days) * 86400 * 1000))
	interval_ms = max(60000, int(float(timeframe_minutes) * 60 * 1000))
	loaded_rows = []
	loaded_count = 0
	purged_count = 0
	fetched_rows = []
	if not cache_enabled:
		_write_trainer_status(state="TRAINING", pct=1, phase="history", detail=f"{timeframe} full download", cache_ok=False)
		rows = _legacy_download_history(coin_symbol, timeframe, timeframe_minutes, last_start_time)
		rows = _merge_dedupe_by_ts([], rows)
		_write_trainer_status(state="TRAINING", pct=30, phase="history", detail=f"{timeframe} ready", cache_ok=False)
		print(f"CACHE {coin_symbol.split('-')[0]} {timeframe} loaded=0 fetched={len(rows)} purged=0 final={len(rows)}")
		return rows
	try:
		loaded_rows = _load_cached_history(coin_symbol, timeframe)
		loaded_count = len(loaded_rows)
		filtered_rows = _purge_old(loaded_rows, min_ts_ms)
		purged_count = max(0, loaded_count - len(filtered_rows))
		loaded_rows = filtered_rows
		if loaded_rows:
			cache_min_ts = int(loaded_rows[0][0])
			cache_max_ts = int(loaded_rows[-1][0])
			if cache_min_ts > (min_ts_ms + interval_ms):
				head_end = max(min_ts_ms, cache_min_ts - interval_ms)
				head_rows = _fetch_kucoin_candles_range(
					coin_symbol, timeframe, timeframe_minutes, min_ts_ms, head_end,
					progress_cb=_write_trainer_status, progress_min=1, progress_max=20
				)
				fetched_rows = _merge_dedupe_by_ts(fetched_rows, head_rows)
			if cache_max_ts < (now_ms - interval_ms):
				tail_start = cache_max_ts + interval_ms
				tail_rows = _fetch_kucoin_candles_range(
					coin_symbol, timeframe, timeframe_minutes, tail_start, now_ms,
					progress_cb=_write_trainer_status, progress_min=20, progress_max=30
				)
				fetched_rows = _merge_dedupe_by_ts(fetched_rows, tail_rows)
		else:
			fetched_rows = _fetch_kucoin_candles_range(
				coin_symbol, timeframe, timeframe_minutes, min_ts_ms, now_ms,
				progress_cb=_write_trainer_status, progress_min=1, progress_max=30
			)
		final_rows = _merge_dedupe_by_ts(loaded_rows, fetched_rows)
		final_rows = _purge_old(final_rows, min_ts_ms)
		if not _history_rows_sorted_unique(final_rows):
			final_rows = _merge_dedupe_by_ts([], final_rows)
		_save_cached_history(coin_symbol, timeframe, final_rows)
		_write_trainer_status(state="TRAINING", pct=30, phase="history", detail=f"{timeframe} ready", cache_ok=True)
		print(
			f"CACHE {coin_symbol.split('-')[0]} {timeframe} loaded={loaded_count} "
			f"fetched={len(fetched_rows)} purged={purged_count} final={len(final_rows)}"
		)
		return final_rows
	except Exception as exc:
		PrintException()
		print(f"CACHE_FALLBACK coin={coin_symbol.split('-')[0]} tf={timeframe} reason={type(exc).__name__}: {exc}")
		_write_trainer_status(
			state="TRAINING",
			pct=1,
			phase="history",
			detail=f"{timeframe} fallback {type(exc).__name__}",
			cache_ok=False,
		)
		rows = _legacy_download_history(coin_symbol, timeframe, timeframe_minutes, last_start_time)
		rows = _merge_dedupe_by_ts([], rows)
		_write_trainer_status(state="TRAINING", pct=30, phase="history", detail=f"{timeframe} ready", cache_ok=False)
		print(f"CACHE {coin_symbol.split('-')[0]} {timeframe} loaded=0 fetched={len(rows)} purged=0 final={len(rows)}")
		return rows

how_far_to_look_back = 100000
number_of_candles = [2]
number_of_candles_index = 0
def restart_program():
	"""Restarts the current program, with file objects and descriptors cleanup"""

	try:
		if psutil is not None:
			p = psutil.Process(os.getpid())
			for handler in p.open_files() + p.connections():
				os.close(handler.fd)
	except Exception as e:
		logging.error(e)
	python = sys.executable
	os.execl(python, python, * sys.argv)
try:
	if restarted_yet > 2:
		restarted_yet = 0
	else:	
		pass
except:
	restarted_yet = 0
tf_choices = ['1hour', '2hour', '4hour', '8hour', '12hour', '1day', '1week']
tf_minutes = [60, 120, 240, 480, 720, 1440, 10080]
# --- GUI HUB INPUT (NO PROMPTS) ---
# Usage: python pt_trainer.py BTC [reprocess_yes|reprocess_no]
_arg_coin = "BTC"

try:
	if len(sys.argv) > 1 and str(sys.argv[1]).strip():
		_arg_coin = str(sys.argv[1]).strip().upper()
except Exception:
	_arg_coin = "BTC"

coin_choice = _arg_coin + '-USDT'

restart_processing = "yes"

# GUI reads this status file to know if this coin is TRAINING or FINISHED
_trainer_started_at = int(time.time())
_trainer_last_status_write = {"ts": 0.0, "state": None, "pct": None, "phase": None, "detail": None, "cache_ok": None}

def _write_trainer_status(state="TRAINING", pct=None, phase="", detail="", finished_at=None, force=False, cache_ok=None):
	try:
		now_ts = int(time.time())
		pct_out = None
		if pct is not None:
			try:
				pct_out = max(0, min(100, int(float(pct))))
			except Exception:
				pct_out = None
		phase_out = str(phase or "").strip()
		detail_out = str(detail or "").strip()
		last = _trainer_last_status_write
		unchanged = (
			(last.get("state") == state) and
			(last.get("pct") == pct_out) and
			(last.get("phase") == phase_out) and
			(last.get("detail") == detail_out) and
			(last.get("cache_ok") == cache_ok)
		)
		if (not force) and unchanged and ((time.time() - float(last.get("ts", 0.0) or 0.0)) < 2.0):
			return
		payload = {
			"coin": _arg_coin,
			"state": str(state or "TRAINING"),
			"started_at": _trainer_started_at,
			"timestamp": now_ts,
		}
		if pct_out is not None:
			payload["pct"] = pct_out
		if phase_out:
			payload["phase"] = phase_out
		if detail_out:
			payload["detail"] = detail_out
		if cache_ok is not None:
			payload["cache_ok"] = bool(cache_ok)
		if finished_at is not None:
			payload["finished_at"] = int(finished_at)
		with open(TRAINER_STATUS_TMP_PATH, "w", encoding="utf-8") as f:
			json.dump(payload, f)
		os.replace(TRAINER_STATUS_TMP_PATH, TRAINER_STATUS_PATH)
		_trainer_last_status_write["ts"] = time.time()
		_trainer_last_status_write["state"] = state
		_trainer_last_status_write["pct"] = pct_out
		_trainer_last_status_write["phase"] = phase_out
		_trainer_last_status_write["detail"] = detail_out
		_trainer_last_status_write["cache_ok"] = cache_ok
	except (PermissionError, OSError, TypeError, ValueError) as exc:
		log_once(
			f"pt_trainer:_write_trainer_status:{type(exc).__name__}",
			f"[pt_trainer._write_trainer_status] path={TRAINER_STATUS_PATH} {type(exc).__name__}: {exc}",
		)

_write_trainer_status(state="TRAINING", pct=0, phase="starting", force=True, cache_ok=True)


the_big_index = 0
while True:
	list_len = 0
	restarting = 'no'
	in_trade = 'no'
	updowncount = 0
	updowncount1 = 0
	updowncount1_2 = 0
	updowncount1_3 = 0
	updowncount1_4 = 0
	high_var2 = 0.0
	low_var2 = 0.0
	last_flipped = 'no'
	starting_amounth02 = 100.0
	starting_amounth05 = 100.0
	starting_amounth10 = 100.0
	starting_amounth20 = 100.0
	starting_amounth50 = 100.0
	starting_amount = 100.0
	starting_amount1 = 100.0
	starting_amount1_2 = 100.0
	starting_amount1_3 = 100.0
	starting_amount1_4 = 100.0
	starting_amount2 = 100.0
	starting_amount2_2 = 100.0
	starting_amount2_3 = 100.0
	starting_amount2_4 = 100.0
	starting_amount3 = 100.0
	starting_amount3_2 = 100.0
	starting_amount3_3 = 100.0
	starting_amount3_4 = 100.0
	starting_amount4 = 100.0
	starting_amount4_2 = 100.0
	starting_amount4_3 = 100.0
	starting_amount4_4 = 100.0
	profit_list = []
	profit_list1 = []
	profit_list1_2 = []
	profit_list1_3 = []
	profit_list1_4 = []
	profit_list2 = []
	profit_list2_2 = []
	profit_list2_3 = []
	profit_list2_4 = []
	profit_list3 = []
	profit_list3_2 = []
	profit_list3_3 = []
	profit_list4 = []
	profit_list4_2 = []
	good_hits = []
	good_preds = []
	good_preds2 = []
	good_preds3 = []
	good_preds4 = []
	good_preds5 = []
	good_preds6 = []
	big_good_preds = []
	big_good_preds2 = []
	big_good_preds3 = []
	big_good_preds4 = []
	big_good_preds5 = []
	big_good_preds6 = []
	big_good_hits = []
	upordown = []
	upordown1 = []
	upordown1_2 = []
	upordown1_3 = []
	upordown1_4 = []
	upordown2 = []
	upordown2_2 = []
	upordown2_3 = []
	upordown2_4 = []
	upordown3 = []
	upordown3_2 = []
	upordown3_3 = []
	upordown3_4 = []
	upordown4 = []
	upordown4_2 = []
	upordown4_3 = []
	upordown4_4 = []
	upordown5 = []
	tf_choice = tf_choices[the_big_index]
	_mem = load_memory(tf_choice)
	memory_list = _mem["memory_list"]
	weight_list = _mem["weight_list"]
	high_weight_list = _mem["high_weight_list"]
	low_weight_list = _mem["low_weight_list"]
	no_list = 'no' if len(memory_list) > 0 else 'yes'

	tf_list = ['1hour',tf_choice,tf_choice]
	choice_index = tf_choices.index(tf_choice)
	minutes_list = [60,tf_minutes[choice_index],tf_minutes[choice_index]]
	if restarted_yet < 2:
		timeframe = tf_list[restarted_yet]#droplet setting (create list for all timeframes)
		timeframe_minutes = minutes_list[restarted_yet]#droplet setting (create list for all timeframe_minutes)
	else:
		timeframe = tf_list[2]#droplet setting (create list for all timeframes)
		timeframe_minutes = minutes_list[2]#droplet setting (create list for all timeframe_minutes)
	start_time = int(time.time())
	restarting = 'no'
	success_rate = 85
	volume_success_rate = 60
	candles_to_predict = 1#droplet setting (Max is half of number_of_candles)(Min is 2)
	max_difference = .5
	preferred_difference = .4 #droplet setting (max profit_margin) (Min 0.01)
	min_good_matches = 1#droplet setting (Max 100) (Min 4)
	max_good_matches = 1#droplet setting (Max 100) (Min is min_good_matches)
	prediction_expander = 1.33
	prediction_expander2 = 1.5
	prediction_adjuster = 0.0
	diff_avg_setting = 0.01
	min_success_rate = 90
	histories = 'off'
	coin_choice_index = 0
	list_of_ys_count = 0
	last_difference_between = 0.0
	history_list = []
	history_list2 = []
	len_avg = []
	list_len = 0
	start_time = int(time.time())
	start_time_yes = start_time
	if 'n' in restart_processing.lower():
		try:
			file = open(TRAINER_LAST_START_PATH,'r')
			last_start_time = int(file.read())
			file.close()
		except:
			last_start_time = 0.0
	else:
		last_start_time = 0.0
	try:
		history_rows = _load_training_history_with_cache(coin_choice, timeframe, timeframe_minutes, last_start_time)
	except Exception:
		PrintException()
		history_rows = _legacy_download_history(coin_choice, timeframe, timeframe_minutes, last_start_time)
	history_list = list(reversed(history_rows))
	_write_trainer_status(state="TRAINING", pct=35, phase="prep", detail=f"{timeframe} parse")
	if timeframe == '1day' or timeframe == '1week':
		if restarted_yet == 0:
			index = int(len(history_list)/2)
		else:
			index = 1
	else:
		index = int(len(history_list)/2)
	price_list = []
	high_price_list = []
	low_price_list = []
	open_price_list = []
	volume_list = []
	minutes_passed = 0
	try:
		while True:
			working_minute = str(history_list[index]).replace('"','').replace("'","").split(", ")
			try:
				if index == 1:
					current_tf_time = float(working_minute[0].replace('[',''))
					last_tf_time = current_tf_time
				else:
					pass
				candle_time = float(working_minute[0].replace('[',''))
				openPrice = float(working_minute[1])                
				closePrice = float(working_minute[2])
				highPrice = float(working_minute[3])
				lowPrice = float(working_minute[4])
				open_price_list.append(openPrice)
				price_list.append(closePrice)
				high_price_list.append(highPrice)
				low_price_list.append(lowPrice)
				index += 1
				if index >= len(history_list):
					break
				else:
					continue
			except:
				PrintException()
				index += 1
				if index >= len(history_list):
					break
				else:
					continue
		open_price_list.reverse()
		price_list.reverse()
		high_price_list.reverse()
		low_price_list.reverse()
		ticker_data = str(market.get_ticker(coin_choice)).replace('"','').replace("'","").replace("[","").replace("{","").replace("]","").replace("}","").replace(",","").lower().split(' ')
		price = float(ticker_data[ticker_data.index('price:')+1])
	except:
		PrintException()
	history_list = []
	history_list2 = []
	perfect_threshold = 1.0
	loop_i = 0  # counts inner training iterations (used to throttle disk IO)
	_write_trainer_status(state="TRAINING", pct=50, phase="fit", detail=f"{timeframe} start")
	if restarted_yet < 2:
		price_list_length = 10
	else:
		price_list_length = int(len(price_list)*0.5)
	while True:
		while True:
			loop_i += 1
			if loop_i == 25:
				_write_trainer_status(state="TRAINING", pct=70, phase="fit", detail=f"{timeframe} mid")
			matched_patterns_count = 0
			list_of_ys = []
			list_of_ys_count = 0
			next_coin = 'no'
			all_current_patterns = []
			memory_or_history = []
			memory_weights = []

			high_memory_weights = []
			low_memory_weights = []
			final_moves = 0.0
			high_final_moves = 0.0
			low_final_moves = 0.0
			memory_indexes = []
			matches_yep = []
			flipped = 'no'
			last_minute = int(time.time()/60)
			overunder = 'nothing'
			overunder2 = 'nothing'
			list_of_ys = []
			all_predictions = []
			all_preds = []
			high_all_predictions = []
			high_all_preds = []
			low_all_predictions = []
			low_all_preds = []
			try:
				open_price_list2 = []
				open_price_list_index = 0
				while True:
					open_price_list2.append(open_price_list[open_price_list_index])
					open_price_list_index += 1
					if open_price_list_index >= price_list_length:
						break
					else:
						continue
			except:
				break
			low_all_preds = []
			try:
				price_list2 = []
				price_list_index = 0
				while True:
					price_list2.append(price_list[price_list_index])
					price_list_index += 1
					if price_list_index >= price_list_length:
						break
					else:
						continue
			except:
				break
			high_price_list2 = []
			high_price_list_index = 0
			while True:
				high_price_list2.append(high_price_list[high_price_list_index])
				high_price_list_index += 1
				if high_price_list_index >= price_list_length:
					break
				else:
					continue
			low_price_list2 = []
			low_price_list_index = 0
			while True:
				low_price_list2.append(low_price_list[low_price_list_index])
				low_price_list_index += 1
				if low_price_list_index >= price_list_length:
					break
				else:
					continue
			index = 0
			index2 = index+1
			price_change_list = []
			while True:
				price_change = 100*((price_list2[index]-open_price_list2[index])/open_price_list2[index])
				price_change_list.append(price_change)
				index += 1
				if index >= len(price_list2):
					break
				else:
					continue
			index = 0
			index2 = index+1
			high_price_change_list = []
			while True:
				high_price_change = 100*((high_price_list2[index]-open_price_list2[index])/open_price_list2[index])
				high_price_change_list.append(high_price_change)
				index += 1
				if index >= len(price_list2):
					break
				else:
					continue
			index = 0
			index2 = index+1
			low_price_change_list = []
			while True:
				low_price_change = 100*((low_price_list2[index]-open_price_list2[index])/open_price_list2[index])
				low_price_change_list.append(low_price_change)
				index += 1
				if index >= len(price_list2):
					break
				else:
					continue
			# Check stop signal occasionally (much less disk IO)
			if should_stop_training(loop_i):
				exited = 'yes'
				print('finished processing')
				file = open(TRAINER_LAST_START_PATH,'w+')
				file.write(str(start_time_yes))
				file.close()

				# Mark training finished for the GUI
				try:
					_trainer_finished_at = int(time.time())
					file = open(TRAINER_LAST_TRAINING_PATH,'w+')
					file.write(str(_trainer_finished_at))
					file.close()
				except:
					pass
				_write_trainer_status(state="TRAINING", pct=90, phase="saving", detail=f"{timeframe} final", force=True)
				_write_trainer_status(state="FINISHED", pct=100, phase="finished", finished_at=_trainer_finished_at, force=True)

				# Flush any cached memory/weights before we spin
				flush_memory(tf_choice, force=True)

				sys.exit(0)

				the_big_index += 1
				restarted_yet = 0
				avg50 = []
				import sys
				import datetime
				import traceback
				import linecache
				import base64
				import calendar
				import hashlib
				import hmac
				from datetime import datetime
				sells_count = 0
				prediction_prices_avg_list = []
				pt_server = 'server'
				import psutil
				import logging
				list_len = 0
				restarting = 'no'
				in_trade = 'no'
				updowncount = 0
				updowncount1 = 0
				updowncount1_2 = 0
				updowncount1_3 = 0
				updowncount1_4 = 0
				high_var2 = 0.0
				low_var2 = 0.0
				last_flipped = 'no'
				starting_amounth02 = 100.0
				starting_amounth05 = 100.0
				starting_amounth10 = 100.0
				starting_amounth20 = 100.0
				starting_amounth50 = 100.0
				starting_amount = 100.0
				starting_amount1 = 100.0
				starting_amount1_2 = 100.0
				starting_amount1_3 = 100.0
				starting_amount1_4 = 100.0
				starting_amount2 = 100.0
				starting_amount2_2 = 100.0
				starting_amount2_3 = 100.0
				starting_amount2_4 = 100.0
				starting_amount3 = 100.0
				starting_amount3_2 = 100.0
				starting_amount3_3 = 100.0
				starting_amount3_4 = 100.0
				starting_amount4 = 100.0
				starting_amount4_2 = 100.0
				starting_amount4_3 = 100.0
				starting_amount4_4 = 100.0
				profit_list = []
				profit_list1 = []
				profit_list1_2 = []
				profit_list1_3 = []
				profit_list1_4 = []
				profit_list2 = []
				profit_list2_2 = []
				profit_list2_3 = []
				profit_list2_4 = []
				profit_list3 = []
				profit_list3_2 = []
				profit_list3_3 = []
				profit_list4 = []
				profit_list4_2 = []
				good_hits = []
				good_preds = []
				good_preds2 = []
				good_preds3 = []
				good_preds4 = []
				good_preds5 = []
				good_preds6 = []
				big_good_preds = []
				big_good_preds2 = []
				big_good_preds3 = []
				big_good_preds4 = []
				big_good_preds5 = []
				big_good_preds6 = []
				big_good_hits = []
				upordown = []
				upordown1 = []
				upordown1_2 = []
				upordown1_3 = []
				upordown1_4 = []
				upordown2 = []
				upordown2_2 = []
				upordown2_3 = []
				upordown2_4 = []
				upordown3 = []
				upordown3_2 = []
				upordown3_3 = []
				upordown3_4 = []
				upordown4 = []
				upordown4_2 = []
				upordown4_3 = []
				upordown4_4 = []
				upordown5 = []
				import json
				import uuid
				how_far_to_look_back = 100000
				list_len = 0
				if the_big_index >= len(tf_choices):
					if len(number_of_candles) == 1:
						print("Finished processing all timeframes (number_of_candles has only one entry). Exiting.")
						try:
							file = open(TRAINER_LAST_START_PATH,'w+')
							file.write(str(start_time_yes))
							file.close()
						except:
							pass

						# Mark training finished for the GUI
						try:
							_trainer_finished_at = int(time.time())
							file = open(TRAINER_LAST_TRAINING_PATH,'w+')
							file.write(str(_trainer_finished_at))
							file.close()
						except:
							pass
						_write_trainer_status(state="TRAINING", pct=90, phase="saving", detail=f"{timeframe} final", force=True)
						_write_trainer_status(state="FINISHED", pct=100, phase="finished", finished_at=_trainer_finished_at, force=True)

						sys.exit(0)
					else:
						the_big_index = 0
				else:
					pass

				break
			else:
				exited = 'no'
			perfect = []
			while True:
				try:
					print('\n\n\n\n')
					print(choice_index)
					print(restarted_yet)
					print(tf_list[restarted_yet])
					try:
						current_pattern_length = number_of_candles[number_of_candles_index]
						index = (len(price_change_list))-(number_of_candles[number_of_candles_index]-1)
						current_pattern = []
						history_pattern_start_index = (len(price_change_list))-((number_of_candles[number_of_candles_index]+candles_to_predict)*2)
						history_pattern_index = history_pattern_start_index
						while True:
							current_pattern.append(price_change_list[index])
							index += 1
							if len(current_pattern) >= (number_of_candles[number_of_candles_index]-1):
								break
							else:
								continue
					except:
						PrintException()
					try:
						high_current_pattern_length = number_of_candles[number_of_candles_index]
						index = (len(high_price_change_list))-(number_of_candles[number_of_candles_index]-1)
						high_current_pattern = []
						while True:
							high_current_pattern.append(high_price_change_list[index])
							index += 1
							if len(high_current_pattern) >= (number_of_candles[number_of_candles_index]-1):
								break
							else:
								continue
					except:
						PrintException()
					try:
						low_current_pattern_length = number_of_candles[number_of_candles_index]
						index = (len(low_price_change_list))-(number_of_candles[number_of_candles_index]-1)
						low_current_pattern = []
						while True:
							low_current_pattern.append(low_price_change_list[index])
							index += 1
							if len(low_current_pattern) >= (number_of_candles[number_of_candles_index]-1):
								break
							else:
								continue
					except:
						PrintException()
					history_diff = 1000000.0
					memory_diff = 1000000.0
					history_diffs = []
					memory_diffs = []
					if 1 == 1:
						try:
							file = open('memories_'+tf_choice+'.txt','r')
							memory_list = file.read().replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split('~')
							file.close()
							file = open('memory_weights_'+tf_choice+'.txt','r')
							weight_list = file.read().replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split(' ')
							file.close()							
							file = open('memory_weights_high_'+tf_choice+'.txt','r')
							high_weight_list = file.read().replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split(' ')
							file.close()
							file = open('memory_weights_low_'+tf_choice+'.txt','r')
							low_weight_list = file.read().replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split(' ')
							file.close()
							mem_ind = 0
							diffs_list = []
							any_perfect = 'no'
							perfect_dexs = []
							perfect_diffs = []
							moves = []
							move_weights = []
							high_move_weights = []
							low_move_weights = []
							unweighted = []
							high_unweighted = []
							low_unweighted = []
							high_moves = []
							low_moves = []
							while True:
								memory_pattern = memory_list[mem_ind].split('{}')[0].replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').split(' ')
								avgs = []
								checks = []
								check_dex = 0
								while True:
									current_candle = float(current_pattern[check_dex])
									memory_candle = float(memory_pattern[check_dex])
									if current_candle + memory_candle == 0.0:
										difference = 0.0
									else:
										try:
											difference = abs((abs(current_candle-memory_candle)/((current_candle+memory_candle)/2))*100)
										except:
											difference = 0.0
									checks.append(difference)
									check_dex += 1
									if check_dex >= len(current_pattern):
										break
									else:
										continue
								diff_avg = sum(checks)/len(checks)
								if diff_avg <= perfect_threshold:
									any_perfect = 'yes'
									high_diff = float(memory_list[mem_ind].split('{}')[1].replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').replace(' ',''))/100
									low_diff = float(memory_list[mem_ind].split('{}')[2].replace("'","").replace(',','').replace('"','').replace(']','').replace('[','').replace(' ',''))/100
									unweighted.append(float(memory_pattern[len(memory_pattern)-1]))
									move_weights.append(float(weight_list[mem_ind]))
									high_move_weights.append(float(high_weight_list[mem_ind]))
									low_move_weights.append(float(low_weight_list[mem_ind]))
									high_unweighted.append(high_diff)
									low_unweighted.append(low_diff)
									moves.append(float(memory_pattern[len(memory_pattern)-1])*float(weight_list[mem_ind]))
									high_moves.append(high_diff*float(high_weight_list[mem_ind]))
									low_moves.append(low_diff*float(low_weight_list[mem_ind]))
									perfect_dexs.append(mem_ind)
									perfect_diffs.append(diff_avg)
								else:
									pass
								diffs_list.append(diff_avg)
								mem_ind += 1
								if mem_ind >= len(memory_list):
									if any_perfect == 'no':
										memory_diff = min(diffs_list)
										which_memory_index = diffs_list.index(memory_diff)
										perfect.append('no')
										final_moves = 0.0
										high_final_moves = 0.0
										low_final_moves = 0.0
										new_memory = 'yes'
									else:
										try:
											final_moves = sum(moves)/len(moves)
											high_final_moves = sum(high_moves)/len(high_moves)
											low_final_moves = sum(low_moves)/len(low_moves)
										except:
											final_moves = 0.0
											high_final_moves = 0.0
											low_final_moves = 0.0
										which_memory_index = perfect_dexs[perfect_diffs.index(min(perfect_diffs))]
										perfect.append('yes')
									break
								else:
									continue
						except:
							PrintException()
							memory_list = []
							weight_list = []
							high_weight_list = []
							low_weight_list = []
							which_memory_index = 'no'
							perfect.append('no')
							diffs_list = []
							any_perfect = 'no'
							perfect_dexs = []
							perfect_diffs = []
							moves = []
							move_weights = []
							high_move_weights = []
							low_move_weights = []
							unweighted = []
							high_moves = []
							low_moves = []
							final_moves = 0.0
							high_final_moves = 0.0
							low_final_moves = 0.0
					else:
						pass
					all_current_patterns.append(current_pattern)
					if len(unweighted) > 20:
						if perfect_threshold < 0.1:
							perfect_threshold -= 0.001
						else:
							perfect_threshold -= 0.01
						if perfect_threshold < 0.0:
							perfect_threshold = 0.0
						else:
							pass
					else:
						if perfect_threshold < 0.1:
							perfect_threshold += 0.001
						else:
							perfect_threshold += 0.01
						if perfect_threshold > 100.0:
							perfect_threshold = 100.0
						else:
							pass
					write_threshold_sometimes(tf_choice, perfect_threshold, loop_i, every=200)

					try:
						index = 0
						current_pattern_length = number_of_candles[number_of_candles_index]
						index = (len(price_list2))-current_pattern_length
						current_pattern = []
						while True:
							current_pattern.append(price_list2[index])
							if len(current_pattern)>=number_of_candles[number_of_candles_index]:
								break
							else:
								index += 1
								if index >= len(price_list2):
									break
								else:
									continue	
					except:
						PrintException()
					if 1==1:
						while True:
							try:
								c_diff = final_moves/100
								high_diff = high_final_moves
								low_diff = low_final_moves
								prediction_prices = [current_pattern[len(current_pattern)-1]]
								high_prediction_prices = [current_pattern[len(current_pattern)-1]]
								low_prediction_prices = [current_pattern[len(current_pattern)-1]]
								start_price = current_pattern[len(current_pattern)-1]
								new_price = start_price+(start_price*c_diff)
								high_new_price = start_price+(start_price*high_diff)
								low_new_price = start_price+(start_price*low_diff)
								prediction_prices = [start_price,new_price]
								high_prediction_prices = [start_price,high_new_price]
								low_prediction_prices = [start_price,low_new_price]
							except:
								start_price = current_pattern[len(current_pattern)-1]
								new_price = start_price
								prediction_prices = [start_price,start_price]
								high_prediction_prices = [start_price,start_price]
								low_prediction_prices = [start_price,start_price]
							break
						index = len(current_pattern)-1
						index2 = 0
						all_preds.append(prediction_prices)
						high_all_preds.append(high_prediction_prices)
						low_all_preds.append(low_prediction_prices)
						overunder = 'within'
						all_predictions.append(prediction_prices)
						high_all_predictions.append(high_prediction_prices)
						low_all_predictions.append(low_prediction_prices)
						index = 0
						print(tf_choice)
						page_info = ''
						current_pattern_length = 3
						index = (len(price_list2)-1)-current_pattern_length
						current_pattern = []
						while True:
							current_pattern.append(price_list2[index])
							index += 1
							if index >= len(price_list2):
								break
							else:
								continue
						high_current_pattern_length = 3
						high_index = (len(high_price_list2)-1)-high_current_pattern_length
						high_current_pattern = []
						while True:
							high_current_pattern.append(high_price_list2[high_index])
							high_index += 1
							if high_index >= len(high_price_list2):
								break
							else:
								continue
						low_current_pattern_length = 3
						low_index = (len(low_price_list2)-1)-low_current_pattern_length
						low_current_pattern = []
						while True:
							low_current_pattern.append(low_price_list2[low_index])
							low_index += 1
							if low_index >= len(low_price_list2):
								break
							else:
								continue
						try:
							which_pattern_length = 0
							new_y = [start_price,new_price]
							high_new_y = [start_price,high_new_price]
							low_new_y = [start_price,low_new_price]
						except:
							PrintException()
							new_y = [current_pattern[len(current_pattern)-1],current_pattern[len(current_pattern)-1]]
							high_new_y = [current_pattern[len(current_pattern)-1],high_current_pattern[len(high_current_pattern)-1]]
							low_new_y = [current_pattern[len(current_pattern)-1],low_current_pattern[len(low_current_pattern)-1]]
					else:
						current_pattern_length = 3
						index = (len(price_list2))-current_pattern_length
						current_pattern = []
						while True:
							current_pattern.append(price_list2[index])
							index += 1
							if index >= len(price_list2):
								break
							else:
								continue
						high_current_pattern_length = 3
						high_index = (len(high_price_list2)-1)-high_current_pattern_length
						high_current_pattern = []
						while True:
							high_current_pattern.append(high_price_list2[high_index])
							high_index += 1
							if high_index >= len(high_price_list2):
								break
							else:
								continue
						low_current_pattern_length = 3
						low_index = (len(low_price_list2)-1)-low_current_pattern_length
						low_current_pattern = []
						while True:
							low_current_pattern.append(low_price_list2[low_index])
							low_index += 1
							if low_index >= len(low_price_list2):
								break
							else:
								continue
						new_y = [current_pattern[len(current_pattern)-1],current_pattern[len(current_pattern)-1]]
						number_of_candles_index += 1
						if number_of_candles_index >= len(number_of_candles):
							print("Processed all number_of_candles. Exiting.")
							sys.exit(0)
					perfect_yes = 'no'
					if 1==1:
						high_current_price = high_current_pattern[len(high_current_pattern)-1]
						low_current_price = low_current_pattern[len(low_current_pattern)-1]
						try:
							try:
								difference_of_actuals = last_actual-new_y[0]
								difference_of_last = last_actual-last_prediction
								percent_difference_of_actuals = ((new_y[0]-last_actual)/abs(last_actual))*100
								high_difference_of_actuals = last_actual-high_current_price
								high_percent_difference_of_actuals = ((high_current_price-last_actual)/abs(last_actual))*100
								low_difference_of_actuals = last_actual-low_current_price
								low_percent_difference_of_actuals = ((low_current_price-last_actual)/abs(last_actual))*100
								percent_difference_of_last = ((last_prediction-last_actual)/abs(last_actual))*100
								high_percent_difference_of_last = ((high_last_prediction-last_actual)/abs(last_actual))*100
								low_percent_difference_of_last = ((low_last_prediction-last_actual)/abs(last_actual))*100
								if in_trade == 'no':
									percent_for_no_sell = ((new_y[1]-last_actual)/abs(last_actual))*100
									og_actual = last_actual
									in_trade = 'yes'
								else:
									percent_for_no_sell = ((new_y[1]-og_actual)/abs(og_actual))*100
							except:
								difference_of_actuals = 0.0
								difference_of_last = 0.0
								percent_difference_of_actuals = 0.0
								percent_difference_of_last = 0.0
								high_difference_of_actuals = 0.0
								high_percent_difference_of_actuals = 0.0
								low_difference_of_actuals = 0.0
								low_percent_difference_of_actuals = 0.0
								high_percent_difference_of_last = 0.0
								low_percent_difference_of_last = 0.0
						except:
							PrintException()
						try:
							perdex = 0
							while True:
								if perfect[perdex] == 'yes':
									perfect_yes = 'yes'
									break
								else:
									perdex += 1
									if perdex >= len(perfect):                                                                        
										perfect_yes = 'no'
										break
									else:
										continue
							high_var = high_percent_difference_of_last
							low_var = low_percent_difference_of_last
							if last_flipped == 'no':
								if high_percent_difference_of_actuals >= high_var2+(high_var2*0.005) and percent_difference_of_actuals < high_var2:
									upordown3.append(1)
									upordown.append(1)
									upordown4.append(1)
									if len(upordown4) > 100:
										del upordown4[0]
									else:
										pass 
								elif low_percent_difference_of_actuals <= low_var2-(low_var2*0.005) and percent_difference_of_actuals > low_var2:
									upordown.append(1)
									upordown3.append(1)
									upordown4.append(1)
									if len(upordown4) > 100:
										del upordown4[0]
									else:
										pass  									
								elif high_percent_difference_of_actuals >= high_var2+(high_var2*0.005) and percent_difference_of_actuals > high_var2:
									upordown3.append(0)
									upordown2.append(0)
									upordown.append(0)
									upordown4.append(0)
									if len(upordown4) > 100:
										del upordown4[0]
									else:
										pass
								elif low_percent_difference_of_actuals <= low_var2-(low_var2*0.005) and percent_difference_of_actuals < low_var2:
									upordown3.append(0)
									upordown2.append(0)
									upordown.append(0)
									upordown4.append(0)
									if len(upordown4) > 100:
										del upordown4[0]
									else:
										pass  
								else:
									pass
							else:
								pass
							try:
								print('(Bounce Accuracy for last 100 Over Limit Candles): ' + format((sum(upordown4)/len(upordown4))*100,'.2f'))
							except:
								pass
							try:
								print('current candle: '+str(len(price_list2)))
							except:
								pass
							try:
								print('Total Candles: '+str(int(len(price_list))))
							except:
								pass
						except:
							PrintException()
					else:
						pass
					cc_on = 'no'
					try:
						long_trade = 'no'
						short_trade = 'no'
						last_moves = moves
						last_high_moves = high_moves
						last_low_moves = low_moves
						last_move_weights = move_weights
						last_high_move_weights = high_move_weights
						last_low_move_weights = low_move_weights
						last_perfect_dexs = perfect_dexs
						last_perfect_diffs = perfect_diffs
						percent_difference_of_now = ((new_y[1]-new_y[0])/abs(new_y[0]))*100
						high_percent_difference_of_now = ((high_new_y[1]-high_new_y[0])/abs(high_new_y[0]))*100
						low_percent_difference_of_now = ((low_new_y[1]-low_new_y[0])/abs(low_new_y[0]))*100
						high_var2 = high_percent_difference_of_now
						low_var2 = low_percent_difference_of_now
						var2 = percent_difference_of_now
						if flipped == 'yes':
							new1 = high_percent_difference_of_now
							high_percent_difference_of_now = low_percent_difference_of_now
							low_percent_difference_of_now = new1
						else:
							pass
					except:
						PrintException()
					last_actual = new_y[0]
					last_prediction = new_y[1]
					high_last_prediction = high_new_y[1]
					low_last_prediction = low_new_y[1]
					prediction_adjuster = 0.0
					prediction_expander2 = 1.5
					ended_on = number_of_candles_index
					next_coin = 'yes'
					profit_hit = 'no'
					long_profit = 0
					short_profit = 0
					"""
					expander_move = input('Expander good? yes or new number: ')
					if expander_move == 'yes':
						pass
					else:
						prediction_expander = expander_move
						continue
					"""
					last_flipped = flipped
					which_candle_of_the_prediction_index = 0
					if 1 == 1:
						current_pattern_ending = [current_pattern[len(current_pattern)-1]]
						while True:
							try:
								try:
									price_list_length += 1		
									which_candle_of_the_prediction_index += 1
									try:
										if len(price_list2)>=int(len(price_list)*0.25) and restarted_yet < 2:
											restarted_yet += 1
											restarting = 'yes'
											break
										else:
											restarting = 'no'
									except:
										restarting = 'no'
									if len(price_list2) == len(price_list):
										the_big_index += 1
										restarted_yet = 0
										print('restarting')
										restarting = 'yes'
										avg50 = []
										import sys
										import datetime
										import traceback
										import linecache
										import base64
										import calendar
										import hashlib
										import hmac
										from datetime import datetime
										sells_count = 0
										prediction_prices_avg_list = []
										pt_server = 'server'
										import psutil
										import logging
										list_len = 0
										in_trade = 'no'
										updowncount = 0
										updowncount1 = 0
										updowncount1_2 = 0
										updowncount1_3 = 0
										updowncount1_4 = 0
										high_var2 = 0.0
										low_var2 = 0.0
										last_flipped = 'no'
										starting_amounth02 = 100.0
										starting_amounth05 = 100.0
										starting_amounth10 = 100.0
										starting_amounth20 = 100.0
										starting_amounth50 = 100.0
										starting_amount = 100.0
										starting_amount1 = 100.0
										starting_amount1_2 = 100.0
										starting_amount1_3 = 100.0
										starting_amount1_4 = 100.0
										starting_amount2 = 100.0
										starting_amount2_2 = 100.0
										starting_amount2_3 = 100.0
										starting_amount2_4 = 100.0
										starting_amount3 = 100.0
										starting_amount3_2 = 100.0
										starting_amount3_3 = 100.0
										starting_amount3_4 = 100.0
										starting_amount4 = 100.0
										starting_amount4_2 = 100.0
										starting_amount4_3 = 100.0
										starting_amount4_4 = 100.0
										profit_list = []
										profit_list1 = []
										profit_list1_2 = []
										profit_list1_3 = []
										profit_list1_4 = []
										profit_list2 = []
										profit_list2_2 = []
										profit_list2_3 = []
										profit_list2_4 = []
										profit_list3 = []
										profit_list3_2 = []
										profit_list3_3 = []
										profit_list4 = []
										profit_list4_2 = []
										good_hits = []
										good_preds = []
										good_preds2 = []
										good_preds3 = []
										good_preds4 = []
										good_preds5 = []
										good_preds6 = []
										big_good_preds = []
										big_good_preds2 = []
										big_good_preds3 = []
										big_good_preds4 = []
										big_good_preds5 = []
										big_good_preds6 = []
										big_good_hits = []
										upordown = []
										upordown1 = []
										upordown1_2 = []
										upordown1_3 = []
										upordown1_4 = []
										upordown2 = []
										upordown2_2 = []
										upordown2_3 = []
										upordown2_4 = []
										upordown3 = []
										upordown3_2 = []
										upordown3_3 = []
										upordown3_4 = []
										upordown4 = []
										upordown4_2 = []
										upordown4_3 = []
										upordown4_4 = []
										upordown5 = []
										import json
										import uuid
										how_far_to_look_back = 100000
										list_len = 0
										print(the_big_index)
										print(len(tf_choices))
										if the_big_index >= len(tf_choices):
											if len(number_of_candles) == 1:
												print("Finished processing all timeframes (number_of_candles has only one entry). Exiting.")
												try:
													file = open(TRAINER_LAST_START_PATH,'w+')
													file.write(str(start_time_yes))
													file.close()
												except:
													pass

												# Mark training finished for the GUI
												try:
													_trainer_finished_at = int(time.time())
													file = open(TRAINER_LAST_TRAINING_PATH,'w+')
													file.write(str(_trainer_finished_at))
													file.close()
												except:
													pass
												_write_trainer_status(state="TRAINING", pct=90, phase="saving", detail=f"{timeframe} final", force=True)
												_write_trainer_status(state="FINISHED", pct=100, phase="finished", finished_at=_trainer_finished_at, force=True)

												sys.exit(0)
											else:
												the_big_index = 0
										else:
											pass
										break
									else:
										exited = 'no'
										try:
											price_list2 = []
											price_list_index = 0
											while True:
												price_list2.append(price_list[price_list_index])
												price_list_index += 1
												if len(price_list2) >= price_list_length:
													break
												else:
													continue
											high_price_list2 = []
											high_price_list_index = 0
											while True:
												high_price_list2.append(high_price_list[high_price_list_index])
												high_price_list_index += 1
												if high_price_list_index >= price_list_length:
													break
												else:
													continue
											low_price_list2 = []
											low_price_list_index = 0
											while True:
												low_price_list2.append(low_price_list[low_price_list_index])
												low_price_list_index += 1
												if low_price_list_index >= price_list_length:
													break
												else:
													continue
											price2 = price_list2[len(price_list2)-1]
											high_price2 = high_price_list2[len(high_price_list2)-1]
											low_price2 = low_price_list2[len(low_price_list2)-1]
											highlowind = 0
											this_differ = ((price2-new_y[1])/abs(new_y[1]))*100
											high_this_differ = ((high_price2-new_y[1])/abs(new_y[1]))*100
											low_this_differ = ((low_price2-new_y[1])/abs(new_y[1]))*100
											this_diff = ((price2-new_y[0])/abs(new_y[0]))*100
											high_this_diff = ((high_price2-new_y[0])/abs(new_y[0]))*100
											low_this_diff = ((low_price2-new_y[0])/abs(new_y[0]))*100
											difference_list = []
											list_of_predictions = all_predictions
											close_enough_counter = []
											which_pattern_length_index = 0								
											while True:
												current_prediction_price = all_predictions[highlowind][which_candle_of_the_prediction_index]
												high_current_prediction_price = high_all_predictions[highlowind][which_candle_of_the_prediction_index]
												low_current_prediction_price = low_all_predictions[highlowind][which_candle_of_the_prediction_index]
												perc_diff_now = ((current_prediction_price-new_y[0])/abs(new_y[0]))*100
												perc_diff_now_actual = ((price2-new_y[0])/abs(new_y[0]))*100
												high_perc_diff_now_actual = ((high_price2-new_y[0])/abs(new_y[0]))*100
												low_perc_diff_now_actual = ((low_price2-new_y[0])/abs(new_y[0]))*100
												try:
													difference = abs((abs(current_prediction_price-float(price2))/((current_prediction_price+float(price2))/2))*100)
												except:
													difference = 100.0
												try:
													direction = 'down'
													try:
														indy = 0
														while True:
															new_memory = 'no'
															var3 = (moves[indy]*100)
															high_var3 = (high_moves[indy]*100)
															low_var3 = (low_moves[indy]*100)
															if high_perc_diff_now_actual > high_var3+(high_var3*0.1):
																high_new_weight = high_move_weights[indy] + 0.25
																if high_new_weight > 2.0:
																	high_new_weight = 2.0
																else:
																	pass
															elif high_perc_diff_now_actual < high_var3-(high_var3*0.1):
																high_new_weight = high_move_weights[indy] - 0.25
																if high_new_weight < 0.0:
																	high_new_weight = 0.0
																else:
																	pass
															else:
																high_new_weight = high_move_weights[indy]
															if low_perc_diff_now_actual < low_var3-(low_var3*0.1):
																low_new_weight = low_move_weights[indy] + 0.25
																if low_new_weight > 2.0:
																	low_new_weight = 2.0
																else:
																	pass
															elif low_perc_diff_now_actual > low_var3+(low_var3*0.1):
																low_new_weight = low_move_weights[indy] - 0.25
																if low_new_weight < 0.0:
																	low_new_weight = 0.0
																else:
																	pass
															else:
																low_new_weight = low_move_weights[indy]
															if perc_diff_now_actual > var3+(var3*0.1):
																new_weight = move_weights[indy] + 0.25
																if new_weight > 2.0:
																	new_weight = 2.0
																else:
																	pass
															elif perc_diff_now_actual < var3-(var3*0.1):
																new_weight = move_weights[indy] - 0.25
																if new_weight < (0.0-2.0):
																	new_weight = (0.0-2.0)
																else:
																	pass
															else:
																new_weight = move_weights[indy]
															del weight_list[perfect_dexs[indy]]
															weight_list.insert(perfect_dexs[indy],new_weight)
															del high_weight_list[perfect_dexs[indy]]
															high_weight_list.insert(perfect_dexs[indy],high_new_weight)
															del low_weight_list[perfect_dexs[indy]]
															low_weight_list.insert(perfect_dexs[indy],low_new_weight)

															# mark dirty (we will flush in batches)
															_mem = load_memory(tf_choice)
															_mem["dirty"] = True

															# occasional batch flush
															if loop_i % 200 == 0:
																flush_memory(tf_choice)

															indy += 1
															if indy >= len(unweighted):
																break
															else:
																pass
													except:
														PrintException()
														all_current_patterns[highlowind].append(this_diff)

														# build the same memory entry format, but store in RAM
														mem_entry = str(all_current_patterns[highlowind]).replace("'","").replace(',','').replace('"','').replace(']','').replace('[','')+'{}'+str(high_this_diff)+'{}'+str(low_this_diff)

														_mem = load_memory(tf_choice)
														_mem["memory_list"].append(mem_entry)
														_mem["weight_list"].append('1.0')
														_mem["high_weight_list"].append('1.0')
														_mem["low_weight_list"].append('1.0')
														_mem["dirty"] = True

														# occasional batch flush
														if loop_i % 200 == 0:
															flush_memory(tf_choice)

												except:
													PrintException()
													pass										
												highlowind += 1
												if highlowind >= len(all_predictions):
													break
												else:
													continue
										except SystemExit:
											raise
										except KeyboardInterrupt:
											raise
										except Exception:
											PrintException()
											break

									if which_candle_of_the_prediction_index >= candles_to_predict:
										break
									else:
										continue
								except SystemExit:
									raise
								except KeyboardInterrupt:
									raise
								except Exception:
									PrintException()
									break

							except SystemExit:
								raise
							except KeyboardInterrupt:
								raise
							except Exception:
								PrintException()
								break

					else:
						pass
					coin_choice_index += 1
					history_list = []
					price_change_list = []
					current_pattern = []
					break
				except SystemExit:
					raise
				except KeyboardInterrupt:
					raise
				except Exception:
					PrintException()
					break

			if restarting == 'yes':
				break
			else:
				continue
		if restarting == 'yes':
			break
		else:
			continue

"""
MT5 Trading Bot for XAU/USD - Vantage Configuration
60-pip TP, 20-pip SL, Simple Compounding from $100

SIMPLIFIED VERSION:
- 6 trading times based on backtesting results
- No email notifications
- No log file generation
- Tick ledger retained for price capture
- All core trading logic preserved
"""
import MetaTrader5 as mt5
from datetime import datetime, timedelta, date, time as dt_time, timezone
import time as _time
from pathlib import Path
import threading
from threading import Event, Lock
from typing import Optional, Tuple, List, Dict
import json
import re

# ========= CONFIG =========
# MT5 Credentials - UPDATE THESE
ACCOUNT  = 11304009  # Your Vantage demo account
PASSWORD = "*WsHy5%X"
SERVER   = "VantageInternational-Demo"
SYMBOL   = "XAUUSD+"

# IST Trading schedule - UPDATED FROM BACKTESTING RESULTS
# Best performing times: 19:35 > 20:35 > 18:35 > 10:35 > 12:35 > 11:35
IST_TRADE_TIMES = [
    "10:35", "11:35", "12:35", "18:35", "19:35", "20:35"
]

# Strategy params
SL_PIPS = 20
TP_PIPS = 60
PIP_SIZE = 0.10
DOLLAR_PER_PIP_PER_001LOT = 1.0
DEVIATION = 20
MAGIC = 20250901

# Partial close configuration
PARTIAL_CLOSE_TRIGGER_PIPS = 50  # Trigger partial close at +50 pips
PARTIAL_CLOSE_PERCENTAGE_EVEN = 0.5  # 50% for even lots
PARTIAL_CLOSE_PERCENTAGE_ODD = 0.66  # 66% for odd lots

# Data directory (for tick ledger only)
DATA_DIR = Path(r"C:\Users\Administrator\Desktop\janvi")
DATA_DIR.mkdir(exist_ok=True)

# Baseline management
BASELINE_STATE_FILE = DATA_DIR / "baseline_state.json"
DEFAULT_BASELINE = 100.0

# Lot sizing
MIN_LOT = 0.01
MAX_LOSS_PER_TRADE_USD = 100.0

# Timezones
IST_TZ = timezone(timedelta(hours=5, minutes=30))
MANUAL_SERVER_DELTA_MINUTES: Optional[int] = -210  # GMT+2 to IST

# Trigger windows - Trades fire 2 seconds before scheduled time
FIRE_WINDOW_SECONDS = 30  # 30 seconds for reliable execution
EARLY_TRIGGER_WINDOW_SECONDS = 2  # Fire 2 seconds early (e.g., 20:34:58 for 20:35:00)

# Watcher config
MAX_POSITION_HOLD_HOURS = 2

# Balance monitor config
BALANCE_MONITOR_INTERVAL_SECONDS = 120

# Balance query timing
BALANCE_QUERY_TIME = "10:30"

# Trading days configuration
SKIP_MONDAY = True  # Skip Monday, trade Tue-Fri
TRADING_DAYS = [1, 2, 3, 4]  # Tuesday, Wednesday, Thursday, Friday

# Skip tracking
SKIPPED_SLOTS = {}
SKIPPED_SLOTS_LOCK = Lock()
LAST_SLOT_TIME = "20:35"

# Tick Price Ledger Configuration
TICK_LEDGER_DIR = DATA_DIR / "tick_ledger"
TICK_LEDGER_DIR.mkdir(exist_ok=True)
TICK_LEDGER_RETENTION_DAYS = 7

# Shared state
OPEN_TRADES_GLOBAL = 0
RUN_HEARTBEAT = Event()
RUN_HEARTBEAT.set()
TRADES_OPENED_TODAY = 0
TRADES_CLOSED_TODAY = 0
TRADES_LOCK = Lock()
ACTIVE_WATCHERS = {}
WATCHERS_LOCK = Lock()

# Balance monitoring
LAST_KNOWN_BALANCE = 0.0
BALANCE_LOCK = Lock()
WITHDRAWAL_DETECTED_TODAY = False
BALANCE_QUERIED_TODAY = False

# Opening prices storage
CANDLE_OPENS = {}
CANDLE_OPENS_LOCK = Lock()

# Internal Ledger Management
LEDGER_DATA = {}
LEDGER_LOCK = Lock()
DAILY_OPENING_BALANCE = 0.0
DAILY_CLOSING_BALANCE = 0.0

# ===== Utility =====
DATE_FMT = "%d-%m-%Y"

def fmt_date(d: date) -> str:
    return d.strftime(DATE_FMT)

def parse_ist_hhmm(hhmm: str) -> Tuple[int,int]:
    m = re.match(r'^(\d{1,2}):(\d{1,2})(?::\d{1,2})?$', str(hhmm).strip())
    if not m:
        raise ValueError(f"Bad time format: {hhmm}")
    h, mn = int(m.group(1)), int(m.group(2))
    if not (0 <= h < 24 and 0 <= mn < 60):
        raise ValueError(f"Out-of-range time: {hhmm}")
    return h, mn

# Calculate required opening times
REQUIRED_OPENING_TIMES = []
for trade_time in IST_TRADE_TIMES:
    hh, mm = parse_ist_hhmm(trade_time)
    opening_dt = datetime.combine(date.today(), dt_time(hh, mm)) - timedelta(minutes=5)
    opening_key = opening_dt.strftime("%H:%M")
    if opening_key not in REQUIRED_OPENING_TIMES:
        REQUIRED_OPENING_TIMES.append(opening_key)

# ===== MT5 helpers =====
def init_mt5():
    mt5_path = r"C:\Users\Administrator\Desktop\janvi\terminal64.exe"
    if not mt5.initialize(path=mt5_path):
        raise RuntimeError(f"MT5 init error: {mt5.last_error()}")
    if not mt5.login(ACCOUNT, password=PASSWORD, server=SERVER):
        raise RuntimeError(f"MT5 login error: {mt5.last_error()}")
    info = mt5.symbol_info(SYMBOL)
    if info is None:
        raise RuntimeError(f"{SYMBOL} not found")
    if not info.visible:
        mt5.symbol_select(SYMBOL, True)

def shutdown_mt5():
    mt5.shutdown()

def _ist_from_epoch(sec: float) -> datetime:
    return datetime.fromtimestamp(sec, tz=timezone.utc).astimezone(IST_TZ)

def ist_to_server_delta_minutes_for_date(d: date) -> int:
    if MANUAL_SERVER_DELTA_MINUTES is not None:
        return MANUAL_SERVER_DELTA_MINUTES
    return -210

def build_server_schedule_for_day(ist_day: date) -> Tuple[Dict[str, datetime], int]:
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    delta = timedelta(minutes=delta_min)
    sched = {}
    for tstr in IST_TRADE_TIMES:
        hh, mm = parse_ist_hhmm(tstr)
        ist_dt = datetime.combine(ist_day, dt_time(hh, mm, tzinfo=IST_TZ))
        server_dt = (ist_dt + delta).replace(tzinfo=None)
        sched[f"{hh:02d}:{mm:02d}"] = server_dt
    return sched, delta_min

# ===== Tick Price Ledger Functions =====

def get_tick_ledger_path(d: date) -> Path:
    return TICK_LEDGER_DIR / f"tick_ledger_{d.isoformat()}.json"

def load_tick_ledger(d: date) -> Dict:
    ledger_path = get_tick_ledger_path(d)
    if ledger_path.exists():
        try:
            with open(ledger_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"[TICK_LEDGER] Loaded {len(data.get('ticks', {}))} recorded ticks for {d.isoformat()}")
                return data
        except Exception as e:
            print(f"[TICK_LEDGER] Error loading ledger: {e}")
            return {"date": d.isoformat(), "ticks": {}, "metadata": {}}
    return {"date": d.isoformat(), "ticks": {}, "metadata": {}}

def save_tick_to_ledger(d: date, time_key: str, price: float, source: str = "live"):
    try:
        ledger_path = get_tick_ledger_path(d)
        
        if ledger_path.exists():
            with open(ledger_path, 'r', encoding='utf-8') as f:
                ledger = json.load(f)
        else:
            ledger = {"date": d.isoformat(), "ticks": {}, "metadata": {}}
        
        if time_key not in ledger["ticks"]:
            ledger["ticks"][time_key] = {
                "price": price,
                "source": source,
                "captured_at": datetime.now(IST_TZ).isoformat(),
                "updates": []
            }
            print(f"[TICK_LEDGER] Saved {time_key} IST: {source.upper()} = {price:.3f}")
        else:
            ledger["ticks"][time_key]["updates"].append({
                "price": price,
                "source": source,
                "timestamp": datetime.now(IST_TZ).isoformat()
            })
            ledger["ticks"][time_key]["price"] = price
        
        with open(ledger_path, 'w', encoding='utf-8') as f:
            json.dump(ledger, f, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f"[TICK_LEDGER] Error saving tick: {e}")

def get_tick_from_ledger(d: date, time_key: str) -> Optional[float]:
    try:
        ledger = load_tick_ledger(d)
        if time_key in ledger.get("ticks", {}):
            price = ledger["ticks"][time_key]["price"]
            source = ledger["ticks"][time_key]["source"]
            print(f"[TICK_LEDGER] Retrieved {time_key} IST from ledger: {price:.3f} ({source})")
            return price
    except Exception as e:
        print(f"[TICK_LEDGER] Error retrieving tick: {e}")
    return None

def cleanup_old_tick_ledgers():
    try:
        cutoff_date = date.today() - timedelta(days=TICK_LEDGER_RETENTION_DAYS)
        removed_count = 0
        
        for ledger_file in TICK_LEDGER_DIR.glob("tick_ledger_*.json"):
            try:
                date_str = ledger_file.stem.replace("tick_ledger_", "")
                file_date = date.fromisoformat(date_str)
                
                if file_date < cutoff_date:
                    ledger_file.unlink()
                    removed_count += 1
                    print(f"[TICK_LEDGER] Removed old ledger: {ledger_file.name}")
            except Exception as e:
                print(f"[TICK_LEDGER] Error processing {ledger_file.name}: {e}")
        
        if removed_count > 0:
            print(f"[TICK_LEDGER] Cleanup complete: {removed_count} old ledgers removed")
    except Exception as e:
        print(f"[TICK_LEDGER] Cleanup error: {e}")

# ===== Baseline management =====
def load_baseline() -> float:
    try:
        if BASELINE_STATE_FILE.exists():
            data = json.loads(BASELINE_STATE_FILE.read_text(encoding="utf-8"))
            return float(data.get("baseline", DEFAULT_BASELINE))
    except Exception:
        pass
    return DEFAULT_BASELINE

def save_baseline(value: float):
    try:
        BASELINE_STATE_FILE.write_text(json.dumps({"baseline": float(value)}), encoding="utf-8")
        print(f"[BASELINE] Saved: ${value:.2f}")
    except Exception as e:
        print(f"[BASELINE] Failed to save: {e}")

def query_and_set_opening_balance(ist_day: date):
    global BALANCE_QUERIED_TODAY, DAILY_OPENING_BALANCE, LAST_KNOWN_BALANCE
    
    try:
        acc = mt5.account_info()
        if not acc:
            print(f"[BALANCE_QUERY] Failed to get account info")
            return
        
        current_balance = float(acc.balance)
        save_baseline(current_balance)
        
        with BALANCE_LOCK:
            LAST_KNOWN_BALANCE = current_balance
            DAILY_OPENING_BALANCE = current_balance
        
        print(f"[BALANCE_QUERY] OK Opening balance for {fmt_date(ist_day)}: ${current_balance:.2f}")
        BALANCE_QUERIED_TODAY = True
        
    except Exception as e:
        print(f"[BALANCE_QUERY] Error: {e}")

# ===== Balance monitor =====
def balance_monitor_thread():
    global LAST_KNOWN_BALANCE, WITHDRAWAL_DETECTED_TODAY
    
    print("[BALANCE_MONITOR] Started - checking every 2 minutes for withdrawals")
    
    last_checked_time = datetime.now(timezone.utc).astimezone().replace(tzinfo=None)
    
    while RUN_HEARTBEAT.is_set():
        try:
            now = datetime.now(timezone.utc).astimezone().replace(tzinfo=None)
            
            deals = mt5.history_deals_get(last_checked_time, now) or []
            
            for deal in deals:
                deal_type = int(getattr(deal, "type", -1))
                
                if deal_type == 2:
                    profit = float(getattr(deal, "profit", 0.0))
                    deal_time_epoch = int(getattr(deal, "time", 0))
                    deal_time_ist = _ist_from_epoch(deal_time_epoch)
                    
                    if profit < 0:
                        withdrawn = abs(profit)
                        
                        acc = mt5.account_info()
                        current_balance = float(acc.balance) if acc else 0.0
                        previous_balance = current_balance + withdrawn
                        
                        print(f"\n{'='*60}")
                        print(f"[WITHDRAWAL] WARNING DETECTED at {deal_time_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")
                        print(f"[WITHDRAWAL] Previous balance: ${previous_balance:.2f}")
                        print(f"[WITHDRAWAL] Current balance: ${current_balance:.2f}")
                        print(f"[WITHDRAWAL] Withdrawn: ${withdrawn:.2f}")
                        print(f"{'='*60}\n")
                        
                        save_baseline(current_balance)
                        
                        with BALANCE_LOCK:
                            LAST_KNOWN_BALANCE = current_balance
                            WITHDRAWAL_DETECTED_TODAY = True
                    
                    elif profit > 0:
                        deposited = profit
                        
                        acc = mt5.account_info()
                        current_balance = float(acc.balance) if acc else 0.0
                        
                        print(f"\n[DEPOSIT] OK DETECTED at {deal_time_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")
                        print(f"[DEPOSIT] Amount: ${deposited:.2f}")
                        print(f"[DEPOSIT] New balance: ${current_balance:.2f}\n")
                        
                        save_baseline(current_balance)
                        
                        with BALANCE_LOCK:
                            LAST_KNOWN_BALANCE = current_balance
            
            last_checked_time = now
            _time.sleep(BALANCE_MONITOR_INTERVAL_SECONDS)
            
        except Exception as e:
            print(f"[BALANCE_MONITOR] Error: {e}")
            _time.sleep(BALANCE_MONITOR_INTERVAL_SECONDS)

# ===== Internal Ledger Management =====

def get_ledger_filepath(ist_day: date) -> Path:
    return DATA_DIR / f"trade_ledger_{fmt_date(ist_day)}.json"

def load_ledger(ist_day: date) -> Dict:
    ledger_file = get_ledger_filepath(ist_day)
    try:
        if ledger_file.exists():
            with open(ledger_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"[LEDGER] Loaded existing ledger: {len(data.get('trades', {}))} trades")
                return data
    except Exception as e:
        print(f"[LEDGER] Error loading ledger: {e}")
    
    return {
        "date": fmt_date(ist_day),
        "opening_balance": 0.0,
        "closing_balance": 0.0,
        "trades": {}
    }

def save_ledger(ist_day: date, ledger_data: Dict):
    ledger_file = get_ledger_filepath(ist_day)
    temp_file = ledger_file.with_suffix('.json.tmp')
    
    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(ledger_data, f, indent=2, ensure_ascii=False)
        
        temp_file.replace(ledger_file)
        
    except Exception as e:
        print(f"[LEDGER] Error saving ledger: {e}")
        if temp_file.exists():
            temp_file.unlink()

def create_ledger_entry(position_id: int, ist_day: date, trade_number: int, 
                       entry_time: str, entry_price: float, sl_price: float, 
                       tp_price: float, lot_size: float, side: str, tag: str):
    global LEDGER_DATA
    
    with LEDGER_LOCK:
        entry_key = tag
        
        LEDGER_DATA["trades"][entry_key] = {
            "position_id": position_id,
            "date": fmt_date(ist_day),
            "trade_number": trade_number,
            "entry_time": entry_time,
            "entry_price": round(entry_price, 3),
            "sl_price": round(sl_price, 3),
            "tp_price": round(tp_price, 3),
            "lot_size": round(lot_size, 2),
            "side": side,
            "tag": tag,
            "trade_outcome": None,
            "pnl": None,
            "exit_time": None,
            "account_balance": None,
            "status": "OPEN",
            "partial_close_triggered": False,
            "partial_close_price": None,
            "partial_close_lots": None,
            "partial_close_pnl": None,
            "remaining_lots": None,
            "remaining_pnl": None,
            "partial_close_net": None
        }
        
        save_ledger(ist_day, LEDGER_DATA)
        
        print(f"[LEDGER] OK Created entry for {tag} (Position: {position_id})")

def update_ledger_entry(position_id: int, ist_day: date, exit_time: str, 
                       trade_outcome: str, pnl: float, account_balance: float):
    global LEDGER_DATA, DAILY_CLOSING_BALANCE
    
    with LEDGER_LOCK:
        entry_key = None
        for key, entry in LEDGER_DATA["trades"].items():
            if entry["position_id"] == position_id:
                entry_key = key
                break
        
        if entry_key:
            LEDGER_DATA["trades"][entry_key].update({
                "exit_time": exit_time,
                "trade_outcome": trade_outcome,
                "pnl": round(pnl, 2),
                "account_balance": round(account_balance, 2),
                "status": "CLOSED"
            })
            
            DAILY_CLOSING_BALANCE = account_balance
            LEDGER_DATA["closing_balance"] = round(account_balance, 2)
            
            save_ledger(ist_day, LEDGER_DATA)
            
            print(f"[LEDGER] OK Updated entry for Position {position_id}: {trade_outcome} ${pnl:.2f}")
        else:
            print(f"[LEDGER] WARNING: Position {position_id} not found in ledger")

def rebuild_ledger_from_mt5(ist_day: date) -> Dict:
    print(f"[LEDGER] Rebuilding ledger from MT5 history for {fmt_date(ist_day)}...")
    
    ledger_data = {
        "date": fmt_date(ist_day),
        "opening_balance": 0.0,
        "closing_balance": 0.0,
        "trades": {}
    }
    
    day_start = datetime.combine(ist_day, dt_time(0,0), tzinfo=IST_TZ)
    day_end = datetime.combine(ist_day, dt_time(23,59,59), tzinfo=IST_TZ)
    
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    start_server = (day_start + timedelta(minutes=delta_min)).replace(tzinfo=None)
    end_server = (day_end + timedelta(minutes=delta_min)).replace(tzinfo=None)
    
    deals = mt5.history_deals_get(start_server, end_server) or []
    
    by_pos = {}
    for d in deals:
        if d.symbol != SYMBOL or int(getattr(d, "magic", 0)) != MAGIC:
            continue
        pid = int(getattr(d, "position_id", 0)) or int(getattr(d, "ticket", 0))
        if pid <= 0:
            continue
        by_pos.setdefault(pid, []).append(d)
    
    trade_number = 0
    running_balance = load_baseline()
    ledger_data["opening_balance"] = running_balance
    
    for pid, lst in sorted(by_pos.items()):
        lst.sort(key=lambda x: x.time)
        
        open_deal = None
        close_deal = None
        for d in lst:
            entry_type = int(getattr(d, "entry", 0))
            if entry_type == mt5.DEAL_ENTRY_IN:
                open_deal = d
            elif entry_type == mt5.DEAL_ENTRY_OUT:
                close_deal = d
        
        if not open_deal:
            continue
        
        trade_number += 1
        
        open_time_ist = _ist_from_epoch(open_deal.time)
        entry_time = open_time_ist.strftime("%H:%M")
        entry_price = float(open_deal.price)
        volume = float(open_deal.volume)
        side = "BUY" if getattr(open_deal, "type", 0) == mt5.DEAL_TYPE_BUY else "SELL"
        comment = getattr(open_deal, "comment", "")
        
        tag = comment.split("|")[-1] if "|" in comment else f"{ist_day.isoformat()}_{entry_time}"
        
        sl, tp = compute_sl_tp(entry_price, side)
        
        entry_key = tag
        
        ledger_data["trades"][entry_key] = {
            "position_id": pid,
            "date": fmt_date(ist_day),
            "trade_number": trade_number,
            "entry_time": entry_time,
            "entry_price": round(entry_price, 3),
            "sl_price": round(sl, 3),
            "tp_price": round(tp, 3),
            "lot_size": round(volume, 2),
            "side": side,
            "tag": tag,
            "trade_outcome": None,
            "pnl": None,
            "exit_time": None,
            "account_balance": None,
            "status": "OPEN" if not close_deal else "CLOSED",
            "partial_close_triggered": False,
            "partial_close_price": None,
            "partial_close_lots": None,
            "partial_close_pnl": None,
            "remaining_lots": None,
            "remaining_pnl": None,
            "partial_close_net": None
        }
        
        if close_deal:
            close_time_ist = _ist_from_epoch(close_deal.time)
            exit_time = close_time_ist.strftime("%H:%M")
            
            net_profit = sum(float(x.profit) for x in lst)
            outcome = "PROFIT" if net_profit > 0 else "LOSS"
            
            running_balance += net_profit
            
            ledger_data["trades"][entry_key].update({
                "exit_time": exit_time,
                "trade_outcome": outcome,
                "pnl": round(net_profit, 2),
                "account_balance": round(running_balance, 2),
                "status": "CLOSED"
            })
    
    ledger_data["closing_balance"] = round(running_balance, 2)
    
    print(f"[LEDGER] OK Rebuilt ledger: {len(ledger_data['trades'])} trades")
    
    return ledger_data

# ===== Crash Recovery Functions =====
def recover_trading_state_from_mt5(ist_day: date):
    global TRADES_OPENED_TODAY, TRADES_CLOSED_TODAY
    
    print(f"[RECOVERY] Rebuilding trading state for {fmt_date(ist_day)}...")
    
    day_start = datetime.combine(ist_day, dt_time(0,0), tzinfo=IST_TZ)
    day_end = datetime.combine(ist_day, dt_time(23,59,59), tzinfo=IST_TZ)
    
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    start_server = (day_start + timedelta(minutes=delta_min)).replace(tzinfo=None)
    end_server = (day_end + timedelta(minutes=delta_min)).replace(tzinfo=None)
    
    deals = mt5.history_deals_get(start_server, end_server) or []
    
    by_pos = {}
    for d in deals:
        if d.symbol != SYMBOL or int(getattr(d, "magic", 0)) != MAGIC:
            continue
        pid = int(getattr(d, "position_id", 0)) or int(getattr(d, "ticket", 0))
        if pid <= 0:
            continue
        by_pos.setdefault(pid, []).append(d)
    
    opened_count = 0
    closed_count = 0
    
    for pid, lst in by_pos.items():
        has_entry_in = False
        has_entry_out = False
        
        for d in lst:
            entry_type = int(getattr(d, "entry", 0))
            if entry_type == mt5.DEAL_ENTRY_IN:
                has_entry_in = True
            elif entry_type == mt5.DEAL_ENTRY_OUT:
                has_entry_out = True
        
        if has_entry_in:
            opened_count += 1
            if has_entry_out:
                closed_count += 1
    
    positions = mt5.positions_get(symbol=SYMBOL)
    currently_open = 0
    if positions:
        for p in positions:
            if getattr(p, "magic", 0) == MAGIC:
                currently_open += 1
    
    with TRADES_LOCK:
        TRADES_OPENED_TODAY = opened_count
        TRADES_CLOSED_TODAY = closed_count
    
    print(f"[RECOVERY] OK Trading state recovered:")
    print(f"[RECOVERY]   - Trades opened today: {opened_count}")
    print(f"[RECOVERY]   - Trades closed today: {closed_count}")
    print(f"[RECOVERY]   - Currently open positions: {currently_open}")
    
    return opened_count, closed_count, currently_open

def recover_executed_slots_from_mt5(ist_day: date) -> set:
    executed_slots = set()
    
    with LEDGER_LOCK:
        for tag, entry in LEDGER_DATA["trades"].items():
            time_part = entry.get("entry_time", "")
            if time_part in IST_TRADE_TIMES:
                executed_slots.add(time_part)
    
    if executed_slots:
        print(f"[RECOVERY] Executed slots from ledger: {sorted(executed_slots)}")
        return executed_slots
    
    day_start = datetime.combine(ist_day, dt_time(0,0), tzinfo=IST_TZ)
    day_end = datetime.combine(ist_day, dt_time(23,59,59), tzinfo=IST_TZ)
    
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    start_server = (day_start + timedelta(minutes=delta_min)).replace(tzinfo=None)
    end_server = (day_end + timedelta(minutes=delta_min)).replace(tzinfo=None)
    
    deals = mt5.history_deals_get(start_server, end_server) or []
    
    for d in deals:
        if d.symbol != SYMBOL or int(getattr(d, "magic", 0)) != MAGIC:
            continue
        
        entry_type = int(getattr(d, "entry", 0))
        if entry_type == mt5.DEAL_ENTRY_IN:
            comment = getattr(d, "comment", "")
            
            if "_" in comment:
                try:
                    time_part = comment.split("_")[-1]
                    if time_part in IST_TRADE_TIMES:
                        executed_slots.add(time_part)
                except Exception:
                    pass
    
    if executed_slots:
        print(f"[RECOVERY] Executed slots from MT5: {sorted(executed_slots)}")
    else:
        print(f"[RECOVERY] No slots executed yet today")
    
    return executed_slots

def recover_withdrawals_from_mt5(ist_day: date):
    global WITHDRAWAL_DETECTED_TODAY
    
    day_start = datetime.combine(ist_day, dt_time(0,0), tzinfo=IST_TZ)
    day_end = datetime.combine(ist_day, dt_time(23,59,59), tzinfo=IST_TZ)
    
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    start_server = (day_start + timedelta(minutes=delta_min)).replace(tzinfo=None)
    end_server = (day_end + timedelta(minutes=delta_min)).replace(tzinfo=None)
    
    deals = mt5.history_deals_get(start_server, end_server) or []
    
    for deal in deals:
        deal_type = int(getattr(deal, "type", -1))
        if deal_type == 2:
            profit = float(getattr(deal, "profit", 0.0))
            if profit < 0:
                WITHDRAWAL_DETECTED_TODAY = True
                print(f"[RECOVERY] WARNING: Withdrawal detected earlier today: ${abs(profit):.2f}")
                return

# ===== Lot sizing =====
def lot_size_simple_compound() -> float:
    acc = mt5.account_info()
    bal = float(getattr(acc, "balance", 0.0)) if acc else 100.0
    
    # Use 0.04 lots until balance reaches $500
    if bal < 500:
        return 0.04
    
    # After $500: normal compounding (0.05 for $500, 0.06 for $600, etc.)
    starting_balance = 500
    starting_lots = 0.05
    
    increments = int((bal - starting_balance) / 100)
    lots = starting_lots + (increments * 0.01)
    
    return max(MIN_LOT, min(lots, 10.0))

def calculate_partial_close_lots(original_lots: float) -> Tuple[float, float]:
    info = mt5.symbol_info(SYMBOL)
    lot_step = getattr(info, "volume_step", 0.01) if info else 0.01
    min_lot = getattr(info, "volume_min", 0.01) if info else 0.01
    
    if original_lots == 0.01:
        return 0.01, 0.0
    elif original_lots == 0.03:
        return 0.02, 0.01
    elif original_lots == 0.05:
        return 0.03, 0.02
    
    lots_as_int = int(original_lots / lot_step)
    is_even = (lots_as_int % 2 == 0)
    
    if is_even:
        close_lots = original_lots * PARTIAL_CLOSE_PERCENTAGE_EVEN
    else:
        close_lots = original_lots * PARTIAL_CLOSE_PERCENTAGE_ODD
    
    close_lots = round(close_lots / lot_step) * lot_step
    close_lots = max(min_lot, close_lots)
    close_lots = min(close_lots, original_lots - min_lot)
    
    remaining_lots = original_lots - close_lots
    remaining_lots = round(remaining_lots / lot_step) * lot_step
    remaining_lots = max(0, remaining_lots)
    
    return round(close_lots, 2), round(remaining_lots, 2)

def calculate_current_pips(entry_price: float, current_price: float, side: str) -> float:
    if side == "BUY":
        pips = (current_price - entry_price) / PIP_SIZE
    else:
        pips = (entry_price - current_price) / PIP_SIZE
    return pips

def compute_sl_tp(entry: float, side: str) -> Tuple[float, float]:
    sl_dist = SL_PIPS * PIP_SIZE
    tp_dist = TP_PIPS * PIP_SIZE
    if side == "BUY":
        sl = entry - sl_dist
        tp = entry + tp_dist
    else:
        sl = entry + sl_dist
        tp = entry - tp_dist
    info = mt5.symbol_info(SYMBOL)
    digits = getattr(info, "digits", 2) if info else 2
    return round(sl, digits), round(tp, digits)

def margin_ok(order_type, volume, price) -> bool:
    try:
        mr = mt5.order_calc_margin(order_type, SYMBOL, volume, price)
        acc = mt5.account_info()
        return (mr is not None) and (acc is not None) and (mr <= acc.margin_free)
    except Exception:
        return False

# ===== Trading logic =====
def place_trade(signal: str, volume: float, tag: str):
    tick = mt5.symbol_info_tick(SYMBOL)
    if not tick:
        print("[TRADE] No tick data")
        return None
    
    price = tick.ask if signal == "BUY" else tick.bid
    sl, tp = compute_sl_tp(price, signal)
    order_type = mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL
    
    if not margin_ok(order_type, volume, price):
        print("[MARGIN] Insufficient margin")
        return None
    
    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": SYMBOL,
        "volume": float(volume),
        "type": order_type,
        "price": price,
        "sl": sl,
        "tp": tp,
        "deviation": DEVIATION,
        "magic": MAGIC,
        "comment": f"60pip_bot|{tag}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC
    }
    result = mt5.order_send(req)
    print(f"[TRADE] {signal} vol={volume:.2f} @{price:.3f} SL={sl:.3f} TP={tp:.3f} ret={getattr(result,'retcode',None)}")
    return result

# ===== Opening price capture =====
def opening_price_monitor_thread():
    print("[OPEN_MONITOR] Started - capturing tick prices at candle open times")
    print(f"[OPEN_MONITOR] Will capture opens at: {REQUIRED_OPENING_TIMES}")
    
    last_captured = {}
    
    while RUN_HEARTBEAT.is_set():
        try:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST_TZ)
            ist_day = now_ist.date()
            current_time_key = now_ist.strftime("%H:%M")
            current_second = now_ist.second
            
            if current_time_key in REQUIRED_OPENING_TIMES:
                if current_second <= 3:
                    capture_id = f"{current_time_key}_{ist_day}"
                    
                    if capture_id not in last_captured:
                        tick = mt5.symbol_info_tick(SYMBOL)
                        if tick:
                            tick_midpoint = (float(tick.bid) + float(tick.ask)) / 2.0
                            
                            with CANDLE_OPENS_LOCK:
                                CANDLE_OPENS[current_time_key] = tick_midpoint
                            
                            last_captured[capture_id] = tick_midpoint
                            print(f"[OPEN_CAPTURE] OK {current_time_key}:{current_second:02d} IST: Opening = {tick_midpoint:.3f}")
                            
                            save_tick_to_ledger(ist_day, current_time_key, tick_midpoint, source="tick_open")
                    
                    elif current_second <= 2:
                        tick = mt5.symbol_info_tick(SYMBOL)
                        if tick:
                            tick_midpoint = (float(tick.bid) + float(tick.ask)) / 2.0
                            
                            if abs(tick_midpoint - last_captured[capture_id]) > 0.01:
                                with CANDLE_OPENS_LOCK:
                                    CANDLE_OPENS[current_time_key] = tick_midpoint
                                last_captured[capture_id] = tick_midpoint
                                print(f"[OPEN_CAPTURE] UPDATED {current_time_key}:{current_second:02d} IST: Opening = {tick_midpoint:.3f}")
                                
                                save_tick_to_ledger(ist_day, current_time_key, tick_midpoint, source="tick_open_updated")
            
            if current_second == 0 and now_ist.minute == 0:
                current_date = ist_day
                old_keys = [k for k in last_captured if not k.endswith(str(current_date))]
                for k in old_keys:
                    del last_captured[k]
            
            _time.sleep(0.2)
            
        except Exception as e:
            print(f"[OPEN_MONITOR] Error: {e}")
            _time.sleep(1)

def backfill_todays_opens(ist_day: date):
    now_ist = datetime.now(timezone.utc).astimezone(IST_TZ)
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    
    backfilled_count = 0
    ledger_used_count = 0
    mt5_used_count = 0
    
    tick_ledger = load_tick_ledger(ist_day)
    
    for time_str in REQUIRED_OPENING_TIMES:
        hh, mm = parse_ist_hhmm(time_str)
        candle_time = datetime.combine(ist_day, dt_time(hh, mm), tzinfo=IST_TZ)
        
        if candle_time > now_ist:
            continue
        
        with CANDLE_OPENS_LOCK:
            if time_str in CANDLE_OPENS:
                continue
        
        if time_str in tick_ledger.get("ticks", {}):
            ledger_price = tick_ledger["ticks"][time_str]["price"]
            with CANDLE_OPENS_LOCK:
                CANDLE_OPENS[time_str] = ledger_price
            print(f"[BACKFILL] OK {time_str} opening: {ledger_price:.3f} (from tick ledger)")
            backfilled_count += 1
            ledger_used_count += 1
            continue
        
        server_time = (candle_time + timedelta(minutes=delta_min)).replace(tzinfo=None)
        
        rates = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, server_time, 1)
        if rates is not None and len(rates) > 0:
            candle_open = float(rates[0]["open"])
            with CANDLE_OPENS_LOCK:
                CANDLE_OPENS[time_str] = candle_open
            print(f"[BACKFILL] OK {time_str} opening: {candle_open:.3f} (from MT5)")
            
            save_tick_to_ledger(ist_day, time_str, candle_open, source="backfill_mt5")
            
            backfilled_count += 1
            mt5_used_count += 1
    
    if backfilled_count > 0:
        print(f"[BACKFILL] Completed: {backfilled_count} opens backfilled "
              f"({ledger_used_count} from ledger, {mt5_used_count} from MT5)")
    else:
        print(f"[BACKFILL] No backfill needed")

def get_signal_from_live_opens(ist_hhmm: str) -> Tuple[Optional[str], Optional[str]]:
    hh, mm = parse_ist_hhmm(ist_hhmm)
    opening_dt = datetime.combine(date.today(), dt_time(hh, mm)) - timedelta(minutes=5)
    opening_key = opening_dt.strftime("%H:%M")
    
    with CANDLE_OPENS_LOCK:
        candle_open = CANDLE_OPENS.get(opening_key)
    
    if candle_open is None:
        skip_reason = f"No opening price captured for {opening_key}"
        print(f"[SIGNAL_ERROR] ERROR {skip_reason}")
        return None, skip_reason
    
    tick = mt5.symbol_info_tick(SYMBOL)
    if not tick:
        skip_reason = "No tick data available"
        print(f"[SIGNAL_ERROR] {skip_reason}")
        return None, skip_reason
    
    current_bid = float(tick.bid)
    current_ask = float(tick.ask)
    current_midpoint = (current_bid + current_ask) / 2.0
    
    threshold = 0.02 * PIP_SIZE
    diff = current_midpoint - candle_open
    diff_pips = diff / PIP_SIZE
    
    if diff >= threshold:
        signal = "BUY"
    elif diff <= -threshold:
        signal = "SELL"
    else:
        skip_reason = f"Price difference {abs(diff_pips):.3f} pips < 0.02 threshold"
        print(f"[SIGNAL_SKIP] {skip_reason}")
        return None, skip_reason
    
    print(f"[SIGNAL] ╔═══════════════════════════════╗")
    print(f"[SIGNAL] Trade Time: {ist_hhmm} IST")
    print(f"[SIGNAL] ┌─ OPENING @ {opening_key}:00")
    print(f"[SIGNAL] │  Tick captured: {candle_open:.3f}")
    print(f"[SIGNAL] └─ CURRENT @ {ist_hhmm} (NOW)")
    print(f"[SIGNAL]    MID: {current_midpoint:.3f}")
    print(f"[SIGNAL] Difference: {diff:.3f} ({diff_pips:+.1f} pips)")
    print(f"[SIGNAL] ➜ Decision: {signal}")
    print(f"[SIGNAL] ╚═══════════════════════════════╝")
    
    return signal, None

def verify_position_exists(result, expected_comment: str) -> Optional[int]:
    if not result or result.retcode != mt5.TRADE_RETCODE_DONE:
        return None
    
    _time.sleep(0.5)
    
    positions = mt5.positions_get(symbol=SYMBOL)
    if not positions:
        return None
    
    for p in positions:
        if getattr(p, "magic", 0) == MAGIC:
            return int(p.ticket)
    
    return None

# ===== Watcher thread =====
def watcher_thread(position_ticket: int, expected_side: str, tag: str, ist_day: date):
    global TRADES_CLOSED_TODAY, LAST_KNOWN_BALANCE
    start_time = _time.time()
    partial_close_executed = False
    original_volume = 0.0
    entry_price = 0.0
    
    print(f"[WATCHER] Started for position {position_ticket}")
    
    with LEDGER_LOCK:
        for key, entry in LEDGER_DATA["trades"].items():
            if entry["position_id"] == position_ticket:
                original_volume = entry["lot_size"]
                entry_price = entry["entry_price"]
                break
    
    while True:
        if (_time.time() - start_time) > (MAX_POSITION_HOLD_HOURS * 3600):
            print(f"[WATCHER] Timeout ({MAX_POSITION_HOLD_HOURS}h) for position {position_ticket}")
            break
        
        positions = mt5.positions_get(symbol=SYMBOL)
        pos = None
        if positions:
            for p in positions:
                if int(p.ticket) == position_ticket:
                    pos = p
                    break
        
        if not pos:
            print(f"[WATCHER] Position {position_ticket} closed")
            
            try:
                now = datetime.now(timezone.utc).astimezone().replace(tzinfo=None)
                start_query = now - timedelta(hours=1)
                
                deals = mt5.history_deals_get(start_query, now) or []
                
                net_profit = 0.0
                for d in deals:
                    if int(getattr(d, "position_id", 0)) == position_ticket:
                        net_profit += float(getattr(d, "profit", 0.0))
                
                acc = mt5.account_info()
                current_balance = float(acc.balance) if acc else 0.0
                
                outcome = "PROFIT" if net_profit > 0 else "LOSS"
                
                with TRADES_LOCK:
                    TRADES_CLOSED_TODAY += 1
                
                exit_time = datetime.now(timezone.utc).astimezone(IST_TZ).strftime("%H:%M")
                
                update_ledger_entry(position_ticket, ist_day, exit_time, outcome, net_profit, current_balance)
                
                with BALANCE_LOCK:
                    LAST_KNOWN_BALANCE = current_balance
                
                save_baseline(current_balance)
                
                print(f"[WATCHER] Position {position_ticket} finalized: {outcome} ${net_profit:.2f}")
                
            except Exception as e:
                print(f"[WATCHER] Error processing close: {e}")
            
            break
        
        # Check for partial close at +50 pips
        if not partial_close_executed and original_volume > 0 and entry_price > 0:
            current_profit = float(pos.profit)
            current_price = float(pos.price_current)
            current_pips = calculate_current_pips(entry_price, current_price, expected_side)
            
            if current_pips >= PARTIAL_CLOSE_TRIGGER_PIPS:
                close_lots, remaining_lots = calculate_partial_close_lots(original_volume)
                
                if close_lots > 0 and remaining_lots > 0:
                    print(f"[PARTIAL_CLOSE] Triggered at +{current_pips:.1f} pips for position {position_ticket}")
                    print(f"[PARTIAL_CLOSE] Closing {close_lots:.2f} lots, keeping {remaining_lots:.2f} lots")
                    
                    # Verify position still exists and has correct volume
                    current_pos_volume = float(pos.volume)
                    print(f"[PARTIAL_CLOSE] Current position volume: {current_pos_volume:.2f} lots")
                    
                    if current_pos_volume < close_lots:
                        print(f"[PARTIAL_CLOSE] ERROR: Not enough volume to close (need {close_lots:.2f}, have {current_pos_volume:.2f})")
                        partial_close_executed = True
                        continue
                    
                    # Get current tick for closing price
                    tick = mt5.symbol_info_tick(SYMBOL)
                    if not tick:
                        print(f"[PARTIAL_CLOSE] Failed to get tick data")
                        partial_close_executed = True
                        continue
                    
                    close_price = tick.bid if expected_side == "BUY" else tick.ask
                    close_type = mt5.ORDER_TYPE_SELL if expected_side == "BUY" else mt5.ORDER_TYPE_BUY
                    
                    print(f"[PARTIAL_CLOSE] Sending order: type={close_type}, volume={close_lots:.2f}, price={close_price:.3f}")
                    
                    req = {
                        "action": mt5.TRADE_ACTION_DEAL,
                        "symbol": SYMBOL,
                        "volume": float(close_lots),
                        "type": close_type,
                        "position": position_ticket,
                        "price": close_price,
                        "deviation": DEVIATION,
                        "magic": MAGIC,
                        "comment": f"partial_close|{tag}",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC
                    }
                    
                    result = mt5.order_send(req)
                    
                    if result is None:
                        # Check MT5 last error
                        error_code, error_msg = mt5.last_error()
                        print(f"[PARTIAL_CLOSE] FAILED - order_send returned None")
                        print(f"[PARTIAL_CLOSE] MT5 last_error: {error_code} - {error_msg}")
                        partial_close_executed = True
                        continue
                    
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        partial_close_executed = True
                        
                        with LEDGER_LOCK:
                            for key, entry in LEDGER_DATA["trades"].items():
                                if entry["position_id"] == position_ticket:
                                    entry["partial_close_triggered"] = True
                                    entry["partial_close_price"] = close_price
                                    entry["partial_close_lots"] = close_lots
                                    entry["remaining_lots"] = remaining_lots
                                    break
                            save_ledger(ist_day, LEDGER_DATA)
                        
                        print(f"[PARTIAL_CLOSE] OK Closed {close_lots:.2f} lots at {close_price:.3f}")
                    else:
                        error_code = result.retcode
                        error_msg = getattr(result, 'comment', 'No error message')
                        print(f"[PARTIAL_CLOSE] FAILED - MT5 Error {error_code}: {error_msg}")
                        partial_close_executed = True
                else:
                    print(f"[PARTIAL_CLOSE] Cannot execute - close_lots={close_lots}, remaining_lots={remaining_lots}")
                    partial_close_executed = True  # Mark as executed to avoid retry
        
        _time.sleep(1)
    
    with WATCHERS_LOCK:
        if position_ticket in ACTIVE_WATCHERS:
            del ACTIVE_WATCHERS[position_ticket]

def spawn_watcher(ticket: int, side: str, tag: str, ist_day: date):
    thr = threading.Thread(target=watcher_thread, args=(ticket, side, tag, ist_day), daemon=True)
    thr.start()
    with WATCHERS_LOCK:
        ACTIVE_WATCHERS[ticket] = thr

def recover_orphaned_positions(ist_day: date):
    positions = mt5.positions_get(symbol=SYMBOL)
    if not positions:
        return
    
    for p in positions:
        if getattr(p, "magic", 0) != MAGIC:
            continue
        
        ticket = int(p.ticket)
        
        with WATCHERS_LOCK:
            if ticket in ACTIVE_WATCHERS:
                continue
        
        side = "BUY" if getattr(p, "type", 0) == mt5.POSITION_TYPE_BUY else "SELL"
        comment = getattr(p, "comment", "")
        tag = comment.split("|")[-1] if "|" in comment else f"recovered_{ticket}"
        
        print(f"[RECOVERY] Found orphaned position {ticket} ({side}), spawning watcher...")
        spawn_watcher(ticket, side, tag, ist_day)

# ===== Heartbeat thread =====
def heartbeat_thread():
    global NEXT_IST_SLOT, NEXT_SERVER_TIME, LAST_KNOWN_BALANCE
    
    while RUN_HEARTBEAT.is_set():
        try:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST_TZ)
            ist_day = now_ist.date()
            
            delta_min = ist_to_server_delta_minutes_for_date(ist_day)
            now_server = (now_ist + timedelta(minutes=delta_min)).replace(tzinfo=None)
            
            day_display = now_ist.strftime("%a %d-%b")
            
            next_ist = NEXT_IST_SLOT
            next_server = NEXT_SERVER_TIME
            
            today_sched, _ = build_server_schedule_for_day(ist_day)
            fired = sum(1 for t, s in today_sched.items() if now_server >= s)
            total = len(today_sched)
            
            with TRADES_LOCK:
                opened = TRADES_OPENED_TODAY
                closed = TRADES_CLOSED_TODAY
            
            with BALANCE_LOCK:
                current_bal = LAST_KNOWN_BALANCE
            
            if current_bal == 0.0:
                try:
                    acc = mt5.account_info()
                    if acc:
                        current_bal = float(acc.balance)
                        with BALANCE_LOCK:
                            LAST_KNOWN_BALANCE = current_bal
                except Exception:
                    pass
            
            withdrawal_flag = "WARNING" if WITHDRAWAL_DETECTED_TODAY else "OK"
            
            is_monday_no_trading = SKIP_MONDAY and now_ist.weekday() == 0
            
            if next_server is None or is_monday_no_trading:
                if is_monday_no_trading:
                    end_message = "no trading today"
                else:
                    end_message = "no more slots"
                    
                line = (
                    f"[HB] {day_display} | {now_ist.strftime('%H:%M:%S')} | "
                    f"slots {fired}/{total} | "
                    f"trades O:{opened}/6 C:{closed}/6 | "
                    f"balance ${current_bal:.2f} {withdrawal_flag} | "
                    f"{end_message}"
                )
            else:
                secs = max(0, int((next_server - now_server).total_seconds()))
                hh = secs // 3600
                mm = (secs % 3600) // 60
                ss = secs % 60
                line = (
                    f"[HB] {day_display} | {now_ist.strftime('%H:%M:%S')} | "
                    f"slots {fired}/{total} | "
                    f"trades O:{opened}/6 C:{closed}/6 | "
                    f"balance ${current_bal:.2f} {withdrawal_flag} | "
                    f"next: {next_ist} in {hh:02d}:{mm:02d}:{ss:02d}"
                )
            
            print(line, flush=True)
            
        except Exception:
            pass
        _time.sleep(1)

# ===== Next slot tracking =====
NEXT_IST_SLOT = None
NEXT_SERVER_TIME = None

def update_next_slot(now_server: datetime, today_sched: dict, executed: set, ist_day: date, delta_min: int):
    global NEXT_IST_SLOT, NEXT_SERVER_TIME
    
    remaining = [(t, s) for t, s in today_sched.items() if t not in executed and s > now_server]
    
    if remaining:
        remaining.sort(key=lambda x: x[1])
        next_ist, next_server = remaining[0]
        NEXT_IST_SLOT = next_ist
        NEXT_SERVER_TIME = next_server
    else:
        NEXT_IST_SLOT = None
        NEXT_SERVER_TIME = None

# ===== Main loop =====
def main():
    global BALANCE_QUERIED_TODAY, WITHDRAWAL_DETECTED_TODAY
    global LEDGER_DATA, DAILY_OPENING_BALANCE, DAILY_CLOSING_BALANCE
    global SKIPPED_SLOTS, LAST_KNOWN_BALANCE
    global TRADES_OPENED_TODAY, TRADES_CLOSED_TODAY, OPEN_TRADES_GLOBAL
    
    init_mt5()
    print("[INIT] MT5 initialized for Vantage")
    print(f"[INIT] SIMPLIFIED VERSION with:")
    print(f"[INIT]   - 6 trading times from backtesting results")
    print(f"[INIT]   - No email notifications")
    print(f"[INIT]   - No log file generation")
    print(f"[INIT]   - Tick Price Ledger (accurate backfill from live data)")
    print(f"[INIT]   - Internal Ledger (trade tracking)")
    print(f"[INIT]   - 24/7 withdrawal detection via MT5 history")
    print(f"[INIT]   - Crash recovery (rebuilds state from MT5)")
    print(f"[INIT]   - Balance query at {BALANCE_QUERY_TIME} IST")
    print(f"[INIT]   - Signal threshold: 0.02 pips")
    print(f"[INIT]   - Monday trading: {'DISABLED' if SKIP_MONDAY else 'ENABLED'}")
    print(f"[INIT]   - Watcher timeout: {MAX_POSITION_HOLD_HOURS}h")
    print(f"[INIT]   - Trading times: {IST_TRADE_TIMES}")
    
    cleanup_old_tick_ledgers()
    
    now_ist = datetime.now(timezone.utc).astimezone(IST_TZ)
    current_ist_day = now_ist.date()
    
    day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][current_ist_day.weekday()]
    print(f"\n[INIT] Today is {day_name}, {fmt_date(current_ist_day)}")
    if SKIP_MONDAY and current_ist_day.weekday() == 0:
        print(f"[INIT] ⚠️  MONDAY - All 6 trades will be skipped today (SKIP_MONDAY=True)")
        print(f"[INIT] Bot will still run and monitor balance\n")
    
    ledger_file = get_ledger_filepath(current_ist_day)
    if ledger_file.exists():
        print(f"[LEDGER] Loading existing ledger for {fmt_date(current_ist_day)}")
        LEDGER_DATA = load_ledger(current_ist_day)
    else:
        print(f"[LEDGER] Creating new ledger for {fmt_date(current_ist_day)}")
        LEDGER_DATA = load_ledger(current_ist_day)
        
        print(f"[LEDGER] Checking if we need to rebuild from MT5...")
        rebuilt_ledger = rebuild_ledger_from_mt5(current_ist_day)
        if rebuilt_ledger["trades"]:
            LEDGER_DATA = rebuilt_ledger
            save_ledger(current_ist_day, LEDGER_DATA)
            print(f"[LEDGER] OK Ledger rebuilt from MT5")
    
    if LEDGER_DATA.get("opening_balance", 0.0) > 0:
        DAILY_OPENING_BALANCE = LEDGER_DATA["opening_balance"]
    else:
        DAILY_OPENING_BALANCE = load_baseline()
        LEDGER_DATA["opening_balance"] = DAILY_OPENING_BALANCE
        save_ledger(current_ist_day, LEDGER_DATA)
    
    print(f"[LEDGER] Opening balance: ${DAILY_OPENING_BALANCE:.2f}")
    
    try:
        acc = mt5.account_info()
        if acc:
            with BALANCE_LOCK:
                LAST_KNOWN_BALANCE = float(acc.balance)
            print(f"[INIT] Current balance initialized: ${LAST_KNOWN_BALANCE:.2f}")
        else:
            print(f"[INIT] WARNING: Could not fetch current balance from MT5")
    except Exception as e:
        print(f"[INIT] ERROR: Failed to initialize balance: {e}")
    
    print(f"\n[RECOVERY] Starting crash recovery...")
    recover_trading_state_from_mt5(current_ist_day)
    executed_ist_today = recover_executed_slots_from_mt5(current_ist_day)
    recover_withdrawals_from_mt5(current_ist_day)
    recover_orphaned_positions(current_ist_day)
    print(f"[RECOVERY] Complete\n")
    
    today_server_sched, delta_min = build_server_schedule_for_day(current_ist_day)
    
    hb = threading.Thread(target=heartbeat_thread, daemon=True)
    hb.start()
    
    open_monitor = threading.Thread(target=opening_price_monitor_thread, daemon=True)
    open_monitor.start()
    
    bal_monitor = threading.Thread(target=balance_monitor_thread, daemon=True)
    bal_monitor.start()
    
    backfill_todays_opens(current_ist_day)
    
    now_server = (now_ist + timedelta(minutes=delta_min)).replace(tzinfo=None)
    update_next_slot(now_server, today_server_sched, executed_ist_today, current_ist_day, delta_min)
    
    try:
        while True:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST_TZ)
            ist_day = now_ist.date()
            current_time_key = now_ist.strftime("%H:%M")
            
            if current_time_key == BALANCE_QUERY_TIME and not BALANCE_QUERIED_TODAY:
                query_and_set_opening_balance(ist_day)
            
            if ist_day != current_ist_day:
                executed_ist_today.clear()
                current_ist_day = ist_day
                today_server_sched, delta_min = build_server_schedule_for_day(ist_day)
                BALANCE_QUERIED_TODAY = False
                WITHDRAWAL_DETECTED_TODAY = False
                
                with TRADES_LOCK:
                    TRADES_OPENED_TODAY = 0
                    TRADES_CLOSED_TODAY = 0
                
                with CANDLE_OPENS_LOCK:
                    CANDLE_OPENS.clear()
                
                with SKIPPED_SLOTS_LOCK:
                    SKIPPED_SLOTS.clear()
                
                LEDGER_DATA = load_ledger(ist_day)
                DAILY_OPENING_BALANCE = load_baseline()
                LEDGER_DATA["opening_balance"] = DAILY_OPENING_BALANCE
                save_ledger(ist_day, LEDGER_DATA)
                
                print(f"\n{'='*60}")
                print(f"[DAY] New IST day: {fmt_date(ist_day)}")
                print(f"[DAY] Day of week: {['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][ist_day.weekday()]}")
                print(f"[DAY] Server-IST delta: {delta_min:+} minutes")
                print(f"[DAY] Opening balance: ${DAILY_OPENING_BALANCE:.2f}")
                if SKIP_MONDAY and ist_day.weekday() == 0:
                    print(f"[DAY] ⚠️  MONDAY - All trading disabled (SKIP_MONDAY=True)")
                print(f"{'='*60}\n")
                
                cleanup_old_tick_ledgers()
                
                backfill_todays_opens(ist_day)
                
                update_next_slot(
                    (now_ist + timedelta(minutes=delta_min)).replace(tzinfo=None),
                    today_server_sched, executed_ist_today, ist_day, delta_min
                )
            
            try:
                positions = mt5.positions_get(symbol=SYMBOL)
                count = 0
                if positions:
                    for p in positions:
                        if getattr(p, "magic", 0) == MAGIC:
                            count += 1
                OPEN_TRADES_GLOBAL = count
            except Exception:
                OPEN_TRADES_GLOBAL = 0
            
            delta_min = ist_to_server_delta_minutes_for_date(ist_day)
            now_server = (now_ist + timedelta(minutes=delta_min)).replace(tzinfo=None)
            
            is_monday = ist_day.weekday() == 0
            
            for ist_hhmm, fire_dt_server in list(today_server_sched.items()):
                if ist_hhmm in executed_ist_today:
                    continue
                
                lag = (now_server - fire_dt_server).total_seconds()
                
                if -EARLY_TRIGGER_WINDOW_SECONDS <= lag <= FIRE_WINDOW_SECONDS:
                    if SKIP_MONDAY and is_monday:
                        skip_reason = "Monday trading disabled (SKIP_MONDAY=True)"
                        with SKIPPED_SLOTS_LOCK:
                            SKIPPED_SLOTS[ist_hhmm] = skip_reason
                        print(f"[SKIP] {ist_hhmm} IST: {skip_reason}")
                        executed_ist_today.add(ist_hhmm)
                        update_next_slot(now_server, today_server_sched, executed_ist_today, ist_day, delta_min)
                        continue
                    
                    signal, skip_reason = get_signal_from_live_opens(ist_hhmm)
                    
                    if not signal:
                        if skip_reason:
                            with SKIPPED_SLOTS_LOCK:
                                SKIPPED_SLOTS[ist_hhmm] = skip_reason
                            print(f"[SKIP] {ist_hhmm} IST: {skip_reason}")
                        
                        executed_ist_today.add(ist_hhmm)
                        update_next_slot(now_server, today_server_sched, executed_ist_today, ist_day, delta_min)
                        continue
                    
                    vol = lot_size_simple_compound()
                    tag = f"{ist_day.isoformat()}_{ist_hhmm}"
                    
                    print(f"[SIGNAL] IST {ist_hhmm} (Server: {now_server.strftime('%H:%M:%S')}) -> {signal}, vol={vol:.2f}")
                    
                    result = place_trade(signal, vol, tag)
                    executed_ist_today.add(ist_hhmm)
                    update_next_slot(now_server, today_server_sched, executed_ist_today, ist_day, delta_min)
                    
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        pos_ticket = verify_position_exists(result, f"60pip_bot|{tag}")
                        
                        if pos_ticket:
                            with TRADES_LOCK:
                                TRADES_OPENED_TODAY += 1
                                trade_number = TRADES_OPENED_TODAY
                            
                            tick = mt5.symbol_info_tick(SYMBOL)
                            entry_price = tick.ask if signal == "BUY" else tick.bid
                            sl, tp = compute_sl_tp(entry_price, signal)
                            
                            create_ledger_entry(
                                pos_ticket, ist_day, trade_number,
                                ist_hhmm, entry_price, sl, tp,
                                vol, signal, tag
                            )
                            
                            spawn_watcher(pos_ticket, signal, tag, ist_day)
                            print(f"[SUCCESS] Position {pos_ticket} opened and watcher spawned")
                        else:
                            print(f"[ERROR] Trade reported success but position not found for {tag}")
                            with SKIPPED_SLOTS_LOCK:
                                SKIPPED_SLOTS[ist_hhmm] = "Trade placement verification failed"
                    else:
                        print(f"[TRADE] Failed: ret={getattr(result, 'retcode', None)}")
                        with SKIPPED_SLOTS_LOCK:
                            SKIPPED_SLOTS[ist_hhmm] = f"Trade placement failed - MT5 error code {getattr(result, 'retcode', 'unknown')}"
                
                elif lag > FIRE_WINDOW_SECONDS:
                    with SKIPPED_SLOTS_LOCK:
                        if ist_hhmm not in SKIPPED_SLOTS:
                            SKIPPED_SLOTS[ist_hhmm] = f"Slot expired (missed time window by {int(lag)}s)"
                    
                    executed_ist_today.add(ist_hhmm)
                    update_next_slot(now_server, today_server_sched, executed_ist_today, ist_day, delta_min)
            
            _time.sleep(1)
    
    except KeyboardInterrupt:
        print("\n[STOP] Bot interrupted by user")
    finally:
        RUN_HEARTBEAT.clear()
        _time.sleep(0.5)
        
        with WATCHERS_LOCK:
            active = list(ACTIVE_WATCHERS.values())
        
        for thr in active:
            if thr.is_alive():
                thr.join(timeout=2.0)
        
        shutdown_mt5()
        print("[SHUTDOWN] MT5 closed")

if __name__ == "__main__":
    main()
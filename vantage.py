"""
MT5 Trading Bot for XAU/USD - Vantage Demo Configuration
60-pip TP, 20-pip SL, Simple Compounding from $500

ENHANCED VERSION - New Features:
- Smart report generation: Waits for all trades to close, then 2-min buffer + verification
- Balance monitor: 24/7 withdrawal detection via MT5 history (deal_type=2)
- Crash recovery: Rebuilds trade counters and executed slots from MT5 on restart
- Balance query at 05:45 IST for accurate opening balance
- Reduced watcher timeout to 2 hours (trades close in 5-15 mins)
- INTERNAL LEDGER: Tracks all trades independently, uses as primary source for reports
- SKIP TRACKING: 0.02 pip threshold, sends reports even with missed trades
- CRASH-PROOF: Triple-layer error handling prevents bot crashes
- FIXED: UTF-8 encoding for all CSV writes to prevent charmap codec errors
- PARTIAL CLOSE: Auto-closes 50%/66% at +50 pips, remaining continues to TP/SL
- TIMING FIX: Trades fire exactly at scheduled time (no early triggers)
- TICK LEDGER: Real-time price capture with persistent JSON storage for accurate backfill
"""
import MetaTrader5 as mt5
from datetime import datetime, timedelta, date, time as dt_time, timezone
import time as _time
from pathlib import Path
import csv
import threading
from threading import Event, Lock
import smtplib, ssl
from email.message import EmailMessage
from typing import Optional, Tuple, List, Dict
import json
import re

# ========= CONFIG =========

# MT5 Credentials - UPDATE THESE
ACCOUNT  = 11293958  # Your Vantage demo account
PASSWORD = "N1kunjAa@231023"
SERVER   = "VantageInternational-Demo"
SYMBOL   = "XAUUSD+"

# IST Trading schedule
IST_TRADE_TIMES = [
    "05:50","06:30","08:05","08:55","09:25","09:45","10:05","10:35",
    "18:30","18:35","19:00","19:05","19:30","19:35","19:45","20:05","20:30","20:35"
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

# Email - UPDATE THESE
EMAIL_SENDER   = "n1kunj7r1v3d1@gmail.com"
EMAIL_PASSWORD = "vzed scso otcq cctn"
EMAIL_RECEIVER = "n1kunj7r1v3d1@gmail.com"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465

# Logging
LOG_DIR = Path("trade_logs")
LOG_DIR.mkdir(exist_ok=True)

# Heartbeat logging
HEARTBEAT_LOG_DIR = LOG_DIR / "heartbeat_logs"
HEARTBEAT_LOG_DIR.mkdir(exist_ok=True)
HEARTBEAT_LOG_EMAIL_TIME = "00:05"  # Email previous day's log at 00:05 IST
HEARTBEAT_LOG_EMAILED_TODAY = False

LOG_HEADER = [
    "(Date)","(Trade_Number)","(Entry Time)","(Entry price)","(SL Price)","(TP Price)",
    "(Partial Close Price)","(Lot Size)","(Trade Outcome)","(Partial Close)","(PnL)","(Account balance)"
]
DATE_FMT = "%d-%m-%Y"

# Baseline management
BASELINE_STATE_FILE = LOG_DIR / "baseline_state.json"
WITHDRAWAL_LOG = LOG_DIR / "withdrawals_history.csv"
DEFAULT_BASELINE = 500.0

# Lot sizing
MIN_LOT = 0.01
MAX_LOSS_PER_TRADE_USD = 100.0

# Timezones
IST_TZ = timezone(timedelta(hours=5, minutes=30))
MANUAL_SERVER_DELTA_MINUTES: Optional[int] = -210  # GMT+2 to IST

# Trigger windows - TIMING ADJUSTED TO FIRE 1 SECOND BEFORE SCHEDULED TIME
FIRE_WINDOW_SECONDS = 10  # Reduced from 60 to 10 for precision
EARLY_TRIGGER_WINDOW_SECONDS = 1  # Changed from 0 to 1 - fires 1 second before scheduled time

# Watcher config
MAX_POSITION_HOLD_HOURS = 2  # Trades close in 5-15 mins typically

# Report generation config
REPORT_HISTORY_WAIT_MINUTES = 5  # Wait 5 mins after all trades close
REPORT_VERIFICATION_RETRIES = 5  # Retry up to 5 times if history incomplete

# Balance monitor config
BALANCE_MONITOR_INTERVAL_SECONDS = 120  # Check every 2 minutes

# Balance query timing
BALANCE_QUERY_TIME = "05:45"  # Query balance at 05:45 IST (5 mins before first trade)

# Trading days configuration
SKIP_THURSDAY = True  # Set to False to enable Thursday trading
TRADING_DAYS = [0, 1, 2, 4]  # Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4
# If SKIP_THURSDAY=True, Thursday (3) is excluded from trading

# NEW: Skip tracking and report timing
SKIPPED_SLOTS = {}  # Track skipped slots with reasons
SKIPPED_SLOTS_LOCK = Lock()
LAST_SLOT_TIME = "20:35"  # Last scheduled slot
REPORT_WAIT_AFTER_LAST_CLOSE = 120  # 2 minutes in seconds

# NEW: Tick Price Ledger Configuration
TICK_LEDGER_DIR = LOG_DIR / "tick_ledger"
TICK_LEDGER_DIR.mkdir(exist_ok=True)
TICK_LEDGER_RETENTION_DAYS = 7  # Keep tick ledgers for 7 days

# Shared state
OPEN_TRADES_GLOBAL = 0
EMAIL_SENT_TODAY = False
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

# Report generation state
REPORT_WAIT_TRIGGERED = False
REPORT_WAIT_START_TIME = None
REPORT_WAIT_LOCK = Lock()
LAST_SLOT_CLOSED = False
LAST_SLOT_CLOSE_TIME = None

# NEW: Internal Ledger Management
LEDGER_DATA = {}
LEDGER_LOCK = Lock()
DAILY_OPENING_BALANCE = 0.0
DAILY_CLOSING_BALANCE = 0.0

# Heartbeat logging
HEARTBEAT_LOG_FILE = None
HEARTBEAT_LOG_LOCK = Lock()

# ===== Utility =====
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
    if not mt5.initialize():
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

# ===== Email =====
def send_email(subject: str, body: str, filepath: Optional[Path] = None):
    try:
        msg = EmailMessage()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = subject
        msg.set_content(body)
        if filepath and filepath.exists():
            with open(filepath, "rb") as f:
                msg.add_attachment(f.read(), maintype="application", subtype="csv", filename=filepath.name)
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"[EMAIL] Sent: {subject} {'(with attachment)' if filepath else ''}")
    except Exception as e:
        print(f"[EMAIL] Failed to send: {e}")
        if filepath:
            backup = LOG_DIR / "failed_emails" / f"{filepath.stem}_BACKUP_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            backup.parent.mkdir(exist_ok=True)
            try:
                import shutil
                shutil.copy(filepath, backup)
                print(f"[EMAIL] Backup saved: {backup}")
            except Exception:
                pass

# ===== Heartbeat Logging Functions =====

def get_heartbeat_log_filepath(ist_day: date) -> Path:
    """Get the heartbeat log file path for a specific date."""
    return HEARTBEAT_LOG_DIR / f"heartbeat_{fmt_date(ist_day)}.txt"

def init_heartbeat_log(ist_day: date):
    """Initialize heartbeat log file for the day."""
    global HEARTBEAT_LOG_FILE
    
    log_path = get_heartbeat_log_filepath(ist_day)
    
    try:
        with HEARTBEAT_LOG_LOCK:
            HEARTBEAT_LOG_FILE = open(log_path, 'a', encoding='utf-8', buffering=1)  # Line buffered
            
            # Write header if new file
            if log_path.stat().st_size == 0:
                day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][ist_day.weekday()]
                HEARTBEAT_LOG_FILE.write(f"=== Heartbeat Log for {day_name}, {fmt_date(ist_day)} ===\n")
                HEARTBEAT_LOG_FILE.write(f"Started: {datetime.now(timezone.utc).astimezone(IST_TZ).strftime('%H:%M:%S')} IST\n")
                
                if SKIP_THURSDAY and ist_day.weekday() == 3:
                    HEARTBEAT_LOG_FILE.write(f"WARNING THURSDAY - All trading disabled (SKIP_THURSDAY=True)\n")
                
                HEARTBEAT_LOG_FILE.write(f"\n")
        
        print(f"[HEARTBEAT_LOG] Initialized: {log_path.name}")
    except Exception as e:
        print(f"[HEARTBEAT_LOG] ERROR: Failed to initialize log file: {e}")

def close_heartbeat_log():
    """Close current heartbeat log file."""
    global HEARTBEAT_LOG_FILE
    
    try:
        with HEARTBEAT_LOG_LOCK:
            if HEARTBEAT_LOG_FILE:
                HEARTBEAT_LOG_FILE.write(f"\n--- End of Day ---\n")
                HEARTBEAT_LOG_FILE.close()
                HEARTBEAT_LOG_FILE = None
    except Exception as e:
        print(f"[HEARTBEAT_LOG] ERROR: Failed to close log file: {e}")

def log_to_heartbeat(message: str):
    """Write a message to both console and heartbeat log file."""
    global HEARTBEAT_LOG_FILE
    
    # Print to console
    print(message, flush=True)
    
    # Write to log file
    try:
        with HEARTBEAT_LOG_LOCK:
            if HEARTBEAT_LOG_FILE:
                HEARTBEAT_LOG_FILE.write(message + '\n')
    except Exception as e:
        # Don't let logging errors crash the bot
        pass

def email_heartbeat_log(ist_day: date):
    """Email the heartbeat log for a specific day."""
    log_path = get_heartbeat_log_filepath(ist_day)
    
    if not log_path.exists():
        print(f"[HEARTBEAT_EMAIL] No log file found for {fmt_date(ist_day)}")
        return
    
    try:
        day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][ist_day.weekday()]
        subject = f"Heartbeat Log - {day_name} {fmt_date(ist_day)}"
        
        # Read file size
        file_size_mb = log_path.stat().st_size / (1024 * 1024)
        
        body = (
            f"Attached is the complete heartbeat log for {day_name}, {fmt_date(ist_day)}.\n\n"
            f"Log file: {log_path.name}\n"
            f"File size: {file_size_mb:.2f} MB\n\n"
            f"This log contains every heartbeat message and system event from the entire trading day.\n"
            f"Use Ctrl+F to search for specific times, trades, or events."
        )
        
        # Rename to .txt for email compatibility
        send_email(subject, body, log_path)
        print(f"[HEARTBEAT_EMAIL] Sent log for {fmt_date(ist_day)}")
        
    except Exception as e:
        print(f"[HEARTBEAT_EMAIL] ERROR: Failed to email log: {e}")

# ===== NEW: Tick Price Ledger Functions =====

def get_tick_ledger_path(d: date) -> Path:
    """Get the tick ledger file path for a specific date."""
    return TICK_LEDGER_DIR / f"tick_ledger_{d.isoformat()}.json"

def load_tick_ledger(d: date) -> Dict:
    """Load tick ledger for a specific date."""
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
    """
    Save a tick price to the ledger immediately.
    
    Args:
        d: Date of the tick
        time_key: Time in HH:MM format (opening time, not trade time)
        price: The captured price
        source: Source of the data ('live', 'backfill', etc.)
    """
    try:
        ledger_path = get_tick_ledger_path(d)
        
        # Load existing ledger
        if ledger_path.exists():
            with open(ledger_path, 'r', encoding='utf-8') as f:
                ledger = json.load(f)
        else:
            ledger = {"date": d.isoformat(), "ticks": {}, "metadata": {}}
        
        # Update or add the tick
        if time_key not in ledger["ticks"]:
            ledger["ticks"][time_key] = {
                "price": price,
                "source": source,
                "captured_at": datetime.now(IST_TZ).isoformat(),
                "updates": []
            }
            print(f"[TICK_LEDGER] Saved {time_key} IST: {source.upper()} = {price:.3f}")
        else:
            # Track updates if price changes
            old_price = ledger["ticks"][time_key]["price"]
            if abs(old_price - price) > 0.001:  # Only log significant changes
                ledger["ticks"][time_key]["updates"].append({
                    "old_price": old_price,
                    "new_price": price,
                    "timestamp": datetime.now(IST_TZ).isoformat()
                })
            ledger["ticks"][time_key]["price"] = price
            ledger["ticks"][time_key]["source"] = source
            print(f"[TICK_LEDGER] Updated {time_key} IST: {source.upper()} = {price:.3f}")
        
        # Update metadata
        ledger["metadata"]["last_updated"] = datetime.now(IST_TZ).isoformat()
        ledger["metadata"]["total_ticks"] = len(ledger["ticks"])
        
        # Save back to file
        with open(ledger_path, 'w', encoding='utf-8') as f:
            json.dump(ledger, f, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f"[TICK_LEDGER] Error saving tick: {e}")

def get_tick_from_ledger(d: date, time_key: str) -> Optional[float]:
    """Get a specific tick price from the ledger."""
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
    """Remove tick ledgers older than TICK_LEDGER_RETENTION_DAYS."""
    try:
        cutoff_date = date.today() - timedelta(days=TICK_LEDGER_RETENTION_DAYS)
        removed_count = 0
        
        for ledger_file in TICK_LEDGER_DIR.glob("tick_ledger_*.json"):
            try:
                # Extract date from filename: tick_ledger_YYYY-MM-DD.json
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

# Query actual MT5 balance at 05:45
def query_and_set_opening_balance(ist_day: date):
    global BALANCE_QUERIED_TODAY, DAILY_OPENING_BALANCE
    
    try:
        acc = mt5.account_info()
        if not acc:
            print(f"[BALANCE_QUERY] Failed to get account info")
            return
        
        current_balance = float(acc.balance)
        
        # Save as today's baseline
        save_baseline(current_balance)
        
        # Update tracked balance
        with BALANCE_LOCK:
            global LAST_KNOWN_BALANCE
            LAST_KNOWN_BALANCE = current_balance
            DAILY_OPENING_BALANCE = current_balance
        
        print(f"[BALANCE_QUERY] OK Opening balance for {fmt_date(ist_day)}: ${current_balance:.2f}")
        BALANCE_QUERIED_TODAY = True
        
    except Exception as e:
        print(f"[BALANCE_QUERY] Error: {e}")

# ===== Withdrawal detection =====
def log_withdrawal(prev_bal: float, curr_bal: float, withdrawn: float, when: date):
    try:
        if not WITHDRAWAL_LOG.exists():
            with open(WITHDRAWAL_LOG, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["Date","Previous_Balance","New_Balance","Withdrawn_Amount"])
        with open(WITHDRAWAL_LOG, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([fmt_date(when), f"{prev_bal:.2f}", f"{curr_bal:.2f}", f"{withdrawn:.2f}"])
        print(f"[WITHDRAWAL] Logged: ${withdrawn:.2f} on {fmt_date(when)}")
    except Exception as e:
        print(f"[WITHDRAWAL] Failed to log: {e}")

def get_withdrawals_for_period(start: date, end: date) -> List[str]:
    try:
        if not WITHDRAWAL_LOG.exists():
            return []
        with open(WITHDRAWAL_LOG, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            results = []
            for row in reader:
                try:
                    d = datetime.strptime(row["Date"], DATE_FMT).date()
                    if start <= d <= end:
                        results.append(f"${row['Withdrawn_Amount']} on {row['Date']}")
                except Exception:
                    continue
            return results
    except Exception:
        return []

# 24/7 Balance monitor - detects withdrawals via MT5 history
def balance_monitor_thread():
    """
    Monitors MT5 history for balance operations (deal_type=2).
    Withdrawals appear as profit < 0, deposits as profit > 0.
    """
    global LAST_KNOWN_BALANCE, WITHDRAWAL_DETECTED_TODAY
    
    print("[BALANCE_MONITOR] Started - checking every 2 minutes for withdrawals")
    
    last_checked_time = datetime.now(timezone.utc).astimezone().replace(tzinfo=None)
    
    while RUN_HEARTBEAT.is_set():
        try:
            now = datetime.now(timezone.utc).astimezone().replace(tzinfo=None)
            
            # Query deals from last check to now
            deals = mt5.history_deals_get(last_checked_time, now) or []
            
            for deal in deals:
                deal_type = int(getattr(deal, "type", -1))
                
                # DEAL_TYPE_BALANCE = 2 (balance operation from Vantage)
                if deal_type == 2:
                    profit = float(getattr(deal, "profit", 0.0))
                    deal_time_epoch = int(getattr(deal, "time", 0))
                    deal_time_ist = _ist_from_epoch(deal_time_epoch)
                    
                    if profit < 0:
                        # WITHDRAWAL DETECTED
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
                        
                        # Log withdrawal
                        log_withdrawal(
                            previous_balance,
                            current_balance,
                            withdrawn,
                            deal_time_ist.date()
                        )
                        
                        # Update baseline immediately
                        save_baseline(current_balance)
                        
                        # Update tracked balance
                        with BALANCE_LOCK:
                            LAST_KNOWN_BALANCE = current_balance
                            WITHDRAWAL_DETECTED_TODAY = True
                        
                        # Send immediate alert
                        send_email(
                            f"WARNING Withdrawal Alert - ${withdrawn:.2f}",
                            f"Withdrawal detected at {deal_time_ist.strftime('%H:%M:%S')} IST\n\n"
                            f"Previous balance: ${previous_balance:.2f}\n"
                            f"Current balance: ${current_balance:.2f}\n"
                            f"Withdrawn: ${withdrawn:.2f}\n\n"
                            f"Lot sizing will adjust automatically for remaining trades today."
                        )
                    
                    elif profit > 0:
                        # DEPOSIT DETECTED
                        deposited = profit
                        
                        acc = mt5.account_info()
                        current_balance = float(acc.balance) if acc else 0.0
                        
                        print(f"\n[DEPOSIT] OK DETECTED at {deal_time_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")
                        print(f"[DEPOSIT] Amount: ${deposited:.2f}")
                        print(f"[DEPOSIT] New balance: ${current_balance:.2f}\n")
                        
                        # Update baseline
                        save_baseline(current_balance)
                        
                        # Update tracked balance
                        with BALANCE_LOCK:
                            LAST_KNOWN_BALANCE = current_balance
            
            # Update last checked time
            last_checked_time = now
            
            # Sleep for 2 minutes
            _time.sleep(BALANCE_MONITOR_INTERVAL_SECONDS)
            
        except Exception as e:
            print(f"[BALANCE_MONITOR] Error: {e}")
            _time.sleep(BALANCE_MONITOR_INTERVAL_SECONDS)

# ===== NEW: Internal Ledger Management =====

def get_ledger_filepath(ist_day: date) -> Path:
    """Get the ledger file path for a specific date."""
    return LOG_DIR / f"trade_ledger_{fmt_date(ist_day)}.json"

def load_ledger(ist_day: date) -> Dict:
    """Load the ledger for a specific day."""
    ledger_file = get_ledger_filepath(ist_day)
    try:
        if ledger_file.exists():
            with open(ledger_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"[LEDGER] Loaded existing ledger: {len(data.get('trades', {}))} trades")
                return data
    except Exception as e:
        print(f"[LEDGER] Error loading ledger: {e}")
    
    # Return empty ledger structure
    return {
        "date": fmt_date(ist_day),
        "opening_balance": 0.0,
        "closing_balance": 0.0,
        "trades": {}
    }

def save_ledger(ist_day: date, ledger_data: Dict):
    """Save the ledger to disk (atomic write)."""
    ledger_file = get_ledger_filepath(ist_day)
    temp_file = ledger_file.with_suffix('.json.tmp')
    
    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(ledger_data, f, indent=2, ensure_ascii=False)
        
        # Atomic replace
        temp_file.replace(ledger_file)
        
    except Exception as e:
        print(f"[LEDGER] Error saving ledger: {e}")
        if temp_file.exists():
            temp_file.unlink()

def create_ledger_entry(position_id: int, ist_day: date, trade_number: int, 
                       entry_time: str, entry_price: float, sl_price: float, 
                       tp_price: float, lot_size: float, side: str, tag: str):
    """Create a new ledger entry when trade opens."""
    global LEDGER_DATA
    
    with LEDGER_LOCK:
        entry_key = tag  # e.g., "2025-10-19_05:50"
        
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
            # New partial close tracking fields
            "partial_close_triggered": False,
            "partial_close_price": None,
            "partial_close_lots": None,
            "partial_close_pnl": None,
            "remaining_lots": None,
            "remaining_pnl": None,
            "partial_close_net": None
        }
        
        # Save to disk
        save_ledger(ist_day, LEDGER_DATA)
        
        print(f"[LEDGER] OK Created entry for {tag} (Position: {position_id})")

def update_ledger_entry(position_id: int, ist_day: date, exit_time: str, 
                       trade_outcome: str, pnl: float, account_balance: float):
    """Update ledger entry when trade closes."""
    global LEDGER_DATA, DAILY_CLOSING_BALANCE
    
    with LEDGER_LOCK:
        # Find entry by position_id
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
            
            # Update daily closing balance
            DAILY_CLOSING_BALANCE = account_balance
            LEDGER_DATA["closing_balance"] = round(account_balance, 2)
            
            # Save to disk
            save_ledger(ist_day, LEDGER_DATA)
            
            print(f"[LEDGER] OK Updated entry for Position {position_id}: {trade_outcome} ${pnl:.2f}")
        else:
            print(f"[LEDGER] WARNING: Position {position_id} not found in ledger")

def rebuild_ledger_from_mt5(ist_day: date) -> Dict:
    """Rebuild ledger from MT5 history (crash recovery)."""
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
    
    # Group by position ID
    by_pos = {}
    for d in deals:
        if d.symbol != SYMBOL or int(getattr(d, "magic", 0)) != MAGIC:
            continue
        pid = int(getattr(d, "position_id", 0)) or int(getattr(d, "ticket", 0))
        if pid <= 0:
            continue
        by_pos.setdefault(pid, []).append(d)
    
    # Rebuild each position
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
        
        # Extract tag from comment
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
    """
    Query MT5 to rebuild today's trading state after restart.
    FIXED: Counts positions correctly, falls back to ledger if mismatch.
    """
    global TRADES_OPENED_TODAY, TRADES_CLOSED_TODAY
    
    print(f"[RECOVERY] Rebuilding trading state for {fmt_date(ist_day)}...")
    
    day_start = datetime.combine(ist_day, dt_time(0,0), tzinfo=IST_TZ)
    day_end = datetime.combine(ist_day, dt_time(23,59,59), tzinfo=IST_TZ)
    
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    start_server = (day_start + timedelta(minutes=delta_min)).replace(tzinfo=None)
    end_server = (day_end + timedelta(minutes=delta_min)).replace(tzinfo=None)
    
    # Query all deals for today
    deals = mt5.history_deals_get(start_server, end_server) or []
    
    # Group by position ID
    by_pos = {}
    for d in deals:
        if d.symbol != SYMBOL or int(getattr(d, "magic", 0)) != MAGIC:
            continue
        pid = int(getattr(d, "position_id", 0)) or int(getattr(d, "ticket", 0))
        if pid <= 0:
            continue
        by_pos.setdefault(pid, []).append(d)
    
    # FIXED: Count POSITIONS, not individual deals
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
        
        # Only count if position was actually opened by our bot
        if has_entry_in:
            opened_count += 1
            # Only count as closed if it was both opened AND closed
            if has_entry_out:
                closed_count += 1
    
    # Check currently open positions
    positions = mt5.positions_get(symbol=SYMBOL)
    currently_open = 0
    if positions:
        for p in positions:
            if getattr(p, "magic", 0) == MAGIC:
                currently_open += 1
    
    # Update global counters
    with TRADES_LOCK:
        TRADES_OPENED_TODAY = opened_count
        TRADES_CLOSED_TODAY = closed_count
    
    print(f"[RECOVERY] OK Trading state recovered:")
    print(f"[RECOVERY]   - Trades opened today: {opened_count}")
    print(f"[RECOVERY]   - Trades closed today: {closed_count}")
    print(f"[RECOVERY]   - Currently open positions: {currently_open}")
    
    # Verify consistency
    expected_open = opened_count - closed_count
    if expected_open != currently_open:
        print(f"[RECOVERY] WARNING: State mismatch detected!")
        print(f"[RECOVERY]   Expected open: {expected_open}")
        print(f"[RECOVERY]   Actually open: {currently_open}")
        # If mismatch, trust the ledger instead
        if expected_open < 0:
            print(f"[RECOVERY]   Correcting: Using ledger data instead")
            with LEDGER_LOCK:
                ledger_opened = sum(1 for t in LEDGER_DATA["trades"].values() if t.get("status") in ["OPEN", "CLOSED"])
                ledger_closed = sum(1 for t in LEDGER_DATA["trades"].values() if t.get("status") == "CLOSED")
            with TRADES_LOCK:
                TRADES_OPENED_TODAY = ledger_opened
                TRADES_CLOSED_TODAY = ledger_closed
            print(f"[RECOVERY]   Corrected - Opened: {ledger_opened}, Closed: {ledger_closed}")
    
    return opened_count, closed_count, currently_open

def recover_executed_slots_from_mt5(ist_day: date) -> set:
    """
    Determine which IST time slots were already executed today.
    FIXED: Checks ledger first (most reliable).
    """
    # First try from ledger (most reliable)
    executed_slots = set()
    
    with LEDGER_LOCK:
        for tag, entry in LEDGER_DATA["trades"].items():
            time_part = entry.get("entry_time", "")
            if time_part in IST_TRADE_TIMES:
                executed_slots.add(time_part)
    
    if executed_slots:
        print(f"[RECOVERY] Executed slots from ledger: {sorted(executed_slots)}")
        return executed_slots
    
    # Fallback to MT5 history if ledger empty
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
        if entry_type == mt5.DEAL_ENTRY_IN:  # Opening trade
            comment = getattr(d, "comment", "")
            
            if "_" in comment:
                try:
                    time_part = comment.split("_")[-1]  # "05:50"
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
    """
    Check if any withdrawals happened today (before restart).
    """
    day_start = datetime.combine(ist_day, dt_time(0,0), tzinfo=IST_TZ)
    day_end = datetime.combine(ist_day, dt_time(23,59,59), tzinfo=IST_TZ)
    
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    start_server = (day_start + timedelta(minutes=delta_min)).replace(tzinfo=None)
    end_server = (day_end + timedelta(minutes=delta_min)).replace(tzinfo=None)
    
    deals = mt5.history_deals_get(start_server, end_server) or []
    
    for deal in deals:
        deal_type = int(getattr(deal, "type", -1))
        if deal_type == 2:  # Balance operation
            profit = float(getattr(deal, "profit", 0.0))
            if profit < 0:
                global WITHDRAWAL_DETECTED_TODAY
                WITHDRAWAL_DETECTED_TODAY = True
                print(f"[RECOVERY] WARNING: Withdrawal detected earlier today: ${abs(profit):.2f}")
                return

# ===== Lot sizing =====
def lot_size_simple_compound() -> float:
    """Simple compounding: Increase 0.01 lot per $100 increment from starting point."""
    acc = mt5.account_info()
    bal = float(getattr(acc, "balance", 0.0)) if acc else 500.0
    
    starting_balance = 500
    starting_lots = 0.05
    
    increments = int((bal - starting_balance) / 100)
    lots = starting_lots + (increments * 0.01)
    
    return max(MIN_LOT, min(lots, 10.0))

def calculate_partial_close_lots(original_lots: float) -> Tuple[float, float]:
    """
    Calculate how many lots to close at +50 pips.
    
    Rules:
    - For 0.01: Close full (0.01)
    - For 0.03: Close 0.02, keep 0.01
    - For 0.05: Close 0.03, keep 0.02
    - For even lots (0.02, 0.04, 0.06...): Close 50%
    - For other odd lots: Close 66% (rounded per MT5)
    
    Returns: (lots_to_close, remaining_lots)
    """
    info = mt5.symbol_info(SYMBOL)
    lot_step = getattr(info, "volume_step", 0.01) if info else 0.01
    min_lot = getattr(info, "volume_min", 0.01) if info else 0.01
    
    # Special cases for small odd lots
    if original_lots == 0.01:
        return 0.01, 0.0  # Close full
    elif original_lots == 0.03:
        return 0.02, 0.01
    elif original_lots == 0.05:
        return 0.03, 0.02
    
    # Check if even or odd lot size
    lots_as_int = int(original_lots / lot_step)
    is_even = (lots_as_int % 2 == 0)
    
    if is_even:
        # Even lots: close exactly 50%
        close_lots = original_lots * PARTIAL_CLOSE_PERCENTAGE_EVEN
    else:
        # Odd lots: close 66%
        close_lots = original_lots * PARTIAL_CLOSE_PERCENTAGE_ODD
    
    # Round to MT5 lot step
    close_lots = round(close_lots / lot_step) * lot_step
    close_lots = max(min_lot, close_lots)
    close_lots = min(close_lots, original_lots - min_lot)  # Ensure at least min_lot remains
    
    remaining_lots = original_lots - close_lots
    remaining_lots = round(remaining_lots / lot_step) * lot_step
    remaining_lots = max(0, remaining_lots)
    
    return round(close_lots, 2), round(remaining_lots, 2)

def calculate_current_pips(entry_price: float, current_price: float, side: str) -> float:
    """Calculate current profit/loss in pips."""
    if side == "BUY":
        pips = (current_price - entry_price) / PIP_SIZE
    else:  # SELL
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
    """
    SIMPLIFIED: Captures tick price at exact candle open time (0-3 seconds).
    This tick price IS the actual opening price of the candle.
    """
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
            
            # Check if we need to capture an opening price
            if current_time_key in REQUIRED_OPENING_TIMES:
                # Capture tick in first 3 seconds of the minute
                if current_second <= 3:
                    capture_id = f"{current_time_key}_{ist_day}"
                    
                    if capture_id not in last_captured:
                        tick = mt5.symbol_info_tick(SYMBOL)
                        if tick:
                            # The tick at candle start IS the opening price
                            tick_midpoint = (float(tick.bid) + float(tick.ask)) / 2.0
                            
                            with CANDLE_OPENS_LOCK:
                                CANDLE_OPENS[current_time_key] = tick_midpoint
                            
                            last_captured[capture_id] = tick_midpoint
                            print(f"[OPEN_CAPTURE] OK {current_time_key}:{current_second:02d} IST: Opening = {tick_midpoint:.3f}")
                            
                            # Save to tick ledger immediately
                            save_tick_to_ledger(ist_day, current_time_key, tick_midpoint, source="tick_open")
                    
                    # Allow updates if price moves significantly in first 3 seconds
                    elif current_second <= 2:
                        tick = mt5.symbol_info_tick(SYMBOL)
                        if tick:
                            tick_midpoint = (float(tick.bid) + float(tick.ask)) / 2.0
                            
                            # Update if price moved more than 0.1 pips
                            if abs(tick_midpoint - last_captured[capture_id]) > 0.01:
                                with CANDLE_OPENS_LOCK:
                                    CANDLE_OPENS[current_time_key] = tick_midpoint
                                last_captured[capture_id] = tick_midpoint
                                print(f"[OPEN_CAPTURE] UPDATED {current_time_key}:{current_second:02d} IST: Opening = {tick_midpoint:.3f}")
                                
                                # Update tick ledger
                                save_tick_to_ledger(ist_day, current_time_key, tick_midpoint, source="tick_open_updated")
            
            # Clean up old captures at hour boundary
            if current_second == 0 and now_ist.minute == 0:
                current_date = ist_day
                last_captured = {k: v for k, v in last_captured.items() if current_date.isoformat() in k}
            
            _time.sleep(0.1)  # Check 10 times per second for accuracy
            
        except Exception as e:
            print(f"[OPEN_MONITOR] Error: {e}")
            _time.sleep(1)

def backfill_todays_opens(ist_day: date):
    """
    SIMPLIFIED: Backfill using tick ledger primarily.
    Falls back to MT5 only if necessary.
    """
    now_ist = datetime.now(timezone.utc).astimezone(IST_TZ)
    delta_min = ist_to_server_delta_minutes_for_date(ist_day)
    
    print(f"[BACKFILL] Checking for missing opening prices...")
    
    backfilled_count = 0
    ledger_used_count = 0
    mt5_used_count = 0
    
    # Load today's tick ledger
    ledger_data = load_tick_ledger(ist_day)
    
    for ist_hhmm in IST_TRADE_TIMES:
        hh, mm = parse_ist_hhmm(ist_hhmm)
        trade_time = datetime.combine(ist_day, dt_time(hh, mm))
        
        # Skip future times
        if now_ist.date() == ist_day and trade_time > now_ist.replace(tzinfo=None):
            continue
        
        # Check opening time (5 mins before trade)
        candle_time = trade_time - timedelta(minutes=5)
        time_str = candle_time.strftime("%H:%M")
        
        # Skip if already have this opening
        with CANDLE_OPENS_LOCK:
            if time_str in CANDLE_OPENS:
                continue
        
        # Skip times too early in the day
        if candle_time.time() < dt_time(0, 5):
            continue
        
        # PRIORITY 1: Check tick ledger for stored price
        if time_str in ledger_data:
            ledger_entry = ledger_data[time_str]
            ledger_price = ledger_entry['price']
            source = ledger_entry.get('source', 'unknown')
            
            with CANDLE_OPENS_LOCK:
                CANDLE_OPENS[time_str] = ledger_price
            print(f"[BACKFILL] OK {time_str} opening: {ledger_price:.3f} (from ledger: {source})")
            backfilled_count += 1
            ledger_used_count += 1
            continue
        
        # PRIORITY 2: Fallback to MT5 historical data
        server_time = (candle_time + timedelta(minutes=delta_min)).replace(tzinfo=None)
        
        rates = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, server_time, 1)
        if rates is not None and len(rates) > 0:
            candle_open = float(rates[0]["open"])
            with CANDLE_OPENS_LOCK:
                CANDLE_OPENS[time_str] = candle_open
            print(f"[BACKFILL] OK {time_str} opening: {candle_open:.3f} (from MT5)")
            
            # Save to ledger for future reference
            save_tick_to_ledger(ist_day, time_str, candle_open, source="backfill_mt5")
            
            backfilled_count += 1
            mt5_used_count += 1
    
    if backfilled_count > 0:
        print(f"[BACKFILL] Completed: {backfilled_count} opens backfilled "
              f"({ledger_used_count} from ledger, {mt5_used_count} from MT5)")
    else:
        print(f"[BACKFILL] No backfill needed")
    
    print("[BACKFILL] Checking for missing opening prices...")
    
    # Load tick ledger for today
    tick_ledger = load_tick_ledger(ist_day)
    ledger_ticks = tick_ledger.get("ticks", {})
    
    backfilled_count = 0
    ledger_used_count = 0
    mt5_used_count = 0
    
    for time_str in REQUIRED_OPENING_TIMES:
        hh, mm = parse_ist_hhmm(time_str)
        candle_time = datetime.combine(ist_day, dt_time(hh, mm), tzinfo=IST_TZ)
        
        if candle_time < now_ist:
            with CANDLE_OPENS_LOCK:
                if time_str in CANDLE_OPENS:
                    continue
            
            # PRIORITY 1: Check tick ledger first
            ledger_price = get_tick_from_ledger(ist_day, time_str)
            
            if ledger_price is not None:
                with CANDLE_OPENS_LOCK:
                    CANDLE_OPENS[time_str] = ledger_price
                print(f"[BACKFILL] OK {time_str} opening: {ledger_price:.3f} (from LEDGER)")
                backfilled_count += 1
                ledger_used_count += 1
                continue
            
            # PRIORITY 2: Fallback to MT5 historical data
            server_time = (candle_time + timedelta(minutes=delta_min)).replace(tzinfo=None)
            
            rates = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, server_time, 1)
            if rates is not None and len(rates) > 0:
                candle_open = float(rates[0]["open"])
                with CANDLE_OPENS_LOCK:
                    CANDLE_OPENS[time_str] = candle_open
                print(f"[BACKFILL] OK {time_str} opening: {candle_open:.3f} (from MT5)")
                
                # Save to ledger for future reference
                save_tick_to_ledger(ist_day, time_str, candle_open, source="backfill_mt5")
                
                backfilled_count += 1
                mt5_used_count += 1
    
    if backfilled_count > 0:
        print(f"[BACKFILL] Completed: {backfilled_count} opens backfilled "
              f"({ledger_used_count} from ledger, {mt5_used_count} from MT5)")
    else:
        print(f"[BACKFILL] No backfill needed")

def get_signal_from_live_opens(ist_hhmm: str) -> Tuple[Optional[str], Optional[str]]:
    """
    SIMPLIFIED: Compare current tick price vs stored opening tick price.
    The stored price is the actual tick at candle open time.
    """
    hh, mm = parse_ist_hhmm(ist_hhmm)
    opening_dt = datetime.combine(date.today(), dt_time(hh, mm)) - timedelta(minutes=5)
    opening_key = opening_dt.strftime("%H:%M")
    
    with CANDLE_OPENS_LOCK:
        candle_open = CANDLE_OPENS.get(opening_key)
    
    if candle_open is None:
        skip_reason = f"No opening price captured for {opening_key}"
        print(f"[SIGNAL_ERROR] ERROR {skip_reason}")
        return None, skip_reason
    
    # Get current tick
    tick = mt5.symbol_info_tick(SYMBOL)
    if not tick:
        skip_reason = "No tick data available"
        print(f"[SIGNAL_ERROR] {skip_reason}")
        return None, skip_reason
    
    current_bid = float(tick.bid)
    current_ask = float(tick.ask)
    current_midpoint = (current_bid + current_ask) / 2.0
    
    # CHANGED: threshold from 1 pip to 0.02 pips
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
    
    # Simple, clear logging
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

def debug_check_candle_data(ist_time_str: str):
    """
    Debug function to verify tick capture vs MT5 candle data.
    Call this when you notice wrong signals.
    Usage: debug_check_candle_data("20:35")
    """
    try:
        hh, mm = parse_ist_hhmm(ist_time_str)
        
        # Get the opening time (5 minutes before)
        opening_dt = datetime.combine(date.today(), dt_time(hh, mm)) - timedelta(minutes=5)
        opening_key = opening_dt.strftime("%H:%M")
        
        print(f"\n[DEBUG] ═══════════════════════════════")
        print(f"[DEBUG] Checking data for trade at {ist_time_str} IST")
        print(f"[DEBUG] Opening time: {opening_key}:00")
        
        # Check stored tick value
        with CANDLE_OPENS_LOCK:
            stored_tick = CANDLE_OPENS.get(opening_key)
        
        print(f"[DEBUG] Stored tick opening: {stored_tick:.3f if stored_tick else 'NOT FOUND'}")
        
        # Get tick ledger data
        ist_day = datetime.now(timezone.utc).astimezone(IST_TZ).date()
        ledger_data = load_tick_ledger(ist_day)
        
        if opening_key in ledger_data:
            ledger_entry = ledger_data[opening_key]
            print(f"[DEBUG] Tick ledger entry:")
            print(f"  - Price: {ledger_entry['price']:.3f}")
            print(f"  - Source: {ledger_entry.get('source', 'unknown')}")
            print(f"  - Time: {ledger_entry.get('timestamp', 'unknown')}")
        
        # Get actual MT5 candle for comparison
        delta_min = ist_to_server_delta_minutes_for_date(ist_day)
        candle_time = datetime.combine(ist_day, dt_time(hh, mm)) - timedelta(minutes=5)
        server_time = (candle_time + timedelta(minutes=delta_min)).replace(tzinfo=None)
        
        rates = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, server_time, 1)
        
        if rates is not None and len(rates) > 0:
            actual_open = float(rates[0]["open"])
            actual_high = float(rates[0]["high"])
            actual_low = float(rates[0]["low"])
            actual_close = float(rates[0]["close"])
            
            print(f"[DEBUG] MT5 candle data:")
            print(f"  Open:  {actual_open:.3f}")
            print(f"  High:  {actual_high:.3f}")
            print(f"  Low:   {actual_low:.3f}")
            print(f"  Close: {actual_close:.3f}")
            print(f"  Color: {'GREEN' if actual_close > actual_open else 'RED'}")
            
            if stored_tick:
                tick_vs_mt5_pips = abs(stored_tick - actual_open) / PIP_SIZE
                print(f"[DEBUG] Tick vs MT5 open: {tick_vs_mt5_pips:.1f} pips difference")
                
                if tick_vs_mt5_pips > 10:
                    print(f"[DEBUG] ⚠️  LARGE DISCREPANCY! This explains wrong signals.")
        else:
            print(f"[DEBUG] Could not fetch MT5 candle data")
        
        # Check current tick
        tick = mt5.symbol_info_tick(SYMBOL)
        if tick:
            bid = float(tick.bid)
            ask = float(tick.ask)
            mid = (bid + ask) / 2
            print(f"[DEBUG] Current tick: Bid={bid:.3f}, Ask={ask:.3f}, Mid={mid:.3f}")
            
            if stored_tick:
                current_diff_pips = (mid - stored_tick) / PIP_SIZE
                print(f"[DEBUG] Price moved {current_diff_pips:+.1f} pips from stored opening")
        
        print(f"[DEBUG] ═══════════════════════════════\n")
    except Exception as e:
        print(f"[DEBUG] Error in debug check: {e}")

# ===== Watcher thread =====
def watcher_thread(position_ticket: int, expected_side: str, tag: str, ist_day: date):
    global TRADES_CLOSED_TODAY, LAST_SLOT_CLOSED, LAST_SLOT_CLOSE_TIME
    start_time = _time.time()
    reanchored = False
    partial_close_executed = False
    original_volume = 0.0
    entry_price = 0.0
    
    print(f"[WATCHER] Started for position {position_ticket}")
    
    # Get original volume from ledger
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
            
            # Get closing info from MT5 history
            try:
                now = datetime.now(timezone.utc).astimezone().replace(tzinfo=None)
                start = now - timedelta(minutes=10)
                deals = mt5.history_deals_get(start, now) or []
                
                # Calculate total PnL and check for partial close
                net_profit = 0.0
                partial_close_pnl = 0.0
                remaining_pnl = 0.0
                partial_close_price = None
                partial_close_lots = 0.0
                
                # Group deals by type
                entry_out_deals = []
                for d in deals:
                    if int(getattr(d, "position_id", 0)) == position_ticket:
                        net_profit += float(getattr(d, "profit", 0.0))
                        entry_type = int(getattr(d, "entry", 0))
                        if entry_type == mt5.DEAL_ENTRY_OUT:
                            entry_out_deals.append(d)
                
                # Get exit time from final close
                exit_time = None
                for d in deals:
                    if int(getattr(d, "position_id", 0)) == position_ticket:
                        entry_type = int(getattr(d, "entry", 0))
                        if entry_type == mt5.DEAL_ENTRY_OUT:
                            close_time_ist = _ist_from_epoch(d.time)
                            exit_time = close_time_ist.strftime("%H:%M")
                
                # Check if partial close was executed
                if partial_close_executed:
                    # We know partial close happened, need to separate PnLs
                    # Sort deals by time
                    entry_out_deals.sort(key=lambda x: x.time)
                    
                    if len(entry_out_deals) >= 1:
                        # First close was partial
                        partial_deal = entry_out_deals[0]
                        partial_close_pnl = float(getattr(partial_deal, "profit", 0.0))
                        partial_close_lots = float(getattr(partial_deal, "volume", 0.0))
                        partial_close_price = float(getattr(partial_deal, "price", 0.0))
                        
                        # If there's a second close, that's the remaining
                        if len(entry_out_deals) >= 2:
                            remaining_deal = entry_out_deals[1]
                            remaining_pnl = float(getattr(remaining_deal, "profit", 0.0))
                        else:
                            # Only partial close happened (for 0.01 lot full close case)
                            remaining_pnl = 0.0
                    
                    # Determine outcome
                    if remaining_pnl > 0:
                        outcome = "Profit"  # Both partial and remaining made profit
                    else:
                        outcome = "Partial Close"  # Partial profit, remaining loss
                    
                    partial_close_net = partial_close_pnl + remaining_pnl
                else:
                    # No partial close - standard full close
                    outcome = "PROFIT" if net_profit > 0 else "LOSS"
                    partial_close_net = None
                
                acc = mt5.account_info()
                current_balance = float(acc.balance) if acc else 0.0
                
                # Update ledger with partial close info
                with LEDGER_LOCK:
                    for key, entry in LEDGER_DATA["trades"].items():
                        if entry["position_id"] == position_ticket:
                            LEDGER_DATA["trades"][key].update({
                                "exit_time": exit_time,
                                "trade_outcome": outcome,
                                "pnl": round(net_profit, 2),
                                "account_balance": round(current_balance, 2),
                                "status": "CLOSED",
                                "partial_close_triggered": partial_close_executed,
                                "partial_close_price": round(partial_close_price, 3) if partial_close_price else None,
                                "partial_close_lots": round(partial_close_lots, 2) if partial_close_executed else None,
                                "partial_close_pnl": round(partial_close_pnl, 2) if partial_close_executed else None,
                                "remaining_lots": round(original_volume - partial_close_lots, 2) if partial_close_executed else None,
                                "remaining_pnl": round(remaining_pnl, 2) if partial_close_executed else None,
                                "partial_close_net": round(partial_close_net, 2) if partial_close_net is not None else None
                            })
                            
                            # Update daily closing balance
                            global DAILY_CLOSING_BALANCE
                            DAILY_CLOSING_BALANCE = current_balance
                            LEDGER_DATA["closing_balance"] = round(current_balance, 2)
                            
                            save_ledger(ist_day, LEDGER_DATA)
                            break
                
                print(f"[LEDGER] OK Updated entry for Position {position_ticket}: {outcome} ${net_profit:.2f}")
                
                # Check if this was the last slot (20:35)
                if LAST_SLOT_TIME in tag:
                    LAST_SLOT_CLOSED = True
                    LAST_SLOT_CLOSE_TIME = _time.time()
                    print(f"[WATCHER] OK Last slot ({LAST_SLOT_TIME}) position closed")
                
            except Exception as e:
                print(f"[WATCHER] Error updating ledger: {e}")
            
            with TRADES_LOCK:
                TRADES_CLOSED_TODAY += 1
            with WATCHERS_LOCK:
                ACTIVE_WATCHERS.pop(position_ticket, None)
            break
        
        # Check for partial close trigger (exactly at +50 pips)
        if not partial_close_executed and original_volume > 0:
            try:
                tick = mt5.symbol_info_tick(SYMBOL)
                if tick:
                    current_price = tick.bid if expected_side == "BUY" else tick.ask
                    current_pips = calculate_current_pips(entry_price, current_price, expected_side)
                    
                    # Trigger partial close exactly at +50 pips
                    if current_pips >= PARTIAL_CLOSE_TRIGGER_PIPS:
                        print(f"[PARTIAL_CLOSE] +50 pips reached for position {position_ticket}")
                        
                        # Calculate how much to close
                        current_volume = float(pos.volume)
                        close_lots, remaining_lots = calculate_partial_close_lots(current_volume)
                        
                        if close_lots > 0 and remaining_lots > 0:
                            print(f"[PARTIAL_CLOSE] Closing {close_lots} lots (keeping {remaining_lots} lots)")
                            
                            # Execute partial close with IOC policy (Vantage requirement)
                            close_request = {
                                "action": mt5.TRADE_ACTION_DEAL,
                                "symbol": SYMBOL,
                                "volume": float(close_lots),
                                "type": mt5.ORDER_TYPE_SELL if expected_side == "BUY" else mt5.ORDER_TYPE_BUY,
                                "position": position_ticket,
                                "price": tick.bid if expected_side == "BUY" else tick.ask,
                                "deviation": DEVIATION,
                                "magic": MAGIC,
                                "comment": f"PC_{position_ticket}",
                                "type_time": mt5.ORDER_TIME_GTC,
                                "type_filling": mt5.ORDER_FILLING_IOC,
                            }
                            
                            result = mt5.order_send(close_request)
                            
                            if result:
                                if result.retcode == mt5.TRADE_RETCODE_DONE:
                                    partial_close_executed = True
                                    print(f"[PARTIAL_CLOSE] SUCCESS - Closed {close_lots} lots at +50 pips")
                                    print(f"[PARTIAL_CLOSE] Remaining {remaining_lots} lots will continue to TP/SL")
                                else:
                                    error_code = result.retcode
                                    error_msg = getattr(result, 'comment', 'No error message')
                                    print(f"[PARTIAL_CLOSE] FAILED - MT5 Error {error_code}: {error_msg}")
                                    partial_close_executed = True  # Don't retry to avoid spam
                            else:
                                print(f"[PARTIAL_CLOSE] FAILED - order_send returned None")
                                partial_close_executed = True  # Don't retry
                        else:
                            print(f"[PARTIAL_CLOSE] Cannot execute - close_lots={close_lots}, remaining_lots={remaining_lots}")
                            partial_close_executed = True  # Mark as executed to avoid retry
                            
            except Exception as e:
                print(f"[PARTIAL_CLOSE] Error: {e}")
                partial_close_executed = True  # Don't retry on exception
        
        # Reanchor SL/TP (only once)
        if not reanchored:
            try:
                entry = float(pos.price_open)
                current_sl = float(pos.sl)
                current_tp = float(pos.tp)
                expected_sl, expected_tp = compute_sl_tp(entry, expected_side)
                
                sl_diff = abs(current_sl - expected_sl)
                tp_diff = abs(current_tp - expected_tp)
                
                if sl_diff > 0.5 or tp_diff > 0.5:
                    print(f"[WATCHER] External modification detected on {position_ticket}")
                else:
                    req = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "symbol": SYMBOL,
                        "position": position_ticket,
                        "sl": float(expected_sl),
                        "tp": float(expected_tp),
                        "magic": MAGIC
                    }
                    res = mt5.order_send(req)
                    print(f"[WATCHER] Reanchored {position_ticket}: ret={getattr(res,'retcode',None)}")
                reanchored = True
            except Exception as e:
                print(f"[WATCHER] Reanchor failed for {position_ticket}: {e}")
        
        _time.sleep(1)

def spawn_watcher(position_ticket: int, side: str, tag: str, ist_day: date):
    thr = threading.Thread(target=watcher_thread, args=(position_ticket, side, tag, ist_day), daemon=True)
    thr.start()
    with WATCHERS_LOCK:
        ACTIVE_WATCHERS[position_ticket] = thr

def verify_position_exists(result, expected_tag: str) -> Optional[int]:
    if not result or result.retcode != mt5.TRADE_RETCODE_DONE:
        return None
    
    _time.sleep(5)
    
    positions = mt5.positions_get(symbol=SYMBOL)
    if positions:
        for p in positions:
            try:
                if getattr(p, "magic", 0) == MAGIC and expected_tag in getattr(p, "comment", ""):
                    return int(p.ticket)
            except Exception:
                continue
    
    try:
        deal_id = int(getattr(result, "deal", 0))
        if deal_id > 0:
            now = datetime.now(timezone.utc).astimezone().replace(tzinfo=None)
            start = now - timedelta(minutes=10)
            deals = mt5.history_deals_get(start, now) or []
            for d in deals:
                if int(getattr(d, "ticket", 0)) == deal_id:
                    pid = int(getattr(d, "position_id", 0))
                    if pid > 0:
                        return pid
    except Exception:
        pass
    
    print(f"[VERIFY] Position not found for tag {expected_tag}")
    return None

def recover_orphaned_positions(ist_day: date):
    try:
        positions = mt5.positions_get(symbol=SYMBOL)
        if not positions:
            return
        
        count = 0
        for p in positions:
            try:
                if getattr(p, "magic", 0) != MAGIC:
                    continue
                ticket = int(p.ticket)
                side = "BUY" if getattr(p, "type", 0) == mt5.POSITION_TYPE_BUY else "SELL"
                tag = getattr(p, "comment", "unknown")
                spawn_watcher(ticket, side, tag, ist_day)
                count += 1
            except Exception:
                continue
        
        if count > 0:
            print(f"[RESTART] Recovered {count} orphaned positions")
    except Exception as e:
        print(f"[RESTART] Recovery failed: {e}")

# ===== History-based reporting =====
def collect_closed_trades(start_ist: datetime, end_ist: datetime) -> List[dict]:
    delta_min = ist_to_server_delta_minutes_for_date(start_ist.date())
    start_server = (start_ist + timedelta(minutes=delta_min)).replace(tzinfo=None)
    end_server = (end_ist + timedelta(minutes=delta_min)).replace(tzinfo=None)
    
    print(f"[HISTORY] Querying deals from {start_server} to {end_server}")
    deals = mt5.history_deals_get(start_server, end_server) or []
    print(f"[HISTORY] Retrieved {len(deals)} total deals from MT5")
    
    by_pos: Dict[int, List] = {}
    
    for d in deals:
        try:
            if d.symbol != SYMBOL or int(getattr(d, "magic", 0)) != MAGIC:
                continue
            pid = int(getattr(d, "position_id", 0)) or int(getattr(d, "ticket", 0))
            if pid <= 0:
                continue
            by_pos.setdefault(pid, []).append(d)
        except Exception:
            continue
    
    print(f"[HISTORY] Found {len(by_pos)} unique positions for {SYMBOL} with MAGIC {MAGIC}")
    
    trades = []
    skipped_positions = []
    
    for pid, lst in by_pos.items():
        lst.sort(key=lambda x: x.time)
        
        open_deal = None
        close_deal = None
        for d in lst:
            entry_type = int(getattr(d, "entry", 0))
            if entry_type == mt5.DEAL_ENTRY_IN:
                open_deal = d
            elif entry_type == mt5.DEAL_ENTRY_OUT:
                close_deal = d
        
        if not open_deal or not close_deal:
            skipped_positions.append(pid)
            has_in = "YES" if open_deal else "NO"
            has_out = "YES" if close_deal else "NO"
            print(f"[WARNING] Position {pid} incomplete - ENTRY_IN:{has_in} ENTRY_OUT:{has_out}")
            continue
        
        open_time_ist = _ist_from_epoch(open_deal.time)
        date_str = open_time_ist.strftime(DATE_FMT)
        time_str = open_time_ist.strftime("%H:%M")
        entry_price = float(open_deal.price)
        volume = float(open_deal.volume)
        side = "BUY" if getattr(open_deal, "type", 0) == mt5.DEAL_TYPE_BUY else "SELL"
        
        net_profit = sum(float(x.profit) for x in lst)
        outcome = "PROFIT" if net_profit > 0 else "LOSS"
        
        sl, tp = compute_sl_tp(entry_price, side)
        
        trades.append({
            "date": date_str,
            "time": time_str,
            "entry": entry_price,
            "sl": sl,
            "tp": tp,
            "vol": round(volume, 2),
            "outcome": outcome,
            "pnl": net_profit,
            "open_epoch": open_deal.time,
            "position_id": pid
        })
    
    if skipped_positions:
        print(f"[ERROR] {len(skipped_positions)} positions skipped - history incomplete!")
        print(f"[ERROR] Missing position IDs: {skipped_positions}")
    
    trades.sort(key=lambda x: x["open_epoch"])
    print(f"[HISTORY] Successfully collected {len(trades)} complete trades")
    return trades

def verify_ledger_against_mt5(ledger_trades: List[dict], mt5_trades: List[dict]) -> Dict:
    """
    Compare ledger trades against MT5 history.
    Returns verification report.
    """
    verification = {
        "status": "VERIFIED",
        "ledger_count": len(ledger_trades),
        "mt5_count": len(mt5_trades),
        "missing_in_mt5": [],
        "discrepancies": []
    }
    
    # Check if all ledger trades are in MT5
    ledger_pids = {t["position_id"] for t in ledger_trades}
    mt5_pids = {t["position_id"] for t in mt5_trades}
    
    missing_pids = ledger_pids - mt5_pids
    
    if missing_pids:
        verification["status"] = "PENDING_SYNC"
        for pid in missing_pids:
            ledger_trade = next((t for t in ledger_trades if t["position_id"] == pid), None)
            if ledger_trade:
                verification["missing_in_mt5"].append({
                    "position_id": pid,
                    "trade_number": ledger_trade["trade_number"],
                    "entry_time": ledger_trade["entry_time"]
                })
    
    # Check for discrepancies in PnL
    for lt in ledger_trades:
        mt5_trade = next((t for t in mt5_trades if t["position_id"] == lt["position_id"]), None)
        if mt5_trade:
            pnl_diff = abs(lt["pnl"] - mt5_trade["pnl"])
            if pnl_diff > 0.01:  # More than 1 cent difference
                verification["status"] = "DISCREPANCY"
                verification["discrepancies"].append({
                    "position_id": lt["position_id"],
                    "trade_number": lt["trade_number"],
                    "ledger_pnl": lt["pnl"],
                    "mt5_pnl": mt5_trade["pnl"],
                    "difference": lt["pnl"] - mt5_trade["pnl"]
                })
    
    return verification

def build_report(period: str, start_ist: datetime, end_ist: datetime, baseline: float, 
                 out_file: Path, skipped_slots_dict: Dict[str, str] = None):
    """
    Build report using INTERNAL LEDGER as primary source.
    MT5 history used only for verification.
    Now includes skipped/missed slots with detailed reasons.
    FIXED: All CSV writes use UTF-8 encoding to prevent charmap codec errors.
    """
    global LEDGER_DATA
    
    if skipped_slots_dict is None:
        skipped_slots_dict = {}
    
    # Get trades from ledger
    ledger_trades = []
    with LEDGER_LOCK:
        for tag, entry in sorted(LEDGER_DATA["trades"].items()):
            if entry["status"] == "CLOSED":
                ledger_trades.append({
                    "date": entry["date"],
                    "trade_number": entry["trade_number"],
                    "time": entry["entry_time"],
                    "entry": entry["entry_price"],
                    "sl": entry["sl_price"],
                    "tp": entry["tp_price"],
                    "partial_close_price": entry.get("partial_close_price"),
                    "vol": entry["lot_size"],
                    "outcome": entry["trade_outcome"],
                    "partial_close_net": entry.get("partial_close_net"),
                    "pnl": entry["pnl"],
                    "balance": entry["account_balance"],
                    "position_id": entry["position_id"],
                    "entry_time": entry["entry_time"]
                })
    
    # Sort by trade number
    ledger_trades.sort(key=lambda x: x["trade_number"])
    
    # Query MT5 for verification
    mt5_trades = collect_closed_trades(start_ist, end_ist)
    
    # Verify ledger against MT5
    verification = verify_ledger_against_mt5(ledger_trades, mt5_trades)
    
    # Build CSV rows from LEDGER (primary source)
    rows = []
    for tr in ledger_trades:
        partial_close_price_str = f"{tr['partial_close_price']:.3f}" if tr['partial_close_price'] else ""
        partial_close_net_str = f"${tr['partial_close_net']:.2f}" if tr['partial_close_net'] is not None else ""
        
        rows.append([
            tr["date"], str(tr["trade_number"]), tr["time"],
            f"{tr['entry']:.3f}", f"{tr['sl']:.3f}", f"{tr['tp']:.3f}",
            partial_close_price_str,
            f"{tr['vol']:.2f}", tr["outcome"],
            partial_close_net_str,
            f"${tr['pnl']:.2f}", f"${tr['balance']:.2f}"
        ])
    
    # Write to CSV with UTF-8 encoding
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(LOG_HEADER)
        w.writerows(rows)
    
    # Add summary and verification status
    opening = float(baseline)
    closing = float(ledger_trades[-1]["balance"]) if ledger_trades else opening
    pnl = closing - opening
    
    withdrawals = get_withdrawals_for_period(start_ist.date(), end_ist.date())
    
    # Count executed vs total slots
    executed_times = {tr["entry_time"] for tr in ledger_trades}
    total_slots = len(IST_TRADE_TIMES)
    executed_count = len(executed_times)
    
    with open(out_file, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        blank = [""] * len(LOG_HEADER)
        w.writerow(blank)
        title_row = [""] * len(LOG_HEADER)
        title_row[8] = f"{period} Summary ({executed_count}/{total_slots} trades executed)"
        w.writerow(title_row)
        
        open_row = [""] * len(LOG_HEADER)
        open_row[8] = "Opening Balance"
        open_row[11] = f"${opening:.2f}"
        w.writerow(open_row)
        
        close_row = [""] * len(LOG_HEADER)
        close_row[8] = "Closing Balance"
        close_row[11] = f"${closing:.2f}"
        w.writerow(close_row)
        
        pnl_row = [""] * len(LOG_HEADER)
        pnl_row[8] = "PnL"
        pnl_row[11] = f"${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        w.writerow(pnl_row)
        
        # NEW: Add skipped/missed slots section
        if skipped_slots_dict:
            w.writerow(blank)
            skip_header = [""] * len(LOG_HEADER)
            skip_header[8] = f"WARNING Skipped/Missed Slots ({len(skipped_slots_dict)}/{total_slots})"
            w.writerow(skip_header)
            
            for slot_time in sorted(skipped_slots_dict.keys()):
                reason = skipped_slots_dict[slot_time]
                skip_row = [""] * len(LOG_HEADER)
                skip_row[8] = f"{slot_time} IST: {reason}"
                w.writerow(skip_row)
        
        # Add verification status
        w.writerow(blank)
        status_row = [""] * len(LOG_HEADER)
        if verification["status"] == "VERIFIED":
            status_row[8] = f"Report Status: OK VERIFIED ({verification['mt5_count']}/{verification['ledger_count']} trades confirmed in MT5)"
        elif verification["status"] == "PENDING_SYNC":
            status_row[8] = f"Report Status: WARNING PENDING_SYNC ({verification['mt5_count']}/{verification['ledger_count']} trades in MT5)"
        else:
            status_row[8] = f"Report Status: WARNING DISCREPANCY (Manual review needed)"
        w.writerow(status_row)
        
        # Add missing trades warning
        if verification["missing_in_mt5"]:
            w.writerow(blank)
            missing_header = [""] * len(LOG_HEADER)
            missing_header[8] = "WARNING MT5 Sync Status"
            w.writerow(missing_header)
            
            for missing in verification["missing_in_mt5"]:
                missing_row = [""] * len(LOG_HEADER)
                missing_row[8] = f"Missing in MT5 History: Trade #{missing['trade_number']} ({missing['entry_time']} IST) - Position {missing['position_id']}"
                w.writerow(missing_row)
            
            note_row = [""] * len(LOG_HEADER)
            note_row[8] = "Note: These trades were executed and closed by bot. MT5 history sync pending."
            w.writerow(note_row)
        
        # Add PnL discrepancies
        if verification["discrepancies"]:
            w.writerow(blank)
            disc_header = [""] * len(LOG_HEADER)
            disc_header[8] = "WARNING PnL Discrepancies"
            w.writerow(disc_header)
            
            for disc in verification["discrepancies"]:
                disc_row = [""] * len(LOG_HEADER)
                disc_row[8] = f"Trade #{disc['trade_number']}: Ledger ${disc['ledger_pnl']:.2f}, MT5 ${disc['mt5_pnl']:.2f} (Diff: ${disc['difference']:.2f})"
                w.writerow(disc_row)
        
        if withdrawals:
            w.writerow(blank)
            w_row = [""] * len(LOG_HEADER)
            w_row[8] = f"Withdrawals this {period.lower()}: " + ", ".join(withdrawals)
            w.writerow(w_row)
    
    print(f"[REPORT] {period} -> {out_file.name} ({len(rows)} trades, {len(skipped_slots_dict)} skipped, Status: {verification['status']})")

# ===== Heartbeat =====
SCHED_LOCK = Lock()
NEXT_IST_HHMM = None
NEXT_SERVER_DT = None
CURRENT_DELTA_MIN = 0
SLOTS_FIRED = 0
SLOTS_TOTAL = 0

def update_next_slot(now_server, today_server_sched, executed_ist_today, ist_day, delta_min):
    global NEXT_IST_HHMM, NEXT_SERVER_DT, CURRENT_DELTA_MIN, SLOTS_FIRED, SLOTS_TOTAL
    next_ist = None
    for ist_hhmm, server_dt in today_server_sched.items():
        if ist_hhmm in executed_ist_today:
            continue
        if now_server <= server_dt:
            next_ist = ist_hhmm
            break
    
    with SCHED_LOCK:
        NEXT_IST_HHMM = next_ist
        NEXT_SERVER_DT = today_server_sched.get(next_ist) if next_ist else None
        CURRENT_DELTA_MIN = delta_min
        SLOTS_FIRED = len(executed_ist_today)
        SLOTS_TOTAL = len(today_server_sched)

def heartbeat_thread():
    global LAST_KNOWN_BALANCE
    
    while RUN_HEARTBEAT.is_set():
        try:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST_TZ)
            
            # Get day of week
            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day_name = day_names[now_ist.weekday()]
            
            # Special format for Thursday if trading disabled
            if SKIP_THURSDAY and now_ist.weekday() == 3:
                day_display = "Thursday(Red Day / Loss Day)"
            else:
                day_display = day_name
            
            with SCHED_LOCK:
                next_ist = NEXT_IST_HHMM
                next_server = NEXT_SERVER_DT
                cfg_delta = CURRENT_DELTA_MIN
                fired = SLOTS_FIRED
                total = SLOTS_TOTAL
            
            now_server = (now_ist + timedelta(minutes=cfg_delta)).replace(tzinfo=None)
            
            with TRADES_LOCK:
                opened = TRADES_OPENED_TODAY
                closed = TRADES_CLOSED_TODAY
            
            with BALANCE_LOCK:
                current_bal = LAST_KNOWN_BALANCE
            
            # Fallback: If balance is 0 or not set, fetch live from MT5
            if current_bal == 0.0:
                try:
                    acc = mt5.account_info()
                    if acc:
                        current_bal = float(acc.balance)
                        # Update the global balance
                        with BALANCE_LOCK:
                            LAST_KNOWN_BALANCE = current_bal
                except Exception:
                    pass
            
            withdrawal_flag = "WARNING" if WITHDRAWAL_DETECTED_TODAY else "OK"
            email_status = 1 if EMAIL_SENT_TODAY else 0
            
            # Check if Thursday and no trading
            is_thursday_no_trading = SKIP_THURSDAY and now_ist.weekday() == 3
            
            if next_server is None or is_thursday_no_trading:
                # Show "no trading today" for Thursday or when no more slots
                if is_thursday_no_trading:
                    end_message = "no trading today"
                else:
                    end_message = "no more slots"
                    
                line = (
                    f"[HB] {day_display} | {now_ist.strftime('%H:%M:%S')} | "
                    f"slots {fired}/{total} | "
                    f"trades O:{opened}/18 C:{closed}/18 | "
                    f"balance ${current_bal:.2f} {withdrawal_flag} | "
                    f"email {email_status}/1 | {end_message}"
                )
            else:
                secs = max(0, int((next_server - now_server).total_seconds()))
                hh = secs // 3600
                mm = (secs % 3600) // 60
                ss = secs % 60
                line = (
                    f"[HB] {day_display} | {now_ist.strftime('%H:%M:%S')} | "
                    f"slots {fired}/{total} | "
                    f"trades O:{opened}/18 C:{closed}/18 | "
                    f"balance ${current_bal:.2f} {withdrawal_flag} | "
                    f"email {email_status}/1 | "
                    f"next: {next_ist} in {hh:02d}:{mm:02d}:{ss:02d}"
                )
            
            # Log to both console and file
            log_to_heartbeat(line)
            
        except Exception:
            pass
        _time.sleep(1)

# ===== New Report Building Functions =====
def build_daily_report_from_ledger(ist_day: date, baseline: float, out_file: Path, skipped_slots_dict: dict):
    """
    Build daily report using JSON LEDGER ONLY (no MT5 verification).
    This ensures ALL trades are present immediately, no sync delay.
    """
    global LEDGER_DATA
    
    # Get trades from ledger for current day
    ledger_trades = []
    with LEDGER_LOCK:
        for tag, entry in sorted(LEDGER_DATA["trades"].items()):
            if entry["status"] == "CLOSED":
                ledger_trades.append({
                    "date": entry["date"],
                    "trade_number": entry["trade_number"],
                    "time": entry["entry_time"],
                    "entry": entry["entry_price"],
                    "sl": entry["sl_price"],
                    "tp": entry["tp_price"],
                    "partial_close_price": entry.get("partial_close_price"),
                    "vol": entry["lot_size"],
                    "outcome": entry["trade_outcome"],
                    "partial_close_net": entry.get("partial_close_net"),
                    "pnl": entry["pnl"],
                    "balance": entry["account_balance"]
                })
    
    # Sort by trade number
    ledger_trades.sort(key=lambda x: x["trade_number"])
    
    # Build CSV rows from LEDGER
    rows = []
    for tr in ledger_trades:
        partial_close_price_str = f"{tr['partial_close_price']:.3f}" if tr['partial_close_price'] else ""
        partial_close_net_str = f"${tr['partial_close_net']:.2f}" if tr['partial_close_net'] is not None else ""
        
        rows.append([
            tr["date"], str(tr["trade_number"]), tr["time"],
            f"{tr['entry']:.3f}", f"{tr['sl']:.3f}", f"{tr['tp']:.3f}",
            partial_close_price_str,
            f"{tr['vol']:.2f}", tr["outcome"],
            partial_close_net_str,
            f"${tr['pnl']:.2f}", f"${tr['balance']:.2f}"
        ])
    
    # Write to CSV with UTF-8 encoding
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(LOG_HEADER)
        w.writerows(rows)
    
    # Add summary
    opening = float(baseline)
    closing = float(ledger_trades[-1]["balance"]) if ledger_trades else opening
    pnl = closing - opening
    
    executed_count = len(ledger_trades)
    total_slots = len(IST_TRADE_TIMES)
    
    with open(out_file, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        blank = [""] * len(LOG_HEADER)
        w.writerow(blank)
        
        title_row = [""] * len(LOG_HEADER)
        title_row[8] = f"Daily Summary ({executed_count}/{total_slots} trades executed)"
        w.writerow(title_row)
        
        open_row = [""] * len(LOG_HEADER)
        open_row[8] = "Opening Balance"
        open_row[11] = f"${opening:.2f}"
        w.writerow(open_row)
        
        close_row = [""] * len(LOG_HEADER)
        close_row[8] = "Closing Balance"
        close_row[11] = f"${closing:.2f}"
        w.writerow(close_row)
        
        pnl_row = [""] * len(LOG_HEADER)
        pnl_row[8] = "PnL"
        pnl_row[11] = f"${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        w.writerow(pnl_row)
        
        # Add skipped slots section
        if skipped_slots_dict:
            w.writerow(blank)
            skip_header = [""] * len(LOG_HEADER)
            skip_header[8] = f"Skipped/Missed Slots ({len(skipped_slots_dict)}/{total_slots})"
            w.writerow(skip_header)
            
            for slot_time in sorted(skipped_slots_dict.keys()):
                reason = skipped_slots_dict[slot_time]
                skip_row = [""] * len(LOG_HEADER)
                skip_row[8] = f"{slot_time} IST: {reason}"
                w.writerow(skip_row)
        
        # Add status
        w.writerow(blank)
        status_row = [""] * len(LOG_HEADER)
        status_row[8] = f"Report Status: SOURCE Internal Ledger (Real-time, {executed_count}/{executed_count} trades)"
        w.writerow(status_row)
    
    print(f"[REPORT] Daily (Ledger-only) -> {out_file.name} ({len(rows)} trades, {len(skipped_slots_dict)} skipped)")


def build_weekly_report_hybrid(week_start: date, week_end: date, baseline: float, out_file: Path):
    """
    Build weekly report using HYBRID approach:
    - Monday-Thursday: MT5 history (old trades, definitely synced)
    - Friday (current day): JSON ledger (real-time, no sync delay)
    """
    all_trades = []
    
    # Days 0-3: Use MT5 history (Monday-Thursday)
    for day_offset in range(4):
        current_date = week_start + timedelta(days=day_offset)
        day_start = datetime.combine(current_date, dt_time(0,0), tzinfo=IST_TZ)
        day_end = datetime.combine(current_date, dt_time(23,59,59), tzinfo=IST_TZ)
        
        # Get trades from MT5 for this day
        trades_from_mt5 = collect_closed_trades(day_start, day_end)
        
        for tr in trades_from_mt5:
            all_trades.append({
                "date": tr["date"],
                "trade_number": len(all_trades) + 1,
                "time": tr["time"],
                "entry": tr["entry"],
                "sl": tr["sl"],
                "tp": tr["tp"],
                "vol": tr["vol"],
                "outcome": tr["outcome"],
                "pnl": tr["pnl"],
                "balance": 0.0  # Will recalculate
            })
    
    # Day 4: Use JSON ledger (Friday - current day)
    friday_date = week_start + timedelta(days=4)
    friday_ledger = load_ledger(friday_date)
    
    for tag, entry in sorted(friday_ledger["trades"].items()):
        if entry["status"] == "CLOSED":
            all_trades.append({
                "date": entry["date"],
                "trade_number": len(all_trades) + 1,
                "time": entry["entry_time"],
                "entry": entry["entry_price"],
                "sl": entry["sl_price"],
                "tp": entry["tp_price"],
                "vol": entry["lot_size"],
                "outcome": entry["trade_outcome"],
                "pnl": entry["pnl"],
                "balance": entry["account_balance"]
            })
    
    # Recalculate balances
    running_balance = baseline
    for tr in all_trades:
        running_balance += tr["pnl"]
        tr["balance"] = running_balance
    
    # Build CSV
    rows = []
    for tr in all_trades:
        rows.append([
            tr["date"], str(tr["trade_number"]), tr["time"],
            f"{tr['entry']:.3f}", f"{tr['sl']:.3f}", f"{tr['tp']:.3f}",
            f"{tr['vol']:.2f}", tr["outcome"],
            f"${tr['pnl']:.2f}", f"${tr['balance']:.2f}"
        ])
    
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(LOG_HEADER)
        w.writerows(rows)
    
    # Summary
    opening = float(baseline)
    closing = float(all_trades[-1]["balance"]) if all_trades else opening
    pnl = closing - opening
    
    with open(out_file, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        blank = [""] * len(LOG_HEADER)
        w.writerow(blank)
        
        title_row = [""] * len(LOG_HEADER)
        title_row[7] = f"Weekly Summary ({len(all_trades)} trades executed)"
        w.writerow(title_row)
        
        open_row = [""] * len(LOG_HEADER)
        open_row[7] = "Opening Balance (Monday)"
        open_row[9] = f"${opening:.2f}"
        w.writerow(open_row)
        
        close_row = [""] * len(LOG_HEADER)
        close_row[7] = "Closing Balance (Friday)"
        close_row[9] = f"${closing:.2f}"
        w.writerow(close_row)
        
        pnl_row = [""] * len(LOG_HEADER)
        pnl_row[7] = "Weekly PnL"
        pnl_row[9] = f"${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        w.writerow(pnl_row)
        
        w.writerow(blank)
        status_row = [""] * len(LOG_HEADER)
        status_row[7] = "Report Status: HYBRID Mon-Thu (MT5 verified), Fri (Ledger real-time)"
        w.writerow(status_row)
    
    print(f"[REPORT] Weekly (Hybrid) -> {out_file.name} ({len(rows)} trades)")


def build_monthly_report_hybrid(month_start: date, month_end: date, baseline: float, out_file: Path):
    """
    Build monthly report using HYBRID approach:
    - All past days: MT5 history (old trades, definitely synced)
    - Current day: JSON ledger (real-time, no sync delay)
    """
    all_trades = []
    current_day_today = datetime.now(timezone.utc).astimezone(IST_TZ).date()
    
    # All past days: Use MT5 history
    current_date = month_start
    while current_date < current_day_today:
        day_start = datetime.combine(current_date, dt_time(0,0), tzinfo=IST_TZ)
        day_end = datetime.combine(current_date, dt_time(23,59,59), tzinfo=IST_TZ)
        
        # Get trades from MT5 for this day
        trades_from_mt5 = collect_closed_trades(day_start, day_end)
        
        for tr in trades_from_mt5:
            all_trades.append({
                "date": tr["date"],
                "trade_number": len(all_trades) + 1,
                "time": tr["time"],
                "entry": tr["entry"],
                "sl": tr["sl"],
                "tp": tr["tp"],
                "vol": tr["vol"],
                "outcome": tr["outcome"],
                "pnl": tr["pnl"],
                "balance": 0.0  # Will recalculate
            })
        
        current_date += timedelta(days=1)
    
    # Current day ONLY: Use JSON ledger
    if current_day_today <= month_end:
        today_ledger = load_ledger(current_day_today)
        
        for tag, entry in sorted(today_ledger["trades"].items()):
            if entry["status"] == "CLOSED":
                all_trades.append({
                    "date": entry["date"],
                    "trade_number": len(all_trades) + 1,
                    "time": entry["entry_time"],
                    "entry": entry["entry_price"],
                    "sl": entry["sl_price"],
                    "tp": entry["tp_price"],
                    "vol": entry["lot_size"],
                    "outcome": entry["trade_outcome"],
                    "pnl": entry["pnl"],
                    "balance": entry["account_balance"]
                })
    
    # Recalculate balances
    running_balance = baseline
    for tr in all_trades:
        running_balance += tr["pnl"]
        tr["balance"] = running_balance
    
    # Build CSV
    rows = []
    for tr in all_trades:
        rows.append([
            tr["date"], str(tr["trade_number"]), tr["time"],
            f"{tr['entry']:.3f}", f"{tr['sl']:.3f}", f"{tr['tp']:.3f}",
            f"{tr['vol']:.2f}", tr["outcome"],
            f"${tr['pnl']:.2f}", f"${tr['balance']:.2f}"
        ])
    
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(LOG_HEADER)
        w.writerows(rows)
    
    # Summary
    opening = float(baseline)
    closing = float(all_trades[-1]["balance"]) if all_trades else opening
    pnl = closing - opening
    
    with open(out_file, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        blank = [""] * len(LOG_HEADER)
        w.writerow(blank)
        
        title_row = [""] * len(LOG_HEADER)
        title_row[7] = f"Monthly Summary ({len(all_trades)} trades executed)"
        w.writerow(title_row)
        
        open_row = [""] * len(LOG_HEADER)
        open_row[7] = "Opening Balance"
        open_row[9] = f"${opening:.2f}"
        w.writerow(open_row)
        
        close_row = [""] * len(LOG_HEADER)
        close_row[7] = "Closing Balance"
        close_row[9] = f"${closing:.2f}"
        w.writerow(close_row)
        
        pnl_row = [""] * len(LOG_HEADER)
        pnl_row[7] = "Monthly PnL"
        pnl_row[9] = f"${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        w.writerow(pnl_row)
        
        w.writerow(blank)
        status_row = [""] * len(LOG_HEADER)
        past_days = (current_day_today - month_start).days
        status_row[7] = f"Report Status: HYBRID Past days (MT5 verified), Today (Ledger real-time)"
        w.writerow(status_row)
    
    print(f"[REPORT] Monthly (Hybrid) -> {out_file.name} ({len(rows)} trades)")


# ===== Smart Report Generation =====
def send_reports(ist_day: date, executed_slots: set, today_server_sched: dict):
    global EMAIL_SENT_TODAY
    
    day_start = datetime.combine(ist_day, dt_time(0,0), tzinfo=IST_TZ)
    day_end = datetime.combine(ist_day, dt_time(23,59,59), tzinfo=IST_TZ)
    baseline = load_baseline()
    
    expected_slots = set(IST_TRADE_TIMES)
    
    # Get skipped slots with reasons
    with SKIPPED_SLOTS_LOCK:
        skipped_dict = dict(SKIPPED_SLOTS)
    
    # Count executed vs total
    executed_count = len(executed_slots)
    total_count = len(IST_TRADE_TIMES)
    
    # CHANGE 4: Daily report uses LEDGER ONLY (no MT5 verification)
    daily_file = LOG_DIR / f"{fmt_date(ist_day)}.csv"
    build_daily_report_from_ledger(ist_day, baseline, daily_file, skipped_dict)
    
    # Updated email subject to show X/18 trades
    subject = f"Daily Trading Log — {fmt_date(ist_day)} ({executed_count}/{total_count} trades executed)"
    send_email(subject, "Attached is today's trading log.", daily_file)
    
    # CHANGE 5: Weekly report uses HYBRID (MT5 for past days, ledger for current day)
    if ist_day.weekday() == 4:
        start_week = ist_day - timedelta(days=4)
        start_dt = datetime.combine(start_week, dt_time(0,0), tzinfo=IST_TZ)
        end_dt = day_end
        weekly_file = LOG_DIR / f"weekly_{fmt_date(ist_day)}.csv"
        
        build_weekly_report_hybrid(start_week, ist_day, baseline, weekly_file)
        send_email(f"Weekly Trading Log — week ending {fmt_date(ist_day)}", "Attached is the weekly trading log.", weekly_file)
        
        # CHANGE 6: Monthly report uses HYBRID (MT5 for past days, ledger for current day)
        y, m = ist_day.year, ist_day.month
        next_month = date(y+1, 1, 1) if m == 12 else date(y, m+1, 1)
        last_day = next_month - timedelta(days=1)
        if last_day.weekday() > 4:
            last_business = last_day - timedelta(days=last_day.weekday() - 4)
        else:
            last_business = last_day
        
        if ist_day == last_business:
            start_month = date(y, m, 1)
            start_dt_m = datetime.combine(start_month, dt_time(0,0), tzinfo=IST_TZ)
            monthly_file = LOG_DIR / f"monthly_{y}-{m:02d}.csv"
            
            build_monthly_report_hybrid(start_month, ist_day, baseline, monthly_file)
            send_email(f"Monthly Trading Log — {y}-{m:02d}", "Attached is the monthly trading log.", monthly_file)
    
    EMAIL_SENT_TODAY = True

def send_emergency_report(ist_day: date, executed_slots: set):
    """
    Emergency fallback report using ONLY ledger data (no MT5 verification).
    This CANNOT crash because it doesn't depend on MT5 history.
    FIXED: Uses UTF-8 encoding for CSV writes.
    """
    global EMAIL_SENT_TODAY, LEDGER_DATA
    
    daily_file = LOG_DIR / f"{fmt_date(ist_day)}_EMERGENCY.csv"
    
    # Get trades from ledger
    with LEDGER_LOCK:
        trades = []
        for tag, entry in sorted(LEDGER_DATA["trades"].items()):
            if entry["status"] == "CLOSED":
                trades.append(entry)
    
    trades.sort(key=lambda x: x["trade_number"])
    
    # Get skipped slots
    with SKIPPED_SLOTS_LOCK:
        skipped_dict = dict(SKIPPED_SLOTS)
    
    # Write CSV with UTF-8 encoding
    with open(daily_file, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(LOG_HEADER)
        
        # Write trades
        for tr in trades:
            partial_close_price_str = f"{tr.get('partial_close_price'):.3f}" if tr.get('partial_close_price') else ""
            partial_close_net_str = f"${tr.get('partial_close_net'):.2f}" if tr.get('partial_close_net') is not None else ""
            
            w.writerow([
                tr["date"], str(tr["trade_number"]), tr["entry_time"],
                f"{tr['entry_price']:.3f}", f"{tr['sl_price']:.3f}", f"{tr['tp_price']:.3f}",
                partial_close_price_str,
                f"{tr['lot_size']:.2f}", tr["trade_outcome"],
                partial_close_net_str,
                f"${tr['pnl']:.2f}", f"${tr['account_balance']:.2f}"
            ])
        
        # Add summary
        blank = [""] * len(LOG_HEADER)
        w.writerow(blank)
        
        # Emergency notice
        notice_row = [""] * len(LOG_HEADER)
        notice_row[8] = "WARNING EMERGENCY REPORT - MT5 verification skipped due to error"
        w.writerow(notice_row)
        
        w.writerow(blank)
        
        # Trade count
        count_row = [""] * len(LOG_HEADER)
        count_row[8] = f"Trades executed: {len(trades)}/18"
        w.writerow(count_row)
        
        # Opening/Closing balance
        opening = LEDGER_DATA.get("opening_balance", 0.0)
        closing = LEDGER_DATA.get("closing_balance", 0.0)
        pnl = closing - opening
        
        open_row = [""] * len(LOG_HEADER)
        open_row[8] = "Opening Balance"
        open_row[11] = f"${opening:.2f}"
        w.writerow(open_row)
        
        close_row = [""] * len(LOG_HEADER)
        close_row[8] = "Closing Balance"
        close_row[11] = f"${closing:.2f}"
        w.writerow(close_row)
        
        pnl_row = [""] * len(LOG_HEADER)
        pnl_row[8] = "PnL"
        pnl_row[11] = f"${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        w.writerow(pnl_row)
        
        # Skipped slots
        if skipped_dict:
            w.writerow(blank)
            skip_header = [""] * len(LOG_HEADER)
            skip_header[8] = f"WARNING Skipped Slots ({len(skipped_dict)}/18)"
            w.writerow(skip_header)
            
            for slot_time in sorted(skipped_dict.keys()):
                reason = skipped_dict[slot_time]
                skip_row = [""] * len(LOG_HEADER)
                skip_row[8] = f"{slot_time} IST: {reason}"
                w.writerow(skip_row)
    
    # Send email
    executed_count = len(executed_slots)
    subject = f"WARNING EMERGENCY Daily Log — {fmt_date(ist_day)} ({executed_count}/18 trades)"
    body = (
        f"Emergency report generated due to MT5 verification failure.\n\n"
        f"All trade data is from internal ledger (reliable).\n"
        f"Bot is still running normally.\n\n"
        f"Check logs for error details."
    )
    send_email(subject, body, daily_file)
    
    EMAIL_SENT_TODAY = True

def check_day_end_conditions(ist_day: date, executed_slots: set, today_server_sched: dict):
    """
    NEW: Smart report generation with time-based trigger
    CRASH-PROOF: Will NEVER crash - has triple-layer error handling
    """
    global EMAIL_SENT_TODAY, REPORT_WAIT_TRIGGERED, REPORT_WAIT_START_TIME, LAST_SLOT_CLOSED
    
    if EMAIL_SENT_TODAY:
        return
    
    # Check: Was last slot (20:35) executed?
    if LAST_SLOT_TIME not in executed_slots:
        return
    
    # Check: Did 20:35 position close?
    if not LAST_SLOT_CLOSED:
        return
    
    # Start 2-minute countdown
    with REPORT_WAIT_LOCK:
        if not REPORT_WAIT_TRIGGERED:
            REPORT_WAIT_TRIGGERED = True
            REPORT_WAIT_START_TIME = LAST_SLOT_CLOSE_TIME
            print(f"\n{'='*60}")
            print(f"[REPORT] Last slot ({LAST_SLOT_TIME}) position closed")
            print(f"[REPORT] Waiting {REPORT_WAIT_AFTER_LAST_CLOSE} seconds for MT5 history sync...")
            print(f"{'='*60}\n")
            return
        
        # Check if 2 minutes elapsed
        elapsed_seconds = _time.time() - REPORT_WAIT_START_TIME
        if elapsed_seconds < REPORT_WAIT_AFTER_LAST_CLOSE:
            return
    
    # 2 minutes elapsed - send reports with TRIPLE-LAYER ERROR HANDLING
    print(f"[REPORT] Wait complete. Generating reports from ledger...")
    
    try:
        # LAYER 1: Try normal report generation
        send_reports(ist_day, executed_slots, today_server_sched)
        print(f"[REPORT] OK Reports sent successfully")
        
    except Exception as e1:
        print(f"[REPORT] WARNING ERROR in normal report generation: {e1}")
        print(f"[REPORT] Attempting emergency fallback...")
        
        try:
            # LAYER 2: Try simplified report (ledger only, no MT5 verification)
            send_emergency_report(ist_day, executed_slots)
            print(f"[REPORT] OK Emergency report sent")
            
        except Exception as e2:
            print(f"[REPORT] WARNING ERROR in emergency report: {e2}")
            print(f"[REPORT] Attempting minimal notification...")
            
            try:
                # LAYER 3: Send minimal email notification
                executed_count = len(executed_slots)
                subject = f"WARNING Bot Report Failed — {fmt_date(ist_day)} ({executed_count}/18 trades)"
                body = (
                    f"Report generation failed with errors:\n\n"
                    f"Primary error: {str(e1)}\n"
                    f"Secondary error: {str(e2)}\n\n"
                    f"Bot is still running. Check logs and ledger file manually:\n"
                    f"{get_ledger_filepath(ist_day)}\n\n"
                    f"Executed slots: {sorted(executed_slots)}"
                )
                send_email(subject, body)
                print(f"[REPORT] OK Minimal notification sent")
                
            except Exception as e3:
                print(f"[REPORT] ERROR ALL report methods failed: {e3}")
                print(f"[REPORT] Bot will continue running despite report failure")
            
            finally:
                # CRITICAL: Always mark as sent to prevent infinite retries
                EMAIL_SENT_TODAY = True

# ===== Main loop =====
def main():
    global BALANCE_QUERIED_TODAY, WITHDRAWAL_DETECTED_TODAY, REPORT_WAIT_TRIGGERED, REPORT_WAIT_START_TIME
    global LEDGER_DATA, DAILY_OPENING_BALANCE, DAILY_CLOSING_BALANCE, LAST_SLOT_CLOSED, LAST_SLOT_CLOSE_TIME
    global SKIPPED_SLOTS, HEARTBEAT_LOG_EMAILED_TODAY
    
    init_mt5()
    print("[INIT] MT5 initialized for Vantage")
    print(f"[INIT] ENHANCED VERSION with:")
    print(f"[INIT]   - Tick Price Ledger (accurate backfill from live data)")
    print(f"[INIT]   - Internal Ledger (primary source for reports)")
    print(f"[INIT]   - Smart report generation (2-min wait after last slot closes)")
    print(f"[INIT]   - 24/7 withdrawal detection via MT5 history")
    print(f"[INIT]   - Crash recovery (rebuilds state from MT5)")
    print(f"[INIT]   - Balance query at {BALANCE_QUERY_TIME} IST")
    print(f"[INIT]   - Signal threshold: 0.02 pips")
    print(f"[INIT]   - Skip tracking with detailed reasons")
    print(f"[INIT]   - CRASH-PROOF: Triple-layer error handling")
    print(f"[INIT]   - FIXED: UTF-8 encoding for all CSV files")
    print(f"[INIT]   - Thursday trading: {'DISABLED' if SKIP_THURSDAY else 'ENABLED'}")
    print(f"[INIT]   - Watcher timeout: {MAX_POSITION_HOLD_HOURS}h")
    
    # Clean up old tick ledgers
    cleanup_old_tick_ledgers()
    
    # Determine current day
    now_ist = datetime.now(timezone.utc).astimezone(IST_TZ)
    current_ist_day = now_ist.date()
    
    # Initialize heartbeat log immediately at startup
    init_heartbeat_log(current_ist_day)
    print(f"[HEARTBEAT_LOG] Started logging for {fmt_date(current_ist_day)}")
    
    # Show day of week and Thursday status
    day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][current_ist_day.weekday()]
    print(f"\n[INIT] Today is {day_name}, {fmt_date(current_ist_day)}")
    if SKIP_THURSDAY and current_ist_day.weekday() == 3:
        print(f"[INIT] ⚠️  THURSDAY - All 18 trades will be skipped today (SKIP_THURSDAY=True)")
        print(f"[INIT] Bot will still run, monitor balance, and generate report at end of day\n")
    
    # Load or rebuild ledger
    ledger_file = get_ledger_filepath(current_ist_day)
    if ledger_file.exists():
        print(f"[LEDGER] Loading existing ledger for {fmt_date(current_ist_day)}")
        LEDGER_DATA = load_ledger(current_ist_day)
    else:
        print(f"[LEDGER] Creating new ledger for {fmt_date(current_ist_day)}")
        LEDGER_DATA = load_ledger(current_ist_day)
        
        # Try to rebuild from MT5 if bot restarted mid-day
        print(f"[LEDGER] Checking if we need to rebuild from MT5...")
        rebuilt_ledger = rebuild_ledger_from_mt5(current_ist_day)
        if rebuilt_ledger["trades"]:
            LEDGER_DATA = rebuilt_ledger
            save_ledger(current_ist_day, LEDGER_DATA)
            print(f"[LEDGER] OK Ledger rebuilt from MT5")
    
    # Set opening balance from ledger or baseline
    if LEDGER_DATA.get("opening_balance", 0.0) > 0:
        DAILY_OPENING_BALANCE = LEDGER_DATA["opening_balance"]
    else:
        DAILY_OPENING_BALANCE = load_baseline()
        LEDGER_DATA["opening_balance"] = DAILY_OPENING_BALANCE
        save_ledger(current_ist_day, LEDGER_DATA)
    
    print(f"[LEDGER] Opening balance: ${DAILY_OPENING_BALANCE:.2f}")
    
    # Initialize heartbeat log file for today
    init_heartbeat_log(current_ist_day)
    
    # Initialize current balance immediately on startup
    try:
        acc = mt5.account_info()
        if acc:
            global LAST_KNOWN_BALANCE
            with BALANCE_LOCK:
                LAST_KNOWN_BALANCE = float(acc.balance)
            print(f"[INIT] Current balance initialized: ${LAST_KNOWN_BALANCE:.2f}")
        else:
            print(f"[INIT] WARNING: Could not fetch current balance from MT5")
    except Exception as e:
        print(f"[INIT] ERROR: Failed to initialize balance: {e}")
    
    # RECOVERY: Rebuild state from MT5
    print(f"\n[RECOVERY] Starting crash recovery...")
    recover_trading_state_from_mt5(current_ist_day)
    executed_ist_today = recover_executed_slots_from_mt5(current_ist_day)
    recover_withdrawals_from_mt5(current_ist_day)
    recover_orphaned_positions(current_ist_day)
    print(f"[RECOVERY] Complete\n")
    
    # Initialize schedule
    today_server_sched, delta_min = build_server_schedule_for_day(current_ist_day)
    
    global EMAIL_SENT_TODAY, TRADES_OPENED_TODAY, TRADES_CLOSED_TODAY
    
    # Start threads
    hb = threading.Thread(target=heartbeat_thread, daemon=True)
    hb.start()
    
    open_monitor = threading.Thread(target=opening_price_monitor_thread, daemon=True)
    open_monitor.start()
    
    # Start balance monitor thread
    bal_monitor = threading.Thread(target=balance_monitor_thread, daemon=True)
    bal_monitor.start()
    
    # Backfill opening prices
    backfill_todays_opens(current_ist_day)
    
    # Update next slot
    now_server = (now_ist + timedelta(minutes=delta_min)).replace(tzinfo=None)
    update_next_slot(now_server, today_server_sched, executed_ist_today, current_ist_day, delta_min)
    
    try:
        while True:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST_TZ)
            ist_day = now_ist.date()
            current_time_key = now_ist.strftime("%H:%M")
            
            # Balance query at 05:45
            if current_time_key == BALANCE_QUERY_TIME and not BALANCE_QUERIED_TODAY:
                query_and_set_opening_balance(ist_day)
            
            # Email heartbeat log at 00:05 IST
            if current_time_key == HEARTBEAT_LOG_EMAIL_TIME and not HEARTBEAT_LOG_EMAILED_TODAY:
                yesterday = ist_day - timedelta(days=1)
                email_heartbeat_log(yesterday)
                HEARTBEAT_LOG_EMAILED_TODAY = True
            
            # Day rollover
            if ist_day != current_ist_day:
                with TRADES_LOCK:
                    if TRADES_OPENED_TODAY != TRADES_CLOSED_TODAY:
                        send_email(
                            "ALERT: Open trades at day rollover",
                            f"Opened: {TRADES_OPENED_TODAY}, Closed: {TRADES_CLOSED_TODAY}\n"
                            "Some positions may still be open from yesterday."
                        )
                
                # Close previous day's heartbeat log
                close_heartbeat_log()
                
                executed_ist_today.clear()
                current_ist_day = ist_day
                today_server_sched, delta_min = build_server_schedule_for_day(ist_day)
                EMAIL_SENT_TODAY = False
                BALANCE_QUERIED_TODAY = False
                WITHDRAWAL_DETECTED_TODAY = False
                REPORT_WAIT_TRIGGERED = False
                REPORT_WAIT_START_TIME = None
                LAST_SLOT_CLOSED = False
                LAST_SLOT_CLOSE_TIME = None
                HEARTBEAT_LOG_EMAILED_TODAY = False  # Reset for new day
                
                with TRADES_LOCK:
                    TRADES_OPENED_TODAY = 0
                    TRADES_CLOSED_TODAY = 0
                
                with CANDLE_OPENS_LOCK:
                    CANDLE_OPENS.clear()
                
                with SKIPPED_SLOTS_LOCK:
                    SKIPPED_SLOTS.clear()
                
                # Create new ledger for new day
                LEDGER_DATA = load_ledger(ist_day)
                DAILY_OPENING_BALANCE = load_baseline()
                LEDGER_DATA["opening_balance"] = DAILY_OPENING_BALANCE
                save_ledger(ist_day, LEDGER_DATA)
                
                print(f"\n{'='*60}")
                print(f"[DAY] New IST day: {fmt_date(ist_day)}")
                print(f"[DAY] Day of week: {['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][ist_day.weekday()]}")
                print(f"[DAY] Server-IST delta: {delta_min:+} minutes")
                print(f"[DAY] Opening balance: ${DAILY_OPENING_BALANCE:.2f}")
                if SKIP_THURSDAY and ist_day.weekday() == 3:
                    print(f"[DAY] ⚠️  THURSDAY - All trading disabled (SKIP_THURSDAY=True)")
                print(f"{'='*60}\n")
                
                # Initialize new heartbeat log for new day
                init_heartbeat_log(ist_day)
                print(f"[HEARTBEAT_LOG] New log started for {fmt_date(ist_day)}")
                
                # Clean up old tick ledgers
                cleanup_old_tick_ledgers()
                
                backfill_todays_opens(ist_day)
                
                update_next_slot(
                    (now_ist + timedelta(minutes=delta_min)).replace(tzinfo=None),
                    today_server_sched, executed_ist_today, ist_day, delta_min
                )
            
            # Update open trades counter
            try:
                positions = mt5.positions_get(symbol=SYMBOL)
                count = 0
                if positions:
                    for p in positions:
                        if getattr(p, "magic", 0) == MAGIC:
                            count += 1
                global OPEN_TRADES_GLOBAL
                OPEN_TRADES_GLOBAL = count
            except Exception:
                OPEN_TRADES_GLOBAL = 0
            
            # Trading logic
            delta_min = ist_to_server_delta_minutes_for_date(ist_day)
            now_server = (now_ist + timedelta(minutes=delta_min)).replace(tzinfo=None)
            
            # Check if today is Thursday and skip trading if configured
            is_thursday = ist_day.weekday() == 3  # Thursday = 3
            
            for ist_hhmm, fire_dt_server in list(today_server_sched.items()):
                if ist_hhmm in executed_ist_today:
                    continue
                
                lag = (now_server - fire_dt_server).total_seconds()
                
                # TIMING FIX: Only fire when within window (0 to 10 seconds after scheduled time)
                if -EARLY_TRIGGER_WINDOW_SECONDS <= lag <= FIRE_WINDOW_SECONDS:
                    # Additional precision check: Only fire when seconds are 0-10
                    current_second = now_server.second
                    
                    if current_second > 10:
                        # Too late in the minute, skip this cycle
                        continue
                    
                    # Skip Thursday trading if configured
                    if SKIP_THURSDAY and is_thursday:
                        skip_reason = "Thursday trading disabled (SKIP_THURSDAY=True)"
                        with SKIPPED_SLOTS_LOCK:
                            SKIPPED_SLOTS[ist_hhmm] = skip_reason
                        print(f"[SKIP] {ist_hhmm} IST: {skip_reason}")
                        executed_ist_today.add(ist_hhmm)
                        update_next_slot(now_server, today_server_sched, executed_ist_today, ist_day, delta_min)
                        continue
                    
                    signal, skip_reason = get_signal_from_live_opens(ist_hhmm)
                    
                    if not signal:
                        # Trade skipped - record reason
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
                            
                            # Get entry details
                            tick = mt5.symbol_info_tick(SYMBOL)
                            entry_price = tick.ask if signal == "BUY" else tick.bid
                            sl, tp = compute_sl_tp(entry_price, signal)
                            
                            # Create ledger entry
                            create_ledger_entry(
                                pos_ticket, ist_day, trade_number,
                                ist_hhmm, entry_price, sl, tp,
                                vol, signal, tag
                            )
                            
                            spawn_watcher(pos_ticket, signal, tag, ist_day)
                            print(f"[SUCCESS] Position {pos_ticket} opened and watcher spawned")
                        else:
                            print(f"[ERROR] Trade reported success but position not found for {tag}")
                            # Record as skipped due to verification failure
                            with SKIPPED_SLOTS_LOCK:
                                SKIPPED_SLOTS[ist_hhmm] = "Trade placement verification failed"
                    else:
                        print(f"[TRADE] Failed: ret={getattr(result, 'retcode', None)}")
                        # Record as skipped due to trade failure
                        with SKIPPED_SLOTS_LOCK:
                            SKIPPED_SLOTS[ist_hhmm] = f"Trade placement failed - MT5 error code {getattr(result, 'retcode', 'unknown')}"
                
                elif lag > FIRE_WINDOW_SECONDS:
                    # Slot expired without execution
                    with SKIPPED_SLOTS_LOCK:
                        if ist_hhmm not in SKIPPED_SLOTS:
                            SKIPPED_SLOTS[ist_hhmm] = f"Slot expired (missed time window by {int(lag)}s)"
                    
                    executed_ist_today.add(ist_hhmm)
                    update_next_slot(now_server, today_server_sched, executed_ist_today, ist_day, delta_min)
            
            # Check for report generation
            check_day_end_conditions(ist_day, executed_ist_today, today_server_sched)
            
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
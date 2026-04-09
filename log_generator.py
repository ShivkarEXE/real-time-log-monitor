import time, random, os
from datetime import datetime

LEVELS   = ["INFO", "WARN", "ERROR", "DEBUG"]
SERVICES = ["auth-service", "api-gateway", "order-service", "payment-service"]
MESSAGES = {
    "INFO":  ["Request processed", "User logged in", "Cache hit", "DB query ok"],
    "WARN":  ["Slow response >500ms", "Retry attempt", "Memory usage 80%"],
    "ERROR": ["Connection refused", "NullPointerException", "Timeout", "OOM"],
    "DEBUG": ["Entering function", "Param received", "Token validated"],
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Find the highest existing file number so we never overwrite old files
existing = [f for f in os.listdir(LOG_DIR) if f.startswith("log_") and f.endswith(".csv")]
if existing:
    nums = []
    for f in existing:
        try:
            nums.append(int(f.replace("log_", "").replace(".csv", "")))
        except ValueError:
            pass
    file_count = max(nums) if nums else 0
else:
    file_count = 0

print(f"Log generator started (resuming from file #{file_count}). Press Ctrl+C to stop.")

while True:
    level   = random.choices(LEVELS, weights=[60, 20, 10, 10])[0]
    service = random.choice(SERVICES)
    msg     = random.choice(MESSAGES[level])
    ts      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    file_count += 1
    fname = os.path.join(LOG_DIR, f"log_{file_count:06d}.csv")

    # Every file is a valid standalone CSV with a header row.
    # Spark schema inference is off; we pass schema explicitly — the header
    # row will be skipped correctly via .option("header", "true").
    with open(fname, "w") as f:
        f.write("timestamp,level,service,message\n")
        f.write(f"{ts},{level},{service},{msg}\n")

    print(f"  Written: [{level}] {service} — {msg}")
    time.sleep(0.5)

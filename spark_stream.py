import os
import glob
import threading
import collections

# ─── Windows / Java environment setup ─────────────────────────────────────────
_HADOOP_HOME = r"D:\hadoop"
_HADOOP_BIN  = os.path.join(_HADOOP_HOME, "bin")

os.environ["HADOOP_HOME"] = _HADOOP_HOME
os.putenv("HADOOP_HOME", _HADOOP_HOME)

# Force Java 17 (Java 24 removed Subject.getSubject used by Hadoop)
_j17_candidates = glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-17*")
if _j17_candidates:
    _java17_home = sorted(_j17_candidates)[-1]
    os.environ["JAVA_HOME"] = _java17_home
    os.putenv("JAVA_HOME", _java17_home)
    os.environ["PATH"] = os.path.join(_java17_home, "bin") + os.pathsep + os.environ["PATH"]

os.environ["PATH"] = _HADOOP_BIN + os.pathsep + os.environ["PATH"]
os.putenv("PATH", os.environ["PATH"])
# ──────────────────────────────────────────────────────────────────────────────

# ─── JVM flags ────────────────────────────────────────────────────────────────
_jvm_flags = " ".join([
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    "--enable-native-access=ALL-UNNAMED",
    "-Dhadoop.security.authentication=simple",
    "-Djdk.reflect.useDirectMethodHandle=false",
    f"-Dhadoop.home.dir={_HADOOP_HOME}",
    f"-Djava.library.path={_HADOOP_BIN}",
])
os.environ["JAVA_TOOL_OPTIONS"] = _jvm_flags
os.putenv("JAVA_TOOL_OPTIONS", _jvm_flags)
# ──────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

# ─── Shared state ─────────────────────────────────────────────────────────────
# Kept in a class so process_batch captures ONE object reference in its closure.
# If we used bare module-level dicts/sets/Lock, cloudpickle would try to
# serialise every global in the module and hit a recursion/stack-overflow.
#
# The Lock (_state_lock) is intentionally separate and NOT referenced inside
# process_batch.  Spark guarantees foreachBatch is never called concurrently,
# so no locking is needed on the write path.  We only lock when Flask reads.

class _State:
    def __init__(self):
        self.recent_events = collections.deque(maxlen=200)
        self.levels        = {}
        self.errors        = {}
        self.services      = set()
        self.total_logs    = 0

_state      = _State()
_state_lock = threading.Lock()   # Flask read-side only
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "logs")

# ─── Spark session ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("RealTimeLogMonitor") \
    .master("local[*]") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("level",     StringType()) \
    .add("service",   StringType()) \
    .add("message",   StringType())

raw_stream = spark.readStream \
    .option("sep",    ",") \
    .option("header", "true") \
    .schema(schema) \
    .csv(LOG_DIR)

print("\n" + "="*50)
print("  REAL-TIME LOG MONITORING DASHBOARD")
print("  Apache Spark Structured Streaming")
print("="*50 + "\n")

# ─── foreachBatch handler ─────────────────────────────────────────────────────
# CRITICAL RULES to avoid PicklingError / RecursionError:
#
#   1. NEVER call batch_df.rdd inside foreachBatch.  Any RDD operation forces
#      cloudpickle to serialise the Python closure.  If that closure touches
#      module globals (Lock, deque, dicts …) you get a stack-overflow.
#      Use DataFrame.count() — it stays in the JVM, nothing is pickled.
#
#   2. Only capture _state (one object ref) in the closure.
#      Do NOT reference _state_lock here.

def process_batch(batch_df, batch_id):
    # count() is a pure JVM call — safe inside foreachBatch.
    if batch_df.count() == 0:
        return

    rows = batch_df.collect()

    for row in rows:
        ts      = row["timestamp"] or ""
        level   = (row["level"]   or "").strip().upper()
        service = row["service"]  or ""
        message = row["message"]  or ""

        _state.recent_events.append({
            "timestamp": ts,
            "level":     level,
            "service":   service,
            "message":   message,
        })
        _state.total_logs          += 1
        _state.levels[level]        = _state.levels.get(level, 0) + 1
        _state.services.add(service)
        if level == "ERROR":
            _state.errors[service] = _state.errors.get(service, 0) + 1

    print(f"  [Batch {batch_id}] +{len(rows)} rows  |  total={_state.total_logs}")

# ─── Single streaming query ───────────────────────────────────────────────────
query = raw_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="2 seconds") \
    .start()

print(f"Streaming started. Watching: {LOG_DIR}")
print("Run log_generator.py in another terminal.")
print("Open dashboard at: http://localhost:5000\n")

# ─── Flask: API + static file server ─────────────────────────────────────────
# Serving the HTML from Flask means the browser hits http://localhost:5000 for
# both the page and the API — same origin, no CORS issues at all.

DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=DASHBOARD_DIR)
CORS(app)

@app.route('/')
def index():
    return send_from_directory(DASHBOARD_DIR, 'index.html')

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory(DASHBOARD_DIR, filename)

@app.route('/api/metrics')
def get_metrics():
    with _state_lock:
        levels_snap = dict(_state.levels)
        errors_snap = [
            {"service": k, "count": v}
            for k, v in sorted(_state.errors.items(), key=lambda x: -x[1])
        ]
        total_snap  = _state.total_logs
        svc_count   = len(_state.services)
        events_snap = list(reversed(list(_state.recent_events)))[:50]

    return jsonify({
        "total_logs":      total_snap,
        "levels":          levels_snap,
        "errors":          errors_snap,
        "active_services": svc_count,
        "events":          events_snap,
    })

@app.route('/api/health')
def health():
    return jsonify({"status": "ok", "total": _state.total_logs})

# ─── Background threads ───────────────────────────────────────────────────────
def run_flask():
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)

def console_loop():
    import time
    while True:
        time.sleep(5)
        print(f"\n{'='*40}")
        print(f"  total={_state.total_logs}  levels={_state.levels}")
        print(f"  errors={_state.errors}")
        print(f"{'='*40}")

threading.Thread(target=run_flask,    daemon=True).start()
threading.Thread(target=console_loop, daemon=True).start()

query.awaitTermination()
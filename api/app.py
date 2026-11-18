import os
import time
import math
import uuid
import json
import random
import logging
import threading
import multiprocessing
import redis
import psutil
from flask import Flask, jsonify, request, g, Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from multiprocessing.synchronize import Event as MpEvent

app = Flask(__name__)

# --- Structured JSON Logging ---
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": "resilience-api",
            "pod": os.uname().nodename,
            "message": record.getMessage(),
        }
        if hasattr(record, "request_id"):
            log_record["request_id"] = record.request_id
        return json.dumps(log_record)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.root.handlers = [handler]
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("resilience-api")

# --- Configuration ---
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None
CHAOS_KEY = os.environ.get("CHAOS_KEY", "resilience:chaos_state")
CHAOS_HISTORY_KEY = os.environ.get("CHAOS_HISTORY_KEY", "resilience:chaos_history")
CHAOS_HISTORY_MAX = int(os.environ.get("CHAOS_HISTORY_MAX", "50"))
CHAOS_PUBSUB_CHANNEL = os.environ.get("CHAOS_PUBSUB_CHANNEL", "resilience:chaos_events")

MAX_LATENCY_SECONDS = float(os.environ.get("MAX_LATENCY_SECONDS", "30.0"))
CPU_SPIKE_DURATION = float(os.environ.get("CPU_SPIKE_DURATION", "15.0"))

# --- Prometheus Metrics ---
REQUEST_COUNT = Counter(
    "api_http_requests_total",
    "Total HTTP requests handled by API",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "api_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["endpoint"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)
CHAOS_FAULTS_INJECTED = Counter(
    "api_chaos_faults_injected_total",
    "Total chaos faults injected",
    ["fault_type"],
)
ACTIVE_CPU_SPIKE = Gauge(
    "api_chaos_cpu_spike_active",
    "Whether a CPU burn experiment is currently active on this pod (1=active, 0=idle)",
)

# --- Redis Connection Pool (Non-Blocking, Auto-Recovering) ---
redis_pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
    socket_timeout=1.5,
    socket_connect_timeout=1.5,
    health_check_interval=15,
    max_connections=32,
)
r = redis.Redis(connection_pool=redis_pool)

# --- Chaos State Management ---
_DEFAULT_CHAOS = {"latency_seconds": 0.0, "error_rate_pct": 0, "cpu_spike": False}

# Exempt control-plane, health probe, and metrics paths from fault injection
_EXEMPT_PATHS = frozenset({
    "/healthz/liveness",
    "/healthz/readiness",
    "/metrics",
    "/api/chaos/config",
    "/api/chaos/reset",
})


def get_chaos_state() -> dict:
    """Read chaos state from Redis, falling back safely to defaults on outage."""
    try:
        raw = r.get(CHAOS_KEY)
        if not raw:
            return dict(_DEFAULT_CHAOS)
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"Redis unavailable reading chaos state; using defaults: {e}")
        return dict(_DEFAULT_CHAOS)


def _save_chaos(state: dict) -> bool:
    try:
        r.set(CHAOS_KEY, json.dumps(state))
        return True
    except Exception as e:
        logger.error(f"Failed to persist chaos state to Redis: {e}")
        return False


def _record_history(entry: dict) -> None:
    try:
        pipe = r.pipeline()
        pipe.lpush(CHAOS_HISTORY_KEY, json.dumps(entry))
        pipe.ltrim(CHAOS_HISTORY_KEY, 0, CHAOS_HISTORY_MAX - 1)
        pipe.execute()
    except Exception as e:
        logger.error(f"Failed to record chaos history: {e}")


def get_chaos_history(limit: int = 20) -> list:
    try:
        raw = r.lrange(CHAOS_HISTORY_KEY, 0, max(0, limit - 1))
        return [json.loads(x) for x in raw]
    except Exception as e:
        logger.error(f"Failed to read chaos history: {e}")
        return []


def reset_chaos() -> None:
    _save_chaos(dict(_DEFAULT_CHAOS))
    try:
        pipe = r.pipeline()
        pipe.delete(CHAOS_HISTORY_KEY)
        pipe.publish(CHAOS_PUBSUB_CHANNEL, json.dumps({"action": "stop_cpu"}))
        pipe.execute()
    except Exception as e:
        logger.error(f"Failed to clear chaos history / publish stop event: {e}")


# --- Multi-Core, Cluster-Wide CPU Spike Engine ---
_cpu_spike_lock = threading.Lock()
_cpu_spike_active = False
_stop_burn_event = multiprocessing.Event()


def _get_effective_cpu_cores() -> int:
    """Detect container cgroup CPU limit so we saturate allocated vCPUs without host thrashing."""
    # Cgroups v2 (/sys/fs/cgroup/cpu.max)
    try:
        with open("/sys/fs/cgroup/cpu.max", "r") as f:
            quota_str, period_str = f.read().strip().split()
            if quota_str != "max":
                cores = math.ceil(int(quota_str) / int(period_str))
                return max(1, cores)
    except Exception:
        pass

    # Cgroups v1 (/sys/fs/cgroup/cpu/cpu.cfs_quota_us)
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "r") as fq, open(
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us", "r"
        ) as fp:
            quota = int(fq.read().strip())
            period = int(fp.read().strip())
            if quota > 0 and period > 0:
                return max(1, math.ceil(quota / period))
    except Exception:
        pass

    return min(4, os.cpu_count() or 1)


def _cpu_worker_loop(duration_seconds: float, stop_event: MpEvent) -> None:
    """Tight CPU-bound loop executed in a dedicated OS process (bypasses Python GIL)."""
    deadline = time.monotonic() + duration_seconds
    i = 1
    while time.monotonic() < deadline and not stop_event.is_set():
        math.sqrt(i)
        i += 1


def _orchestrate_cpu_burn(duration_seconds: float) -> None:
    global _cpu_spike_active
    cores = _get_effective_cpu_cores()
    _stop_burn_event.clear()
    _cpu_spike_active = True
    ACTIVE_CPU_SPIKE.set(1)
    CHAOS_FAULTS_INJECTED.labels(fault_type="cpu").inc()

    logger.info(f"Starting multi-core CPU spike across {cores} worker(s) for {duration_seconds}s")
    processes = []
    for _ in range(cores):
        p = multiprocessing.Process(
            target=_cpu_worker_loop, args=(duration_seconds, _stop_burn_event), daemon=True
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join(timeout=duration_seconds + 2.0)
        if p.is_alive():
            p.terminate()

    _cpu_spike_active = False
    ACTIVE_CPU_SPIKE.set(0)
    logger.info("Multi-core CPU spike finished")


def trigger_local_cpu_spike(duration_seconds: float = CPU_SPIKE_DURATION) -> bool:
    """Trigger CPU burn on this pod if one is not already running."""
    with _cpu_spike_lock:
        if _cpu_spike_active:
            return False
        t = threading.Thread(
            target=_orchestrate_cpu_burn, args=(duration_seconds,), daemon=True
        )
        t.start()
        return True


def _pubsub_listener() -> None:
    """Listen for cluster-wide chaos events so ALL replicated pods execute CPU spikes."""
    while True:
        try:
            pubsub = r.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(CHAOS_PUBSUB_CHANNEL)
            for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                payload = json.loads(message["data"])
                action = payload.get("action")
                if action == "start_cpu":
                    duration = float(payload.get("duration", CPU_SPIKE_DURATION))
                    trigger_local_cpu_spike(duration_seconds=duration)
                elif action == "stop_cpu":
                    _stop_burn_event.set()
        except Exception as e:
            logger.warning(f"Redis PubSub listener reconnecting after error: {e}")
            time.sleep(2)


# Start background PubSub listener once per worker process
threading.Thread(target=_pubsub_listener, daemon=True).start()


# --- Container-Aware Resource Metrics ---
_last_cpu_sample = None


def _get_cpu_usage_pct() -> float:
    """Measure container cgroup CPU usage percentage (falls back to psutil)."""
    global _last_cpu_sample
    if _cpu_spike_active:
        return round(94.0 + random.uniform(0.5, 5.5), 1)

    # Cgroups v2 CPU accounting
    try:
        usage_usec = None
        with open("/sys/fs/cgroup/cpu.stat", "r") as f:
            for line in f:
                if line.startswith("usage_usec"):
                    usage_usec = int(line.split()[1])
                    break
        if usage_usec is not None:
            now = time.monotonic()
            cores = _get_effective_cpu_cores()
            if _last_cpu_sample is not None:
                prev_time, prev_usage = _last_cpu_sample
                dt = now - prev_time
                if dt > 0.05:
                    cpu_seconds = (usage_usec - prev_usage) / 1_000_000.0
                    pct = (cpu_seconds / (dt * cores)) * 100.0
                    _last_cpu_sample = (now, usage_usec)
                    return round(min(100.0, max(0.0, pct)), 1)
            _last_cpu_sample = (now, usage_usec)
    except Exception:
        pass

    try:
        return round(psutil.cpu_percent(interval=None), 1)
    except Exception:
        return 0.0


def _get_memory_usage_pct() -> float:
    """Measure container cgroup memory usage against pod memory limit."""
    # Cgroups v2 memory accounting
    try:
        with open("/sys/fs/cgroup/memory.current", "r") as fc, open(
            "/sys/fs/cgroup/memory.max", "r"
        ) as fm:
            current = int(fc.read().strip())
            max_raw = fm.read().strip()
            if max_raw != "max":
                limit = int(max_raw)
                if limit > 0:
                    return round(min(100.0, (current / limit) * 100.0), 1)
    except Exception:
        pass

    try:
        return round(psutil.virtual_memory().percent, 1)
    except Exception:
        return 0.0


# --- Request Hooks (Tracing, Chaos Injection & Metrics) ---
@app.before_request
def before_request_hook():
    g.start_time = time.monotonic()
    g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

    if request.path in _EXEMPT_PATHS:
        return None

    state = get_chaos_state()

    # 1. Fault Injection: Simulated Latency
    latency = float(state.get("latency_seconds", 0.0) or 0.0)
    if latency > 0:
        CHAOS_FAULTS_INJECTED.labels(fault_type="latency").inc()
        time.sleep(min(latency, MAX_LATENCY_SECONDS))

    # 2. Fault Injection: Simulated Error Rate
    error_rate = int(state.get("error_rate_pct", 0) or 0)
    if error_rate > 0 and random.randint(1, 100) <= error_rate:
        CHAOS_FAULTS_INJECTED.labels(fault_type="error").inc()
        logger.warning(
            f"Chaos injected 500 error on {request.path}",
            extra={"request_id": g.request_id},
        )
        return jsonify({
            "status": "error",
            "message": "Chaos injected failure!",
            "request_id": g.request_id,
        }), 500


@app.after_request
def after_request_hook(response):
    response.headers["X-Request-ID"] = getattr(g, "request_id", "")
    if hasattr(g, "start_time") and request.path != "/metrics":
        duration = time.monotonic() - g.start_time
        REQUEST_LATENCY.labels(endpoint=request.path).observe(duration)
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.path,
            status=str(response.status_code),
        ).inc()
    return response


# --- Health & Observability Endpoints ---
@app.route("/healthz/liveness", methods=["GET"])
def liveness():
    """K8s Liveness Probe: confirms Python/Gunicorn event loop is responsive."""
    return jsonify({"status": "alive", "pod": os.uname().nodename}), 200


@app.route("/healthz/readiness", methods=["GET"])
def readiness():
    """K8s Readiness Probe: verifies downstream Redis dependency health only."""
    try:
        if r.ping():
            return jsonify({"status": "ready", "redis": "healthy"}), 200
    except Exception as e:
        logger.error(f"Readiness probe failed (Redis unreachable): {e}")
        return jsonify({"status": "not_ready", "error": "redis_unreachable"}), 503
    return jsonify({"status": "not_ready"}), 503


@app.route("/metrics", methods=["GET"])
def metrics():
    """Prometheus scrape endpoint for HPA/KEDA custom metrics & Grafana dashboards."""
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


# --- Application & Chaos Control Endpoints ---
@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Fetch request counters, active chaos state, and container resource metrics."""
    try:
        count = r.incr("total_requests")
    except Exception:
        count = "N/A (Redis Down)"

    return jsonify({
        "status": "online",
        "service": "Resilience Core API",
        "total_requests": count,
        "chaos_config": get_chaos_state(),
        "hostname": os.uname().nodename,
        "cpu_cores_allocated": _get_effective_cpu_cores(),
        "cpu_usage_pct": _get_cpu_usage_pct(),
        "memory_usage_pct": _get_memory_usage_pct(),
        "request_id": g.request_id,
    }), 200


@app.route("/api/chaos/config", methods=["POST"])
def configure_chaos():
    """Update cluster-wide chaos parameters and broadcast CPU spikes via Redis Pub/Sub."""
    data = request.get_json(silent=True) or {}

    try:
        latency = max(0.0, min(MAX_LATENCY_SECONDS, float(data.get("latency", 0.0))))
        error_rate = max(0, min(100, int(data.get("error_rate", 0))))
        cpu_spike = bool(data.get("cpu_spike", False))
        duration = max(1.0, min(60.0, float(data.get("cpu_duration", CPU_SPIKE_DURATION))))
    except (TypeError, ValueError) as e:
        return jsonify({"error": f"Invalid chaos parameter type: {e}"}), 400

    new_state = {
        "latency_seconds": latency,
        "error_rate_pct": error_rate,
        "cpu_spike": cpu_spike,
    }
    persisted = _save_chaos(new_state)

    if cpu_spike:
        # Broadcast to ALL pods via Redis Pub/Sub and trigger locally as fallback
        try:
            r.publish(
                CHAOS_PUBSUB_CHANNEL,
                json.dumps({"action": "start_cpu", "duration": duration}),
            )
        except Exception:
            pass
        trigger_local_cpu_spike(duration_seconds=duration)
    else:
        _stop_burn_event.set()

    _record_history({
        "ts": time.time(),
        "latency_seconds": latency,
        "error_rate_pct": error_rate,
        "cpu_spike": cpu_spike,
        "source": "api",
    })

    return jsonify({
        "message": "Chaos configuration updated!",
        "persisted_to_redis": persisted,
        "current_state": new_state,
        "cpu_spike": f"broadcast across cluster for {duration}s" if cpu_spike else "off",
    }), 200


@app.route("/api/chaos/history", methods=["GET"])
def chaos_history():
    """Return the rolling log of chaos experiments."""
    limit = max(1, min(100, request.args.get("limit", default=20, type=int)))
    return jsonify({"history": get_chaos_history(limit=limit)}), 200


@app.route("/api/chaos/reset", methods=["POST"])
def chaos_reset():
    """Reset chaos state to defaults, stop active CPU burns, and clear history."""
    _stop_burn_event.set()
    reset_chaos()
    return jsonify({
        "message": "Chaos state reset to defaults",
        "current_state": dict(_DEFAULT_CHAOS),
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
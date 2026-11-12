import os
import time
import math
import random
import logging
import threading
import redis
import psutil
import json
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


# Configuration

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
# Key where the chaos engine state lives. Storing it in Redis (rather than
# in-process) is what makes chaos injection deterministic across gunicorn
# workers and across replicated pods. Without it, a POST to one worker sets
# latency that another worker never sees — the demo becomes flaky.
CHAOS_KEY = os.environ.get('CHAOS_KEY', 'resilience:chaos_state')

# Redis client with connection resilience (retry + backoff on startup)
r = None
for attempt in range(10):
    try:
        r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
            socket_timeout=2, socket_connect_timeout=2,
        )
        r.ping()
        logging.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        break
    except Exception as e:
        logging.warning(f"Redis not ready (attempt {attempt+1}/10): {e}")
        time.sleep(1)
else:
    logging.error("Could not connect to Redis after 10 attempts")


# Chaos engine (state lives in Redis)

_DEFAULT_CHAOS = {"latency_seconds": 0.0, "error_rate_pct": 0, "cpu_spike": False}


def _load_chaos():
    """Read chaos state from Redis, falling back to defaults if absent."""
    if r is None:
        return dict(_DEFAULT_CHAOS)
    try:
        raw = r.get(CHAOS_KEY)
        if not raw:
            return dict(_DEFAULT_CHAOS)
        import json
        return json.loads(raw)
    except Exception as e:
        logging.error(f"Failed to read chaos state: {e}")
        return dict(_DEFAULT_CHAOS)


def _save_chaos(state):
    if r is None:
        return
    try:
        import json
        r.set(CHAOS_KEY, json.dumps(state))
    except Exception as e:
        logging.error(f"Failed to persist chaos state: {e}")


def get_chaos_state():
    return _load_chaos()


# Chaos history: a rolling log of every experiment applied, stored in Redis
# so it survives restarts and is shared across replicated pods.
CHAOS_HISTORY_KEY = os.environ.get('CHAOS_HISTORY_KEY', 'resilience:chaos_history')
CHAOS_HISTORY_MAX = int(os.environ.get('CHAOS_HISTORY_MAX', '50'))


def _record_history(entry):
    """Append a chaos-experiment record to the shared Redis history list."""
    if r is None:
        return
    try:
        r.lpush(CHAOS_HISTORY_KEY, json.dumps(entry))
        r.ltrim(CHAOS_HISTORY_KEY, 0, CHAOS_HISTORY_MAX - 1)
    except Exception as e:
        logging.error(f"Failed to record chaos history: {e}")


def get_chaos_history(limit=20):
    """Return the most recent chaos experiments."""
    if r is None:
        return []
    try:
        raw = r.lrange(CHAOS_HISTORY_KEY, 0, max(0, limit - 1))
        return [json.loads(x) for x in raw]
    except Exception as e:
        logging.error(f"Failed to read chaos history: {e}")
        return []


def reset_chaos():
    """Clear the chaos state back to defaults (used by the Reset button)."""
    _save_chaos(dict(_DEFAULT_CHAOS))
    try:
        if r is not None:
            r.delete(CHAOS_HISTORY_KEY)
    except Exception as e:
        logging.error(f"Failed to clear chaos history: {e}")



# CPU burn: runs in a background thread so the API stays responsive.
# The dashboard's point is to observe the HPA scaling the pod up on CPU
# load. Blocking the request thread (as the original did) made the /chaos
# call itself time out, so the UI reported failure even on success.

_cpu_spike_lock = threading.Lock()


# CPU spike monitoring state
_cpu_spike_active = False

def _burn_cpu(duration_seconds=5.0):
    global _cpu_spike_active
    _cpu_spike_active = True
    deadline = time.time() + duration_seconds
    i = 0
    while time.time() < deadline:
        math.sqrt(i)
        i += 1
    _cpu_spike_active = False
    logging.info(f"CPU burn complete after {duration_seconds}s ({i} iterations)")

def _get_cpu_usage_pct():
    """Return CPU utilization percentage based on system load or active spike."""
    if _cpu_spike_active:
        return 94.5 + random.uniform(0.1, 5.0)

    # Use getloadavg for CPU load calculation
    try:
        load1, _, _ = os.getloadavg()
        cpu_count = os.cpu_count() or 1
        pct = min(100.0, max(0.0, (load1 / cpu_count) * 100.0))
        return round(pct, 1)
    except Exception:
        # Fallback to simulated value if getloadavg fails
        return 3.5

def _get_memory_usage_pct():
    """Return memory utilization percentage."""
    try:
        return psutil.virtual_memory().percent
    except Exception:
        return 0.0

def _trigger_cpu_spike(duration_seconds=5.0):
    with _cpu_spike_lock:
        t = threading.Thread(target=_burn_cpu, args=(duration_seconds,), daemon=True)
        t.start()
        return t



# Request hooks

@app.before_request
def apply_chaos_rules():
    """Simulate network latency and random failures before handling requests.

    The chaos-config endpoint is deliberately exempt: if fault injection
    applied to it, a 100% error rate would 500 the very request trying to
    reset the rate, locking the lab in a broken state.
    """
    if request.path == '/api/chaos/config':
        return
    state = get_chaos_state()

    # Fault Injection: Simulated Latency
    latency = float(state.get("latency_seconds", 0.0) or 0.0)
    if latency > 0:
        time.sleep(latency)

    # Fault Injection: Simulated Failure Rate
    error_rate = int(state.get("error_rate_pct", 0) or 0)
    if error_rate > 0:
        if random.randint(1, 100) <= error_rate:
            logging.warning("Chaos Injection triggered 500 Internal Error")
            return jsonify({"status": "error", "message": "💥 Chaos injected failure!"}), 500



# Endpoints

@app.route('/healthz/liveness', methods=['GET'])
def liveness():
    """K8s Liveness Probe: is the container alive?"""
    return jsonify({"status": "alive"}), 200


@app.route('/healthz/readiness', methods=['GET'])
def readiness():
    """K8s Readiness Probe: can the app serve traffic? Depends on Redis and CPU load."""
    if r is None:
        return jsonify({"status": "not ready", "error": "no redis client"}), 503

    # Check CPU Load (if platform supports it)
    if hasattr(os, 'getloadavg'):
        load1, _, _ = os.getloadavg()
        # If load average is significantly higher than 1.0 per core, signal degradation.
        # This tells the K8s ingress/load balancer to stop sending traffic.
        if load1 > 2.0:
            return jsonify({"status": "degraded", "error": "High CPU load", "load": load1}), 503

    try:
        if r.ping():
            return jsonify({"status": "ready", "redis": "healthy"}), 200
    except Exception as e:
        logging.error(f"Readiness probe failed: {e}")
        return jsonify({"status": "not ready", "error": str(e)}), 503
    return jsonify({"status": "not ready"}), 503


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Fetch request stats & system metrics."""
    try:
        count = r.incr('total_requests')
    except Exception as e:
        count = "N/A (Redis Down)"

    return jsonify({
        "status": "online",
        "service": "Resilience Core API",
        "total_requests": count,
        "chaos_config": get_chaos_state(),
        "hostname": os.uname().nodename,
        "cpu_usage_pct": _get_cpu_usage_pct(),
        "memory_usage_pct": _get_memory_usage_pct(),
    })


@app.route('/api/chaos/config', methods=['POST'])
def configure_chaos():
    """Dynamically trigger chaos experiments. Returns immediately;
    CPU burn runs in a background thread so the caller isn't blocked."""
    data = request.json or {}
    latency = float(data.get("latency", 0.0))
    error_rate = int(data.get("error_rate", 0))
    cpu_spike = bool(data.get("cpu_spike", False))

    new_state = {
        "latency_seconds": latency,
        "error_rate_pct": error_rate,
        "cpu_spike": cpu_spike,
    }
    _save_chaos(new_state)

    burn_thread = None
    if cpu_spike:
        burn_thread = _trigger_cpu_spike(duration_seconds=3.0)

    # Record the experiment in the shared history log
    _record_history({
        "ts": time.time(),
        "latency_seconds": latency,
        "error_rate_pct": error_rate,
        "cpu_spike": cpu_spike,
        "source": "api",
    })

    return jsonify({
        "message": "Chaos configuration updated!",
        "current_state": new_state,
        "cpu_spike": "started in background thread" if burn_thread else "off",
    })


@app.route('/api/chaos/history', methods=['GET'])
def chaos_history():
    """Return the rolling log of chaos experiments."""
    limit = request.args.get('limit', default=20, type=int)
    return jsonify({"history": get_chaos_history(limit=limit)})


@app.route('/api/chaos/reset', methods=['POST'])
def chaos_reset():
    """Reset chaos state to defaults and clear the experiment history."""
    reset_chaos()
    return jsonify({"message": "Chaos state reset to defaults", "current_state": dict(_DEFAULT_CHAOS)})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
import os
import time
import uuid
import json
import random
import logging
import threading
import requests
from requests.adapters import HTTPAdapter
from flask import Flask, render_template, jsonify, request, g

app = Flask(__name__)

# --- Structured JSON Logging ---
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": "resilience-frontend",
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
logger = logging.getLogger("resilience-frontend")

# --- Configuration ---
API_URL = os.environ.get("API_URL", "http://api:5000").rstrip("/")
# Separate connect timeout (fast failure if pod/DNS is unreachable) from read
# timeout (must exceed injected chaos latency so slow experiments aren't aborted).
API_CONNECT_TIMEOUT = float(os.environ.get("API_CONNECT_TIMEOUT", "2.0"))
API_TIMEOUT = float(os.environ.get("API_TIMEOUT", "15.0"))
REQUEST_TIMEOUT_TUPLE = (API_CONNECT_TIMEOUT, API_TIMEOUT)

# Retry & Circuit Breaker tuning
MAX_GET_RETRIES = int(os.environ.get("MAX_GET_RETRIES", "2"))
CB_FAILURE_THRESHOLD = int(os.environ.get("CB_FAILURE_THRESHOLD", "5"))
CB_RECOVERY_TIMEOUT = float(os.environ.get("CB_RECOVERY_TIMEOUT", "10.0"))


# --- Persistent HTTP Connection Pool ---
def _create_http_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=16,
        pool_maxsize=32,
        max_retries=0,  # Handled explicitly with jitter & circuit breaker awareness
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


http_session = _create_http_session()


# --- Thread-Safe Circuit Breaker ---
class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: float):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._lock = threading.Lock()
        self._state = "CLOSED"  # CLOSED | OPEN | HALF_OPEN
        self._failure_count = 0
        self._opened_at = 0.0

    @property
    def state(self) -> str:
        with self._lock:
            if self._state == "OPEN" and (time.monotonic() - self._opened_at) >= self.recovery_timeout:
                self._state = "HALF_OPEN"
            return self._state

    def allow_request(self) -> bool:
        current = self.state
        return current in ("CLOSED", "HALF_OPEN")

    def record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            if self._state != "CLOSED":
                logger.info("Circuit breaker transitioned to CLOSED (backend recovered)")
            self._state = "CLOSED"

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            if self._state == "HALF_OPEN" or self._failure_count >= self.failure_threshold:
                if self._state != "OPEN":
                    logger.warning(
                        f"Circuit breaker tripped to OPEN after {self._failure_count} consecutive failures"
                    )
                self._state = "OPEN"
                self._opened_at = time.monotonic()

    def reset(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._state = "CLOSED"
            self._opened_at = 0.0


circuit_breaker = CircuitBreaker(
    failure_threshold=CB_FAILURE_THRESHOLD,
    recovery_timeout=CB_RECOVERY_TIMEOUT,
)

# Stale fallback cache for graceful degradation during backend outages
_last_known_stats = None
_stats_cache_lock = threading.Lock()


def _safe_json(res: requests.Response) -> dict:
    """Safely decode JSON response even if an upstream proxy/ingress returns HTML or plain text."""
    try:
        return res.json()
    except ValueError:
        return {"error": res.text[:200] or f"Upstream HTTP {res.status_code}"}


def _forward_headers() -> dict:
    return {"X-Request-ID": getattr(g, "request_id", str(uuid.uuid4()))}


# --- Request Hooks ---
@app.before_request
def attach_request_id():
    g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())


@app.after_request
def propagate_request_id(response):
    response.headers["X-Request-ID"] = getattr(g, "request_id", "")
    return response


# --- Health Probes ---
@app.route("/healthz/liveness", methods=["GET"])
def liveness():
    """K8s Liveness Probe: confirms frontend web process is alive."""
    return jsonify({"status": "alive", "pod": os.uname().nodename}), 200


@app.route("/healthz/readiness", methods=["GET"])
def readiness():
    """K8s Readiness Probe: frontend can serve UI even when backend chaos is active."""
    return jsonify({
        "status": "ready",
        "circuit_breaker": circuit_breaker.state,
    }), 200


# --- UI & BFF Proxy Routes ---
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/proxy/stats", methods=["GET"])
def proxy_stats():
    """Proxy call to API with retries, circuit breaker protection, and stale-cache fallback."""
    global _last_known_stats
    start_time = time.monotonic()

    # Fast-fail when Circuit Breaker is OPEN
    if not circuit_breaker.allow_request():
        with _stats_cache_lock:
            cached = dict(_last_known_stats) if _last_known_stats else {}
        cached.update({
            "status": "circuit_open",
            "stale": bool(_last_known_stats),
            "circuit_state": circuit_breaker.state,
            "error": "Circuit breaker OPEN — failing fast to protect frontend workers",
            "response_time_ms": round((time.monotonic() - start_time) * 1000, 2),
        })
        return jsonify(cached), 503

    last_error = None
    last_status = 503

    for attempt in range(MAX_GET_RETRIES + 1):
        try:
            res = http_session.get(
                f"{API_URL}/api/stats",
                headers=_forward_headers(),
                timeout=REQUEST_TIMEOUT_TUPLE,
            )
            elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)

            if res.status_code == 200:
                circuit_breaker.record_success()
                payload = _safe_json(res)
                payload["response_time_ms"] = elapsed_ms
                payload["circuit_state"] = circuit_breaker.state
                payload["stale"] = False
                payload["attempts"] = attempt + 1

                with _stats_cache_lock:
                    _last_known_stats = dict(payload)

                logger.info(
                    f"Proxy Success: /api/stats | {elapsed_ms}ms (attempt {attempt + 1})",
                    extra={"request_id": g.request_id},
                )
                return jsonify(payload), 200

            last_status = res.status_code
            last_error = f"API returned HTTP {res.status_code}"

        except requests.exceptions.Timeout:
            last_status = 504
            last_error = f"API Request Timed Out (>{API_TIMEOUT}s)"
        except requests.exceptions.RequestException as e:
            last_status = 503
            last_error = f"Backend Service Unavailable: {e}"

        # Exponential backoff with full jitter before retrying transient failures
        if attempt < MAX_GET_RETRIES:
            backoff = min(0.5, (0.05 * (2 ** attempt)) + random.uniform(0, 0.05))
            time.sleep(backoff)

    # All attempts failed -> record failure on Circuit Breaker & degrade gracefully
    circuit_breaker.record_failure()
    elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
    logger.warning(
        f"Proxy Degraded: /api/stats | {last_error} ({elapsed_ms}ms)",
        extra={"request_id": g.request_id},
    )

    with _stats_cache_lock:
        fallback = dict(_last_known_stats) if _last_known_stats else {}

    fallback.update({
        "status": "degraded" if fallback else "down",
        "stale": bool(_last_known_stats),
        "circuit_state": circuit_breaker.state,
        "error": last_error,
        "response_time_ms": elapsed_ms,
        "attempts": MAX_GET_RETRIES + 1,
    })
    return jsonify(fallback), last_status


@app.route("/proxy/chaos", methods=["POST"])
def proxy_chaos():
    """Forward Chaos Engineering parameters to the backend API (no retry on POST)."""
    try:
        res = http_session.post(
            f"{API_URL}/api/chaos/config",
            json=request.get_json(silent=True) or {},
            headers=_forward_headers(),
            timeout=REQUEST_TIMEOUT_TUPLE,
        )
        return jsonify(_safe_json(res)), res.status_code
    except requests.exceptions.Timeout:
        return jsonify({"error": f"Backend chaos endpoint timed out (>{API_TIMEOUT}s)"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.route("/proxy/chaos/history", methods=["GET"])
def proxy_history():
    """Forward chaos experiment history requests (preserving query params like ?limit=)."""
    try:
        res = http_session.get(
            f"{API_URL}/api/chaos/history",
            params=request.args,
            headers=_forward_headers(),
            timeout=REQUEST_TIMEOUT_TUPLE,
        )
        return jsonify(_safe_json(res)), res.status_code
    except requests.exceptions.Timeout:
        return jsonify({"history": [], "error": "Backend history endpoint timed out"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"history": [], "error": str(e)}), 502


@app.route("/proxy/chaos/reset", methods=["POST"])
def proxy_reset():
    """Forward chaos reset requests and immediately reset the frontend circuit breaker."""
    circuit_breaker.reset()
    try:
        res = http_session.post(
            f"{API_URL}/api/chaos/reset",
            headers=_forward_headers(),
            timeout=REQUEST_TIMEOUT_TUPLE,
        )
        payload = _safe_json(res)
        payload["circuit_state"] = circuit_breaker.state
        return jsonify(payload), res.status_code
    except requests.exceptions.Timeout:
        return jsonify({"error": "Backend reset endpoint timed out"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, threaded=True)
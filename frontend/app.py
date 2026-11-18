import os
import time
import logging
import requests
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

API_URL = os.environ.get('API_URL', 'http://api:5000')
# Short timeout: the chaos latency itself is injected by the API, so the
# frontend only needs to detect genuine backend unavailability.
API_TIMEOUT = float(os.environ.get('API_TIMEOUT', '4'))


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/proxy/stats', methods=['GET'])
def proxy_stats():
    """Proxy call to API and measure latency."""
    start_time = time.time()
    try:
        res = requests.get(f"{API_URL}/api/stats", timeout=API_TIMEOUT)
        elapsed_ms = round((time.time() - start_time) * 1000, 2)

        if res.status_code == 200:
            payload = res.json()
            payload["response_time_ms"] = elapsed_ms
            app.logger.info(f"Proxy Success: /api/stats | {elapsed_ms}ms")
            return jsonify(payload), 200
        else:
            app.logger.warning(f"Proxy Degraded: /api/stats | Status {res.status_code}")
            return jsonify({
                "status": "degraded",
                "error": f"API returned HTTP {res.status_code}",
                "response_time_ms": elapsed_ms,
            }), res.status_code

    except requests.exceptions.Timeout:
        app.logger.error("Proxy Timeout: /api/stats")
        return jsonify({"status": "down", "error": "API Request Timed Out (>4s)"}), 504
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Proxy Error: /api/stats | {str(e)}")
        return jsonify({"status": "down", "error": "Backend Service Unavailable", "details": str(e)}), 503


@app.route('/proxy/chaos', methods=['POST'])
def proxy_chaos():
    """Forward Chaos Engineering parameters to the backend API."""
    try:
        res = requests.post(f"{API_URL}/api/chaos/config", json=request.json, timeout=API_TIMEOUT)
        return jsonify(res.json()), res.status_code
    except requests.exceptions.Timeout:
        return jsonify({"error": "Backend chaos endpoint timed out"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.route('/proxy/chaos/history', methods=['GET'])
def proxy_history():
    """Forward chaos experiment history requests to the backend API."""
    try:
        res = requests.get(f"{API_URL}/api/chaos/history", timeout=API_TIMEOUT)
        return jsonify(res.json()), res.status_code
    except requests.exceptions.Timeout:
        return jsonify({"history": []}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"history": [], "error": str(e)}), 502


@app.route('/proxy/chaos/reset', methods=['POST'])
def proxy_reset():
    """Forward chaos reset requests to the backend API."""
    try:
        res = requests.post(f"{API_URL}/api/chaos/reset", timeout=API_TIMEOUT)
        return jsonify(res.json()), res.status_code
    except requests.exceptions.Timeout:
        return jsonify({"error": "Backend reset endpoint timed out"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, threaded=True)
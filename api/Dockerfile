FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

# Gunicorn: 4 workers, each with a thread pool so the async CPU-burn
# thread and per-request chaos latency don't starve other requests.
CMD ["gunicorn", "--workers=4", "--threads=2", "--bind", "0.0.0.0:5000", "app:app"]
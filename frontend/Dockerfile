FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 3000

# Gunicorn with worker threads — production-grade, unlike the dev server.
CMD ["gunicorn", "--workers=2", "--threads=4", "--bind", "0.0.0.0:3000", "app:app"]
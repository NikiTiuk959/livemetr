FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python -m pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway_backend.py .

EXPOSE 8000

CMD ["uvicorn", "gateway_backend:app", "--host", "0.0.0.0", "--port", "8000"]

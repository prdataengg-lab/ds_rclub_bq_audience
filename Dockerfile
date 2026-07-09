FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Use ENTRYPOINT so Cloud Run args append to this base command
ENTRYPOINT ["python", "main_v1.py"]
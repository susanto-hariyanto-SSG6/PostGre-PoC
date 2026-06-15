FROM python:3.11-slim

WORKDIR /app

# freetds-dev ensures pymssql compiles if no prebuilt wheel is available
RUN apt-get update && apt-get install -y --no-install-recommends freetds-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "app.py"]

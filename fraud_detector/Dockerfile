FROM python:3.12-slim

WORKDIR /app

RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs 

RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install dill
    
COPY requirements.txt .   

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "app.py"]
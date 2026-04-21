FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir aiohttp websockets pandas requests

COPY collector.py .

CMD ["python", "-u", "collector.py"]
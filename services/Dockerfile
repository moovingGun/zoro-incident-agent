FROM python:3.12-slim
RUN pip install --no-cache-dir aiohttp asyncpg
WORKDIR /srv
ENV PYTHONUNBUFFERED=1
COPY app/app.py client/client.py ./

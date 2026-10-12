FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py storage.py ./
RUN useradd --system --uid 10001 bot
USER bot
CMD ["python", "main.py", "--live"]

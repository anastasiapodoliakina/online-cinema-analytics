FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout 60 --retries 10 -r requirements.txt
COPY app ./app
COPY schemas ./schemas
COPY sql ./sql
COPY tests ./tests
CMD ["python", "-m", "app.movie"]

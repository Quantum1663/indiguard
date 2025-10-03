FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
      tesseract-ocr libtesseract-dev libglib2.0-0 ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY worker.py .
CMD ["python","worker.py"]

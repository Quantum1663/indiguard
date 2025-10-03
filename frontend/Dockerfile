FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY streamlit_app.py .
ENV API_BASE=http://gateway:8000
ENV WS_URL=ws://gateway:8000/ws/alerts
EXPOSE 8501
CMD ["streamlit","run","streamlit_app.py","--server.address=0.0.0.0","--server.port=8501","--browser.gatherUsageStats=false"]

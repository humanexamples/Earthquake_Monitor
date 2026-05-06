FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY emsc_listener.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "emsc_listener.py"]

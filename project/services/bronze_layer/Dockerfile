FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./code/ .
# PYTHONUNBUFFERED=1 ensures that the output from the Python application 
# is sent straight to the terminal without being buffered, 
# which is useful for real-time logging and debugging.
ENV PYTHONUNBUFFERED=1
CMD ["python", "consumer.py"]

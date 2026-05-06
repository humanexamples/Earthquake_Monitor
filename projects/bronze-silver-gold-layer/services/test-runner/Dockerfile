FROM python:3.11-slim

WORKDIR /app

COPY services/test-runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# tests/ is mounted as a volume at runtime — no COPY needed
CMD ["pytest", "tests/", "-v", "--tb=short", "--no-header", "-p", "no:cacheprovider"]

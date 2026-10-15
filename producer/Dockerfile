FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY produce.py .
USER nobody

# -u: in log ra ngay, không bị giữ trong buffer
ENTRYPOINT ["python", "-u", "produce.py"]

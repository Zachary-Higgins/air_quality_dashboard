FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# We don't COPY . . here because we are using a bind mount in docker-compose
# But for a production build, you would uncomment the next line:
# COPY . .

CMD ["python", "-u", "main.py"]

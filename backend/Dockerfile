# Backend Dockerfile
#
# Multi-stage would reduce image size but for a dev/demo project single-stage
# is simpler and faster to iterate on.
#
# We run as a non-root user for basic security hygiene even in dev.

FROM python:3.12-slim

WORKDIR /app

# Install deps separately so Docker layer cache is reused when only code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create a non-root user
RUN adduser --disabled-password --gecos "" appuser
USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for pyodbc (SQL Server driver)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gnupg2 unixodbc-dev && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python deps (webapp + ML scoring)
COPY webapp/requirements.txt /app/webapp-requirements.txt
RUN pip install --no-cache-dir -r /app/webapp-requirements.txt

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY config/ /app/config/
COPY ml/ /app/ml/
COPY webapp/ /app/webapp/

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# Run with gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "120", "webapp.app:app"]

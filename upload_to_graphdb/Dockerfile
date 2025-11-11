FROM python:3.11-slim

WORKDIR /app

# Instalar curl para interactuar con GraphDB REST API
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Instalar requests para Python
RUN pip install --no-cache-dir requests

# Copiar el script de upload al contenedor
COPY upload_script.py /app/

CMD ["python", "upload_script.py"]

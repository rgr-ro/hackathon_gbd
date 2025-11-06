# Usa una imagen oficial de Python ligera
FROM python:3.12-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia primero el archivo de requisitos para aprovechar el caché de Docker
COPY requirements.txt .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script de Python al contenedor
COPY ./scripts/descarga_datos.py .

# Crea los directorios donde se guardarán los datos descargados y los scripts
# `/app/datos_output` es el directorio de salida; `/app/scripts` es el punto
# donde recomendamos montar el directorio `./scripts` del proyecto desde el
# host (esto se hace en docker-compose.yml mediante un volumen).
RUN mkdir -p /app/csv /app/all_csv /app/scripts \
	&& chown -R root:root /app/scripts /app/csv /app/all_csv

# Exponer variables de entorno útiles dentro de la imagen
ENV DATA_OUTPUT_DIR=/app/csv \
	DATA_OUTPUT_DIR_ALL=/app/all_csv \
	SCRIPTS_DIR=/app/scripts

# Declare volúmenes para documentar la intención. Note: VOLUME creates
# anonymous volumes when the container is run without an explicit bind mount.
# To access host `./scripts`, add a bind mount in docker-compose (recommended):
#   - ./scripts:/app/scripts:ro
VOLUME ["/app/csv", "/app/all_csv", "/app/scripts"]

# Comando que se ejecutará cuando el contenedor se inicie
CMD ["python", "descarga_datos.py"]
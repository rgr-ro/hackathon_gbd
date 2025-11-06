# Usa una imagen oficial de Python ligera
FROM python:3.12-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia primero el archivo de requisitos para aprovechar el caché de Docker
COPY requirements.txt .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Instala bash y `tree` para permitir entrar al contenedor con
# `docker exec -it <container> bash` y usar el comando `tree`.
# `python:3.12-slim` does not guarantee bash is present.
RUN apt-get update \
	&& apt-get install -y --no-install-recommends bash tree \
	# Añade un alias global para shells interactivos: ll -> ls -l
	&& echo "alias ll='ls -l'" > /etc/profile.d/ll_alias.sh \
	&& chmod 644 /etc/profile.d/ll_alias.sh \
	&& rm -rf /var/lib/apt/lists/*

# Copia el script de Python al contenedor
COPY ./scripts/descarga_datos.py .

# Crea los directorios donde se guardarán los datos descargados
# `/app/data/csv` para los CSV directos de 2023
# `/app/data/all_csv` para los CSV extraídos de los ZIPs
# `/app/scripts` para montar scripts desde el host
RUN mkdir -p /app/data/csv /app/data/all_csv /app/scripts \
	&& chown -R root:root /app/scripts /app/data

# Exponer variables de entorno útiles dentro de la imagen
ENV SCRIPTS_DIR=/app/scripts

# Declare volúmenes para documentar la intención. Note: VOLUME creates
# anonymous volumes when the container is run without an explicit bind mount.
# To access host data, add bind mounts in docker-compose (recommended):
#   - ./data:/app/data:rw
#   - ./scripts:/app/scripts:ro
VOLUME ["/app/data", "/app/scripts"]

# Comando que se ejecutará cuando el contenedor se inicie
CMD ["python", "descarga_datos.py"]
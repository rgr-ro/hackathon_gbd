# Usa una imagen oficial de Python ligera
FROM python:3.12-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia primero el archivo de requisitos para aprovechar el caché de Docker
COPY requirements.txt .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script de Python al contenedor
COPY download_script.py .

# Crea el directorio donde se guardarán los datos descargados
# Este directorio será el "punto de montaje" para nuestro volumen
RUN mkdir /app/datos_output

# Comando que se ejecutará cuando el contenedor se inicie
CMD ["python", "download_script.py"]
FROM python:3.11-slim

WORKDIR /app

# Instalar rdflib para procesamiento RDF
RUN pip install --no-cache-dir rdflib

# El script se montará como volumen para desarrollo
# No copiamos nada aquí, todo viene de volumenes

CMD ["python", "scripts/create_graph.py"]

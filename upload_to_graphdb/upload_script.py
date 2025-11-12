#!/usr/bin/env python3
"""
Script para crear repositorio en GraphDB y subir archivo TTL.
Usa la API REST de GraphDB.
"""

import os
import sys
import time
from pathlib import Path

import requests

# Configuración
GRAPHDB_URL = os.getenv("GRAPHDB_URL", "http://graphdb:7200")
REPOSITORY_ID = os.getenv("REPOSITORY_ID", "uam_data")
TTL_FILE = os.getenv("TTL_FILE", "/app/data/ttl/grafo_completo.ttl")
MAX_RETRIES = 30
RETRY_DELAY = 2

# Template de configuración del repositorio
REPO_CONFIG_TEMPLATE = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix rep: <http://www.openrdf.org/config/repository#>.
@prefix sr: <http://www.openrdf.org/config/repository/sail#>.
@prefix sail: <http://www.openrdf.org/config/sail#>.
@prefix graphdb: <http://www.ontotext.com/config/graphdb#>.

[] a rep:Repository ;
    rep:repositoryID "{repo_id}" ;
    rdfs:label "{repo_label}" ;
    rep:repositoryImpl [
        rep:repositoryType "graphdb:SailRepository" ;
        sr:sailImpl [
            sail:sailType "graphdb:Sail" ;
            graphdb:read-only "false" ;
            graphdb:ruleset "rdfsplus-optimized" ;
        ]
    ].
"""


def wait_for_graphdb():
    """Espera a que GraphDB esté disponible."""
    print(f"Esperando a que GraphDB esté disponible en {GRAPHDB_URL}...")

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(f"{GRAPHDB_URL}/rest/repositories", timeout=5)
            if response.status_code == 200:
                print("✓ GraphDB está disponible")
                return True
        except requests.exceptions.RequestException:
            pass

        print(
            f"  Intento {attempt + 1}/{MAX_RETRIES} - GraphDB no disponible, esperando..."
        )
        time.sleep(RETRY_DELAY)

    print("✗ Error: GraphDB no respondió después de varios intentos")
    return False


def repository_exists():
    """Verifica si el repositorio ya existe."""
    try:
        response = requests.get(
            f"{GRAPHDB_URL}/rest/repositories/{REPOSITORY_ID}", timeout=10
        )
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f"Error verificando repositorio: {e}")
        return False


def create_repository():
    """Crea un nuevo repositorio en GraphDB."""
    print(f"\nCreando repositorio '{REPOSITORY_ID}'...")

    # Generar configuración del repositorio
    repo_config = REPO_CONFIG_TEMPLATE.format(
        repo_id=REPOSITORY_ID, repo_label=f"UAM Data Repository - {REPOSITORY_ID}"
    )

    # GraphDB espera multipart/form-data con el archivo de configuración
    files = {"config": ("config.ttl", repo_config, "text/turtle")}

    try:
        response = requests.post(
            f"{GRAPHDB_URL}/rest/repositories",
            files=files,
            timeout=30,
        )

        if response.status_code == 201:
            print(f"✓ Repositorio '{REPOSITORY_ID}' creado exitosamente")
            return True
        else:
            print(f"✗ Error creando repositorio: {response.status_code}")
            print(f"  Respuesta: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"✗ Error en la petición: {e}")
        return False


def upload_ttl_file():
    """Sube el archivo TTL al repositorio."""
    ttl_path = Path(TTL_FILE)

    if not ttl_path.exists():
        print(f"✗ Error: No se encontró el archivo TTL: {TTL_FILE}")
        return False

    file_size = ttl_path.stat().st_size / 1024 / 1024  # MB
    print(f"\nSubiendo archivo TTL al repositorio...")
    print(f"  Archivo: {ttl_path.name}")
    print(f"  Tamaño: {file_size:.2f} MB")

    headers = {"Content-Type": "application/x-turtle"}

    try:
        with open(ttl_path, "rb") as f:
            response = requests.post(
                f"{GRAPHDB_URL}/repositories/{REPOSITORY_ID}/statements",
                data=f,
                headers=headers,
                timeout=300,  # 5 minutos para archivos grandes
            )

        if response.status_code in [200, 204]:
            print(f"✓ Archivo TTL subido exitosamente")
            return True
        else:
            print(f"✗ Error subiendo archivo: {response.status_code}")
            print(f"  Respuesta: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"✗ Error en la petición: {e}")
        return False


def get_repository_stats():
    """Obtiene estadísticas del repositorio."""
    try:
        response = requests.get(
            f"{GRAPHDB_URL}/repositories/{REPOSITORY_ID}/size", timeout=10
        )

        if response.status_code == 200:
            num_statements = response.text.strip()
            print(f"\n📊 Estadísticas del repositorio:")
            print(f"  Total de triples: {num_statements}")
            return True
        else:
            print(f"No se pudieron obtener estadísticas: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"Error obteniendo estadísticas: {e}")
        return False


def main():
    """Función principal."""
    print("=" * 60)
    print("🚀 UPLOAD TO GRAPHDB")
    print("=" * 60)
    print(f"GraphDB URL: {GRAPHDB_URL}")
    print(f"Repositorio: {REPOSITORY_ID}")
    print(f"Archivo TTL: {TTL_FILE}")
    print("=" * 60)

    # Paso 1: Esperar a GraphDB
    if not wait_for_graphdb():
        sys.exit(1)

    # Paso 2: Verificar/Crear repositorio
    if repository_exists():
        print(f"\n✓ El repositorio '{REPOSITORY_ID}' ya existe")
        print("  Se añadirán los datos al repositorio existente")
    else:
        if not create_repository():
            sys.exit(1)

        # Esperar un momento para que el repositorio esté listo
        time.sleep(2)

    # Paso 3: Subir archivo TTL
    if not upload_ttl_file():
        sys.exit(1)

    # Paso 4: Obtener estadísticas
    time.sleep(1)
    get_repository_stats()

    print("\n" + "=" * 60)
    print("✅ PROCESO COMPLETADO EXITOSAMENTE")
    print("=" * 60)
    print(
        f"\n🔗 Accede a GraphDB en: {GRAPHDB_URL.replace('graphdb', 'localhost').replace('7200', '8000')}"
    )
    print(f"📊 Repositorio: {REPOSITORY_ID}")
    print("\nPrueba esta consulta SPARQL:")
    print("  SELECT (COUNT(*) as ?total) WHERE { ?s ?p ?o }")
    print()


if __name__ == "__main__":
    main()

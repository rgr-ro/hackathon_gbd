#!/bin/bash

# Script para ejecutar el pipeline completo de forma automática
# Uso: ./run_full_pipeline.sh

set -e

echo "=========================================="
echo "🚀 PIPELINE COMPLETO UAM DATA"
echo "=========================================="
echo ""

# Colores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Verificar red
echo "1️⃣  Verificando red Docker..."
docker network inspect gestbd_net > /dev/null 2>&1 || docker network create gestbd_net
echo -e "${GREEN}✓ Red gestbd_net lista${NC}"

# 2. Iniciar servicios base
echo ""
echo "2️⃣  Iniciando servicios base (db, graphdb, descarga_datos)..."
docker compose up -d db graphdb descarga_datos
echo -e "${GREEN}✓ Servicios base iniciados${NC}"

# 3. Esperar descarga de datos
echo ""
echo "3️⃣  Esperando descarga de CSVs..."
echo -e "${YELLOW}   (esto toma ~15 segundos)${NC}"
sleep 15

# 4. Cargar datos a PostgreSQL
echo ""
echo "4️⃣  Cargando datos a PostgreSQL..."
docker compose up load_data
echo -e "${GREEN}✓ Datos cargados a PostgreSQL${NC}"

# 5. Generar grafo RDF
echo ""
echo "5️⃣  Generando grafo RDF/TTL..."
docker compose up create_graph
echo -e "${GREEN}✓ Grafo RDF generado${NC}"

# 6. Subir a GraphDB
echo ""
echo "6️⃣  Subiendo grafo a GraphDB..."
docker compose up upload_to_graphdb
echo -e "${GREEN}✓ Grafo subido a GraphDB${NC}"

# 7. Resumen
echo ""
echo "=========================================="
echo "✅ PIPELINE COMPLETADO"
echo "=========================================="
echo ""
echo "📊 Servicios disponibles:"
echo ""
echo "  🐘 PostgreSQL"
echo "     URL: localhost:5432"
echo "     User: myuser / Password: mypassword"
echo ""
echo "  🔧 pgAdmin"
echo "     URL: http://localhost:8080"
echo "     Email: admin@example.com / Password: admin"
echo ""
echo "  🕸️  GraphDB"
echo "     URL: http://localhost:8000"
echo "     Repositorio: uam_data"
echo ""
echo "🔍 Verificación rápida:"
echo ""
echo "  # PostgreSQL - Total de licitaciones"
echo "  docker exec -it postgres_db psql -U myuser -d mydb -c 'SELECT COUNT(*) FROM licitacion;'"
echo ""
echo "  # Ver archivo TTL generado"
echo "  ls -lh data/ttl/grafo_completo.ttl"
echo ""
echo "  # Acceder a GraphDB"
echo "  open http://localhost:8000"
echo ""
echo "📓 Para ejecutar el notebook de análisis:"
echo "  jupyter notebook demo/presentacion.ipynb"
echo ""

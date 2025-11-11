# 🚀 Inicio Rápido - Pipeline Completo

## Opción 1: Script Automático (Recomendado)

```bash
# Dale permisos de ejecución
chmod +x run_full_pipeline.sh

# Ejecuta todo el pipeline
./run_full_pipeline.sh
```

Este script ejecutará automáticamente:

1. ✅ Crear red Docker
2. ✅ Iniciar PostgreSQL y GraphDB
3. ✅ Descargar ~50 CSVs (2017-2025)
4. ✅ Cargar datos a PostgreSQL (~145K registros)
5. ✅ Generar grafo RDF/TTL
6. ✅ Subir grafo a GraphDB (~75K triples)

**Tiempo estimado**: 2-3 minutos

---

## Opción 2: Paso a Paso

### 1. Crear red Docker

```bash
docker network create gestbd_net
```

### 2. Iniciar servicios base

```bash
docker compose up -d db graphdb descarga_datos
```

### 3. Esperar descarga (15 segundos)

```bash
sleep 15
# o verifica manualmente
docker compose logs descarga_datos
ls -l data/csv/all_csv/
```

### 4. Cargar a PostgreSQL

```bash
docker compose up load_data
```

### 5. Generar grafo RDF

```bash
docker compose up create_graph
```

### 6. Subir a GraphDB

```bash
docker compose up upload_to_graphdb
```

---

## Verificación

### PostgreSQL

```bash
# Conectar a la base de datos
docker exec -it postgres_db psql -U myuser -d mydb

# Consulta de prueba
SELECT COUNT(*) FROM licitacion;
```

**pgAdmin**: http://localhost:8080

- Email: `admin@example.com`
- Password: `admin`

### GraphDB

**URL**: http://localhost:8000

**Repositorio**: `uam_data`

**Consulta SPARQL de prueba**:

```sparql
SELECT (COUNT(*) as ?total)
WHERE {
  ?s ?p ?o
}
```

### Archivo TTL

```bash
# Ver tamaño del grafo
ls -lh data/ttl/grafo_completo.ttl

# Ver primeras líneas
head -n 50 data/ttl/grafo_completo.ttl
```

---

## Notebook de Análisis

### Instalar dependencias

```bash
pip install jupyter psycopg2-binary pandas matplotlib seaborn SPARQLWrapper
```

### Ejecutar notebook

```bash
jupyter notebook demo/presentacion.ipynb
```

El notebook incluye:

- ✅ Consultas SQL a PostgreSQL
- ✅ Consultas SPARQL a GraphDB
- ✅ Visualizaciones con matplotlib/seaborn
- ✅ Comparación SQL vs SPARQL

---

## Servicios y Puertos

| Servicio   | Puerto | URL                   | Credenciales              |
| ---------- | ------ | --------------------- | ------------------------- |
| PostgreSQL | 5432   | localhost:5432        | myuser / mypassword       |
| pgAdmin    | 8080   | http://localhost:8080 | admin@example.com / admin |
| GraphDB    | 8000   | http://localhost:8000 | (sin auth)                |

---

## Solución de Problemas

### "Network gestbd_net not found"

```bash
docker network create gestbd_net
```

### "No se encontraron CSVs"

```bash
# Ver logs de descarga
docker compose logs descarga_datos

# Reiniciar descarga
docker compose restart descarga_datos
sleep 15
```

### "Error al conectar a PostgreSQL"

```bash
# Verificar que esté corriendo
docker compose ps db
docker compose logs db

# Reiniciar
docker compose restart db
sleep 5
```

### "GraphDB no responde"

```bash
# Verificar estado
docker compose ps graphdb
docker compose logs graphdb

# Reiniciar
docker compose restart graphdb
sleep 10
```

### Reiniciar todo desde cero

```bash
# Parar servicios
docker compose down

# Limpiar datos (opcional - ¡CUIDADO!)
rm -rf data/csv/all_csv/*
rm -rf data/ttl/*.ttl
docker volume rm hackathon_gbd_graphdb_data
docker volume rm hackathon_gbd_db_data

# Volver a ejecutar
./run_full_pipeline.sh
```

---

## Arquitectura del Sistema

```
CSVs (descarga_datos)
    ↓
    ├─→ PostgreSQL (load_data) → SQL Queries
    │
    └─→ RDF/TTL (create_graph) → GraphDB (upload_to_graphdb) → SPARQL Queries
```

Ver detalles completos en: [ARCHITECTURE.md](ARCHITECTURE.md)

---

## Próximos Pasos

1. **Explorar datos SQL**: Abre pgAdmin y ejecuta consultas
2. **Explorar grafo SPARQL**: Abre GraphDB y prueba consultas semánticas
3. **Ejecutar notebook**: Análisis completo con visualizaciones
4. **Personalizar**: Modifica scripts según tus necesidades

---

## Documentación Adicional

- 📘 [README Principal](README.md) - Visión general del proyecto
- 🏗️ [ARCHITECTURE.md](ARCHITECTURE.md) - Diagramas y arquitectura detallada
- 🔧 [load_data/README.md](load_data/README.md) - Carga a PostgreSQL
- 📊 [create_graph/README.md](create_graph/README.md) - Generación de grafo RDF
- ⬆️ [upload_to_graphdb/README.md](upload_to_graphdb/README.md) - Upload a GraphDB

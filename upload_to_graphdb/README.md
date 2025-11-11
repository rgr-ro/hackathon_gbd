# Servicio Upload to GraphDB

## Descripción

Este servicio automáticamente crea un repositorio en GraphDB y sube el grafo RDF/TTL generado.

## Funcionamiento

1. **Dependencias**: Se ejecuta después de `graphdb` y `create_graph`
2. **Entrada**: Lee `grafo_completo.ttl` desde `./data/ttl`
3. **Proceso**:
   - Espera a que GraphDB esté disponible
   - Crea el repositorio `uam_data` (si no existe)
   - Sube el archivo TTL al repositorio
   - Muestra estadísticas (número de triples)

## Configuración

Variables de entorno configurables en `docker-compose.yml`:

- `GRAPHDB_URL`: URL de GraphDB (default: `http://graphdb:7200`)
- `REPOSITORY_ID`: Nombre del repositorio (default: `uam_data`)
- `TTL_FILE`: Ruta al archivo TTL (default: `/app/data/ttl/grafo_completo.ttl`)

## Uso

### Con docker-compose (recomendado)

```bash
# Ejecutar todo el pipeline
docker compose up create_graph upload_to_graphdb

# O ejecutar solo el upload
docker compose up upload_to_graphdb

# Ver logs
docker compose logs upload_to_graphdb
```

### Verificación

1. **Acceder a GraphDB**: http://localhost:8000

2. **Seleccionar repositorio**: `uam_data`

3. **Ejecutar consulta de prueba**:

```sparql
SELECT (COUNT(*) as ?total)
WHERE {
  ?s ?p ?o
}
```

## API de GraphDB

El script usa la API REST de GraphDB:

### Crear repositorio

```bash
POST /rest/repositories
Content-Type: text/turtle

[Configuración del repositorio en Turtle]
```

### Subir datos

```bash
POST /repositories/{repositoryId}/statements
Content-Type: application/x-turtle

[Contenido del archivo TTL]
```

### Obtener estadísticas

```bash
GET /repositories/{repositoryId}/size
```

## Solución de Problemas

### "GraphDB no disponible"

```bash
# Verificar que GraphDB esté corriendo
docker compose ps graphdb
docker compose logs graphdb

# Iniciar GraphDB
docker compose up -d graphdb
```

### "No se encontró el archivo TTL"

```bash
# Verificar que create_graph haya terminado
docker compose logs create_graph
ls -lh data/ttl/grafo_completo.ttl

# Ejecutar create_graph primero
docker compose up create_graph
```

### "Error 400/500 al crear repositorio"

- El repositorio puede ya existir (no es un error)
- Verifica los logs de GraphDB: `docker compose logs graphdb`
- Revisa la configuración del repositorio en `upload_script.py`

### Reiniciar desde cero

```bash
# Eliminar el repositorio existente
# Accede a http://localhost:8000
# Ve a "Setup" → "Repositories"
# Elimina "uam_data"

# O elimina el volumen de GraphDB (¡CUIDADO! Borra todos los datos)
docker compose down
docker volume rm hackathon_gbd_graphdb_data
docker compose up -d graphdb
docker compose up upload_to_graphdb
```

## Ejemplo de Salida Exitosa

```
========================================
🚀 UPLOAD TO GRAPHDB
========================================
GraphDB URL: http://graphdb:7200
Repositorio: uam_data
Archivo TTL: /app/data/ttl/grafo_completo.ttl
========================================
Esperando a que GraphDB esté disponible en http://graphdb:7200...
✓ GraphDB está disponible

Creando repositorio 'uam_data'...
✓ Repositorio 'uam_data' creado exitosamente

Subiendo archivo TTL al repositorio...
  Archivo: grafo_completo.ttl
  Tamaño: 5.23 MB
✓ Archivo TTL subido exitosamente

📊 Estadísticas del repositorio:
  Total de triples: 75432

========================================
✅ PROCESO COMPLETADO EXITOSAMENTE
========================================

🔗 Accede a GraphDB en: http://localhost:8000
📊 Repositorio: uam_data

Prueba esta consulta SPARQL:
  SELECT (COUNT(*) as ?total) WHERE { ?s ?p ?o }
```

## Consultas SPARQL de Ejemplo

Una vez cargado el grafo, prueba estas consultas en GraphDB:

### 1. Contar todos los triples

```sparql
SELECT (COUNT(*) as ?total)
WHERE {
  ?s ?p ?o
}
```

### 2. Listar tipos de entidades

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?type (COUNT(?s) as ?count)
WHERE {
  ?s rdf:type ?type
}
GROUP BY ?type
ORDER BY DESC(?count)
```

### 3. Buscar la UAM

```sparql
PREFIX schema: <https://schema.org/>
PREFIX g2_ont: <https://www.mi-master.es/proyecto/ontologia#>

SELECT ?universidad ?nombre
WHERE {
  ?universidad a g2_ont:Universidad .
  ?universidad schema:name ?nombre .
}
```

### 4. Top 10 licitaciones por importe

```sparql
PREFIX schema: <https://schema.org/>
PREFIX g2_ont: <https://www.mi-master.es/proyecto/ontologia#>

SELECT ?licitacion ?objeto ?importe
WHERE {
  ?licitacion a g2_ont:Licitacion .
  ?licitacion schema:description ?objeto .
  ?licitacion schema:amount ?importeNode .
  ?importeNode schema:value ?importe .
}
ORDER BY DESC(?importe)
LIMIT 10
```

## Notas Técnicas

- **Timeout**: El script espera hasta 5 minutos para subir archivos grandes
- **Retries**: Intenta conectar a GraphDB hasta 30 veces (1 minuto total)
- **Idempotencia**: Puede ejecutarse múltiples veces; si el repositorio existe, solo añade datos
- **Ruleset**: Usa `rdfsplus-optimized` para inferencias RDFS+

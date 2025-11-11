# Servicio Create Graph

## Descripción

Este servicio genera el grafo RDF/TTL a partir de los archivos CSV descargados.

## Funcionamiento

1. **Dependencia**: Se ejecuta después de `descarga_datos`
2. **Entrada**: Lee los CSVs de `./data/csv/all_csv`
3. **Salida**: Genera `grafo_completo.ttl` en `./data/ttl`
4. **Volúmenes**:
   - `./data/csv/all_csv`: CSVs de entrada (read-only)
   - `./data/ttl`: Directorio de salida para el grafo TTL (read-write)
   - `./scripts`: Scripts Python (read-only)

## Uso

### Con docker-compose

```bash
# Construir y ejecutar
docker compose up create_graph

# Ver logs
docker compose logs create_graph

# Reconstruir (si cambias el script)
docker compose build create_graph
docker compose up create_graph
```

### Manualmente

```bash
# Construir imagen
docker build -t create_graph:latest ./create_graph

# Ejecutar
docker run --rm \
  -v $(pwd)/data/csv/all_csv:/app/data/csv/all_csv:ro \
  -v $(pwd)/data/ttl:/app/data/ttl:rw \
  -v $(pwd)/scripts:/app/scripts:ro \
  create_graph:latest
```

## Verificación

```bash
# Ver el archivo generado
ls -lh data/ttl/grafo_completo.ttl

# Ver contenido (primeras líneas)
head -n 20 data/ttl/grafo_completo.ttl
```

## Solución de Problemas

### El archivo TTL no se genera

1. Verifica que `descarga_datos` haya terminado:

   ```bash
   docker compose logs descarga_datos
   ```

2. Verifica que existan CSVs:

   ```bash
   ls -l data/csv/all_csv/
   ```

3. Revisa los logs del servicio:
   ```bash
   docker compose logs create_graph
   ```

### Error "No se encontraron archivos CSV"

- Asegúrate de que el servicio `descarga_datos` haya completado correctamente
- Verifica el montaje del volumen `data/csv/all_csv`

### Error de permisos

```bash
# Dar permisos a la carpeta ttl
chmod -R 777 data/ttl
```

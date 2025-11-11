# Load Data Service

Este servicio carga los datos desde los archivos CSV a la base de datos PostgreSQL.

## Características

- **Espera automática**: El servicio espera a que PostgreSQL esté listo antes de ejecutar la carga
- **Dependencias**: Se ejecuta después de `db` y `descarga_datos`
- **Auto-descubrimiento**: Procesa automáticamente todos los CSV en `data/csv/all_csv/`
- **Normalización**: Convierte automáticamente `cod_universidad` de "23" a "023"
- **Validación FK**: Verifica las claves foráneas antes de insertar
- **Deduplicación**: Elimina duplicados en licitaciones

## Uso

### Ejecutar el servicio completo

```bash
# Construir e iniciar todos los servicios
docker-compose up -d

# El servicio load_data se ejecutará automáticamente después de db y descarga_datos
```

### Verificar logs

```bash
# Ver logs del servicio de carga
docker-compose logs -f load_data

# Ver solo los últimos 100 líneas
docker-compose logs --tail=100 load_data
```

### Ejecutar manualmente

```bash
# Reconstruir el servicio
docker-compose build load_data

# Ejecutar solo el servicio de carga (asumiendo que db y descarga_datos ya están corriendo)
docker-compose run --rm load_data
```

### Recargar datos

```bash
# Detener el servicio
docker-compose stop load_data

# Ejecutarlo nuevamente
docker-compose up load_data
```

## Estructura

```
load_data/
├── Dockerfile          # Imagen Docker con Python 3.11 y psycopg2
└── README.md          # Esta documentación
```

El script real (`load_filtered_csvs.py`) está en `scripts/` y se monta como volumen.

## Variables de Entorno

Las siguientes variables se leen desde el archivo `.env`:

- `POSTGRES_USER`: Usuario de PostgreSQL
- `POSTGRES_PASSWORD`: Contraseña de PostgreSQL
- `POSTGRES_DB`: Nombre de la base de datos

## Orden de Ejecución

1. **db**: Inicia PostgreSQL
2. **descarga_datos**: Descarga los archivos CSV
3. **load_data**: Carga los datos a PostgreSQL (este servicio)

## Volúmenes

- `./data/csv/all_csv:/app/data/csv/all_csv:ro` - Archivos CSV (solo lectura)
- `./scripts:/app/scripts:ro` - Scripts de carga (solo lectura)

## Troubleshooting

### El servicio falla al conectar a PostgreSQL

```bash
# Verificar que PostgreSQL está corriendo
docker-compose ps db

# Verificar logs de PostgreSQL
docker-compose logs db
```

### Los archivos CSV no se encuentran

```bash
# Verificar que descarga_datos completó correctamente
docker-compose logs descarga_datos

# Verificar los archivos en el host
ls -la data/csv/all_csv/
```

### Errores de Foreign Key

Los errores de FK son normales si algunos CSV tienen referencias a convocatorias que no existen. El script los reporta pero continúa con los registros válidos.

### Reiniciar desde cero

```bash
# Detener todos los servicios
docker-compose down

# Eliminar el volumen de la base de datos (¡CUIDADO: borra todos los datos!)
docker volume rm hackathon_gbd_db_data

# Reiniciar
docker-compose up -d
```

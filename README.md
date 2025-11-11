# Hackathon GBD - Sistema de Gestión de Datos UAM

Proyecto de hackathon para gestión de datos de la Universidad Autónoma de Madrid utilizando PostgreSQL, pgVector y GraphDB.

## 🚀 Inicio Rápido

### Prerequisitos

- Docker y Docker Compose
- Al menos 4GB de RAM disponible

### Configuración Inicial

1. **Clonar el repositorio**

```bash
git clone <repo-url>
cd hackathon_gbd
```

2. **Configurar variables de entorno**

```bash
# El archivo .env ya está configurado con valores por defecto
# Editar si es necesario
nano .env
```

3. **Iniciar todos los servicios**

```bash
# Crear la red Docker
docker network create gestbd_net

# Iniciar servicios
docker-compose up -d
```

### Orden de Ejecución de Servicios

Los servicios se ejecutan en el siguiente orden automáticamente:

1. **db** (PostgreSQL) - Base de datos principal
2. **descarga_datos** - Descarga archivos CSV de datos abiertos UAM
3. **load_data** - Carga los CSV en PostgreSQL (NUEVO ✨)
4. **pgadmin** - Interfaz web para PostgreSQL
5. **graphdb** - Base de datos de grafos

### Verificar el Estado

```bash
# Ver estado de todos los servicios
docker-compose ps

# Ver logs del servicio de carga de datos
docker-compose logs -f load_data

# Ver logs de todos los servicios
docker-compose logs -f
```

## 📦 Servicios

### PostgreSQL (db)

- Puerto: `5432`
- Usuario: `myuser`
- Password: `mypassword`
- Base de datos: `mydb`

### pgAdmin

- URL: http://localhost:8080
- Email: `admin@example.com`
- Password: `admin`

### GraphDB

- URL: http://localhost:8000

### Descarga de Datos (descarga_datos)

Descarga automáticamente todos los CSV de datos abiertos de la UAM desde 2017 hasta 2025.

### Carga de Datos (load_data) ✨ NUEVO

**Carga automática de datos a PostgreSQL con:**

- Auto-descubrimiento de archivos CSV
- Normalización de códigos de universidad ("23" → "023")
- Validación de claves foráneas
- Deduplicación de licitaciones
- Estadísticas detalladas por archivo

**Gestión del servicio:**

```bash
# Desde el directorio load_data/
./manage.sh build      # Construir imagen
./manage.sh run        # Ejecutar carga
./manage.sh logs       # Ver logs
./manage.sh status     # Ver estado
./manage.sh restart    # Reiniciar servicio
./manage.sh clean      # Limpiar y recargar desde cero
```

Ver documentación completa en [load_data/README.md](load_data/README.md)

## 📊 Estructura de Datos

### Tablas en PostgreSQL

1. **UNIVERSIDAD** - Información de universidades
2. **PRESUPUESTO_GASTOS** - Gastos por año y concepto (~115K registros)
3. **PRESUPUESTO_INGRESOS** - Ingresos por año y concepto (~18K registros)
4. **CONVOCATORIA_AYUDA** - Convocatorias de ayudas
5. **AYUDA** - Ayudas concedidas (~10K registros)
6. **LICITACION** - Licitaciones y contratos mayores

### Datos Cargados (2017-2025)

- **14 archivos** de presupuestos de gastos
- **14 archivos** de presupuestos de ingresos
- **7 archivos** de convocatorias de ayudas
- **7 archivos** de ayudas
- **8 archivos** de licitaciones

## 🔍 TODO

- [ ] Hacer todo el pgvector:
  - [ ] creacion/modificacion de tablas para meter los embeddings
  - [ ] Modificar los scripts para coger el texto de los csv y que los inserte en la tabla correspondiente
  - [ ] probar la busqueda semantica por consola
- [ ] Adaptar scripts para que coja los csv y los inyecte en el postgresql
- [ ] Apartado de graphdb
  - [ ] Probar el rdf si vale la pena hacerlos a partir de los csv o a partir de la tabla sql
  - [ ] O hacer un dump de la BBDD y apartir de ella generar los RDF (aplicando reglas `YARRRML` o `RML` para generacion de `ttl`s) hacerlo directamente en el script
  - [ ] Probar consultas de SPARQL
- [ ] Organizar presentacion
  - [ ] Montar un notebook que pueda consumir esta kk y mostrar las consultas
  - [ ] Preparar consultas demos
  - [ ] Preparar explicacion de la estructura

---

## CONSULTAS DE TEST DE DATOS

```
SELECT * FROM UNIVERSIDAD;
```

```
SELECT COUNT(*) AS total_licitaciones
FROM LICITACION;
```

```
SELECT
    a.cuantia_total,
    c.nombre_convocatoria,
    c.des_categoria
FROM
    AYUDA a
JOIN
    CONVOCATORIA_AYUDA c ON a.cod_convocatoria_ayuda = c.cod_convocatoria
WHERE
    a.cuantia_total > 3000  -- Filtramos para ver solo ayudas significativas
ORDER BY
    a.cuantia_total DESC
LIMIT 10;
```

```
SELECT
    l.objeto_licitacion_o_lote,
    l.importe_adjudicacion_sin_impuestos_licitacion_o_lote,
    l.adjudicatario_licitacion_o_lote,
    u.nombre_corto
FROM
    LICITACION l
JOIN
    UNIVERSIDAD u ON l.nif_oc = u.nifoc
WHERE
    u.nombre_corto = 'UAM'
ORDER BY
    l.importe_adjudicacion_sin_impuestos_licitacion_o_lote DESC NULLS LAST
LIMIT 5;
```

```
SELECT
    des_capitulo,
    SUM(credito_total) AS total_gastado
FROM
    PRESUPUESTO_GASTOS
WHERE
    cod_universidad = '23' -- Filtramos por la UAM
GROUP BY
    des_capitulo
ORDER BY
    total_gastado DESC;
```

```
SELECT COUNT(*) FROM presupuesto_gastos;
SELECT COUNT(*) FROM presupuesto_ingresos;
SELECT COUNT(*) FROM convocatoria_ayuda;
SELECT COUNT(*) FROM ayuda;
SELECT COUNT(*) FROM licitacion;
```

## Ejecutar pruebas unitarias dentro del contenedor `descarga_datos` (contra el servicio `db`)

El repositorio incluye un módulo de pruebas unitarias pequeño en `tests/test_pgvector_ingest_and_query.py`.
Puedes ejecutar esta prueba dentro del servicio `descarga_datos` para que se ejecute en la misma red de Docker que el servicio `db`.

Desde la raíz del repositorio (PowerShell), ejecuta:

```powershell
# Ejecución puntual que monta el repositorio en el contenedor para que la carpeta `tests/` esté disponible
docker-compose run --rm -v ${PWD}:/app descarga_datos /bin/bash -c "python -m pip install -r /app/descarga_datos/requirements.txt && python -m unittest tests.test_pgvector_ingest_and_query -v"
```

Notas:
- El comando monta el directorio de trabajo actual en `/app` dentro del contenedor para que las carpetas `tests/` y `scripts/` sean visibles. Si `${PWD}` no funciona en tu PowerShell, prueba con `${PWD}.Path`.
- `docker-compose run` se conecta a la red de Compose, por lo que `db` será accesible por el nombre de servicio `db` (las pruebas podrán conectar con la base de datos si usan variables de entorno o el host `db`).
- Si ya tienes el contenedor `descarga_datos` en ejecución (por ejemplo con `docker-compose up -d`), puedes ejecutar las pruebas dentro de él sin montar nada adicional así:

```powershell
docker-compose exec descarga_datos /bin/bash -c "python -m pip install -r /app/descarga_datos/requirements.txt && python -m unittest tests.test_pgvector_ingest_and_query -v"
```

- El servicio `descarga_datos` ya declara `env_file: .env` en `docker-compose.yml`, por lo que las variables de entorno de la base de datos estarán disponibles dentro del contenedor (asegúrate de que `.env` contiene los valores `POSTGRES_*` correctos para el servicio `db`).
- La prueba incluida es ligera y no requiere acceso a la base de datos; si añades pruebas que dependan de la BD, asegúrate de que la BD esté inicializada (`docker-compose up db`) y de que el esquema/datos estén cargados (el repo incluye `init-sql/` para la inicialización).

Si quieres, puedo añadir un script auxiliar que ejecute las pruebas y espere a que la base de datos esté accesible antes de ejecutar las pruebas dependientes de ella.

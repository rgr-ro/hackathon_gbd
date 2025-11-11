# TODO

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

----

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

-- init-sql/init.sql
-- Este script se ejecutará automáticamente al crear el contenedor de Postgres

SET client_encoding = 'UTF8';

-------------------------------------------------
-- TAREA 1: CREACIÓN DE TABLAS (DDL)
-------------------------------------------------
-- He adaptado tu ERD para que las FK coincidan con los datos reales
-- (ej. '23' vs 'Q2818013A')

CREATE TABLE UNIVERSIDAD (
    cod_universidad VARCHAR(10) PRIMARY KEY, -- e.g., '23'
    nifoc VARCHAR(15) UNIQUE NOT NULL,       -- e.g., 'Q2818013A'
    des_universidad VARCHAR(255),
    nombre_corto VARCHAR(50)
);
COMMENT ON TABLE UNIVERSIDAD IS 'Entidad principal para la Universidad (UAM)';


CREATE TABLE PRESUPUESTO_GASTOS (
    id_gasto SERIAL PRIMARY KEY,
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    anio INT,
    des_capitulo VARCHAR(255),
    des_articulo VARCHAR(255),
    des_concepto VARCHAR(255),
    credito_inicial DECIMAL(19, 2),
    modificaciones DECIMAL(19, 2),
    credito_total DECIMAL(19, 2)
);
COMMENT ON TABLE PRESUPUESTO_GASTOS IS 'Datos del CSV uam-presupuesto-de-gastos-cierre-2017_1.csv';


CREATE TABLE PRESUPUESTO_INGRESOS (
    id_ingreso SERIAL PRIMARY KEY,
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    anio INT,
    des_capitulo VARCHAR(255),
    des_articulo VARCHAR(255),
    des_concepto VARCHAR(255),
    credito_inicial DECIMAL(19, 2),
    modificaciones DECIMAL(19, 2),
    credito_total DECIMAL(19, 2)
);
COMMENT ON TABLE PRESUPUESTO_INGRESOS IS 'Datos del CSV uam-presupuesto-de-ingresos-cierre-2017_0.csv';


CREATE TABLE CONVOCATORIA_AYUDA (
    cod_convocatoria VARCHAR(255) PRIMARY KEY, -- Renombrada de 'id_convocatoria' para coincidir con CSV
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    nombre_convocatoria TEXT,
    fecha_inicio_solicitudes DATE,
    fecha_fin_solicitudes DATE,
    des_categoria VARCHAR(255)
);
COMMENT ON TABLE CONVOCATORIA_AYUDA IS 'Datos del CSV uam-conv-ayudas-2017-18.csv';


CREATE TABLE AYUDA (
    id_ayuda SERIAL PRIMARY KEY,
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    cod_convocatoria_ayuda VARCHAR(255) REFERENCES CONVOCATORIA_AYUDA(cod_convocatoria), -- Renombrada de 'id_convocatoria'
    cuantia_total DECIMAL(19, 2),
    fecha_concesion DATE,
    anio INT -- Columna 'anio' del CSV de ayudas
);
COMMENT ON TABLE AYUDA IS 'Datos del CSV uam-ayudas-2017-18-anonimizado.csv';


CREATE TABLE LICITACION (
    identificador BIGINT PRIMARY KEY, -- Renombrada de 'id_licitacion'
    nif_oc VARCHAR(15) REFERENCES UNIVERSIDAD(nifoc), -- Renombrada de 'cod_universidad'
    primera_publicacion TIMESTAMP,
    presupuesto_base_sin_impuestos_licitacion_o_lote DECIMAL(19, 2),
    importe_adjudicacion_sin_impuestos_licitacion_o_lote DECIMAL(19, 2),
    resultado_licitacion_o_lote VARCHAR(100),
    identificador_adjudicatario_de_la_licitacion_o_lote VARCHAR(255),
    adjudicatario_licitacion_o_lote VARCHAR(255), -- Columna que faltaba en el ERD
    lote VARCHAR(255), -- Columna que faltaba en el ERD
    objeto_licitacion_o_lote TEXT,
    link_licitacion TEXT,
    descripcion_de_la_financiacion_europea TEXT
);
COMMENT ON TABLE LICITACION IS 'Datos del CSV uam-licitaciones-contratos-mayores-2019.csv';


-------------------------------------------------
-- TAREA 2: INGESTA DE DATOS (DML)
-------------------------------------------------
-- Primero, insertamos la entidad de la que dependen todas las demás
INSERT INTO UNIVERSIDAD (cod_universidad, nifoc, des_universidad, nombre_corto)
VALUES ('23', 'Q2818013A', 'Universidad Autónoma de Madrid', 'UAM');

-- Ahora, usamos COPY para una carga masiva ultra-rápida
-- La ruta '/data/csv/' es la que definimos en el docker-compose.yml

-- NOTA: Algunos CSVs usan codificación 'latin1'
--       La opción NULL AS '' trata las celdas vacías como NULOs

COPY PRESUPUESTO_GASTOS (cod_universidad, anio, des_capitulo, des_articulo, des_concepto, credito_inicial, modificaciones, credito_total)
FROM '/data/csv/uam-presupuesto-de-gastos-cierre-2017_1.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'UTF8';

COPY PRESUPUESTO_INGRESOS (cod_universidad, anio, des_capitulo, des_articulo, des_concepto, credito_inicial, modificaciones, credito_total)
FROM '/data/csv/uam-presupuesto-de-ingresos-cierre-2017_0.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'UTF8';

COPY CONVOCATORIA_AYUDA (cod_convocatoria, cod_universidad, nombre_convocatoria, fecha_inicio_solicitudes, fecha_fin_solicitudes, des_categoria)
FROM '/data/csv/uam-conv-ayudas-2017-18.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'LATIN1';

COPY AYUDA (cod_universidad, cod_convocatoria_ayuda, cuantia_total, fecha_concesion, anio)
FROM '/data/csv/uam-ayudas-2017-18-anonimizado.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'LATIN1';

COPY LICITACION (identificador, nif_oc, primera_publicacion, presupuesto_base_sin_impuestos_licitacion_o_lote, importe_adjudicacion_sin_impuestos_licitacion_o_lote, resultado_licitacion_o_lote, identificador_adjudicatario_de_la_licitacion_lote, adjudicatario_licitacion_o_lote, lote, objeto_licitacion_o_lote, link_licitacion, descripcion_de_la_financiacion_europea)
FROM '/data/csv/uam-licitaciones-contratos-mayores-2019.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'LATIN1';
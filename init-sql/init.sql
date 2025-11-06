-- init-sql/init.sql
-- Este script se ejecutará automáticamente al crear el contenedor de Postgres
-- Contiene todas las correcciones (PK de Licitacion y Staging de Presupuestos)

SET client_encoding = 'UTF8';

-------------------------------------------------
-- TAREA 1: CREACIÓN DE TABLAS (DDL)
-------------------------------------------------

-- Borramos en orden inverso a la creación por las FKs
DROP TABLE IF EXISTS LICITACION;
DROP TABLE IF EXISTS AYUDA;
DROP TABLE IF EXISTS CONVOCATORIA_AYUDA;
DROP TABLE IF EXISTS PRESUPUESTO_GASTOS;
DROP TABLE IF EXISTS PRESUPUESTO_INGRESOS;
DROP TABLE IF EXISTS UNIVERSIDAD;


CREATE TABLE UNIVERSIDAD (
    cod_universidad VARCHAR(10) PRIMARY KEY, -- e.g., '23'
    nifoc VARCHAR(15) UNIQUE NOT NULL,       -- e.g., 'Q2818013A'
    des_universidad VARCHAR(255),
    nombre_corto VARCHAR(50)
);
COMMENT ON TABLE UNIVERSIDAD IS 'Entidad principal para la Universidad (UAM)';


CREATE TABLE CONVOCATORIA_AYUDA (
    cod_convocatoria VARCHAR(255) PRIMARY KEY, 
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    nombre_convocatoria TEXT,
    fecha_inicio_solicitudes DATE,
    fecha_fin_solicitudes DATE,
    des_categoria VARCHAR(255)
);
COMMENT ON TABLE CONVOCATORIA_AYUDA IS 'Datos del CSV uam-conv-ayudas-2017-18.csv';

-- Tabla temporal para cargar el CSV completo de convocatorias (64 columnas)
CREATE TEMPORARY TABLE staging_convocatoria (
    anio TEXT,
    fecha_inicio_solicitudes TEXT,
    fecha_fin_solicitudes TEXT,
    cod_universidad TEXT,
    des_universidad TEXT,
    cod_convocatoria TEXT,
    des_convocatoria TEXT,
    nombre_convocatoria TEXT,
    cod_univ_aplicacion TEXT,
    des_univ_aplicacion TEXT,
    cod_ambito_aplicacion TEXT,
    des_ambito_aplicacion TEXT,
    cod_agente_financiador TEXT,
    des_agente_financiador TEXT,
    cod_agente_cofinanciador TEXT,
    des_agente_cofinanciador TEXT,
    ind_efectos_multianuales TEXT,
    num_periodos TEXT,
    cod_categoria TEXT,
    des_categoria TEXT,
    cod_financia_matricula TEXT,
    des_financia_matricula TEXT,
    ind_modalidad_compensatoria TEXT,
    cod_modalidad_residencia TEXT,
    des_modalidad_residencia TEXT,
    cod_modalidad_mov_interna TEXT,
    des_modalidad_mov_interna TEXT,
    cod_modalidad_mov_internacional TEXT,
    des_modalidad_mov_internacional TEXT,
    cod_modalidad_gastos_despl TEXT,
    des_modalidad_gastos_despl TEXT,
    cod_modalidad_material TEXT,
    des_modalidad_material TEXT,
    cod_modalidad_comedor TEXT,
    des_modalidad_comedor TEXT,
    cod_modalidad_contraprestacion_serv TEXT,
    des_modalidad_contraprestacion_serv TEXT,
    cod_modalidad_idiomas TEXT,
    des_modalidad_idiomas TEXT,
    ind_modalidad_discapacidad TEXT,
    ind_modalidad_seguros TEXT,
    ind_otras_modalidades TEXT,
    cod_nivel_estudios TEXT,
    des_nivel_estudios TEXT,
    ind_req_rendimiento TEXT,
    ind_req_creditos TEXT,
    ind_req_renta TEXT,
    ind_req_patrimonio TEXT,
    ind_req_discapacidad TEXT,
    ind_req_genero TEXT,
    ind_req_edad TEXT,
    ind_req_residencia_curso TEXT,
    ind_req_residencia_habitual TEXT,
    ind_req_vinc_territorial TEXT,
    cod_req_nacionalidad TEXT,
    des_req_nacionalidad TEXT,
    ind_req_presencialidad TEXT,
    ind_req_credencial_becario TEXT,
    ind_req_desempleado TEXT,
    ind_complementaria TEXT,
    ind_incompatible TEXT,
    presupuesto_inicial TEXT,
    presupuesto_ejecutado TEXT,
    num_plazas TEXT
);


CREATE TABLE AYUDA (
    id_ayuda SERIAL PRIMARY KEY,
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    cod_convocatoria_ayuda VARCHAR(255) REFERENCES CONVOCATORIA_AYUDA(cod_convocatoria), 
    cuantia_total DECIMAL(19, 2),
    fecha_concesion DATE,
    anio INT
);
COMMENT ON TABLE AYUDA IS 'Datos del CSV uam-ayudas-2017-18-anonimizado.csv';

-- Tabla temporal para cargar el CSV completo de ayudas (29 columnas)
CREATE TEMPORARY TABLE staging_ayuda (
    importante TEXT,
    cod_genero TEXT,
    des_genero TEXT,
    cod_pais_nacionalidad TEXT,
    des_pais_nacionalidad TEXT,
    lat_pais_nacionalidad TEXT,
    lon_pais_nacionalidad TEXT,
    cod_continente_nacionalidad TEXT,
    des_continente_nacionalidad TEXT,
    cod_agregacion_paises_nacionalidad TEXT,
    des_agregacion_paises_nacionalidad TEXT,
    anio_nacimiento TEXT,
    anio TEXT,
    cod_universidad TEXT,
    des_universidad TEXT,
    cod_convocatoria_ayuda TEXT,
    des_convocatoria_ayuda TEXT,
    cod_universidad_mat TEXT,
    des_universidad_mat TEXT,
    cod_titulacion TEXT,
    des_titulacion TEXT,
    cod_estado TEXT,
    des_estado TEXT,
    cod_periodo_conv TEXT,
    des_periodo_conv TEXT,
    cod_cobertura_mat TEXT,
    des_cobertura_mat TEXT,
    cuantia_total TEXT,
    cuantia_matricula TEXT
);


-- === CORRECCIÓN LICITACION (Clave Primaria Compuesta) ===
CREATE TABLE LICITACION (
    identificador BIGINT,
    nif_oc VARCHAR(15) REFERENCES UNIVERSIDAD(nifoc), 
    primera_publicacion TIMESTAMP,
    presupuesto_base_sin_impuestos_licitacion_o_lote DECIMAL(19, 2),
    importe_adjudicacion_sin_impuestos_licitacion_o_lote DECIMAL(19, 2),
    resultado_licitacion_o_lote VARCHAR(100),
    identificador_adjudicatario_de_la_licitacion_lote VARCHAR(255),
    adjudicatario_licitacion_o_lote VARCHAR(255), 
    lote VARCHAR(255), 
    objeto_licitacion_o_lote TEXT,
    link_licitacion TEXT,
    descripcion_de_la_financiacion_europea TEXT,
    -- Clave Primaria Compuesta para manejar lotes
    PRIMARY KEY (identificador, lote)
);
COMMENT ON TABLE LICITACION IS 'Datos del CSV uam-licitaciones-contratos-mayores-2019.csv';

-- Tabla temporal para cargar el CSV completo de licitaciones (146 columnas)
CREATE TEMPORARY TABLE staging_licitacion (
    identificador TEXT,
    link_licitacion TEXT,
    fecha_actualizacion TEXT,
    vigente_o_anulada_o_archivada TEXT,
    primera_publicacion TEXT,
    estado TEXT,
    numero_de_expediente TEXT,
    objeto_del_contrato TEXT,
    identificador_unico_ted TEXT,
    valor_estimado_del_contrato TEXT,
    presupuesto_base_sin_impuestos TEXT,
    presupuesto_base_con_impuestos TEXT,
    cpvs TEXT,
    numero_cpvs TEXT,
    cpv_1 TEXT, des_cpv_1 TEXT, cpv_2 TEXT, des_cpv_2 TEXT, cpv_3 TEXT, des_cpv_3 TEXT,
    cpv_4 TEXT, des_cpv_4 TEXT, cpv_5 TEXT, des_cpv_5 TEXT, cpv_6 TEXT, des_cpv_6 TEXT,
    cpv_7 TEXT, des_cpv_7 TEXT, cpv_8 TEXT, des_cpv_8 TEXT, cpv_9 TEXT, des_cpv_9 TEXT,
    cpv_10 TEXT, des_cpv_10 TEXT, cpv_11 TEXT, des_cpv_11 TEXT, cpv_12 TEXT, des_cpv_12 TEXT,
    cpv_13 TEXT, des_cpv_13 TEXT, cpv_14 TEXT, des_cpv_14 TEXT, cpv_15 TEXT, des_cpv_15 TEXT,
    cpv_16 TEXT, des_cpv_16 TEXT, cpv_17 TEXT, des_cpv_17 TEXT, cpv_18 TEXT, des_cpv_18 TEXT,
    cpv_19 TEXT, des_cpv_19 TEXT, cpv_20 TEXT, des_cpv_20 TEXT,
    tipo_de_contrato TEXT,
    contrato_mixto TEXT,
    lugar_de_ejecucion TEXT,
    des_lugar_de_ejecucion TEXT,
    lat_lugar_de_ejecucion TEXT,
    lon_lugar_de_ejecucion TEXT,
    organo_de_contratacion TEXT,
    id_oc_en_placsp TEXT,
    nif_oc TEXT,
    dir3 TEXT,
    enlace_al_perfil_de_contratante_del_oc TEXT,
    tipo_de_administracion TEXT,
    codigo_postal TEXT,
    tipo_de_procedimiento TEXT,
    sistema_de_contratacion TEXT,
    tramitacion TEXT,
    forma_de_presentacion_de_la_oferta TEXT,
    fecha_de_presentacion_de_ofertas TEXT,
    fecha_de_presentacion_de_solicitudes_de_participacion TEXT,
    directiva_de_aplicacion TEXT,
    contrato_sara_o_umbral TEXT,
    financiacion_europea_y_fuente TEXT,
    descripcion_de_la_financiacion_europea TEXT,
    subasta_electronica TEXT,
    subcontratacion_permitida TEXT,
    subcontratacion_permitida_porcentaje TEXT,
    lote TEXT,
    objeto_licitacion_o_lote TEXT,
    valor_estimado_licitacion_o_lote TEXT,
    presupuesto_base_con_impuestos_licitacion_o_lote TEXT,
    presupuesto_base_sin_impuestos_licitacion_o_lote TEXT,
    cpvs_lote TEXT,
    numero_cpvs_lote TEXT,
    cpv_lote_1 TEXT, des_cpv_lote_1 TEXT, cpv_lote_2 TEXT, des_cpv_lote_2 TEXT,
    cpv_lote_3 TEXT, des_cpv_lote_3 TEXT, cpv_lote_4 TEXT, des_cpv_lote_4 TEXT,
    cpv_lote_5 TEXT, des_cpv_lote_5 TEXT, cpv_lote_6 TEXT, des_cpv_lote_6 TEXT,
    cpv_lote_7 TEXT, des_cpv_lote_7 TEXT, cpv_lote_8 TEXT, des_cpv_lote_8 TEXT,
    cpv_lote_9 TEXT, des_cpv_lote_9 TEXT, cpv_lote_10 TEXT, des_cpv_lote_10 TEXT,
    cpv_lote_11 TEXT, des_cpv_lote_11 TEXT, cpv_lote_12 TEXT, des_cpv_lote_12 TEXT,
    cpv_lote_13 TEXT, des_cpv_lote_13 TEXT, cpv_lote_14 TEXT, des_cpv_lote_14 TEXT,
    cpv_lote_15 TEXT, des_cpv_lote_15 TEXT, cpv_lote_16 TEXT, des_cpv_lote_16 TEXT,
    cpv_lote_17 TEXT, des_cpv_lote_17 TEXT, cpv_lote_18 TEXT, des_cpv_lote_18 TEXT,
    cpv_lote_19 TEXT, des_cpv_lote_19 TEXT, cpv_lote_20 TEXT, des_cpv_lote_20 TEXT,
    lugar_ejecucion_licitacion_o_lote TEXT,
    des_lugar_de_ejecucion_licitacion_o_lote TEXT,
    lat_lugar_de_ejecucion_licitacion_o_lote TEXT,
    lon_lugar_de_ejecucion_licitacion_o_lote TEXT,
    resultado_licitacion_o_lote TEXT,
    fecha_del_acuerdo_licitacion_o_lote TEXT,
    numero_de_ofertas_recibidas_por_licitacion_o_lote TEXT,
    precio_de_la_oferta_mas_baja_por_licitacion_o_lote TEXT,
    precio_de_la_oferta_mas_alta_por_licitacion_o_lote TEXT,
    ofertas_excluidas_por_anormalmente_bajas_por_licitacion_o_lote TEXT,
    numero_del_contrato_licitacion_o_lote TEXT,
    fecha_formalizacion_del_contrato_licitacion_o_lote TEXT,
    fecha_entrada_en_vigor_del_contrato_de_licitacion_o_lote TEXT,
    adjudicatario_licitacion_o_lote TEXT,
    tipo_de_identificador_de_adjudicatario_por_licitacion_o_lote TEXT,
    identificador_adjudicatario_de_la_licitacion_lote TEXT,
    el_adjudicatario_es_o_no_pyme_de_la_licitacion_o_lote TEXT,
    importe_adjudicacion_sin_impuestos_licitacion_o_lote TEXT,
    importe_adjudicacion_con_impuestos_licitacion_o_lote TEXT
);


-- === PROCESO STAGING PARA PRESUPUESTOS (GASTOS) ===
-- 1. Crea la tabla FINAL (la de tu E-R)
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
COMMENT ON TABLE PRESUPUESTO_GASTOS IS 'Tabla final (limpia) de Gastos, segun E-R';

-- 2. Crea la tabla TEMPORAL (calco COMPLETO del CSV con TODAS las 40 columnas)
-- Usamos TEXT para TODOS los campos (incluso numéricos) porque el CSV usa coma decimal europea
CREATE TEMPORARY TABLE staging_gastos (
    cod_universidad TEXT,
    des_universidad TEXT,
    anio TEXT,
    cod_capitulo TEXT,
    des_capitulo TEXT,
    cod_articulo TEXT,
    des_articulo TEXT,
    cod_concepto TEXT,
    des_concepto TEXT,
    cod_subconcepto TEXT,
    des_subconcepto TEXT,
    cod_partida TEXT,
    des_partida TEXT,
    cod_seccion TEXT,
    des_seccion TEXT,
    cod_servicio TEXT,
    des_servicio TEXT,
    cod_unidad_de_coste TEXT,
    des_unidad_de_coste TEXT,
    cod_organica_libre_1 TEXT,
    des_organica_libre_1 TEXT,
    cod_organica_libre_2 TEXT,
    des_organica_libre_2 TEXT,
    cod_grupo TEXT,
    des_grupo TEXT,
    cod_funcion TEXT,
    des_funcion TEXT,
    cod_subfuncion TEXT,
    des_subfuncion TEXT,
    cod_programa TEXT,
    des_programa TEXT,
    cod_subprograma TEXT,
    des_subprograma TEXT,
    fecha_referencia TEXT,
    credito_inicial TEXT,
    modificaciones TEXT,
    credito_total TEXT,
    gastos_comprometidos TEXT,
    obligaciones_reconocidas TEXT,
    pagos_netos TEXT
);


-- === PROCESO STAGING PARA PRESUPUESTOS (INGRESOS) ===
-- 1. Crea la tabla FINAL (la de tu E-R)
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
COMMENT ON TABLE PRESUPUESTO_INGRESOS IS 'Tabla final (limpia) de Ingresos, segun E-R';

-- 2. Crea la tabla TEMPORAL (calco COMPLETO del CSV con TODAS las 29 columnas)
-- Usamos TEXT para TODOS los campos (incluso numéricos) porque el CSV usa coma decimal europea
CREATE TEMPORARY TABLE staging_ingresos (
    cod_universidad TEXT,
    des_universidad TEXT,
    anio TEXT,
    cod_capitulo TEXT,
    des_capitulo TEXT,
    cod_articulo TEXT,
    des_articulo TEXT,
    cod_concepto TEXT,
    des_concepto TEXT,
    cod_subconcepto TEXT,
    des_subconcepto TEXT,
    cod_partida TEXT,
    des_partida TEXT,
    cod_seccion TEXT,
    des_seccion TEXT,
    cod_servicio TEXT,
    des_servicio TEXT,
    cod_unidad_de_coste TEXT,
    des_unidad_de_coste TEXT,
    cod_organica_libre_1 TEXT,
    des_organica_libre_1 TEXT,
    cod_organica_libre_2 TEXT,
    des_organica_libre_2 TEXT,
    fecha_referencia TEXT,
    credito_inicial TEXT,
    modificaciones TEXT,
    credito_total TEXT,
    derechos_reconocidos_netos TEXT,
    recaudacion_neta TEXT
);


-------------------------------------------------
-- TAREA 2: INGESTA DE DATOS (DML)
-------------------------------------------------
-- Primero, insertamos la entidad de la que dependen todas las demás
-- IMPORTANTE: el CSV usa '023' (con ceros a la izquierda), no '23'
INSERT INTO UNIVERSIDAD (cod_universidad, nifoc, des_universidad, nombre_corto)
VALUES ('023', 'Q2818013A', 'Universidad Autónoma de Madrid', 'UAM');


-- 3. Carga el CSV en la tabla TEMPORAL de GASTOS
COPY staging_gastos
FROM '/data/csv/uam-presupuesto-de-gastos-cierre-2017_1.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'UTF8';

-- 4. Mueve solo las columnas deseadas a la tabla FINAL
-- Convierte coma decimal europea a punto decimal para PostgreSQL
INSERT INTO PRESUPUESTO_GASTOS (
    cod_universidad, anio, des_capitulo, des_articulo, des_concepto, 
    credito_inicial, modificaciones, credito_total
)
SELECT 
    cod_universidad, 
    anio::INT, 
    des_capitulo, 
    des_articulo, 
    des_concepto, 
    REPLACE(credito_inicial, ',', '.')::DECIMAL(19, 2),
    REPLACE(modificaciones, ',', '.')::DECIMAL(19, 2),
    REPLACE(credito_total, ',', '.')::DECIMAL(19, 2)
FROM staging_gastos;


-- 3. Carga el CSV en la tabla TEMPORAL de INGRESOS
COPY staging_ingresos
FROM '/data/csv/uam-presupuesto-de-ingresos-cierre-2017_0.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'UTF8';

-- 4. Mueve solo las columnas deseadas a la tabla FINAL
-- Convierte coma decimal europea a punto decimal para PostgreSQL
INSERT INTO PRESUPUESTO_INGRESOS (
    cod_universidad, anio, des_capitulo, des_articulo, des_concepto, 
    credito_inicial, modificaciones, credito_total
)
SELECT 
    cod_universidad, 
    anio::INT, 
    des_capitulo, 
    des_articulo, 
    des_concepto, 
    REPLACE(credito_inicial, ',', '.')::DECIMAL(19, 2),
    REPLACE(modificaciones, ',', '.')::DECIMAL(19, 2),
    REPLACE(credito_total, ',', '.')::DECIMAL(19, 2)
FROM staging_ingresos;


-- Carga del resto de CSVs usando el mismo patrón de staging

-- CONVOCATORIA_AYUDA
COPY staging_convocatoria
FROM '/data/csv/uam-conv-ayudas-2017-18.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'LATIN1';

INSERT INTO CONVOCATORIA_AYUDA (
    cod_convocatoria, cod_universidad, nombre_convocatoria, 
    fecha_inicio_solicitudes, fecha_fin_solicitudes, des_categoria
)
SELECT 
    cod_convocatoria,
    cod_universidad,
    nombre_convocatoria,
    TO_DATE(fecha_inicio_solicitudes, 'YYYYMMDD'),
    TO_DATE(fecha_fin_solicitudes, 'YYYYMMDD'),
    des_categoria
FROM staging_convocatoria;


-- AYUDA
COPY staging_ayuda
FROM '/data/csv/uam-ayudas-2017-18-anonimizado.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'LATIN1';

INSERT INTO AYUDA (cod_universidad, cod_convocatoria_ayuda, cuantia_total, fecha_concesion, anio)
SELECT 
    cod_universidad,
    cod_convocatoria_ayuda,
    REPLACE(cuantia_total, ',', '.')::DECIMAL(19, 2),
    NULL, -- No hay fecha_concesion en el CSV
    anio::INT
FROM staging_ayuda;


-- LICITACION
COPY staging_licitacion
FROM '/data/csv/uam-licitaciones-contratos-mayores-2019.csv'
CSV HEADER DELIMITER ',' NULL AS '' ENCODING 'LATIN1';

INSERT INTO LICITACION (
    identificador, nif_oc, primera_publicacion, 
    presupuesto_base_sin_impuestos_licitacion_o_lote, 
    importe_adjudicacion_sin_impuestos_licitacion_o_lote, 
    resultado_licitacion_o_lote, 
    identificador_adjudicatario_de_la_licitacion_lote, 
    adjudicatario_licitacion_o_lote, 
    lote, 
    objeto_licitacion_o_lote, 
    link_licitacion, 
    descripcion_de_la_financiacion_europea
)
SELECT 
    identificador::BIGINT,
    nif_oc,
    primera_publicacion::TIMESTAMP,
    REPLACE(presupuesto_base_sin_impuestos_licitacion_o_lote, ',', '.')::DECIMAL(19, 2),
    REPLACE(importe_adjudicacion_sin_impuestos_licitacion_o_lote, ',', '.')::DECIMAL(19, 2),
    resultado_licitacion_o_lote,
    identificador_adjudicatario_de_la_licitacion_lote,
    adjudicatario_licitacion_o_lote,
    COALESCE(lote, ''), -- Si lote es NULL, usar string vacío para la PK
    objeto_licitacion_o_lote,
    link_licitacion,
    descripcion_de_la_financiacion_europea
FROM staging_licitacion;
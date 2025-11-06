#!/usr/bin/env python3
"""
Load filtered CSV data into PostgreSQL according to the provided ER model.
- Filters only the required columns from each CSV
- Creates tables (DROP + CREATE) following the ER
- Inserts data with correct type/encoding conversions

Assumptions
- Year (anio) is stored as INT (year number)
- AYUDA CSV doesn't contain fecha_concesion -> we set it to NULL
- LICITACION usa el nombre original de columnas del CSV (identificador, nif_oc, ...).
    Si existen múltiples lotes con el mismo identificador, se conserva la primera
    ocurrencia para respetar una PK simple (posible pérdida de detalle por lote).
- cod_universidad for UAM is '023' and NIF (nifoc) in licitaciones is 'Q2818013A'

Usage:
  python scripts/load_filtered_csvs.py \
    --host localhost --port 5432 --user postgres --password postgres --dbname postgres

Relies on CSVs located under data/csv/ in the repository root.
"""

import argparse
import csv
import os
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg2
import psycopg2.extras as extras

# --- File paths (relative to repo root) ---
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV_DIR = os.path.join(ROOT, "data", "csv")

CSV_GASTOS = os.path.join(CSV_DIR, "uam-presupuesto-de-gastos-cierre-2017_1.csv")
CSV_INGRESOS = os.path.join(CSV_DIR, "uam-presupuesto-de-ingresos-cierre-2017_0.csv")
CSV_CONV = os.path.join(CSV_DIR, "uam-conv-ayudas-2017-18.csv")
CSV_AYUDA = os.path.join(CSV_DIR, "uam-ayudas-2017-18-anonimizado.csv")
CSV_LICIT = os.path.join(CSV_DIR, "uam-licitaciones-contratos-mayores-2019.csv")

# Constants for mapping licitaciones -> universidad
UAM_COD = "023"
UAM_NIF = "Q2818013A"


def to_decimal(s):
    if s is None:
        return None
    s = s.strip()
    if s == "" or s.upper() == "NA" or s.upper() == "NULL":
        return None
    # Replace european comma decimal with dot
    s2 = s.replace(".", "")  # in case thousands separator is dot
    s2 = s2.replace(",", ".")
    try:
        return Decimal(s2)
    except InvalidOperation:
        return None


def to_int(s):
    if s is None:
        return None
    s = s.strip()
    if s == "" or not s.isdigit():
        # try removing quotes and non-numeric
        try:
            return int("".join(ch for ch in s if ch.isdigit()))
        except Exception:
            return None
    return int(s)


def parse_date_yyyymmdd(s):
    if not s:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def parse_ts(s):
    if not s:
        return None
    s = s.strip()
    if s == "":
        return None
    # Try several formats
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    return None


def connect_db(args):
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
    )
    conn.autocommit = False
    return conn


DDL_SQL = r"""
DROP TABLE IF EXISTS AYUDA;
DROP TABLE IF EXISTS CONVOCATORIA_AYUDA;
DROP TABLE IF EXISTS LICITACION;
DROP TABLE IF EXISTS PRESUPUESTO_GASTOS;
DROP TABLE IF EXISTS PRESUPUESTO_INGRESOS;
DROP TABLE IF EXISTS UNIVERSIDAD;

CREATE TABLE UNIVERSIDAD (
    cod_universidad VARCHAR(10) PRIMARY KEY,
    nifoc VARCHAR(15) UNIQUE NOT NULL,
    des_universidad VARCHAR(255),
    nombre_corto VARCHAR(50)
);

CREATE TABLE PRESUPUESTO_GASTOS (
    id_gasto SERIAL PRIMARY KEY,
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    anio INT,
    des_capitulo VARCHAR(255),
    des_articulo VARCHAR(255),
    des_concepto VARCHAR(255),
    credito_inicial DECIMAL(19,2),
    modificaciones DECIMAL(19,2),
    credito_total DECIMAL(19,2)
);

CREATE TABLE PRESUPUESTO_INGRESOS (
    id_ingreso SERIAL PRIMARY KEY,
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    anio INT,
    des_capitulo VARCHAR(255),
    des_articulo VARCHAR(255),
    des_concepto VARCHAR(255),
    credito_inicial DECIMAL(19,2),
    modificaciones DECIMAL(19,2),
    credito_total DECIMAL(19,2)
);

CREATE TABLE LICITACION (
    identificador BIGINT PRIMARY KEY,
    nif_oc VARCHAR(15) REFERENCES UNIVERSIDAD(nifoc),
    primera_publicacion TIMESTAMP,
    presupuesto_base_sin_impuestos_licitacion_o_lote DECIMAL(19,2),
    importe_adjudicacion_sin_impuestos_licitacion_o_lote DECIMAL(19,2),
    resultado_licitacion_o_lote VARCHAR(100),
    identificador_adjudicatario_de_la_licitacion_o_lote VARCHAR(255),
    objeto_licitacion_o_lote TEXT,
    link_licitacion TEXT,
    descripcion_de_la_financiacion_europea TEXT
);

CREATE TABLE CONVOCATORIA_AYUDA (
    cod_convocatoria VARCHAR(255) PRIMARY KEY,
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    nombre_convocatoria TEXT,
    fecha_inicio_solicitudes DATE,
    fecha_fin_solicitudes DATE,
    des_categoria VARCHAR(255)
);

CREATE TABLE AYUDA (
    id_ayuda SERIAL PRIMARY KEY,
    cod_universidad VARCHAR(10) REFERENCES UNIVERSIDAD(cod_universidad),
    cod_convocatoria_ayuda VARCHAR(255) REFERENCES CONVOCATORIA_AYUDA(cod_convocatoria),
    cuantia_total DECIMAL(19,2),
    fecha_concesion DATE
);
"""


def create_tables(cur):
    cur.execute(DDL_SQL)


def seed_universidad(cur):
    # Minimal seed for UAM (matches CSVs)
    cur.execute(
        "INSERT INTO UNIVERSIDAD (cod_universidad, nifoc, des_universidad, nombre_corto) VALUES (%s, %s, %s, %s)",
        (UAM_COD, UAM_NIF, "Universidad Autónoma de Madrid", "UAM"),
    )


def load_gastos(conn):
    print("Loading PRESUPUESTO_GASTOS from", CSV_GASTOS)
    rows = []
    with open(CSV_GASTOS, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                (
                    (r.get("cod_universidad") or "").strip().strip('"'),
                    to_int(r.get("anio")),
                    (r.get("des_capitulo") or "").strip(),
                    (r.get("des_articulo") or "").strip(),
                    (r.get("des_concepto") or "").strip(),
                    to_decimal(r.get("credito_inicial")),
                    to_decimal(r.get("modificaciones")),
                    to_decimal(r.get("credito_total")),
                )
            )
    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            """
            INSERT INTO PRESUPUESTO_GASTOS (
                cod_universidad, anio, des_capitulo, des_articulo, des_concepto,
                credito_inicial, modificaciones, credito_total
            ) VALUES %s
            """,
            rows,
        )


def load_ingresos(conn):
    print("Loading PRESUPUESTO_INGRESOS from", CSV_INGRESOS)
    rows = []
    with open(CSV_INGRESOS, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                (
                    (r.get("cod_universidad") or "").strip().strip('"'),
                    to_int(r.get("anio")),
                    (r.get("des_capitulo") or "").strip(),
                    (r.get("des_articulo") or "").strip(),
                    (r.get("des_concepto") or "").strip(),
                    to_decimal(r.get("credito_inicial")),
                    to_decimal(r.get("modificaciones")),
                    to_decimal(r.get("credito_total")),
                )
            )
    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            """
            INSERT INTO PRESUPUESTO_INGRESOS (
                cod_universidad, anio, des_capitulo, des_articulo, des_concepto,
                credito_inicial, modificaciones, credito_total
            ) VALUES %s
            """,
            rows,
        )


def load_convocatoria(conn):
    print("Loading CONVOCATORIA_AYUDA from", CSV_CONV)
    rows = []
    with open(CSV_CONV, "r", encoding="latin1", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                (
                    (r.get("cod_convocatoria") or "").strip(),
                    (r.get("cod_universidad") or "").strip().strip('"'),
                    (r.get("nombre_convocatoria") or "").strip(),
                    parse_date_yyyymmdd(r.get("fecha_inicio_solicitudes")),
                    parse_date_yyyymmdd(r.get("fecha_fin_solicitudes")),
                    (r.get("des_categoria") or "").strip(),
                )
            )
    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            """
            INSERT INTO CONVOCATORIA_AYUDA (
                cod_convocatoria, cod_universidad, nombre_convocatoria,
                fecha_inicio_solicitudes, fecha_fin_solicitudes, des_categoria
            ) VALUES %s
            ON CONFLICT (cod_convocatoria) DO NOTHING
            """,
            rows,
        )


def load_ayuda(conn):
    print("Loading AYUDA from", CSV_AYUDA)
    # Obtener las convocatorias válidas ya insertadas
    valid_conv = set()
    with conn.cursor() as cur:
        cur.execute("SELECT cod_convocatoria FROM CONVOCATORIA_AYUDA")
        for (code,) in cur.fetchall():
            valid_conv.add(code)

    rows = []
    kept = 0
    skipped_empty = 0
    skipped_missing_fk = 0
    with open(CSV_AYUDA, "r", encoding="latin1", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cod_univ = (r.get("cod_universidad") or "").strip().strip('"')
            cod_conv = (r.get("cod_convocatoria_ayuda") or "").strip()
            if not cod_conv:
                skipped_empty += 1
                continue
            if cod_conv not in valid_conv:
                skipped_missing_fk += 1
                continue
            rows.append(
                (
                    cod_univ,
                    cod_conv,
                    to_decimal(r.get("cuantia_total")),
                    None,  # fecha_concesion not present -> NULL
                )
            )
            kept += 1
    print(
        f"AYUDA: kept {kept}, skipped empty conv {skipped_empty}, skipped missing FK {skipped_missing_fk}"
    )
    if rows:
        with conn.cursor() as cur:
            extras.execute_values(
                cur,
                """
                INSERT INTO AYUDA (
                    cod_universidad, cod_convocatoria_ayuda, cuantia_total, fecha_concesion
                ) VALUES %s
                """,
                rows,
            )


def load_licitacion(conn):
    print("Loading LICITACION from", CSV_LICIT)
    rows = []
    seen_ids = set()
    kept = 0
    skipped_dups = 0
    skipped_nif = 0
    with open(CSV_LICIT, "r", encoding="latin1", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            nif = (r.get("nif_oc") or "").strip()
            if nif != UAM_NIF:
                skipped_nif += 1
                continue  # keep only UAM rows
            ident = r.get("identificador")
            if ident in seen_ids:
                skipped_dups += 1
                continue  # keep first occurrence only to respect ER PK
            seen_ids.add(ident)
            rows.append(
                (
                    to_int(ident),
                    (r.get("nif_oc") or "").strip(),
                    parse_ts(r.get("primera_publicacion")),
                    to_decimal(
                        r.get("presupuesto_base_sin_impuestos_licitacion_o_lote")
                    ),
                    to_decimal(
                        r.get("importe_adjudicacion_sin_impuestos_licitacion_o_lote")
                    ),
                    (r.get("resultado_licitacion_o_lote") or "").strip(),
                    (
                        r.get("identificador_adjudicatario_de_la_licitacion_o_lote")
                        or ""
                    ).strip(),
                    (r.get("objeto_licitacion_o_lote") or "").strip(),
                    (r.get("link_licitacion") or "").strip(),
                    (r.get("descripcion_de_la_financiacion_europea") or "").strip(),
                )
            )
            kept += 1
    print(
        f"LICITACION: kept {kept}, skipped non-UAM {skipped_nif}, skipped dups {skipped_dups}"
    )
    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            """
            INSERT INTO LICITACION (
                identificador, nif_oc, primera_publicacion,
                presupuesto_base_sin_impuestos_licitacion_o_lote,
                importe_adjudicacion_sin_impuestos_licitacion_o_lote,
                resultado_licitacion_o_lote,
                identificador_adjudicatario_de_la_licitacion_o_lote,
                objeto_licitacion_o_lote,
                link_licitacion,
                descripcion_de_la_financiacion_europea
            ) VALUES %s
            """,
            rows,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Load filtered CSVs into PostgreSQL according to ER."
    )
    parser.add_argument("--host", default=os.environ.get("POSTGRES_HOST", "localhost"))
    parser.add_argument("--port", default=int(os.environ.get("DB_HOST_PORT", "5432")))
    parser.add_argument("--user", default=os.environ.get("POSTGRES_USER", "postgres"))
    parser.add_argument(
        "--password", default=os.environ.get("POSTGRES_PASSWORD", "postgres")
    )
    parser.add_argument("--dbname", default=os.environ.get("POSTGRES_DB", "postgres"))
    args = parser.parse_args()

    for path in [CSV_GASTOS, CSV_INGRESOS, CSV_CONV, CSV_AYUDA, CSV_LICIT]:
        if not os.path.exists(path):
            print(f"ERROR: CSV not found: {path}", file=sys.stderr)
            sys.exit(1)

    conn = connect_db(args)
    try:
        with conn.cursor() as cur:
            create_tables(cur)
            seed_universidad(cur)
        load_gastos(conn)
        load_ingresos(conn)
        load_convocatoria(conn)
        load_ayuda(conn)
        load_licitacion(conn)
        conn.commit()
        print("DONE: All data loaded successfully.")
    except Exception as e:
        conn.rollback()
        print("ERROR:", e, file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

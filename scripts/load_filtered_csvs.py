#!/usr/bin/env python3
"""
Load filtered CSV data into PostgreSQL according to the provided ER model.
- Filters only the required columns from each CSV
- Creates tables (DROP + CREATE) following the ER
- Inserts data with correct type/encoding conversions
- Auto-discovers CSV files by pattern matching

Assumptions
- Year (anio) is stored as INT (year number)
- AYUDA CSV doesn't contain fecha_concesion -> we set it to NULL
- LICITACION usa el nombre original de columnas del CSV (identificador, nif_oc, ...).
    Si existen múltiples lotes con el mismo identificador, se conserva la primera
    ocurrencia para respetar una PK simple (posible pérdida de detalle por lote).
- cod_universidad for UAM is '023' and NIF (nifoc) in licitaciones is 'Q2818013A'

Usage:
  # Load from data/csv/ (default)
  python scripts/load_filtered_csvs.py \
    --host localhost --port 5432 --user postgres --password postgres --dbname postgres

  # Load from data/csv/all_csv/
  python scripts/load_filtered_csvs.py --csv-dir data/csv/all_csv \
    --host localhost --port 5432 --user postgres --password postgres --dbname postgres
"""

import argparse
import csv
import os
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from contextlib import contextmanager

import psycopg2
import psycopg2.extras as extras
import importlib.util

# Try to import sentence-transformers for embeddings
HAS_TRANSFORMERS = False
try:
    from sentence_transformers import SentenceTransformer
    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False
    print("Warning: sentence-transformers not installed. Embeddings will not be computed.")

# --- File paths (relative to repo root) ---
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Constants for mapping licitaciones -> universidad
UAM_COD = "023"
UAM_NIF = "Q2818013A"


def to_pgvector_literal(vec):
    """Convert a list of floats to pgvector literal format."""
    return "[" + ",".join(f"{float(x):.10f}" for x in vec) + "]"


def compute_transformer_embeddings(model, texts):
    """Compute embeddings using sentence-transformers model."""
    embeddings = model.encode(texts, convert_to_numpy=False, show_progress_bar=True)
    # Normalize to list of lists
    return [list(emb) for emb in embeddings]


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
    # Ensure the client encoding is UTF8 so text sent to Postgres is stored as UTF-8
    conn.set_client_encoding('UTF8')
    conn.autocommit = False
    return conn


@contextmanager
def csv_open_reader(csv_path):
    """Context manager that yields a csv.DictReader opened with a best-effort
    UTF-8 encoding. It tries encodings in order: utf-8-sig, utf-8, latin1.
    This avoids UnicodeDecodeError when CSVs have different encodings and
    ensures text rows are returned as Python str (UTF-8 decoded).
    """
    encodings = ("utf-8-sig", "utf-8", "latin1")
    last_exc = None
    f = None
    for enc in encodings:
        try:
            f = open(csv_path, "r", encoding=enc, newline="")
            reader = csv.DictReader(f)
            yield reader
            return
        except UnicodeDecodeError as e:
            last_exc = e
            if f:
                try:
                    f.close()
                except Exception:
                    pass
            f = None
            continue
    # If all encodings failed to decode without error, fall back to latin1 to
    # at least provide a best-effort decoding (latin1 never fails).
    if f is None:
        f = open(csv_path, "r", encoding="latin1", newline="")
        reader = csv.DictReader(f)
        try:
            yield reader
        finally:
            f.close()
    else:
        # Close file if we somehow exited loop without yielding
        try:
            f.close()
        except Exception:
            pass


def discover_csv_files(csv_dir):
    """
    Discover CSV files in the given directory by pattern matching.
    Returns dict with keys: 'gastos', 'ingresos', 'convocatorias', 'ayudas', 'licitaciones'
    Each value is a list of file paths.
    """
    csv_dir_abs = os.path.abspath(csv_dir)
    if not os.path.isdir(csv_dir_abs):
        print(f"ERROR: CSV directory not found: {csv_dir_abs}", file=sys.stderr)
        sys.exit(1)

    all_files = [
        os.path.join(csv_dir_abs, f)
        for f in os.listdir(csv_dir_abs)
        if f.endswith(".csv")
    ]

    discovered = {
        "gastos": [],
        "ingresos": [],
        "convocatorias": [],
        "ayudas": [],
        "licitaciones": [],
    }

    for path in all_files:
        fname = os.path.basename(path).lower()
        if "presupuesto-de-gastos" in fname or "presupuesto_de_gastos" in fname:
            discovered["gastos"].append(path)
        elif "presupuesto-de-ingresos" in fname or "presupuesto_de_ingresos" in fname:
            discovered["ingresos"].append(path)
        elif "conv-ayudas" in fname or "conv_ayudas" in fname:
            discovered["convocatorias"].append(path)
        elif "ayudas" in fname and "conv" not in fname:
            discovered["ayudas"].append(path)
        elif "licitaciones" in fname or "contratos-mayores" in fname:
            discovered["licitaciones"].append(path)

    # Sort files for deterministic order
    for key in discovered:
        discovered[key].sort()

    return discovered


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
    descripcion_de_la_financiacion_europea TEXT,
    embedding vector(384)
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
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute(DDL_SQL)


def seed_universidad(cur):
    # Minimal seed for UAM (matches CSVs)
    cur.execute(
        "INSERT INTO UNIVERSIDAD (cod_universidad, nifoc, des_universidad, nombre_corto) VALUES (%s, %s, %s, %s)",
        (UAM_COD, UAM_NIF, "Universidad Autónoma de Madrid", "UAM"),
    )


def load_gastos(conn, csv_files):
    """Load PRESUPUESTO_GASTOS from one or more CSV files."""
    total_rows = 0
    for csv_path in csv_files:
        print(f"Loading PRESUPUESTO_GASTOS from {csv_path}")
        rows = []
        with csv_open_reader(csv_path) as reader:
            for r in reader:
                cod_univ = (r.get("cod_universidad") or "").strip().strip('"')
                # Normalize UAM code: "23" -> "023"
                if cod_univ == "23":
                    cod_univ = UAM_COD
                rows.append(
                    (
                        cod_univ,
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
        total_rows += len(rows)
        print(f"  -> Inserted {len(rows)} rows from {os.path.basename(csv_path)}")
    print(f"Total PRESUPUESTO_GASTOS: {total_rows} rows")


def load_ingresos(conn, csv_files):
    """Load PRESUPUESTO_INGRESOS from one or more CSV files."""
    total_rows = 0
    for csv_path in csv_files:
        print(f"Loading PRESUPUESTO_INGRESOS from {csv_path}")
        rows = []
        with csv_open_reader(csv_path) as reader:
            for r in reader:
                cod_univ = (r.get("cod_universidad") or "").strip().strip('"')
                # Normalize UAM code: "23" -> "023"
                if cod_univ == "23":
                    cod_univ = UAM_COD
                rows.append(
                    (
                        cod_univ,
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
        total_rows += len(rows)
        print(f"  -> Inserted {len(rows)} rows from {os.path.basename(csv_path)}")
    print(f"Total PRESUPUESTO_INGRESOS: {total_rows} rows")


def load_convocatoria(conn, csv_files):
    """Load CONVOCATORIA_AYUDA from one or more CSV files."""
    total_rows = 0
    for csv_path in csv_files:
        print(f"Loading CONVOCATORIA_AYUDA from {csv_path}")
        rows = []
        with csv_open_reader(csv_path) as reader:
            for r in reader:
                cod_univ = (r.get("cod_universidad") or "").strip().strip('"')
                # Normalize UAM code: "23" -> "023"
                if cod_univ == "23":
                    cod_univ = UAM_COD
                rows.append(
                    (
                        (r.get("cod_convocatoria") or "").strip(),
                        cod_univ,
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
        total_rows += len(rows)
        print(f"  -> Inserted {len(rows)} rows from {os.path.basename(csv_path)}")
    print(f"Total CONVOCATORIA_AYUDA: {total_rows} rows")


def load_ayuda(conn, csv_files):
    """Load AYUDA from one or more CSV files."""
    # Obtener las convocatorias válidas ya insertadas
    valid_conv = set()
    with conn.cursor() as cur:
        cur.execute("SELECT cod_convocatoria FROM CONVOCATORIA_AYUDA")
        for (code,) in cur.fetchall():
            valid_conv.add(code)

    total_kept = 0
    total_skipped_empty = 0
    total_skipped_missing_fk = 0

    for csv_path in csv_files:
        print(f"Loading AYUDA from {csv_path}")
        rows = []
        kept = 0
        skipped_empty = 0
        skipped_missing_fk = 0
        with csv_open_reader(csv_path) as reader:
            for r in reader:
                cod_univ = (r.get("cod_universidad") or "").strip().strip('"')
                # Normalize UAM code: "23" -> "023"
                if cod_univ == "23":
                    cod_univ = UAM_COD
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
        total_kept += kept
        total_skipped_empty += skipped_empty
        total_skipped_missing_fk += skipped_missing_fk
        print(
            f"  -> {os.path.basename(csv_path)}: kept {kept}, skipped empty conv {skipped_empty}, skipped missing FK {skipped_missing_fk}"
        )
    print(
        f"Total AYUDA: {total_kept} rows, skipped empty {total_skipped_empty}, skipped missing FK {total_skipped_missing_fk}"
    )


def load_licitacion(conn, csv_files):
    """Load LICITACION from one or more CSV files."""
    seen_ids = set()
    total_kept = 0
    total_skipped_dups = 0
    total_skipped_nif = 0

    # Initialize the transformer model if available
    model = None
    if HAS_TRANSFORMERS:
        try:
            print("Loading sentence-transformers model 'all-MiniLM-L6-v2' (this may take a while)...")
            model = SentenceTransformer('all-MiniLM-L6-v2')
            print("Model loaded successfully.")
        except Exception as e:
            print(f"Warning: failed to load transformer model: {e}")
            model = None

    for csv_path in csv_files:
        print(f"Loading LICITACION from {csv_path}")
        rows = []
        texts_for_embedding = []
        kept = 0
        skipped_dups = 0
        skipped_nif = 0
        with csv_open_reader(csv_path) as reader:
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
                
                # Extract text fields for embedding
                objeto = (r.get("objeto_licitacion_o_lote") or "").strip()
                descripcion = (r.get("descripcion_de_la_financiacion_europea") or "").strip()
                combined_text = (objeto + "\n" + descripcion).strip() if (objeto or descripcion) else ""
                
                rows.append(
                    (
                        to_int(ident),
                        (r.get("nif_oc") or "").strip(),
                        parse_ts(r.get("primera_publicacion")),
                        to_decimal(
                            r.get("presupuesto_base_sin_impuestos_licitacion_o_lote")
                        ),
                        to_decimal(
                            r.get(
                                "importe_adjudicacion_sin_impuestos_licitacion_o_lote"
                            )
                        ),
                        (r.get("resultado_licitacion_o_lote") or "").strip(),
                        (
                            r.get("identificador_adjudicatario_de_la_licitacion_o_lote")
                            or ""
                        ).strip(),
                        objeto,
                        (r.get("link_licitacion") or "").strip(),
                        descripcion,
                    )
                )
                texts_for_embedding.append(combined_text)
                kept += 1
        
        if rows:
            # Compute embeddings in batch if model is available
            embeddings = []
            if model is not None and texts_for_embedding:
                print(f"Computing embeddings for {len(texts_for_embedding)} LICITACION rows...")
                try:
                    embeddings = compute_transformer_embeddings(model, texts_for_embedding)
                    print(f"Embeddings computed successfully. Dimension: {len(embeddings[0])}")
                except Exception as e:
                    print(f"Warning: failed to compute embeddings: {e}")
                    embeddings = []
            
            # Prepare rows with embeddings
            rows_with_embeddings = []
            for i, row in enumerate(rows):
                if embeddings and i < len(embeddings):
                    emb_literal = to_pgvector_literal(embeddings[i])
                    rows_with_embeddings.append(row + (emb_literal,))
                else:
                    rows_with_embeddings.append(row + (None,))
            
            # Insert rows into database
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
                        descripcion_de_la_financiacion_europea,
                        embedding
                    ) VALUES %s
                    """,
                    rows_with_embeddings,
                )
            
            # Create index for efficient similarity search if embeddings were added
            if embeddings:
                print("Creating vector index for similarity search...")
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "CREATE INDEX IF NOT EXISTS licitacion_embedding_idx ON LICITACION USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);"
                        )
                    print("Vector index created successfully.")
                except Exception as e:
                    print(f"Warning: failed to create vector index: {e}")
        
        total_kept += kept
        total_skipped_nif += skipped_nif
        total_skipped_dups += skipped_dups
        print(
            f"  -> {os.path.basename(csv_path)}: kept {kept}, skipped non-UAM {skipped_nif}, skipped dups {skipped_dups}"
        )
    print(
        f"Total LICITACION: {total_kept} rows, skipped non-UAM {total_skipped_nif}, skipped dups {total_skipped_dups}"
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
    parser.add_argument(
        "--csv-dir",
        default="data/csv",
        help="Directory containing CSV files (default: data/csv)",
    )
    args = parser.parse_args()

    # Resolve CSV directory path
    csv_dir = args.csv_dir
    if not os.path.isabs(csv_dir):
        csv_dir = os.path.join(ROOT, csv_dir)

    print(f"Discovering CSV files in: {csv_dir}")
    discovered = discover_csv_files(csv_dir)

    # Print discovered files
    for category, files in discovered.items():
        if files:
            print(f"  {category}: {len(files)} file(s)")
            for f in files:
                print(f"    - {os.path.basename(f)}")
        else:
            print(f"  {category}: No files found")

    # Validate we have at least some files
    total_files = sum(len(files) for files in discovered.values())
    if total_files == 0:
        print(f"ERROR: No CSV files found in {csv_dir}", file=sys.stderr)
        sys.exit(1)

    conn = connect_db(args)
    try:
        with conn.cursor() as cur:
            create_tables(cur)
            seed_universidad(cur)

        # Load data in correct FK order
        if discovered["gastos"]:
            load_gastos(conn, discovered["gastos"])
        if discovered["ingresos"]:
            load_ingresos(conn, discovered["ingresos"])
        if discovered["convocatorias"]:
            load_convocatoria(conn, discovered["convocatorias"])
        if discovered["ayudas"]:
            load_ayuda(conn, discovered["ayudas"])
        if discovered["licitaciones"]:
            load_licitacion(conn, discovered["licitaciones"])

        conn.commit()
        print("\n✅ DONE: All data loaded successfully.")
    except Exception as e:
        conn.rollback()
        print("ERROR:", e, file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

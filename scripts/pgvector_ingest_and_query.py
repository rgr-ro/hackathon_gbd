"""Ingest CSV texts into Postgres (pgvector) and query them with natural-language queries.

Two primary modes:
  - ingest: read a CSV file, compute embeddings for a text column, and store rows in `LICITACION` table
  - query: compute an embedding for a user query and retrieve top-K similar documents using pgvector

Usage (PowerShell):
  # Ingest:
  $env:PGHOST='localhost'; $env:PGPORT='5432'; $env:PGUSER='myuser'; $env:PGPASSWORD='mypassword'; $env:PGDATABASE='mydb'
  python .\scripts\pgvector_ingest_and_query.py ingest --csv data.csv --text-col content --id-col id --mode dummy --dim 128

  # Query:
  python .\scripts\pgvector_ingest_and_query.py query --q "What is the summary of X?" --k 5 --mode dummy --dim 128

Notes:
 - The script supports a lightweight deterministic `dummy` embedding (fast) or
   `transformer` mode using `sentence-transformers` (install separately).
 - The `LICITACION` table will contain: id (serial if no id supplied), source_id (optional original id), text, embedding (vector(dim)).
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from typing import List, Optional, Tuple

try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:
    print("Error: psycopg2 is required. Install with 'pip install psycopg2-binary'", file=sys.stderr)
    raise

try:
    import numpy as np
except Exception:
    np = None

HAS_TRANSFORMERS = False
try:
    from sentence_transformers import SentenceTransformer
    HAS_TRANSFORMERS = True
except Exception:
    HAS_TRANSFORMERS = False


def dummy_embedding(text: str, dim: int = 128) -> List[float]:
    vals = [ord(c) for c in text]
    if not vals:
        vals = [0]
    vec = [float(vals[i % len(vals)]) for i in range(dim)]
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / norm for x in vec]


def transformer_embeddings(model: SentenceTransformer, texts: List[str]) -> List[List[float]]:
    arr = model.encode(texts, convert_to_numpy=False)
    # model.encode may return list or numpy array; normalize to list of lists
    return [list(a) for a in arr]


def to_pgvector_literal(vec: List[float]) -> str:
    return "[" + ",".join(f"{float(x):.10f}" for x in vec) + "]"


def connect_db():
    params = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "user": os.getenv("PGUSER", "myuser"),
        "password": os.getenv("PGPASSWORD", "mypassword"),
        "dbname": os.getenv("PGDATABASE", "mydb"),
    }
    return psycopg2.connect(**params)


def ensure_table(cur, dim: int):
    # Ensure pgvector extension exists and LICITACION has an embedding column.
    # Create a minimal LICITACION table compatible with the project's loader
    # (`scripts/load_filtered_csvs.py`) if it doesn't exist.
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute("SELECT to_regclass('public.LICITACION')")
    exists = cur.fetchone()[0]
    if not exists:
        cur.execute(
            f"""
            CREATE TABLE LICITACION (
                identificador BIGINT PRIMARY KEY,
                nif_oc VARCHAR(15),
                primera_publicacion TIMESTAMP,
                presupuesto_base_sin_impuestos_licitacion_o_lote DECIMAL(19,2),
                importe_adjudicacion_sin_impuestos_licitacion_o_lote DECIMAL(19,2),
                resultado_licitacion_o_lote VARCHAR(100),
                identificador_adjudicatario_de_la_licitacion_o_lote VARCHAR(255),
                objeto_licitacion_o_lote TEXT,
                link_licitacion TEXT,
                descripcion_de_la_financiacion_europea TEXT,
                embedding vector({dim})
            );
            """
        )
        return

    # If table exists, ensure embedding column exists and has the expected dim
    cur.execute(
        "SELECT attname, atttypmod FROM pg_attribute WHERE attrelid = 'public.LICITACION'::regclass AND attname = 'embedding';"
    )
    info = cur.fetchone()
    if not info:
        cur.execute(f"ALTER TABLE LICITACION ADD COLUMN embedding vector({dim});")
        return

    _, atttypmod = info
    try:
        existing_dim = atttypmod - 4 if atttypmod is not None else None
    except Exception:
        existing_dim = None
    if existing_dim and existing_dim != dim:
        print(
            f"Warning: existing LICITACION.embedding dim={existing_dim} differs from required dim={dim}. Replacing column (will DROP existing embedding column)."
        )
        cur.execute("ALTER TABLE LICITACION DROP COLUMN embedding;")
        cur.execute(f"ALTER TABLE LICITACION ADD COLUMN embedding vector({dim});")


def ingest_csv(csv_path: str, text_col: Optional[str], id_col: Optional[str], lote_col: Optional[str], mode: str, dim: int, **kwargs):
    # read CSV and collect texts
    texts_for_batch: List[str] = []
    source_ids: List[Optional[str]] = []
    lote_vals: List[Optional[str]] = []

    with open(csv_path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames or text_col not in reader.fieldnames:
            # If user didn't supply a text_col, require the two LICITACION text
            # columns to exist and combine them.
            if not text_col:
                needed = ['objeto_licitacion_o_lote', 'descripcion_de_la_financiacion_europea']
                for n in needed:
                    if n not in reader.fieldnames:
                        raise ValueError(f"Required column '{n}' not found in CSV; available: {reader.fieldnames}")
            else:
                raise ValueError(f"text column '{text_col}' not found in CSV; available: {reader.fieldnames}")
        for row in reader:
            # Build text: either a single column (text_col) or the combination of
            # objeto_licitacion_o_lote + descripcion_de_la_financiacion_europea
            if text_col:
                txt = row.get(text_col, "")
            else:
                a = (row.get('objeto_licitacion_o_lote') or "").strip()
                b = (row.get('descripcion_de_la_financiacion_europea') or "").strip()
                txt = (a + "\n" + b).strip() if (a or b) else ""
            sid = row[id_col] if id_col and id_col in row else None
            texts_for_batch.append(txt)
            source_ids.append(sid)

    if not texts_for_batch:
        print("No rows found in CSV; nothing to ingest.")
        return

    # compute embeddings first so we know the correct dimension
    if mode == 'transformer':
        if not HAS_TRANSFORMERS:
            print("sentence-transformers not installed. Install with 'pip install sentence-transformers' or use --mode dummy", file=sys.stderr)
            sys.exit(1)
        model_name = kwargs.get('model_name') if kwargs and 'model_name' in kwargs else 'paraphrase-multilingual-MiniLM-L12-v2'
        model = SentenceTransformer(model_name)
        embeddings = transformer_embeddings(model, texts_for_batch)
        actual_dim = len(embeddings[0])
    else:
        embeddings = [dummy_embedding(t, dim) for t in texts_for_batch]
        actual_dim = len(embeddings[0])

    # connect to db and ensure table uses actual_dim
    conn = connect_db()
    conn.autocommit = True
    cur = conn.cursor()
    # ensure LICITACION has embedding column with correct dim
    ensure_table(cur, actual_dim)

    # prepare tuples for bulk insert into LICITACION.
    # We'll insert: identificador, objeto_licitacion_o_lote,
    # descripcion_de_la_financiacion_europea, embedding
    def clean_ident(x):
        if x is None or x == '':
            return None
        try:
            return int(x)
        except Exception:
            return None

    tuples = []
    for sid, txt, emb in zip(source_ids, texts_for_batch, embeddings):
        ident = clean_ident(sid)
        # split combined text back into objeto and descripcion if possible
        if "\n" in txt:
            objeto, descripcion = txt.split("\n", 1)
            objeto = objeto.strip()
            descripcion = descripcion.strip()
        else:
            objeto = txt
            descripcion = None
        tuples.append((ident, objeto, descripcion, to_pgvector_literal(emb)))

    print(f"Inserting {len(tuples)} documents into DB...")
    # use execute_values for fast bulk insert; pass the literal vector string as text (psycopg2 will quote)
    execute_values(
        cur,
        "INSERT INTO LICITACION (identificador, objeto_licitacion_o_lote, descripcion_de_la_financiacion_europea, embedding) VALUES %s;",
        tuples,
    )

    # try create an index
    try:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS licitacion_embedding_idx ON LICITACION USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);"
        )
    except Exception as e:
        print("Warning: index creation failed:", e)

    cur.close()
    conn.close()
    print("Ingest complete.")


def query_documents(query: str, mode: str, dim: int, k: int = 5, model_name: Optional[str] = None):
    # compute query embedding
    if mode == 'transformer':
        if not HAS_TRANSFORMERS:
            print("sentence-transformers not installed. Install with 'pip install sentence-transformers' or use --mode dummy", file=sys.stderr)
            sys.exit(1)
        model_name = model_name or 'paraphrase-multilingual-MiniLM-L12-v2'
        model = SentenceTransformer(model_name)
        qemb = transformer_embeddings(model, [query])[0]
    else:
        qemb = dummy_embedding(query, dim)

    qlit = to_pgvector_literal(qemb)

    conn = connect_db()
    cur = conn.cursor()

    # Run similarity query using pgvector <-> operator against LICITACION
    cur.execute(
        "SELECT identificador, nif_oc, objeto_licitacion_o_lote, descripcion_de_la_financiacion_europea, embedding <-> %s AS distance FROM LICITACION ORDER BY distance LIMIT %s;",
        (qlit, k),
    )
    rows = cur.fetchall()

    print(f"Top {k} LICITACION rows for query: {query!r}")
    for identificador, nif_oc, objeto, descripcion, dist in rows:
        text_preview = objeto or ""
        if descripcion:
            text_preview += "\n" + descripcion
        print(f"identificador={identificador} nif_oc={nif_oc} distance={dist:.6f}\n{text_preview}\n---")

    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Ingest CSV and query documents using pgvector')
    sub = parser.add_subparsers(dest='cmd')

    p_ingest = sub.add_parser('ingest')
    p_ingest.add_argument('--csv', required=True, help='Path to CSV file')
    p_ingest.add_argument('--text-col', required=True, help='Column name containing text')
    p_ingest.add_argument('--id-col', required=False, help='Optional column to use as source_id (maps to LICITACION.identificador)')
    p_ingest.add_argument('--lote-col', required=False, help='Optional column to use as lote (maps to LICITACION.lote)')
    p_ingest.add_argument('--mode', choices=['dummy', 'transformer'], default='dummy')
    p_ingest.add_argument('--dim', type=int, default=128, help='Embedding dim (dummy mode)')
    p_ingest.add_argument('--model', dest='model_name', help='SentenceTransformer model name to use when --mode transformer (default: paraphrase-multilingual-MiniLM-L12-v2)')

    p_query = sub.add_parser('query')
    p_query.add_argument('--q', required=True, help='Query text')
    p_query.add_argument('--k', type=int, default=5, help='Number of results')
    p_query.add_argument('--mode', choices=['dummy', 'transformer'], default='dummy')
    p_query.add_argument('--dim', type=int, default=128, help='Embedding dim (dummy mode)')
    p_query.add_argument('--model', dest='model_name', help='SentenceTransformer model name to use when --mode transformer (default: paraphrase-multilingual-MiniLM-L12-v2)')

    args = parser.parse_args()
    if args.cmd == 'ingest':
        ingest_csv(args.csv, args.text_col, args.id_col, getattr(args, 'lote_col', None), args.mode, args.dim)
    elif args.cmd == 'query':
        query_documents(args.q, args.mode, args.dim, args.k, getattr(args, 'model_name', None))
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

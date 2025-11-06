"""Ingest CSV texts into Postgres (pgvector) and query them with natural-language queries.

Two primary modes:
  - ingest: read a CSV file, compute embeddings for a text column, and store rows in `documents` table
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
 - The `documents` table will contain: id (serial if no id supplied), source_id (optional original id), text, embedding (vector(dim)).
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
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS documents (
        id serial PRIMARY KEY,
        source_id text,
        text text,
        embedding vector({dim})
    );
    """)


def ingest_csv(csv_path: str, text_col: str, id_col: Optional[str], mode: str, dim: int, **kwargs):
    # read CSV and collect texts
    texts_for_batch: List[str] = []
    source_ids: List[Optional[str]] = []

    with open(csv_path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames or text_col not in reader.fieldnames:
            raise ValueError(f"text column '{text_col}' not found in CSV; available: {reader.fieldnames}")
        for row in reader:
            txt = row[text_col]
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

    # ensure extension and table exist; if table exists check embedding dim
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    # create table if not exists
    cur.execute("SELECT to_regclass('public.documents')")
    exists = cur.fetchone()[0]
    if not exists:
        cur.execute(f"CREATE TABLE documents (id serial PRIMARY KEY, source_id text, text text, embedding vector({actual_dim}));")
    else:
        # table exists: check if embedding column exists and matches dimension
        cur.execute("SELECT attname, atttypmod FROM pg_attribute WHERE attrelid = 'public.documents'::regclass AND attname = 'embedding';")
        info = cur.fetchone()
        if not info:
            # add embedding column
            cur.execute(f"ALTER TABLE documents ADD COLUMN embedding vector({actual_dim});")
        else:
            _, atttypmod = info
            try:
                existing_dim = atttypmod - 4 if atttypmod is not None else None
            except Exception:
                existing_dim = None
            if existing_dim and existing_dim != actual_dim:
                print(f"Warning: existing embedding dim={existing_dim} differs from actual dim={actual_dim}. Replacing column (will DROP existing embedding column).")
                cur.execute("ALTER TABLE documents DROP COLUMN embedding;")
                cur.execute(f"ALTER TABLE documents ADD COLUMN embedding vector({actual_dim});")

    # prepare tuples for bulk insert
    tuples = [(sid, txt, to_pgvector_literal(emb)) for sid, txt, emb in zip(source_ids, texts_for_batch, embeddings)]

    print(f"Inserting {len(tuples)} documents into DB...")
    # use execute_values for fast bulk insert; pass the literal vector string as text (psycopg2 will quote)
    execute_values(cur,
                   "INSERT INTO documents (source_id, text, embedding) VALUES %s;",
                   tuples)

    # try create an index
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS documents_embedding_idx ON documents USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);")
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

    # Run similarity query using pgvector <-> operator
    cur.execute("SELECT id, source_id, text, embedding <-> %s AS distance FROM documents ORDER BY distance LIMIT %s;", (qlit, k))
    rows = cur.fetchall()

    print(f"Top {k} documents for query: {query!r}")
    for rid, sid, text, dist in rows:
        print(f"id={rid} source_id={sid} distance={dist:.6f}\n{text}\n---")

    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Ingest CSV and query documents using pgvector')
    sub = parser.add_subparsers(dest='cmd')

    p_ingest = sub.add_parser('ingest')
    p_ingest.add_argument('--csv', required=True, help='Path to CSV file')
    p_ingest.add_argument('--text-col', required=True, help='Column name containing text')
    p_ingest.add_argument('--id-col', required=False, help='Optional column to use as source_id')
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
        ingest_csv(args.csv, args.text_col, args.id_col, args.mode, args.dim)
    elif args.cmd == 'query':
        query_documents(args.q, args.mode, args.dim, args.k)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

"""pgvector_text_distance.py

Simple script to test storing text embeddings in Postgres (pgvector) and
calculating distances between two texts using the pgvector <-> operator.

Usage (PowerShell):
    $env:PGHOST='localhost'; $env:PGPORT='5432'; $env:PGUSER='myuser'; $env:PGPASSWORD='mypassword'; $env:PGDATABASE='mydb'
    python .\scripts\pgvector_text_distance.py "Hello world" "Hello" --mode dummy

Options:
  --mode dummy       Use a simple deterministic character-based embedding (no heavy deps)
  --mode transformer Use sentence-transformers (if installed) to compute embeddings

The script will:
 - create the pgvector extension (if needed)
 - create a small `items` table with a vector column
 - insert two texts with embeddings
 - run a KNN query using the <-> operator to get distances from the query vector
 - print the DB distance and the local (Python) Euclidean distance for verification

This is intentionally small and dependency-light; the dummy embedding allows you to
test pgvector behavior without external models.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from typing import List

try:
    import psycopg2
except Exception as e:
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
    """Deterministic, lightweight embedding: use normalized character ordinals.

    This is not semantic — it's intended to test vector storage and distance ops.
    """
    vals = [ord(c) for c in text]
    if not vals:
        vals = [0]
    # repeat/truncate to dim
    vec = [float(vals[i % len(vals)]) for i in range(dim)]
    # normalize to unit length so distances are well-behaved
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / norm for x in vec]


def transformer_embedding(model: SentenceTransformer, texts: List[str]) -> List[List[float]]:
    return model.encode(texts, convert_to_numpy=False).tolist()


def to_pgvector_literal(vec: List[float]) -> str:
    # Postgres pgvector text literal format: '[0.1,0.2,...]'
    return "[" + ",".join(f"{float(x):.10f}" for x in vec) + "]"


def euclidean(a: List[float], b: List[float]) -> float:
    if np is not None:
        return float(np.linalg.norm(np.array(a) - np.array(b)))
    # fallback
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def main():
    p = argparse.ArgumentParser(description="Test pgvector distance between two texts")
    p.add_argument("text1", help="First text")
    p.add_argument("text2", help="Second text")
    p.add_argument("--mode", choices=["dummy", "transformer"], default="dummy", help="Embedding mode")
    p.add_argument("--dim", type=int, default=128, help="Embedding dimension (dummy mode)")
    args = p.parse_args()

    # DB connection settings from env with sensible defaults matching the repo compose
    db_params = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "user": os.getenv("PGUSER", "myuser"),
        "password": os.getenv("PGPASSWORD", "mypassword"),
        "dbname": os.getenv("PGDATABASE", "mydb"),
    }

    print("Using DB:", db_params)

    # compute embeddings
    if args.mode == "transformer":
        if not HAS_TRANSFORMERS:
            print("sentence-transformers not installed. Install with 'pip install sentence-transformers' or use --mode dummy", file=sys.stderr)
            sys.exit(1)
        print("Loading sentence-transformers model (this may take a while)...")
        model = SentenceTransformer("all-MiniLM-L6-v2")
        e1, e2 = transformer_embedding(model, [args.text1, args.text2])
        dim = len(e1)
    else:
        dim = args.dim
        e1 = dummy_embedding(args.text1, dim)
        e2 = dummy_embedding(args.text2, dim)

    print(f"Embedding dim = {dim}")

    # connect to db
    conn = psycopg2.connect(**db_params)
    conn.autocommit = True
    cur = conn.cursor()

    # create extension and table
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute(f"CREATE TABLE IF NOT EXISTS items (id serial PRIMARY KEY, text text, embedding vector({dim}));")

    # Clear previous items to keep run idempotent for tests
    cur.execute("TRUNCATE TABLE items;")

    # insert embeddings
    literal1 = to_pgvector_literal(e1)
    literal2 = to_pgvector_literal(e2)

    print("Inserting embeddings into DB...")
    cur.execute("INSERT INTO items (text, embedding) VALUES (%s, %s) RETURNING id;", (args.text1, literal1))
    id1 = cur.fetchone()[0]
    cur.execute("INSERT INTO items (text, embedding) VALUES (%s, %s) RETURNING id;", (args.text2, literal2))
    id2 = cur.fetchone()[0]

    # Create ivfflat index for speed (only useful for larger datasets). Safe to run repeatedly.
    try:
        cur.execute(f"CREATE INDEX IF NOT EXISTS items_embedding_idx ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);")
    except Exception as e:
        # index creation might require higher privileges or might fail if pgvector version differs; continue anyway
        print("Warning: failed to create ivfflat index:", e)

    # Query: get distances from a query vector (we'll use text1's vector as query)
    print("Querying distances from text1 vector (DB-side <-> operator)")
    cur.execute("SELECT id, text, embedding <-> %s AS distance FROM items ORDER BY distance;", (literal1,))
    rows = cur.fetchall()
    for row in rows:
        rid, text, dist = row
        print(f"DB -> id={rid} text={text!r} distance={dist}")

    # compute local Python euclidean distances for verification
    print("Local Python distances (euclidean):")
    for rid, txt, _ in rows:
        vec = e1 if rid == id1 else e2
        # distance between e1 and vec
        d = euclidean(e1, vec)
        print(f"Local -> id={rid} text={txt!r} distance={d}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

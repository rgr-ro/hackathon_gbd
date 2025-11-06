# pgvector CSV ingest & multilingual query helper

This small helper script ingests a CSV of texts into Postgres (with the pgvector extension) and
provides a simple query mode to retrieve top-K similar documents for a natural-language query.

Features
- Ingest mode: bulk insert CSV texts into a `documents` table with a `vector(dim)` column.
- Query mode: compute an embedding for a user query and return the most similar documents using pgvector `<->`.
- Supports multilingual semantic embeddings via `sentence-transformers` (default model is `paraphrase-multilingual-MiniLM-L12-v2`).
- Lightweight `dummy` embeddings for quick tests without heavy dependencies.

Files
- `scripts/pgvector_ingest_and_query.py` — the script (ingest/query)
- `data/demo_texts.csv` — small demo CSV with English and Spanish rows

Quick start (PowerShell)

1) Start the docker stack with pgvector support (see repository README):

```powershell
docker compose up -d --build
```

2) Install Python dependencies (optional transformer):

```powershell
# minimal
python -m pip install -r .\requirements.txt
# for semantic multilingual embeddings (optional)
python -m pip install sentence-transformers
```

3) Ingest the demo CSV:

```powershell
$env:PGHOST='localhost'; $env:PGPORT='5432'; $env:PGUSER='myuser'; $env:PGPASSWORD='mypassword'; $env:PGDATABASE='mydb'
python .\scripts\pgvector_ingest_and_query.py ingest --csv .\data\demo_texts.csv --text-col content --id-col id --mode transformer --model paraphrase-multilingual-MiniLM-L12-v2
```

4) Query in English or Spanish (example):

```powershell
# English query
python .\scripts\pgvector_ingest_and_query.py query --q "How do I connect to pgAdmin?" --k 5 --mode transformer --model paraphrase-multilingual-MiniLM-L12-v2

# Spanish query
python .\scripts\pgvector_ingest_and_query.py query --q "¿Cómo ver mis bases de datos en pgAdmin?" --k 5 --mode transformer --model paraphrase-multilingual-MiniLM-L12-v2
```

Notes
- The multilingual transformer model handles multiple languages and will return semantically similar documents across languages.
- If you don't want to install heavy ML packages, use `--mode dummy` for quick tests (not semantic).
- The script attempts to create an ivfflat index for faster search; for production you should tune the index parameters and consider using HNSW or other index types supported by pgvector.

If you'd like, I can add a small wrapper that concatenates top-K documents into a prompt for an LLM to answer questions using the retrieved context.
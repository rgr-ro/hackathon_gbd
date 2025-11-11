import os
import math
import unittest
import importlib.util


def load_module():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    script_path = os.path.join(repo_root, 'scripts', 'pgvector_ingest_and_query.py')
    spec = importlib.util.spec_from_file_location('pgvector_ingest_and_query', script_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestPgVectorUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_module()

    def test_dummy_embedding_length_and_norm(self):
        vec = self.mod.dummy_embedding('abc', dim=8)
        self.assertEqual(len(vec), 8)
        norm = math.sqrt(sum(x * x for x in vec))
        # dummy_embedding normalizes the vector
        self.assertAlmostEqual(norm, 1.0, places=6)

    def test_dummy_embedding_empty(self):
        vec = self.mod.dummy_embedding('', dim=4)
        self.assertEqual(len(vec), 4)
        self.assertTrue(all(isinstance(x, float) for x in vec))

    def test_to_pgvector_literal_format(self):
        v = [0.1, 0.2]
        lit = self.mod.to_pgvector_literal(v)
        # expect 10 decimal places formatting per implementation
        self.assertEqual(lit, "[0.1000000000,0.2000000000]")

    def test_end_to_end_ingest_and_query(self):
        """Small end-to-end test that ingests two rows and queries them.

        This test monkeypatches DB access (connect_db, execute_values and
        ensure_table) to use an in-memory fake table so it doesn't require a
        real Postgres instance.
        """
        mod = self.mod
        # in-memory storage for LICITACION rows
        fake_db = {"rows": []}

        # Stub for execute_values: capture inserted tuples into fake_db
        def fake_execute_values(cur, sql, tuples):
            # tuples is an iterable of tuples as inserted by ingest_csv
            for t in tuples:
                # expected t -> (ident, objeto, descripcion, embedding_literal)
                ident, objeto, descripcion, emb_lit = t
                fake_db["rows"].append({
                    "ident": ident,
                    "nif_oc": None,
                    "objeto": objeto,
                    "descripcion": descripcion,
                    "emb_lit": emb_lit,
                })

        # Fake cursor/connection used for query: compute distances against stored rows
        class FakeCursor:
            def __init__(self):
                self._lastrows = None

            def execute(self, sql, params=None):
                # Intercept the similarity SELECT and compute distances
                if sql.strip().upper().startswith("SELECT IDENTIFICADOR"):
                    qlit = params[0]
                    k = params[1]
                    # parse qlit like "[0.1,0.2]"
                    qvec = [float(x) for x in qlit.strip("[]").split(",") if x != ""]
                    rows = []
                    for r in fake_db["rows"]:
                        # parse stored embedding literal
                        s = r["emb_lit"].strip("[]")
                        if s == "":
                            emb = []
                        else:
                            emb = [float(x) for x in s.split(",")]
                        # compute L2 distance
                        dist = 0.0
                        for a, b in zip(qvec, emb):
                            dist += (a - b) ** 2
                        dist = dist ** 0.5
                        rows.append((r["ident"], r["nif_oc"], r["objeto"], r["descripcion"], dist))
                    # sort by distance
                    rows.sort(key=lambda x: x[4])
                    self._lastrows = rows[:k]
                else:
                    # no-op for other SQL
                    self._lastrows = None

            def fetchone(self):
                # used by some code paths if needed; return a truthy value
                return (None,)

            def fetchall(self):
                return self._lastrows or []

            def close(self):
                pass

        class FakeConn:
            def cursor(self):
                return FakeCursor()

            def close(self):
                pass

        # Patch module functions
        orig_execute_values = mod.execute_values
        orig_connect_db = mod.connect_db
        orig_ensure_table = mod.ensure_table
        try:
            mod.execute_values = fake_execute_values
            mod.connect_db = lambda: FakeConn()
            mod.ensure_table = lambda cur, dim: None

            # Create a temp CSV file with two rows
            import tempfile, io

            header = [
                "identificador",
                "nif_oc",
                "objeto_licitacion_o_lote",
                "descripcion_de_la_financiacion_europea",
            ]
            rows = [
                ["1", "Q2818013A", "contrato limpieza", "financiado por X"],
                ["2", "Q2818013A", "servicios comida", "sin financiación"],
            ]
            tf = tempfile.NamedTemporaryFile("w+", delete=False, newline="", encoding="utf-8")
            try:
                import csv as _csv

                writer = _csv.writer(tf)
                writer.writerow(header)
                for r in rows:
                    writer.writerow(r)
                tf.flush()
                tf.close()

                # Run ingest (use combined fields by passing text_col=None)
                mod.ingest_csv(tf.name, None, "identificador", None, mode="dummy", dim=8)

                # Ensure rows inserted into fake_db
                self.assertEqual(len(fake_db["rows"]), 2)

                # Capture printed query output
                import io, sys, contextlib

                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    mod.query_documents("contrato", mode="dummy", dim=8, k=2)
                out = buf.getvalue()
                # Should contain the identificador of the closest row (1 or 2)
                self.assertIn("identificador=", out)
            finally:
                try:
                    os.unlink(tf.name)
                except Exception:
                    pass
        finally:
            mod.execute_values = orig_execute_values
            mod.connect_db = orig_connect_db
            mod.ensure_table = orig_ensure_table


if __name__ == '__main__':
    unittest.main()

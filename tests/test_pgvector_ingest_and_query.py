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


if __name__ == '__main__':
    unittest.main()

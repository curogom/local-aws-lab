"""Fast tests for advanced scenario mechanics, without Docker or network."""
import tempfile
import unittest
from pathlib import Path
from lab import apply, ledger
from scenario_support import percentile


class MetricTest(unittest.TestCase):
    def test_nearest_rank(self):
        self.assertEqual(percentile(list(range(1, 101)), 50), 50)
        self.assertEqual(percentile(list(range(1, 101)), 95), 95)
        self.assertEqual(percentile([9], 99), 9)
        self.assertIsNone(percentile([], 95))

    def test_effect_survives_new_connection(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'ledger.sqlite'
            first = ledger(path)
            self.assertTrue(apply(first, {'id': 'x', 'text': 'synthetic'}))
            first.close()
            second = ledger(path)
            self.assertFalse(apply(second, {'id': 'x', 'text': 'synthetic'}))
            self.assertEqual(second.execute('SELECT result FROM results').fetchone()[0], 'SYNTHETIC')
            second.close()


if __name__ == '__main__':
    unittest.main()

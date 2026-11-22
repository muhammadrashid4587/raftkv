"""Smoke test for scripts/bench.py: it runs end to end against a real
3-node cluster and exits 0 with a verification line.

Kept small (10 writes, 2 threads) so the suite stays fast; the script
itself is exercised with its default sizes by hand, not here.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class TestBenchScript(unittest.TestCase):
    def test_bench_runs_and_verifies(self):
        with tempfile.TemporaryDirectory(prefix="raftkv-bench-test-") as d:
            proc = subprocess.run(
                [sys.executable, "scripts/bench.py",
                 "--writes", "10", "--threads", "2",
                 "--port-base", "5490",
                 "--data-dir", str(Path(d) / "cluster")],
                cwd=ROOT, capture_output=True, text=True, timeout=120,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            for phase in ("sequential writes", "concurrent writes x2",
                          "sequential reads", "concurrent reads x2"):
                self.assertIn(phase, proc.stdout)
            self.assertIn("verified 10 keys read back correctly",
                          proc.stdout)


if __name__ == "__main__":
    unittest.main()

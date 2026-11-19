import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "sync_snapshots.sh"


class SyncScriptTests(unittest.TestCase):
    def make_project(self, root: Path) -> Path:
        script = root / "sync_snapshots.sh"
        shutil.copy2(SCRIPT, script)
        bin_dir = root / ".venv" / "bin"
        bin_dir.mkdir(parents=True)
        python = bin_dir / "python"
        python.write_text(
            '#!/bin/sh\nif [ "$1" = "-m" ]; then printf \'%s\\n\' "$*"; fi\n',
            encoding="utf-8",
        )
        python.chmod(0o755)
        (root / "config.toml").write_text("", encoding="utf-8")
        return script

    def run_script(self, script: Path, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env.update({"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"})
        return subprocess.run(
            ["bash", str(script), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
            check=False,
        )

    def test_node_fills_worker_runs_four_cli_steps(self):
        with tempfile.TemporaryDirectory() as raw_root:
            script = self.make_project(Path(raw_root))
            result = self.run_script(
                script,
                "--worker",
                "2026-11-01",
                "2026-11-01",
                "node-fills",
                "test.log",
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count("-m hyperliquid_etl.cli node-fills"), 4)
        self.assertNotIn("--dataset", result.stdout)

    def test_mixed_worker_separates_snapshots_and_node_fills(self):
        with tempfile.TemporaryDirectory() as raw_root:
            script = self.make_project(Path(raw_root))
            result = self.run_script(
                script,
                "--worker",
                "2026-11-01",
                "2026-11-01",
                "perp,node-fills,spot",
                "test.log",
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count("--dataset perp,spot"), 4)
        self.assertEqual(result.stdout.count("-m hyperliquid_etl.cli node-fills"), 4)

    def test_unknown_dataset_is_rejected_before_start(self):
        with tempfile.TemporaryDirectory() as raw_root:
            script = self.make_project(Path(raw_root))
            result = self.run_script(
                script,
                "start",
                "2026-11-01",
                "2026-11-01",
                "fills",
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn("Unknown dataset: fills", result.stderr)


if __name__ == "__main__":
    unittest.main()

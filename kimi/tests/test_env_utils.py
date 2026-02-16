import os
import tempfile
import unittest
from pathlib import Path

import sys

CURRENT_DIR = Path(__file__).resolve().parent
KIMI_DIR = CURRENT_DIR.parent
if str(KIMI_DIR) not in sys.path:
    sys.path.insert(0, str(KIMI_DIR))

from env_utils import env_int, load_env_file


class EnvUtilsTests(unittest.TestCase):
    def test_load_env_file_parses_and_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text(
                """
# comentario
MYSQL_HOST=127.0.0.1
MYSQL_PORT='3307'
MYSQL_USER = admin
INVALID_LINE
""".strip(),
                encoding="utf-8",
            )

            os.environ["MYSQL_HOST"] = "localhost"
            load_env_file(env_path, override=True)

            self.assertEqual(os.getenv("MYSQL_HOST"), "127.0.0.1")
            self.assertEqual(os.getenv("MYSQL_PORT"), "3307")
            self.assertEqual(os.getenv("MYSQL_USER"), "admin")

    def test_env_int_fallback_when_invalid(self):
        os.environ["MAX_RETRIES"] = "not-a-number"
        self.assertEqual(env_int("MAX_RETRIES", 3), 3)


if __name__ == "__main__":
    unittest.main()

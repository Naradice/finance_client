"""Load SBI settings from the checkout's .env without depending on the working directory."""

from pathlib import Path

from dotenv import load_dotenv


def load_sbi_env(env_file=None):
    if env_file is None:
        env_file = next(
            (parent / ".env" for parent in Path(__file__).resolve().parents if (parent / "pyproject.toml").is_file()),
            None,
        )
    if env_file is None:
        return False
    return load_dotenv(env_file, override=False, encoding="utf-8-sig", interpolate=False)

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv_if_present(path: str | None = None) -> None:
    """
    Lightweight .env loader used for local development.

    - Reads KEY=VALUE pairs from the given file (default: ".env" in CWD).
    - Ignores empty lines and comments starting with "#".
    - Does *not* overwrite variables that are already present in os.environ.

    In ambientes como AWS Lambda, normalmente as variA!veis de ambiente
    sA# configuradas diretamente no console, entA#o o arquivo .env nA#o
    precisa existir e serA! simplesmente ignorado.
    """
    env_path = Path(path or ".env")
    if not env_path.exists():
        return

    try:
        text = env_path.read_text(encoding="utf-8")
    except OSError:
        return

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        # NA#o sobrescreve valores jA! definidos no ambiente.
        if key not in os.environ:
            os.environ[key] = value


__all__ = ["load_dotenv_if_present"]


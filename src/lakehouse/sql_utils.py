from __future__ import annotations

from pathlib import Path
from string import Template

from .config import SQL_DIR


def render(relative: str, **params) -> str:
    """Load a SQL file and substitute $placeholders. Paths are passed as POSIX
    strings so the same SQL works on Windows, macOS and Linux."""
    text = (SQL_DIR / relative).read_text(encoding="utf-8")
    clean = {k: (v.as_posix() if isinstance(v, Path) else v) for k, v in params.items()}
    return Template(text).substitute(clean)


def copy_to_parquet(con, query: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO '{dest.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")

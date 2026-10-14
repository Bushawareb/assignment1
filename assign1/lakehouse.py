"""Shared helpers for the MovieLens 25M DuckLake pipeline.

Every notebook imports from here so paths and the lakehouse connection are
defined once. All paths are RELATIVE to the repository root, which is also the
working directory Jupyter uses for notebooks stored at the root.
"""
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

# ---------------------------------------------------------------- paths
RAW_DIR = Path("data/raw")            # unzipped Kaggle download goes here
LANDING_DIR = Path("data/landing")    # simulated source drops (batch files)
LAKE_DIR = Path("lakehouse")          # DuckLake catalog + Parquet data files
CATALOG_FILE = LAKE_DIR / "catalog.ducklake"
DATA_PATH = LAKE_DIR / "data"
TMP_DIR = LAKE_DIR / "tmp"            # DuckDB spill space for big sorts/joins
FIG_DIR = Path("figures")
DOCS_DIR = Path("docs")


def sql_path(p: Path) -> str:
    """Quote a path as a SQL string literal (forward slashes, quotes escaped)."""
    return "'" + Path(p).as_posix().replace("'", "''") + "'"


def connect() -> duckdb.DuckDBPyConnection:
    """Open DuckDB, attach the DuckLake lakehouse and make sure the
    bronze / silver / gold schemas exist.

    Only one process can hold the DuckLake catalog at a time, so every
    notebook calls con.close() in its last cell.
    """
    for d in (LAKE_DIR, DATA_PATH, TMP_DIR):
        d.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute(f"SET temp_directory = {sql_path(TMP_DIR)}")
    # 25M-row tables: let DuckDB stream without holding insertion order.
    con.execute("SET preserve_insertion_order = false")
    con.execute(
        f"ATTACH 'ducklake:{CATALOG_FILE.as_posix()}' AS lake "
        f"(DATA_PATH '{DATA_PATH.as_posix()}/')"
    )
    con.execute("USE lake")
    for schema in ("bronze", "silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    return con


def find_raw_file(name: str) -> Path:
    """Locate a source CSV anywhere under data/raw (the Kaggle zip may unpack
    into data/raw/ml-25m/ or straight into data/raw/)."""
    matches = sorted(RAW_DIR.rglob(name))
    if not matches:
        raise FileNotFoundError(
            f"{name} not found under {RAW_DIR}/. Download the dataset and unzip "
            f"it into {RAW_DIR}/ (see README.md, 'Setup')."
        )
    return matches[0]


def rebuild_table(con, table: str, select_sql: str) -> int:
    """Drop and recreate a lakehouse table from a SELECT, atomically.
    Returns the new row count. Silver and Gold are rebuilt deterministically
    from the layer below, so rerunning a notebook never duplicates data."""
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS {select_sql}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------- chart style
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e4e3df"
SURFACE = "#fcfcfb"
BLUE = "#2a78d6"     # series 1
ORANGE = "#eb6834"   # series 2


def apply_chart_style() -> None:
    """Thin marks, recessive grid/axes, text in ink colours (not series colours)."""
    plt.rcParams.update({
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "axes.edgecolor": GRID,
        "axes.labelcolor": INK_SECONDARY,
        "axes.titlecolor": INK,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "xtick.color": INK_SECONDARY,
        "ytick.color": INK_SECONDARY,
        "legend.frameon": False,
        "font.size": 10,
        "lines.linewidth": 2,
        "figure.dpi": 110,
        "savefig.bbox": "tight",
    })

from __future__ import annotations

from pathlib import Path

# Project roots
PROJECT_ROOT = Path(__file__).resolve().parent
RUNTIME_ROOT = PROJECT_ROOT / "runtime"
DEFAULT_RUN_CONFIG_PATH = PROJECT_ROOT / "run_config.json"
PASS_2_RUNTIME_ROOT = RUNTIME_ROOT / "pass_2"

# External executables
KARMALEGO_EXE = Path(r"C:\KarmaLego\KarmaLegoConsoleApp\bin\Release\net8.0\KarmaLegoConsoleApp.exe")
MEDIATOR_API_EXE = Path(r"C:\MediatorCore-main\MediatorCore\APICore\bin\Release\net8.0\API.exe")

# Mediator / SQL
PROJECT_ID = "40137"
TAKS_DEST_FOLDER = Path(r"C:\MediatorCore-main\TakEntities\1900")
INPUT_PATIENTS_TABLE = "InputPatientsData"
KNOWLEDGE_TABLE = "KnowledgeTable"

DB_CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=mls05-t\medlab_dev;"
    "DATABASE=EEG_Memo;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# IO / runtime behavior
CSV_CHUNK_SIZE = 100_000
KARMALEGO_SUCCESS_MARKERS = (
    "KarmaLego finished with result: Done",
    "KarmaLego ran successfully",
)

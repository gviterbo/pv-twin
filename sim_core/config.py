CSV_PATH = "./data.csv"

INVERTER_COLS = [    "Inverter 1","Inverter 2","Inverter 3","Inverter 4",
    "Inverter 5","Inverter 6","Inverter 7","Inverter 8"
]
POA_COL = "POA Irradiation 1"
TCELL_COL = "PV Cell Temperature"
TMOD_COL = "PV Module Temperature 1"
DATE_COL = "Timestamp"
TIME_COL = "Time"

MODULE_NAME = "Jinko_Solar_Co___Ltd_JKM320PP_72"
MODULES_BY_INVERTER = 11340
N_INVERTERS = 8
DERATE = 0.8644

SUNNY_GHI_THRESHOLD = 400.0
DAY_GHI_THRESHOLD   = 20.0

VM_URL = "http://localhost:8428/api/v1/import/prometheus"

BACKFILL_HORIZON_DAYS = 3

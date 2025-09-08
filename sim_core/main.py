from __future__ import annotations
import threading, signal, time

import config as C
from layers.generation.file_provider import FileDataProvider
from layers.emission.victoria import DataEmitter
from pipelines.realtime_file import loop_realtime_from_file
from pipelines.backfill_file import run_backfill_from_file
from layers.alerts.alarms import (
    AlarmManager, PRLowAlarm, InverterOfflineAlarm, SunnyNoProductionAlarm,
    TemperatureDeltaAlarm, RampIrradianceAlarm
)
from layers.calibration.derate import DerateCalibrator

def main():
    provider = FileDataProvider(
        csv_path=C.CSV_PATH,
        date_col=C.DATE_COL,
        time_col=C.TIME_COL,
        inverter_cols=C.INVERTER_COLS,
        poa_col=C.POA_COL,
        tcell_col=C.TCELL_COL,
        tmod_col=C.TMOD_COL if hasattr(C, "TMOD_COL") else None,
        decimal=",", sep=","
    )
    emitter = DataEmitter(C.VM_URL)

    auto = getattr(C, "AUTO_CALIBRATE_DERATE", True)
    derate = getattr(C, "DERATE", 1.0)
    if auto:
        print(">> Iniciando calibração do derate pelos últimos 60 dias (OLS -> Huber)...")
        calib = DerateCalibrator(
            provider=provider,
            module_name=C.MODULE_NAME,
            modules_by_inverter=C.MODULES_BY_INVERTER,
            n_inverters=C.N_INVERTERS,
            day_thr=C.DAY_GHI_THRESHOLD,
            days=60,
            method="both",
            dmin=0.5, dmax=1.3
        )
        try:
            derate, met = calib.estimate()
            print(f">> DERATE estimado = {derate:.6f} | R²={met['r2']:.4f} | RMSE={met['rmse']:.3f} kW | pontos={met['n_points']}")
        except Exception as e:
            print(f">> Aviso: calibração falhou ({e}). Usando DERATE do config = {derate}")

    alert_emitter = emitter.make_alert_emitter()
    alarms = [
        PRLowAlarm(warn=0.82, crit=0.70, clear=0.86, labels={"plant": "UFV_X"}),
        InverterOfflineAlarm(n_inverters=C.N_INVERTERS, min_kw=0.05, min_poa_wm2=200.0, labels={"plant": "UFV_X"}),
        SunnyNoProductionAlarm(poa_thr=600.0, pac_kw_thr=0.05, labels={"plant": "UFV_X"}),
        TemperatureDeltaAlarm(warn_delta=8.0, crit_delta=12.0, clear_delta=6.0, labels={"plant": "UFV_X"}),
        RampIrradianceAlarm(dpoa_warn=250.0, dpoa_crit=400.0, labels={"plant": "UFV_X"}),
    ]
    alarm_manager = AlarmManager(alert_emitter, alarms)

    t_back = threading.Thread(
        target=run_backfill_from_file,
        kwargs=dict(
            provider=provider,
            emitter=emitter,
            module_name=C.MODULE_NAME,
            modules_by_inverter=C.MODULES_BY_INVERTER,
            n_inverters=C.N_INVERTERS,
            sunny_thr=C.SUNNY_GHI_THRESHOLD,
            day_thr=C.DAY_GHI_THRESHOLD,
            horizon_days=C.BACKFILL_HORIZON_DAYS,
            derate=derate,
            alarm_manager=alarm_manager,
        ),
        daemon=True
    )
    t_rt = threading.Thread(
        target=loop_realtime_from_file,
        kwargs=dict(
            provider=provider,
            emitter=emitter,
            module_name=C.MODULE_NAME,
            modules_by_inverter=C.MODULES_BY_INVERTER,
            n_inverters=C.N_INVERTERS,
            sunny_thr=C.SUNNY_GHI_THRESHOLD,
            day_thr=C.DAY_GHI_THRESHOLD,
            derate=derate,
            alarm_manager=alarm_manager,
        ),
        daemon=True
    )

    t_back.start()
    t_rt.start()

    stop = False
    def handle_sig(_sig, _frm):
        nonlocal stop
        stop = True
        print("Shutting down...")

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    while not stop:
        time.sleep(1)

if __name__ == "__main__":
    main()

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

from layers.generation.file_provider import FileDataProvider
from layers.simulation.pv_funcs import simulate, array_p0_kw
from layers.emission.victoria import DataEmitter
from layers.alerts.alarms import Observation, AlarmManager

def run_backfill_from_file(
    provider: FileDataProvider,
    emitter: DataEmitter,
    *,
    module_name: str,
    modules_by_inverter: int,
    n_inverters: int,
    sunny_thr: float,
    day_thr: float,
    horizon_days: int,
    derate: float = 1.0,
    alarm_manager: Optional[AlarmManager] = None
):
    step_s = provider.step_minutes * 60
    dt_h = step_s / 3600.0
    P0_total_kW = array_p0_kw(module_name, modules_by_inverter, n_inverters)

    daily_eac = {}
    daily_hpoa = {}

    cum_real_kwh = 0.0
    cum_ideal_kwh = 0.0

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(days=horizon_days)
    series = provider.get_series_between(start, now)

    poa_points: List[Tuple[int, float]] = []
    pr_inst_points: List[Tuple[int, float]] = []
    pv_batches = []
    flag_points = []
    temps_points = []
    acc_points = []

    for p in series:
        ts_ms = p["ts_ms"]
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        poa = p["poa_wm2"]
        tcell = p["tcell_c"]
        tmod = p.get("tmod_c", None)
        real_inverters = p["inverters_kw"]
        pac_kw_total = float(sum(real_inverters))

        ideal_per_inv_kw = simulate(module_name, poa, tcell, modules_by_inverter, derate=derate)
        ideal_total_kw = ideal_per_inv_kw * n_inverters

        day_key = dt.strftime("%Y-%m-%d")
        if poa > day_thr:
            daily_eac[day_key]  = daily_eac.get(day_key, 0.0)  + pac_kw_total * dt_h
            daily_hpoa[day_key] = daily_hpoa.get(day_key, 0.0) + (poa / 1000.0) * dt_h

        cum_real_kwh  += pac_kw_total * dt_h
        cum_ideal_kwh += ideal_total_kw * dt_h
        acc_points.append((ts_ms, cum_real_kwh, cum_ideal_kwh))

        poa_points.append((ts_ms, poa))
        pv_batches.append((ts_ms, ideal_per_inv_kw, real_inverters))
        temps_points.append((ts_ms, tmod, tcell))

        pr_inst = 0.0
        if poa > day_thr and P0_total_kW > 0:
            pr_inst = pac_kw_total / (P0_total_kW * (poa / 1000.0))
        pr_inst = max(0.0, min(1.5, pr_inst))
        pr_inst_points.append((ts_ms, pr_inst))

        sunny_flag = 1 if poa >= sunny_thr else 0
        day_flag = 1 if poa > day_thr else 0
        flag_points.append((ts_ms, sunny_flag, day_flag))

        if alarm_manager is not None:
            obs = Observation(
                ts_ms=ts_ms,
                poa_wm2=poa,
                pac_kw_total=pac_kw_total,
                ideal_total_kw=ideal_total_kw,
                pr_inst=pr_inst,
                sunny_flag=sunny_flag,
                day_flag=day_flag,
                tmod_c=tmod,
                tcell_c=tcell,
                inverter_kw=real_inverters
            )
            alarm_manager.step(obs)

    emitter.emit_poa(poa_points, source_label='source="file"')

    for ts_ms, ideal_per_inv_kw, real_inverters in pv_batches:
        emitter.emit_pv_inverters(ts_ms, ideal_per_inv_kw, real_inverters)

    for ts_ms, tmod, tcell in temps_points:
        emitter.emit_temps(ts_ms, tmod_c=tmod, tcell_c=tcell)

    for ts_ms, pr in pr_inst_points:
        emitter.emit_pr_inst(ts_ms, pr)

    for ts_ms, sunny, day in flag_points:
        emitter.emit_flags(ts_ms, sunny, day)

    for ts_ms, real_kwh, ideal_kwh in acc_points:
        emitter.emit_cumulative_energy(ts_ms, real_kwh, ideal_kwh)

    daily_items = []
    for day_key in sorted(daily_hpoa.keys()):
        H = daily_hpoa[day_key]
        E = daily_eac.get(day_key, 0.0)
        if H > 0 and P0_total_kW > 0:
            pr_daily = (E / P0_total_kW) / H
            pr_daily = max(0.0, min(1.5, pr_daily))
            noon = datetime.strptime(day_key, "%Y-%m-%d").replace(
                tzinfo=timezone.utc, hour=12, minute=0, second=0, microsecond=0
            )
            daily_items.append((int(noon.timestamp() * 1000), pr_daily))
    emitter.emit_pr_daily_bulk(daily_items)

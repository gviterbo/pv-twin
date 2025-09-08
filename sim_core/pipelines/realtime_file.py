from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional

from layers.generation.file_provider import FileDataProvider
from layers.simulation.pv_funcs import simulate, array_p0_kw
from layers.emission.victoria import DataEmitter
from layers.alerts.alarms import Observation, AlarmManager

def loop_realtime_from_file(
    provider: FileDataProvider,
    emitter: DataEmitter,
    *,
    module_name: str,
    modules_by_inverter: int,
    n_inverters: int,
    sunny_thr: float,
    day_thr: float,
    derate: float = 1.0,
    alarm_manager: Optional[AlarmManager] = None
):
    current_day = None
    eac_kwh_sum = 0.0
    hpoa_kwhm2_sum = 0.0

    cum_real_kwh = 0.0
    cum_ideal_kwh = 0.0

    P0_total_kW = array_p0_kw(module_name, modules_by_inverter, n_inverters)
    last_ts = 0
    step_s = provider.step_minutes * 60
    dt_h = step_s / 3600.0

    while True:
        point = provider.get_point_now()
        if point is None:
            _sleep_to_next_tick(step_s)
            continue

        ts_ms = point["ts_ms"]
        if ts_ms == last_ts:
            _sleep_to_next_tick(step_s)
            continue
        last_ts = ts_ms

        poa = point["poa_wm2"]
        tcell = point["tcell_c"]
        tmod = point.get("tmod_c", None)
        real_inverters = point["inverters_kw"]
        pac_kw_total = float(sum(real_inverters))

        ideal_per_inv_kw = simulate(module_name, poa, tcell, modules_by_inverter, derate=derate)
        ideal_total_kw = ideal_per_inv_kw * n_inverters

        emitter.emit_pv_inverters(ts_ms, ideal_per_inv_kw, real_inverters)
        emitter.emit_poa([(ts_ms, poa)], source_label='source="file"')
        emitter.emit_temps(ts_ms, tmod_c=tmod, tcell_c=tcell)

        pr_inst = 0.0
        if poa > day_thr and P0_total_kW > 0:
            pr_inst = pac_kw_total / (P0_total_kW * (poa / 1000.0))
        pr_inst = max(0.0, min(1.5, pr_inst))
        emitter.emit_pr_inst(ts_ms, pr_inst)

        day_flag = 1 if poa > day_thr else 0
        sunny_flag = 1 if poa >= sunny_thr else 0
        emitter.emit_flags(ts_ms, sunny_flag, day_flag)

        if poa > day_thr:
            eac_kwh_sum     += pac_kw_total * dt_h
            hpoa_kwhm2_sum  += (poa / 1000.0) * dt_h

        cum_real_kwh  += pac_kw_total * dt_h
        cum_ideal_kwh += ideal_total_kw * dt_h
        emitter.emit_cumulative_energy(ts_ms, cum_real_kwh, cum_ideal_kwh)

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

        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        day_key = dt.strftime("%Y-%m-%d")
        if current_day is None:
            current_day = day_key

        if day_key != current_day:
            if hpoa_kwhm2_sum > 0 and P0_total_kW > 0:
                pr_daily = (eac_kwh_sum / P0_total_kW) / hpoa_kwhm2_sum
                pr_daily = max(0.0, min(1.5, pr_daily))
                prev_noon = datetime.strptime(current_day, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc, hour=12, minute=0, second=0, microsecond=0
                )
                emitter.emit_pr_daily_bulk([(int(prev_noon.timestamp()*1000), pr_daily)])
            current_day = day_key
            eac_kwh_sum = 0.0
            hpoa_kwhm2_sum = 0.0

        _sleep_to_next_tick(step_s)

def _sleep_to_next_tick(step_s: int):
    import time
    now = time.time()
    next_tick = ((int(now) // step_s) + 1) * step_s
    time.sleep(max(0.0, next_tick - now))

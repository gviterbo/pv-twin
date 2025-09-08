from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Literal, Dict
from datetime import datetime, timedelta, timezone

import numpy as np
import pvlib

from layers.generation.file_provider import FileDataProvider
from layers.simulation.pv_funcs import retrieve_module_data

def _pvwatts_dc_vectorized(poa_wm2: np.ndarray, tcell_c: np.ndarray, module: dict) -> np.ndarray:
    gamma_pdc = float(module["gamma_r"]) / 100.0
    stc_w = float(module["STC"])
    pdc = pvlib.pvsystem.pvwatts_dc(poa_wm2, tcell_c, stc_w, gamma_pdc, temp_ref=25.0)
    return np.asarray(pdc, dtype=float)

def _ols_closed_form(y: np.ndarray, pac: np.ndarray) -> float:
    y2 = float(np.dot(y, y))
    if y2 <= 0:
        return 1.0
    ypac = float(np.dot(y, pac))
    return ypac / y2

def _huber_irls(y: np.ndarray, pac: np.ndarray, delta: float = 1.5, max_iter: int = 50, tol: float = 1e-8, d0: float | None = None) -> float:
    d = _ols_closed_form(y, pac) if d0 is None else float(d0)
    for _ in range(max_iter):
        r = d * y - pac
        absr = np.abs(r)
        w = np.ones_like(absr)
        big = absr > delta
        w[big] = delta / absr[big]
        wy = w * y
        ywy = float(np.dot(y, wy))
        if ywy <= 0:
            break
        d_new = float(np.dot(wy, pac) / ywy)
        if abs(d_new - d) <= tol * max(1.0, abs(d)):
            d = d_new
            break
        d = d_new
    return float(d)

def _metrics(y: np.ndarray, pac: np.ndarray, d: float) -> Dict[str, float]:
    pred = d * y
    resid = pac - pred
    sse = float(np.dot(resid, resid))
    sst = float(np.dot(pac - pac.mean(), pac - pac.mean()))
    r2 = 1.0 - (sse / sst) if sst > 0 else 0.0
    rmse = float(np.sqrt(sse / len(pac))) if len(pac) else np.nan
    mape = float(np.mean(np.abs(resid) / np.maximum(1e-9, np.abs(pac)))) * 100.0
    return {"sse": sse, "rmse": rmse, "r2": r2, "mape_pct": mape}

@dataclass
class DerateCalibrator:
    provider: FileDataProvider
    module_name: str
    modules_by_inverter: int
    n_inverters: int
    day_thr: float = 20.0
    days: int = 60
    method: Literal["ols", "huber", "both"] = "both"
    dmin: float = 0.5
    dmax: float = 1.3

    def _load_window(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        start = now - timedelta(days=self.days)
        series = self.provider.get_series_between(start, now)
        poa, tcell, pac = [], [], []
        for p in series:
            if p["poa_wm2"] > self.day_thr:
                poa.append(p["poa_wm2"])
                tcell.append(p["tcell_c"])
                pac.append(float(sum(p["inverters_kw"])))
        if not poa:
            raise RuntimeError("Sem amostras acima de day_thr no intervalo escolhido para calibrar.")
        return np.array(poa, dtype=float), np.array(tcell, dtype=float), np.array(pac, dtype=float)

    def estimate(self) -> tuple[float, dict]:
        poa, tcell, pac_kw = self._load_window()
        module = retrieve_module_data(self.module_name)
        pdc_w_per_module = _pvwatts_dc_vectorized(poa, tcell, module)
        per_inv_kw_no_derate = (self.modules_by_inverter * pdc_w_per_module) / 1000.0
        y_base_kw = per_inv_kw_no_derate * self.n_inverters

        d_ols = _ols_closed_form(y_base_kw, pac_kw)
        d = d_ols

        if self.method in ("huber", "both"):
            d_huber = _huber_irls(y_base_kw, pac_kw, delta=1.5, max_iter=50, tol=1e-8, d0=d_ols)
            d = d_huber

        d = float(np.clip(d, self.dmin, self.dmax))

        met = _metrics(y_base_kw, pac_kw, d)
        met["derate_ols"] = float(np.clip(d_ols, self.dmin, self.dmax))
        met["derate_final"] = d
        met["n_points"] = int(len(poa))

        return d, met

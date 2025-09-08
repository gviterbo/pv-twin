from __future__ import annotations
import pvlib
import pandas as pd

def retrieve_module_data(module_name: str) -> dict:
    cec = pvlib.pvsystem.retrieve_sam("CECMod")
    if module_name in cec:
        return dict(cec[module_name])
    first_key = list(cec.keys())[0]
    return dict(cec[first_key])

def module_stc_w(module: dict) -> float:
    for key in ("STC", "pdc0"):
        if key in module and pd.notna(module[key]):
            return float(module[key])
    if "Vmpo" in module and "Impo" in module:
        return float(module["Vmpo"]) * float(module["Impo"])
    raise ValueError("Não foi possível obter STC do módulo.")

def array_p0_kw(module_name: str, modules_by_inverter: int, n_inverters: int) -> float:
    """Potência nominal DC a STC do arranjo TOTAL (kW) = n_inverters * módulos_por_inv * STC / 1000."""
    module = retrieve_module_data(module_name)
    stc_w = module_stc_w(module)
    return (n_inverters * modules_by_inverter * stc_w) / 1000.0

def simulate(module_name, poa, temp_cell, modules_by_inverter, derate=1.0):
    module = retrieve_module_data(module_name)
    gamma_pdc = (module['gamma_r']) / 100.0
    dc_power = pvlib.pvsystem.pvwatts_dc(poa, temp_cell, module['STC'], gamma_pdc, temp_ref=25.0)
    per_inv_kw_no_derate = (modules_by_inverter * dc_power) / 1000.0
    return float(per_inv_kw_no_derate * derate)

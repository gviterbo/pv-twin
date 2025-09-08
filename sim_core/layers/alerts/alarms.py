from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Iterable
from datetime import datetime, timezone

class AlertEmitterProtocol:
    def emit_alert_point(self, ts_ms: int, name: str, state: int, labels: Dict[str, str] | None = None): ...
    def emit_alert_count(self, ts_ms: int, name: str, count: int, labels: Dict[str, str] | None = None): ...

ALARM_OK   = 0
ALARM_WARN = 1
ALARM_CRIT = 2

@dataclass
class Observation:
    ts_ms: int
    poa_wm2: float
    pac_kw_total: float
    ideal_total_kw: float
    pr_inst: float
    sunny_flag: int
    day_flag: int
    tmod_c: Optional[float]
    tcell_c: Optional[float]
    inverter_kw: List[float]

class Alarm:
    name: str
    labels: Dict[str, str]

    def __init__(self, name: str, labels: Dict[str, str] | None = None):
        self.name = name
        self.labels = labels or {}
        self._state = ALARM_OK
        self._count = 0

    def evaluate(self, obs: Observation) -> int:
        """Deve retornar ALARM_OK/WARN/CRIT."""
        raise NotImplementedError

    def hysteresis(self, new_state: int) -> int:
        """Gancho para histerese/debounce simples (sobrescrever se quiser)."""
        return new_state

    def step(self, obs: Observation, emitter: AlertEmitterProtocol):
        new_state = self.hysteresis(self.evaluate(obs))
        self._state = new_state
        if new_state != ALARM_OK:
            self._count += 1
        emitter.emit_alert_point(obs.ts_ms, self.name, new_state, self.labels)
        emitter.emit_alert_count(obs.ts_ms, self.name, self._count, self.labels)


class PRLowAlarm(Alarm):
    """PR instantâneo baixo quando é dia. Usa dois limiares para histerese."""
    def __init__(self, warn: float = 0.82, crit: float = 0.70, clear: float = 0.86, labels: Dict[str, str] | None = None):
        super().__init__("alarm_pr_low", labels)
        self.warn = warn
        self.crit = crit
        self.clear = clear

    def evaluate(self, obs: Observation) -> int:
        if obs.day_flag == 0:
            return ALARM_OK
        pr = obs.pr_inst
        if pr <= self.crit:
            return ALARM_CRIT
        if pr <= self.warn:
            return ALARM_WARN
        return ALARM_OK

    def hysteresis(self, new_state: int) -> int:
        if self._state in (ALARM_WARN, ALARM_CRIT) and new_state == ALARM_OK:
            return ALARM_OK
        return new_state

class InverterOfflineAlarm(Alarm):
    """Detecta inversor parado/zerado em horário de dia (por rampa)."""
    def __init__(self, n_inverters: int, min_kw: float = 0.01, min_poa_wm2: float = 200.0, labels: Dict[str, str] | None = None):
        super().__init__("alarm_inverter_offline", labels)
        self.n = n_inverters
        self.min_kw = min_kw
        self.min_poa = min_poa_wm2

    def evaluate(self, obs: Observation) -> int:
        if obs.day_flag == 0 or obs.poa_wm2 < self.min_poa:
            return ALARM_OK
        zeros = [i for i, kw in enumerate(obs.inverter_kw) if kw <= self.min_kw]
        if len(zeros) >= 2:
            self.labels["detail"] = f"{len(zeros)}_inverters_zero"
            return ALARM_CRIT
        if len(zeros) == 1:
            self.labels["detail"] = f"inverter_{zeros[0]}_zero"
            return ALARM_WARN
        self.labels.pop("detail", None)
        return ALARM_OK

class SunnyNoProductionAlarm(Alarm):
    """Irradiância alta mas potência quase nula (disjuntor, falha geral, trip)."""
    def __init__(self, poa_thr: float = 600.0, pac_kw_thr: float = 0.05, labels: Dict[str, str] | None = None):
        super().__init__("alarm_sunny_no_production", labels)
        self.poa_thr = poa_thr
        self.pac_thr = pac_kw_thr

    def evaluate(self, obs: Observation) -> int:
        if obs.poa_wm2 >= self.poa_thr and obs.pac_kw_total <= self.pac_thr:
            return ALARM_CRIT
        return ALARM_OK

class TemperatureDeltaAlarm(Alarm):
    """Desvio entre Tcell e Tmod (sensor ruim, sujeira térmica, contato)."""
    def __init__(self, warn_delta: float = 8.0, crit_delta: float = 12.0, clear_delta: float = 6.0, labels: Dict[str, str] | None = None):
        super().__init__("alarm_temp_delta", labels)
        self.warn = warn_delta
        self.crit = crit_delta
        self.clear = clear_delta

    def evaluate(self, obs: Observation) -> int:
        if obs.tcell_c is None or obs.tmod_c is None:
            return ALARM_OK
        delta = abs(obs.tcell_c - obs.tmod_c)
        if delta >= self.crit:
            return ALARM_CRIT
        if delta >= self.warn:
            return ALARM_WARN
        return ALARM_OK

    def hysteresis(self, new_state: int) -> int:
        if new_state == ALARM_OK and self._state in (ALARM_WARN, ALARM_CRIT):
            return ALARM_OK
        return new_state

class RampIrradianceAlarm(Alarm):
    """Variação brusca de POA entre passos (intermitência/sombreamento rápido)."""
    def __init__(self, dpoa_warn: float = 250.0, dpoa_crit: float = 400.0, labels: Dict[str, str] | None = None):
        super().__init__("alarm_poa_ramp", labels)
        self.warn = dpoa_warn
        self.crit = dpoa_crit
        self._prev_poa: Optional[float] = None

    def evaluate(self, obs: Observation) -> int:
        if self._prev_poa is None:
            self._prev_poa = obs.poa_wm2
            return ALARM_OK
        dpoa = abs(obs.poa_wm2 - self._prev_poa)
        self._prev_poa = obs.poa_wm2
        if dpoa >= self.crit:
            return ALARM_CRIT
        if dpoa >= self.warn:
            return ALARM_WARN
        return ALARM_OK

class AlarmManager:
    def __init__(self, emitter: AlertEmitterProtocol, alarms: Iterable[Alarm]):
        self.emitter = emitter
        self.alarms = list(alarms)

    def step(self, obs: Observation):
        for a in self.alarms:
            a.step(obs, self.emitter)

class AlertEmitter(AlertEmitterProtocol):
    """
    Emite para VictoriaMetrics usando nomes padrão:
      - alert_state{name="<alarm>", ...} -> 0/1/2
      - alert_count{name="<alarm>", ...} -> contagem cumulativa
    """
    def __init__(self, post_line_fn):
        self._post_line = post_line_fn

    @staticmethod
    def _labels(base: Dict[str, str] | None, extra: Dict[str, str] | None) -> str:
        merged = dict(base or {})
        merged.update(extra or {})
        if "name" in merged:
            merged["alarm"] = merged.pop("name")
        return ",".join(f'{k}="{v}"' for k, v in merged.items())

    def emit_alert_point(self, ts_ms: int, name: str, state: int, labels: Dict[str, str] | None = None):
        lb = self._labels(labels, {"name": name})
        self._post_line(f'alert_state{{{lb}}} {int(state)} {ts_ms}\n')

    def emit_alert_count(self, ts_ms: int, name: str, count: int, labels: Dict[str, str] | None = None):
        lb = self._labels(labels, {"name": name})
        self._post_line(f'alert_count{{{lb}}} {int(count)} {ts_ms}\n')

from __future__ import annotations
import requests

class DataEmitter:
    def __init__(self, vm_url: str):
        self.vm_url = vm_url

    @staticmethod
    def _line(metric, labels, value, ts_ms):
        labels_str = f"{{{labels}}}" if labels else ""
        return f"{metric}{labels_str} {value} {ts_ms}\n"

    def _post_lines(self, payload: str):
        headers = {"Content-Type": "text/plain"}
        resp = requests.post(self.vm_url, data=payload.encode("utf-8"), headers=headers, timeout=10)
        resp.raise_for_status()

    def emit_poa(self, points, source_label='source="file"'):
        if not points:
            return
        lines = []
        for ts_ms, poa in points:
            lines.append(self._line("solar_ghi_wm2", source_label, float(poa), ts_ms))
        self._post_lines("".join(lines))

    def emit_pv_inverters(self, ts_ms, ideal_per_inv_kw, real_per_inv_kw):
        lines = []
        for i, real_kw in enumerate(real_per_inv_kw):
            lines.append(self._line("pv_ideal_kw", f'inverter="{i}"', round(float(ideal_per_inv_kw), 3), ts_ms))
            lines.append(self._line("pv_real_kw",  f'inverter="{i}"', round(float(real_kw), 3), ts_ms))
        self._post_lines("".join(lines))

    def emit_pr_inst(self, ts_ms, pr_value):
        self._post_lines(self._line("plant_pr_inst", "", round(float(pr_value), 4), ts_ms))

    def emit_pr_daily_bulk(self, items):
        if not items:
            return
        lines = []
        for day_ms, pr in items:
            lines.append(self._line("plant_pr_daily", "", round(float(pr), 4), day_ms))
        self._post_lines("".join(lines))

    def emit_flags(self, ts_ms, sunny_flag, day_flag):
        lines = []
        lines.append(self._line("weather_sunny_flag", "", int(bool(sunny_flag)), ts_ms))
        lines.append(self._line("day_flag", "", int(bool(day_flag)), ts_ms))
        self._post_lines("".join(lines))

    def emit_temps(self, ts_ms, tmod_c=None, tcell_c=None):
        lines = []
        if tmod_c is not None:
            lines.append(self._line("pv_module_temp_c", "", round(float(tmod_c), 3), ts_ms))
        if tcell_c is not None:
            lines.append(self._line("pv_cell_temp_c", "", round(float(tcell_c), 3), ts_ms))
        if lines:
            self._post_lines("".join(lines))

    def emit_cumulative_energy(self, ts_ms, real_kwh_total, ideal_kwh_total):
        lines = []
        real_kwh_total = float(real_kwh_total)
        ideal_kwh_total = float(ideal_kwh_total)
        lines.append(self._line("plant_real_energy_kwh_total", "", round(real_kwh_total, 6), ts_ms))
        lines.append(self._line("plant_ideal_energy_kwh_total", "", round(ideal_kwh_total, 6), ts_ms))
        acc_pct = 100.0 * (real_kwh_total / ideal_kwh_total) if ideal_kwh_total > 0 else 0.0
        lines.append(self._line("model_accuracy_pct", "", round(acc_pct, 4), ts_ms))
        self._post_lines("".join(lines))

    def emit_alert_raw_lines(self, payload: str):
        """Permite postar linhas já formatadas (Prometheus line protocol) para alertas."""
        self._post_lines(payload)

    def make_alert_emitter(self):
        """Cria um adaptador que publica alertas via _post_lines."""
        from layers.alerts.alarms import AlertEmitter
        return AlertEmitter(self._post_lines)

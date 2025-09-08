from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd

@dataclass
class FileDataProvider:
    csv_path: str
    date_col: str
    time_col: str
    inverter_cols: List[str]
    poa_col: str
    tcell_col: str
    decimal: str = ","
    sep: str = ","
    tmod_col: Optional[str] = None

    def __post_init__(self):
        self.df = pd.read_csv(self.csv_path, sep=self.sep, decimal=self.decimal, engine="python")
        self.df["__src_dt"] = pd.to_datetime(
            self.df[self.date_col].astype(str) + " " + self.df[self.time_col].astype(str),
            dayfirst=True, errors="coerce"
        )
        self.df["__src_dt"] = self.df["__src_dt"].dt.tz_localize("UTC")
        self.df["__key"] = self.df["__src_dt"].dt.strftime("%m-%d %H:%M")
        self._keys_set = set(self.df["__key"].dropna().unique())
        self.step_minutes = self._infer_step_minutes()

    def _infer_step_minutes(self) -> int:
        s = self.df["__src_dt"].dropna().sort_values().unique()
        if len(s) >= 2:
            dt0 = pd.Timestamp(s[0]).to_pydatetime()
            for i in range(1, min(10, len(s))):
                dt1 = pd.Timestamp(s[i]).to_pydatetime()
                delta = int((dt1 - dt0).total_seconds() // 60)
                if delta > 0:
                    return delta
        return 15

    def _map_target_to_src_key(self, dt_target: datetime) -> Optional[str]:
        key = dt_target.strftime("%m-%d %H:%M")
        return key if key in self._keys_set else None

    def get_point_now(self, now_utc: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        if now_utc is None:
            now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        step_s = self.step_minutes * 60
        epoch = int(now_utc.timestamp())
        aligned = datetime.fromtimestamp((epoch // step_s) * step_s, tz=timezone.utc)

        key = self._map_target_to_src_key(aligned)
        if key is None:
            return None

        row = self.df[self.df["__key"] == key].iloc[0]
        return self._row_to_payload(row, target_ts=aligned)

    def get_series_between(self, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
        if end_utc < start_utc:
            start_utc, end_utc = end_utc, start_utc

        out: List[Dict[str, Any]] = []
        step = timedelta(minutes=self.step_minutes)
        t = start_utc.replace(second=0, microsecond=0)
        step_s = self.step_minutes * 60
        epoch = int(t.timestamp())
        t = datetime.fromtimestamp((epoch // step_s) * step_s, tz=timezone.utc)

        while t <= end_utc:
            key = self._map_target_to_src_key(t)
            if key is not None:
                row = self.df[self.df["__key"] == key].iloc[0]
                out.append(self._row_to_payload(row, target_ts=t))
            t += step
        return out

    def _row_to_payload(self, row: pd.Series, target_ts: datetime) -> Dict[str, Any]:
        ts_ms = int(target_ts.timestamp() * 1000)
        poa = float(row[self.poa_col])
        tcell = float(row[self.tcell_col])
        tmod = float(row[self.tmod_col]) if self.tmod_col else None
        inv = [float(row[c]) for c in self.inverter_cols]
        return {
            "ts_ms": ts_ms,
            "poa_wm2": poa,
            "tcell_c": tcell,
            "tmod_c": tmod,
            "inverters_kw": inv,
            "step_s": self.step_minutes * 60
        }

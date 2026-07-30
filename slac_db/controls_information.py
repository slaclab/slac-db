from typing import List, Dict
import numpy as np
from epics import caget, caget_many


def get_magnet_controls_information(magnet_names: List[str] = None):
    # return a data structure of the form:
    # {
    #  mag-name-1 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  mag-name-2 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  ...
    # }
    if magnet_names:
        raise NotImplementedError(
            "No method of getting additional controls_information for magnets."
        )
    return {}


def get_screen_controls_information(screen_information: Dict = None):
    # return a data structure of the form:
    # {
    #  scr-name-1 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  scr-name-2 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  ...
    # }

    # Stuff like Device-Position mappings for motor/ladder-based screens
    controls_information = {}
    for k, v in screen_information.items():
        pv_cache = {}
        pvs = v["controls_information"]["PVs"]
        if "orient_x" in pvs and "orient_y" in pvs:
            pv_cache["orient_x"] = caget(pvs["orient_x"], as_string=True)
            pv_cache["orient_y"] = caget(pvs["orient_y"], as_string=True)
        controls_information[k] = {"pv_cache": pv_cache}
    return controls_information


def get_wire_controls_information(wire_names: List[str] = None):
    # return a data structure of the form:
    # {
    #  scr-name-1 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  scr-name-2 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  ...
    # }

    # Stuff like Device-Position mappings for motor/ladder-based screens
    if wire_names:
        raise NotImplementedError(
            "No method of getting additional controls_information for wires."
        )
    return {}


def get_lblm_controls_information(lblm_names: List[str] = None):
    # return a data structure of the form:
    # {
    #  scr-name-1 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  scr-name-2 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  ...
    # }

    # Stuff like Device-Position mappings for motor/ladder-based screens
    if lblm_names:
        raise NotImplementedError(
            "No method of getting additional controls_information for LBLMs."
        )
    return {}


def get_bpm_controls_information(bpm_names: List[str] = None):
    # return a data structure of the form:
    # {
    #  bpm-name-1 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  bpm-name-2 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  ...
    # }

    # Stuff like Device-Position mappings for motor/ladder-based screens
    if bpm_names:
        raise NotImplementedError(
            "No method of getting additional controls_information for bpms."
        )
    return {}


def get_tcav_controls_information(tcav_names: List[str] = []):
    # return a data structure of the form:
    # {
    #  lblm-name-1 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  lblm-name-2 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  ...
    # }
    if tcav_names:
        raise NotImplementedError(
            "No method of getting additional controls_information for TCAVs."
        )
    return {}


def get_pmt_controls_information(pmt_names: List[str] = None):
    # return a data structure of the form:
    # {
    #  pmt-name-1 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  pmt-name-2 : {controls-information-field-1 : value-1, controls-information-field-2 : value-2, ...},
    #  ...
    # }

    # Stuff like Device-Position mappings for motor/ladder-based screens
    if pmt_names:
        raise NotImplementedError(
            "No method of getting additional controls_information for PMTs."
        )
    return {}


_LI2631 = 'KLYS:LI26:31'
_LI2631_DSTA = np.array([1.610612737e9, 5.2864e5])


def get_klystron_controls_information(klystron_names: List[str] = None, beam_code: int = 1):
    # return a data structure of the form:
    # {
    #  klys-name-1 : {'act': value, 'stat': value, 'swrd': value, 'hdsc': value, 'dsta': array, 'enld': value},
    #  klys-name-2 : {...},
    #  ...
    # }
    if not klystron_names:
        return {}

    names = list(klystron_names)
    n = len(names)

    stat = np.zeros(n)
    swrd = np.zeros(n)
    hdsc = np.zeros(n)
    dsta = np.zeros((n, 2))
    enld = np.zeros(n)
    act = np.zeros(n)

    is263 = np.array([nm == _LI2631 for nm in names])
    is_mk2 = np.array([nm.startswith('KLYS:DMP') or nm.startswith('KLYS:DIAG0') for nm in names])
    use_std = ~is263 & ~is_mk2

    # LI26:31 special case
    if is263.any():
        i = np.where(is263)[0][0]
        if not caget('KLYS:LI26:31:IGNORE.RVAL'):
            act[i] = caget('KLYS:LI26:31:TMODE_DES.RVAL') or 0
            stat[i] = 1
            hdsc[i] = 32
            dsta[i] = _LI2631_DSTA
            enld[i] = caget('KLYS:LI26:31:ENLD') or 0

    # MKSU-II — only act is set; other fields stay 0
    if is_mk2.any():
        for idx in np.where(is_mk2)[0]:
            nm = names[idx]
            prefix = nm[:9]
            if prefix == 'KLYS:DMPH':
                fac_pv = 'PHYS:UNDH:1:FACMODE' if beam_code == 1 else 'PHYS:UNDS:1:FACMODE'
                facmode = caget(fac_pv)
                cfg_pv = 'TCAV:DMP0:360:0:MODECFG' if facmode == 0 else 'TCAV:DMP0:360:2:MODECFG'
                act[idx] = caget(cfg_pv) or 0
            elif prefix == 'KLYS:DIAG':
                act[idx] = caget('TCAV:DIAG0:11:MODECFG') or 0
            else:
                val = caget(f'{nm}:MOD')
                act[idx] = 4 if (val is None or np.isnan(float(val)) or val == 3) else val

    # Standard EPICS path
    if use_std.any():
        std_idx = np.where(use_std)[0]
        std_names = [names[i] for i in std_idx]
        ns = len(std_names)

        pvs = (
            [f'{nm}:STAT' for nm in std_names] +
            [f'{nm}:SWRD' for nm in std_names] +
            [f'{nm}:HDSC' for nm in std_names] +
            [f'{nm}:ENLD' for nm in std_names] +
            [f'{nm}:BEAMCODE{beam_code}_STAT' for nm in std_names]
        )
        vals = caget_many(pvs)

        def _clean(arr):
            a = np.array(arr, dtype=float)
            a[np.isnan(a)] = 0.0
            return a

        stat[std_idx] = _clean(vals[:ns])
        swrd[std_idx] = _clean(vals[ns:2*ns]).astype(int) & 0xFFFF
        hdsc[std_idx] = _clean(vals[2*ns:3*ns])
        enld[std_idx] = _clean(vals[3*ns:4*ns])
        act[std_idx] = _clean(vals[4*ns:])

        for i, nm in zip(std_idx, std_names):
            raw = caget(f'{nm}:DSTA', count=2)
            if raw is not None:
                dsta[i] = np.nan_to_num(raw)

    return {
        nm: {
            'act': act[i],
            'stat': stat[i],
            'swrd': swrd[i],
            'hdsc': hdsc[i],
            'dsta': dsta[i],
            'enld': enld[i],
        }
        for i, nm in enumerate(names)
    }

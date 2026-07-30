import copy
import re
from typing import Any, Dict, List
from epics import caget
import os
import slac_db.config
import yaml

# Exceptions to the standard KLYS:{cs_name}:{SUFFIX} PV derivation rule.
# Keys are cs_name strings; values override only the differing fields.
_KLYSTRON_PV_OVERRIDES = {
    "KLYS:LI24:11": {"phase_pvname": "ACCL:LI24:100:KLY_PDES"},
    "KLYS:LI24:21": {"phase_pvname": "ACCL:LI24:200:KLY_PDES"},
    "KLYS:LI24:31": {"phase_pvname": "ACCL:LI24:300:KLY_PDES"},
    "KLYS:LI26:31": {"accelerate_pvname": ""},
}


def get_magnet_metadata(
    magnet_names: List[str] = [], method: callable = None, **kwargs
):
    # return a data structure of the form:
    # {
    #  mag-name-1 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  mag-name-2 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  ...
    # }
    if magnet_names and method:
        # Add any additional metadata fields here
        additional_fields = ["Element", "Effective Length (m)"]
        device_elements = method(magnet_names, additional_fields)
        # change field names and values to be in different format
        # if needed
        for magnet in device_elements:
            if "Effective Length (m)" in device_elements[magnet]:
                if device_elements[magnet]["Effective Length (m)"] == "":
                    device_elements[magnet]["Effective Length (m)"] = 0.0
                device_elements[magnet]["l_eff"] = round(
                    float(device_elements[magnet]["Effective Length (m)"]), 3
                )
                del device_elements[magnet]["Effective Length (m)"]
        return device_elements
    else:
        return {}


def get_screen_metadata(basic_screen_data: dict):
    # return a data structure of the form:
    # {
    #  scr-name-1 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  scr-name-2 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  ...
    # }
    from meme.names import list_pvs

    metadata = {}
    for mad_name, info in basic_screen_data.items():
        metadata[mad_name] = {}
        ctrl_name = info["controls_information"]["control_name"]
        flags = list_pvs(ctrl_name + "%INSTALLED")
        hardware = {}
        for i in flags:
            name = re.search("(?<=^" + ctrl_name + ":).*(?=INSTALLED)", i)
            if name is None:
                continue
            name = name.group(0)
            status = caget(i)
            if status is not None:
                hardware[name] = status

        metadata[mad_name]["hardware"] = hardware
    return metadata


def get_wire_metadata(basic_wire_data: dict) -> Dict[str, Dict[str, Any]]:
    """Load wire and area metadata, merge into per-wire dict.

    Args:
        basic_wire_data: {wire_name: {"metadata": {"area": ...}, ...}}

    Returns: {wire_name: {field: value, ...}}
    """
    here = slac_db.config.package_data()

    with open(os.path.join(here, "wire_area_metadata.yaml"), "r") as f:
        area_raw = yaml.safe_load(f)

    with open(os.path.join(here, "wire_metadata.yaml"), "r") as f:
        wire_raw = yaml.safe_load(f)

    result = {}
    for wire_name, info in basic_wire_data.items():
        area_name = info["metadata"]["area"]
        if area_name not in area_raw:
            continue
        entry = copy.deepcopy(area_raw[area_name])
        wire_overrides = wire_raw.get(wire_name, {})
        if wire_overrides:
            entry.update(wire_overrides)
        result[wire_name] = entry

    return result


def get_lblm_metadata(lblm_names: List[str] = []):
    # return a data structure of the form:
    # {
    #  lblm-name-1 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  lblm-name-2 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  ...
    # }
    if lblm_names:
        raise NotImplementedError("No method of getting additional metadata for lblms.")
    return {}


def get_bpm_metadata(bpm_names: List[str] = []):
    # return a data structure of the form:
    # {
    #  bpm-name-1 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  bpm-name-2 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  ...
    # }
    if bpm_names:
        raise NotImplementedError("No method of getting additional metadata for bpms.")
    return {}


def get_tcav_metadata(tcav_names: List[str] = [], method: callable = None, **kwargs):
    # return a data structure of the form:
    # {
    #  tcav-name-1 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  tcav-name-2 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  ...
    # }
    if tcav_names and method:
        # Add any additional metadata fields here
        additional_fields = [
            "Element",
            "Effective Length (m)",
            "Rf Frequency (MHz)",
        ]
        device_elements = method(tcav_names, additional_fields)
        # change field names and values to be in different format
        # if needed
        for tcav in device_elements:
            if "Effective Length (m)" in device_elements[tcav]:
                if device_elements[tcav]["Effective Length (m)"] == "":
                    device_elements[tcav]["Effective Length (m)"] = 0.0
                device_elements[tcav]["l_eff"] = round(
                    float(device_elements[tcav]["Effective Length (m)"]), 3
                )
                del device_elements[tcav]["Effective Length (m)"]

            if "Rf Frequency (MHz)" in device_elements[tcav]:
                if device_elements[tcav]["Rf Frequency (MHz)"] == "":
                    device_elements[tcav]["Rf Frequency (MHz)"] = 0.0
                device_elements[tcav]["rf_freq"] = float(
                    device_elements[tcav]["Rf Frequency (MHz)"]
                )
                del device_elements[tcav]["Rf Frequency (MHz)"]

        return device_elements
    else:
        return {}


def get_klystron_metadata(klystron_cs_names: List[str] = []) -> Dict[str, Dict[str, Any]]:
    """Return metadata and PV names for klystron stations.

    All fields are derived algorithmically from the cs_name
    (e.g. 'KLYS:LI21:31'). Four stations with non-standard PV names
    are handled via _KLYSTRON_PV_OVERRIDES.

    Only cs_names starting with 'KLYS:' are supported. Injector and
    sub-booster stations (GUN:, ACCL:) are not handled here.

    Parameters
    ----------
    klystron_cs_names : list of str
        Control system names to include (e.g. ['KLYS:LI21:31']). If empty,
        returns an empty dict.

    Returns
    -------
    dict
        {cs_name: {field: value, ...}} where fields are: name, sector,
        station, description, enld_pvname, phase_pvname, accelerate_pvname,
        swrd_pvname, stat_pvname, hdsc_pvname, dsta_pvname.
    """
    result = {}
    for cs_name in klystron_cs_names:
        if not cs_name.startswith("KLYS:"):
            continue
        # cs_name format: KLYS:LI{SS}:{S}1
        # e.g. KLYS:LI21:31 -> sector=21, station=3
        parts = cs_name.split(":")
        sector = int(parts[1][2:])
        station = int(parts[2]) // 10
        name = f"K{sector}_{station}"
        entry = {
            "name": name,
            "sector": sector,
            "station": station,
            "description": f"Klystron in sector {sector}, station {station}",
            "enld_pvname":       f"{cs_name}:ENLD",
            "phase_pvname":      f"{cs_name}:PHAS",
            "accelerate_pvname": f"{cs_name}:BEAMCODE1_STAT",
            "swrd_pvname":       f"{cs_name}:SWRD",
            "stat_pvname":       f"{cs_name}:STAT",
            "hdsc_pvname":       f"{cs_name}:HDSC",
            "dsta_pvname":       f"{cs_name}:DSTA",
        }
        entry.update(_KLYSTRON_PV_OVERRIDES.get(cs_name, {}))
        result[cs_name] = entry
    return result


def get_pmt_metadata(pmt_names: List[str] = []):
    # return a data structure of the form:
    # {
    #  pmt-name-1 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  pmt-name-2 : {metadata-field-1 : value-1, metadata-field-2 : value-2},
    #  ...
    # }
    pmt_metadata = {}

    here = slac_db.config.package_data()
    yaml_path = os.path.join(here, "pmt_metadata.yaml")

    with open(yaml_path, "r") as f:
        pmt_metadata = yaml.safe_load(f)

    return pmt_metadata

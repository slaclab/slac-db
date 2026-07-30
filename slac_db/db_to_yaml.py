import slac_db.device
import slac_db.oracle
import slac_db.create.combined
from slac_db.metadata import get_klystron_metadata

_ORACLE_TO_YAML_TYPE_MAP = {
    "SOLE": "magnets",
    "QUAD": "magnets",
    "XCOR": "magnets",
    "YCOR": "magnets",
    "BEND": "magnets",
    "PROF": "screens",
    "WIRE": "wires",
    "LBLM": "lblms",
    "BPM": "bpms",
    "LCAV": "tcavs",
    "KLYSTRON": "klystrons",
    "INST": "pmts"
}

def _oracle_name(device_name):
    """Return the element name to use for oracle lookups.

    Station-level klystron names (e.g. 'K21_5') no longer exist in
    lcls_elements.sqlite3 which still holds sub-cavity rows (K21_5A/B/C/D).
    Try each common sub-cavity letter in order so we can retrieve
    beampath/area/type info, which is identical across all sub-cavities.
    """
    try:
        slac_db.oracle.get_device_row(device_name)
        return device_name
    except Exception:
        pass
    for letter in ("A", "B", "C", "D"):
        try:
            slac_db.oracle.get_device_row(device_name + letter)
            return device_name + letter
        except Exception:
            pass
    return device_name


def _build_metadata(device_name):
    """Generate Metadata field for a YAML device.

    Args:
        device_name (str): MAD Name of Device

    Returns:
        rv (dict): Device Metadata
    """
    def parse_beampaths(beampath_csv):
            if beampath_csv is None:
                return []
            beampaths = beampath_csv.replace(' ', '').split(',')
            beampaths = filter(None, beampaths)
            yield from beampaths

    def _round_values(meta):
        for i, v in meta.items():
            if type(v) is float:
                meta[i] = round(v, 3)
        return meta

    oracle_name = _oracle_name(device_name)
    beampath_csv = slac_db.oracle.get_device_row(oracle_name)["beampath"]
    rv =  {
        "area": slac_db.device.get_attribute(device_name, "area"),
        "beam_path": list(parse_beampaths(beampath_csv)),
        "type": slac_db.device.get_attribute(device_name, "device_type")
    }
    rv.update(_round_values(slac_db.device.get_all_meta(device_name)))
    expected_meta = slac_db.create.combined._DEVICE_META_MAP.get(
        slac_db.device.get_attribute(device_name, "device_type"), []
    )
    for m in expected_meta + slac_db.create.combined._DEFAULT_DEVICE_META:
        if m[1] not in rv:
            if m[1] == "l_eff" or m[1] == "rf_freq":
                rv.update({m[1]: 0.0})
            else:
                rv.update({m[1]: None})
    return rv


def _build_metadata_from_device_db(device_name):
    """Generate Metadata using only device.sqlite3 (no oracle query).

    Used for klystrons where lcls_elements.sqlite3 may be empty.

    Args:
        device_name (str): MAD Name of Device

    Returns:
        rv (dict): Device Metadata
    """
    def _round_values(meta):
        for i, v in meta.items():
            if type(v) is float:
                meta[i] = round(v, 3)
        return meta

    beampaths = slac_db.device.get_all_device_beampaths(device_name)
    rv = {
        "area": slac_db.device.get_attribute(device_name, "area"),
        "beam_path": beampaths,
        "type": slac_db.device.get_attribute(device_name, "device_type"),
    }
    rv.update(_round_values(slac_db.device.get_all_meta(device_name)))
    expected_meta = slac_db.create.combined._DEVICE_META_MAP.get(
        slac_db.device.get_attribute(device_name, "device_type"), []
    )
    for m in expected_meta + slac_db.create.combined._DEFAULT_DEVICE_META:
        if m[1] not in rv:
            if m[1] == "l_eff" or m[1] == "rf_freq":
                rv.update({m[1]: 0.0})
            else:
                rv.update({m[1]: None})
    return rv

def _build_controls_information(device_name):
    """Returns controls information in YAML Device Format.

    Args:
        device_name (str): MAD Device Name
    """
    return {
        "PVs": slac_db.device.get_all_accessors(device_name),
        "control_name": slac_db.device.get_attribute(device_name, "cs_name"),
    }

def _build_devices(area, device_type):
    """Generator for all devices of a given type in a given area.

    For LCAV device_type, yields tcavs (Engineering Name == TRANS_DEFL).
    For KLYSTRON pseudo-type, yields klystrons deduped by cs_name, keyed
    by cs_name (e.g. 'KLYS:LI21:31'), enriched with lcls-live metadata.

    Args:
        area (str): Area Name
        device_type (str): Oracle Device Type, or 'KLYSTRON' for klystrons
    """
    if device_type == "KLYSTRON":
        yield from _build_klystron_devices(area)
        return

    devices = slac_db.device.get_devices(
        area=area, device_type=device_type
    )
    for d in devices:
        if device_type == "INST" and not d.startswith("PMT"):
            continue
        if device_type == "LCAV":
            r = slac_db.oracle.get_device_row(_oracle_name(d))
            if r["engineering name"] != "TRANS_DEFL":
                continue
        cs = _build_controls_information(d)
        meta = _build_metadata(d)
        yield d, {
            "controls_information": cs,
            "metadata": meta,
        }


def _build_klystron_devices(area):
    """Generator yielding one entry per klystron station in an area.

    Deduplicates LCAV sub-cavities (K21_3B/C/D) by cs_name, keying each
    entry by its cs_name (e.g. 'KLYS:LI21:31'). Only stations whose
    cs_name starts with 'KLYS:' are included. Uses only device.sqlite3 —
    does not query lcls_elements.sqlite3.

    Args:
        area (str): Area Name
    """
    devices = slac_db.device.get_devices(area=area, device_type="LCAV")
    seen = {}
    for d in devices:
        cs_name = slac_db.device.get_attribute(d, "cs_name")
        if not cs_name or not cs_name.startswith("KLYS:"):
            continue
        if cs_name in seen:
            continue
        seen[cs_name] = True

        controls = {
            "PVs": slac_db.device.get_all_accessors(d),
            "control_name": cs_name,
        }
        meta = _build_metadata_from_device_db(d)
        klys_meta = get_klystron_metadata([cs_name])
        if cs_name in klys_meta:
            meta.update(klys_meta[cs_name])

        yield cs_name, {
            "controls_information": controls,
            "metadata": meta,
        }

def _build_types(area):
    """Generator for all devices in an area sorted by type.

    Args:
        area (str): Area Name
    """
    all_types = {}
    for oracle, yaml_type in _ORACLE_TO_YAML_TYPE_MAP.items():
        d = {name: data for name, data in _build_devices(area, oracle)}
        if not d:
            continue
        if yaml_type not in all_types:
            all_types[yaml_type] = d
        else:
            all_types[yaml_type].update(d)
        yield yaml_type, all_types[yaml_type]


def _build_areas(areas):
    """Generator for all devices in a given area.

    Args:
        areas (list): List of Area Names
    """
    for a in areas:
        yv = {t: d for t, d in _build_types(a)}
        if not yv:
            continue
        yield a, yv


def get_device(device_name):
    """Returns the expected device dict for a given device.

    Args:
        device_name (str): MAD Device Name

    Returns:
        (dict): YAML Device Dictionary
    """
    device_dict = {
        "name": device_name,
        "controls_information": _build_controls_information(device_name),
        "metadata": _build_metadata(device_name),
    }
    device_type = device_dict["metadata"]["type"]
    # LCAV maps to tcavs for TRANS_DEFL, klystrons otherwise
    if device_type == "LCAV":
        r = slac_db.oracle.get_device_row(_oracle_name(device_name))
        cs_name = slac_db.device.get_attribute(device_name, "cs_name")
        if r["engineering name"] == "TRANS_DEFL":
            device_dict["yaml_type"] = "tcavs"
        elif cs_name and cs_name.startswith("KLYS:"):
            device_dict["yaml_type"] = "klystrons"
        else:
            device_dict["yaml_type"] = None
    else:
        device_dict["yaml_type"] = _ORACLE_TO_YAML_TYPE_MAP.get(device_type, None)
    if device_dict["yaml_type"] is None:
        return None
    return device_dict


def build():
    """Builds the full device YAML dict for all areas.

    Returns:
        (dict): {area: {yaml_type: {device_name: {...}}}}
    """
    def _parse_areas():
        areas = slac_db.device.get_all_areas()
        for a in areas:
            if " " in a or "*" in a:
                continue
            yield a
    areas = list(_parse_areas())
    out = {a: d for a, d in _build_areas(areas)}
    return out


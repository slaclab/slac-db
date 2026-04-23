import yaml
import slac_db.device

_ORACLE_TO_YAML_TYPE_MAP = {
    "SOLE": "magnets",
    "QUAD": "magnets",
    "XCOR": "magnets",
    "YCOR": "magnets",
    "BEND": "magnets",
    "PROF": "screens",
    "WIRE": "wire",
    "LBLM": "lblms",
    "BPM": "bpms",
    "LCAV": "tcavs",
    "INST": "pmts"
}

def _build_metadata(area, device_name):
    rv =  {
        "area": area,
        "beam_path": slac_db.device.get_all_device_beampaths(device_name),
    }
    rv.update(slac_db.device.get_all_meta(device_name))
    return rv

def _build_controls_information(device_name):
    return {
        "pv": slac_db.device.get_all_accessors(device_name),
        "control_name": slac_db.device.get_cs_name(device_name),
    }

def _build_devices(area, device_type):
    devices = slac_db.device.get_devices(
        area=area, device_type=device_type
    )
    for d in devices:
        cs = _build_controls_information(d)
        meta = _build_metadata(area, d)
        yield d, {
            "controls_information": cs,
            "metadata": meta,
        }

def _build_types(area):
    for oracle, yaml in _ORACLE_TO_YAML_TYPE_MAP.items():
        devices = {name: data for name, data in _build_devices(area, oracle)}
        if not devices:
            continue
        yield yaml, devices

def _build_areas(areas):
    for a in areas:
        yv = {t: d for t, d in _build_types(a)}
        if not yv:
            continue        
        yield a, yv

def write():
    def _parse_areas():
        areas = slac_db.device.get_all_areas()
        for a in areas:
            if " " in a or "*" in a:
                continue
            yield a
    areas = list(_parse_areas())
    v = {a: d for a, d in _build_areas(areas)}
    return v

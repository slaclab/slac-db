import slac_db.config
import slac_db.directory_service
import slac_db.io
import slac_db.element_tables
import slac_db.device
from pykern.pkcollections import PKDict
import yaml

_ACCESSOR_YAML = slac_db.config.package_data() / "accessor_names.yaml"
_DELIM = ":"
_DEFAULT_DEVICE_META = [("suml (m)", "sum_l_meters")]
_MAGNET_META = [("effective length (m)", "l_eff")]
_DEVICE_META_MAP = {
    "SOLE": _MAGNET_META,
    "QUAD": _MAGNET_META,
    "XCOR": _MAGNET_META,
    "YCOR": _MAGNET_META,
    "BEND": _MAGNET_META,
    "LCAV": [("effective length (m)", "l_eff"), ("rf frequency (mhz)", "rf_freq")],
}

def to_device_db():
    """Build  device DB with SQLAlchemy"""
    slac_db.device.recreate(_Parser())

class _Parser:
    accessor_map = dict()
    accessor_meta = list()
    # address_map = dict()
    address_meta = list()
    areas = set()
    area_map = set()
    devices = list()
    device_names = set()
    device_meta = list()
    device_meta_float = list()
    device_meta_string = list()


    def __init__(self):
        device_yaml = {}
        area_yaml = {}
        for f in slac_db.config.package_data().rglob('*_metadata.yaml'):
            if f.name.endswith('_area_metadata.yaml'):
                area_yaml[f.name[:-19]] = slac_db.io.read_dict(f)
            elif f.name.endswith('_metadata.yaml'):
                device_yaml.update(slac_db.io.read_dict(f))

        self.accessor_map = slac_db.io.read_dict(_ACCESSOR_YAML)
        self.accessor_overrides = self.accessor_map.pop("_overrides", {})
        for r in slac_db.element_tables.get_all_rows():
            # Skip a row if it is an excluded device.
            if not self._devices(r):
                continue
            self._area_map(r)
            self._device_meta_float(r)
            self._device_meta_string(r, area_yaml, device_yaml)
            addresses = slac_db.directory_service.get_addresses(r['element'])
            self._address_meta(r, addresses)
            self._accessor_meta(r, addresses)

    def _accessor_meta(self, r, addresses):
        """Create a dictionary that combines accessor names
        with device names and addresses.

        Sets:
            self.accessor_meta
            self.accessor_map
        """

        def _build():
            cs_name = r["control system name"] or ""
            # Klystron stations share keyword=LCAV with TCAVs but need
            # their own keyword.
            d_type = "KLYS" if "KLYS" in cs_name else r["keyword"]
            yield from _meta(
                r["element"],
                cs_name,
                d_type,
            )

        def _get_accessors(d_type, address):
            if not (device_map := self.accessor_map.get(d_type, None)):
                return
            if not (accessor := device_map.get(address, None)):
                return
            if f"_{address}_attributes" in device_map:
                attribute_map = device_map[f"_{address}_attributes"]
                yield from [
                    (".".join([address, attr]), a) for attr, a in attribute_map.items()
                ]
            if accessor is not None:
                yield (address, accessor)

        def _build_accessors(device, d_type, pv_head, pv_tail):
            rv = []
            override = self.accessor_overrides.get(device, {})
            accessor_names = _get_accessors(d_type, pv_tail)
            for address, accessor in accessor_names:
                rv = PKDict(
                        device_name=device,
                        cs_address=_DELIM.join([pv_head, address]),
                        accessor_name=accessor,
                    )
                if accessor in override:
                    rv["cs_address"] = override.pop(device)
                yield rv

        def _build_overrides(device):
            override = self.accessor_overrides.get(device, {})
            for accessor_name, cs_address in override.items():
                if cs_address:
                    yield PKDict(
                        device_name=device,
                        cs_address=cs_address,
                        accessor_name=accessor_name,
                    )

        def _meta(device, pv_head, d_type):
            for pv in addresses:
                pv_tail = pv[len(pv_head)+1:]
                if pv_tail is None:
                    continue
                yield from _build_accessors(device, d_type, pv_head, pv_tail)
            yield from _build_overrides(device)

        self.accessor_meta += list(_build())

    def _address_meta(self, row, addresses):
        """Create a list of tuples connecting device names
        to device addresses.

        Sets:
            self.address_meta
        """
        if not addresses:
            return
        cs_name = row["control system name"]
        self.address_meta += [
            PKDict(device_name=row["element"], cs_address=c)
            for c in addresses
        ]

    def _area_map(self, row):
        """Creates a list of tuples with beampaths and their member areas.

        Sets:
            self.area_map
        """

        def parse_beampaths(beampath_csv):
            if beampath_csv is None:
                return []
            beampaths = beampath_csv.replace(" ", "").split(",")
            beampaths = filter(None, beampaths)
            yield from beampaths

        beampath_csv = row["beampath"]
        area = row["area"]
        self.area_map.update(
            set(
                (area, b) for b in parse_beampaths(beampath_csv)
            )
        )
        self.areas.add(area)

    def _devices(self, row):
        """Creates a list of devices and their basic meta.

        Sets:
            self.devices
            self.device_name
        """

        entry = {
            "device_name": row["element"],
            "area": row["area"],
            "device_type": row["keyword"],
            "cs_name": row["control system name"],
        }
        if not all(entry.values()):
            return
        if ":" in row["element"]:
            return
        entry["is_active"] = (
            row["active"] == 'A' if row["active"] is not None else None
        )
        self.devices.append(entry)
        self.device_names.update(entry["device_name"])
        return True

    def _device_meta_float(self, row):
        
        def get_meta_float():
            meta = _DEVICE_META_MAP.get(row["keyword"], []) + _DEFAULT_DEVICE_META
            for column, meta_name in meta:
                entry = {
                    "device_name": row["element"],
                    "device_meta_name": meta_name,
                    "meta_value": row[column],
                }
                if None in entry.values():
                    continue
                yield entry

        new_meta = [m for m in get_meta_float()]
        self.device_meta_float += new_meta
        self.device_meta += [
            {
                "device_name": m["device_name"],
                "device_meta_name": m["device_meta_name"],
                "meta_type": "float",
            }
            for m in new_meta
        ]

    def _device_meta_string(self, row, area_yaml, device_yaml):
        def _fixup_string(value):
            return yaml.safe_dump(value)

        def _parse_meta_string(device_name, meta):
            for meta_name, value in meta.items():
                yv = {
                    "device_name": device_name,
                    "device_meta_name": meta_name,
                    "meta_value": _fixup_string(value),
                }
                if None in yv.values():
                    continue
                yield yv

        def _parse_yaml():
            area = row["area"]
            name = row["element"]
            area_meta = area_yaml.get(row["yaml_type"], {})
            meta = (
                area_meta.get(area, {}) | device_yaml.get(name, {})
            )
            yield from _parse_meta_string(name, meta)

        new_meta = [m for m in _parse_yaml()]
        self.device_meta_string += new_meta
        self.device_meta += [
            {
                "device_name": m["device_name"],
                "device_meta_name": m["device_meta_name"],
                "meta_type": "string",
            }
            for m in new_meta
        ]

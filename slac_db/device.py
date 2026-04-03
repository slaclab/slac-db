import os
import pykern.sql_db
import slac_db.config
import sqlalchemy

_meta = None

def get_all_accessors(device):
    """Get all accessors from a device.
    An accessor is a developer friendly name for a PV
    associated with a single device.

    Args:
        device (str): MAD name for a device.
    """
    with _session() as s:
        return [r for r in s.select(
            sqlalchemy.select(
                s.t.accessors
            ).where(
                s.t.accessors.c["device_name"] == device
            )
        )]

def get_all_addresses(device):
    """Get all accessors from a device.
    An address is a PV.

    Args:
        device (str): MAD name for a device.
    """
    with _session() as s:
        return [r["cs_address"] for r in s.select(
            sqlalchemy.select(
                s.t.addresses
            ).where(
                s.t.addresses.c["device_name"] == device
            )
        )]

def recreate(parser):
    """Rebuilds the device database.
    Fails if a connection has already been made.

    Args:
        Parser object with attribute 'device_address_meta' for row data.
    """
    if _meta:
        raise AssertionError(
            "Database connnection already initialized. "
            + "Restart Python interpreter."
        )
    if not hasattr(parser, "device_address_meta"):
        raise AssertionError(
            "Parser is missing attribute 'device_address_meta'. "
        )
    if os.path.exists(_device_db_location()):
        os.remove(_device_db_location())
    _Inserter(parser)

class _Inserter():
    """Inserts rows into sqllite database.
    """
    def __init__(self, parser):
        self.parser = parser
        with _session() as s:
            self.create_address_db(s)
            self.create_accessor_db(s)

    def create_address_db(self, s):
        for p in self.parser.device_address_meta:
            ins = {}
            ins["device_name"] = p.device_name
            ins["cs_address"] = p.cs_address
            s.insert("addresses", **ins)

    def create_accessor_db(self, s):
        for p in self.parser.device_address_meta:
            if not p.accessor_name:
                continue
            ins = {}
            ins["device_name"] = p.device_name
            ins["accessor_name"] = p.accessor_name
            ins["cs_address"] = p.cs_address
            s.insert("accessors", **ins)

def _db_type_prefix(uri):
    if not uri.startswith("sqlite"):
        uri = 'sqlite:///' + uri
    return uri

def _init_db(location=None):
    """Initializes pykern sqlalchemy wrapper. Initialization
    occurs when a session is first created.

       _meta: wrapper that holds sqlalchemy metadata.
    """
    global _meta
    if location is None:
        location = _device_db_location()
    uri = _db_type_prefix(location)
    schema = {
        "accessors": {
            "device_name": "str 64 primary_key",
            "accessor_name": "str 64 primary_key",
            "cs_address": "str 64",
        },
        "addresses": {
            "device_name": "str 64 primary_key",
            "cs_address": "str 64 primary_key",
        },
    }
    _meta = pykern.sql_db.Meta(
        uri=uri,
        schema=schema
    )

def _device_db_location():
    db_location = (
        slac_db.config.package_data() / 'device.sqlite3'
    )
    return str(db_location)

def _session():
    if _meta is None:
        _init_db()
    return _meta.session()

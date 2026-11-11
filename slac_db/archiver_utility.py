import datetime

import requests

import slac_db.device
from slac_tools.epics_data import get_archived



def get_archived_values_for_pvs(
    pv_names: list[str], at_time: str | datetime.datetime | None = None
) -> dict:
    if at_time is None:
        at_time = datetime.datetime.now(datetime.timezone.utc)

    return get_archived(pv_names, at_time, allow_missing=True)


def get_archived_values_for_device(
    device_name: str, at_time: str | datetime.datetime | None = None
) -> dict:
    accessor_to_pv = slac_db.device.get_all_accessors(device_name)
    pv_to_value = get_archived_values_for_pvs(list(accessor_to_pv.values()), at_time)

    return {
        accessor_name: {"pv": pv_name, "value": pv_to_value[pv_name]}
        for accessor_name, pv_name in accessor_to_pv.items()
    }

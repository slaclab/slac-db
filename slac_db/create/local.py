import collections.abc
import slac_db.aida
import slac_db.oracle
import slac_db.device
from pykern.pkcollections import PKDict

def to_accessor_device():
    return slac_db.device.recreate(_Parser())

class _Parser():
    def __init__(self):
        def address_append(head, tail):
            if tail != '':
                tail = ':' + tail
            return head + tail
        def address_add(table, parts):
            if len(parts) == 0:
                return {'': None}
            a = address_add(table.get(parts[0], {}), parts[1:])
            table.update({parts[0]: a})
            return(table)
        def address_get(table, parts):
            t = table
            for p in parts:
                if p not in t:
                    return None
                t = t[p]
            return t
        def address_list(table):
            o = []
            for key, value in table.items():
                if value is None:
                    return [key]
                else:
                    a = address_list(value)
                    o.extend([address_append(a, end) for end in a])
            return o

        address_map = {}
        for a in slac_db.aida.get_all():
            address_add(address_map, a.split(':'))
        self.device_address_pairs = []
        for d in slac_db.oracle.get_all_address_headers():
            if d is None:
                continue
            t = address_get(address_map, d.split(':'))
            if t is None:
                continue
            l = [address_append(d, a) for a in address_list(t)]
            self.device_address_pairs.append((d, l))

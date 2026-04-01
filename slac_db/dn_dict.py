class DNDict():
    def __init__(self, delimiter=":"):
        self._dn_map = {}
        self._delim = delimiter

    def add(self, address):
        def recur(table, parts):
            if len(parts) == 0:
                return {'': None}
            a = recur(
                table.get(parts[0], {}),
                parts[1:]
            )
            table.update({parts[0]: a})
            return(table)
        p = address.split(self._delim)
        recur(self._dn_map, p)

    def get(self, address):
        tails = self._rebuild(address)
        return [self._cat(address, t) for t in tails]

    def _cat(self, head, tail):
        if tail != '':
            tail = f"{self._delim}{tail}"
        return f"{head}{tail}"

    def _submap(self, address):
        parts = address.split(self._delim)
        t = self._dn_map
        for p in parts:
            if p not in t:
                return None
            t = t[p]
        return t

    def _rebuild(self, address):
        def recur(table):
            o = []
            for key, value in table.items():
                if value is None:
                    return [key]
                else:
                    a = recur(value)
                    o.extend([self._cat(a, end) for end in a])
            return o
        s = self._submap(address)
        return recur(s)

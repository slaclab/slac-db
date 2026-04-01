def deep_update(original, update):
    for k, v in update.items():
        if v is dict:
            original = deep_update(original.get(k, {}), v)
        else:
            original[k] = v
    return original

def address_add(table, parts):
    if len(parts) == 0:
        return {'': None}
    a = address_add(table.get(parts[0], {}), parts[1:])
    table.update({parts[0]: a})
    return(table)

t = {}
p = ['a', 'b', 'c', 'd']
i = ['a', 'b', 'c', 'd', '1']
address_add(address_add(t, p), i)
print(t)

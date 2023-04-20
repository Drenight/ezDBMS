from sortedcontainers import SortedDict

sd = SortedDict({'a': 1, 'b': 2, 'c': 3, 'd': 4})

def rank(sd, key):
    pos = sd.bisect_left(key)
    return pos + sum(1 for k in sd.keys()[:pos] if sd[k] == sd[key])

print(rank(sd, 'b'))

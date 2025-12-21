# src/utils/hash.py
from blake3 import blake3

def hash_hex(t: str) -> str:
    return blake3(t.encode("utf-8")).hexdigest()

def ordered_sum(t: dict) -> str:
    s = dict(sorted(t.items()))
    m = ''
    for t in s.values():
        if isinstance(t, list): t = "".join(t)
        if isinstance(t, dict):
            d = dict(sorted(t.items()))
            for t1 in d.values():
                m += str(t1)
            continue
        m += str(t)
    return m
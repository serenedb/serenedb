import json


def load_queries(path, limit):
    out = []
    with open(path) as f:
        for line in f:
            out.append(json.loads(line)["query"])
            if limit and len(out) == limit:
                break
    return out


def node_cpus(node):
    with open(f"/sys/devices/system/node/node{node}/cpulist") as f:
        spec = f.read().strip()
    cpus = []
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        cpus.extend(range(int(lo), int(hi or lo) + 1))
    return cpus


def cpu_busy(cpus):
    busy = 0
    with open("/proc/stat") as f:
        for line in f:
            name, *fields = line.split()
            if name.startswith("cpu") and name[3:].isdigit() and int(name[3:]) in cpus:
                values = list(map(int, fields))
                busy += sum(values) - values[3] - values[4]
    return busy


def pct(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(q * (len(values) - 1) + 0.5))]

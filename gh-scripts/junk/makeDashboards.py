#!/usr/bin/python3

"""A script to automatically make dashboards for Grafana."""

import os
import sys
import json

from yaml import load, YAMLError
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

# Usage:

def print_usage():
    """Print usage."""
    print("""Usage: scripts/makeDashboards.py [-p PERSONA] [-h] [--help]

    where PERSONA is one of:
      - "dbadmin": Database administrator
      - "sysadmin": System administrator
      - "user": Pure user of SereneDB
      - "customer": Customer of Oasis
      - "appdeveloper": Application developer
      - "support": Oasis on call personnel
      - "serenedbdeveloper": SereneDB developer
      - "all": Produce all metrics (default)

    use -h or --help for this help.
""")

# Check that we are in the right place:
LS_HERE = os.listdir(".")
if not("server" in LS_HERE and "clients" in LS_HERE and \
        "resources" in LS_HERE and "CMakeLists.txt" in LS_HERE):
    print("Please execute me in the main source dir!", file=sys.stderr)
    sys.exit(1)

YAMLFILE = "resources/metrics/allMetrics.yaml"

# Database about categories and personas and their interests:

CATEGORYNAMES = ["Health", "AQL", "Transactions", \
                 "Statistics", "Replication", "Disk", "Errors", \
                 "RocksDB", "Backup", "k8s", "Connectivity", "Network",\
                 "Agency", "Scheduler", "Maintenance", "kubeserenedb", "Search"]

COMPLEXITIES = ["none", "simple", "medium", "advanced"]

PERSONAINTERESTS = {}
PERSONAINTERESTS["all"] = \
    {"Health":       "advanced", \
     "AQL":          "advanced", \
     "Transactions": "advanced", \
     "Statistics":   "advanced", \
     "Replication":  "advanced", \
     "Disk":         "advanced", \
     "Errors":       "advanced", \
     "RocksDB":      "advanced", \
     "Backup":       "advanced", \
     "k8s":          "advanced", \
     "Connectivity": "advanced", \
     "Network":      "advanced", \
     "Agency":       "advanced", \
     "Scheduler":    "advanced", \
     "Maintenance":  "advanced", \
     "kubeserenedb": "advanced", \
     "Search":       "advanced", \
    }
PERSONAINTERESTS["dbadmin"] = \
    {"Health":       "advanced", \
     "AQL":          "advanced", \
     "Transactions": "simple", \
     "Statistics":   "medium", \
     "Replication":  "advanced", \
     "Disk":         "advanced", \
     "Errors":       "simple", \
     "RocksDB":      "simple", \
     "Backup":    "simple", \
     "k8s":          "medium", \
     "Connectivity": "simple", \
     "Network":      "medium", \
     "Agency":       "none", \
     "Scheduler":    "none", \
     "Maintenance":  "none", \
     "kubeserenedb": "medium", \
     "Search": "advanced", \
    }
PERSONAINTERESTS["sysadmin"] = \
    {"Health":       "advanced", \
     "AQL":          "simple", \
     "Transactions": "simple", \
     "Statistics":   "medium", \
     "Replication":  "advanced", \
     "Disk":         "advanced", \
     "Errors":       "simple", \
     "RocksDB":      "none", \
     "Backup":       "none", \
     "k8s":          "medium", \
     "Connectivity": "simple", \
     "Network":      "medium", \
     "Agency":       "none", \
     "Scheduler":    "none", \
     "Maintenance":  "none", \
     "kubeserenedb": "simple", \
     "Search":       "simple", \
    }
PERSONAINTERESTS["user"] = \
    {"Health":       "simple", \
     "AQL":          "medium", \
     "Transactions": "advanced", \
     "Statistics":   "simple", \
     "Replication":  "simple", \
     "Disk":         "simple", \
     "Errors":       "medium", \
     "RocksDB":      "none", \
     "Backup":       "none", \
     "k8s":          "medium", \
     "Connectivity": "simple", \
     "Network":      "simple", \
     "Agency":       "none", \
     "Scheduler":    "none", \
     "Maintenance":  "none", \
     "kubeserenedb": "none", \
     "Search":       "simple", \
    }
PERSONAINTERESTS["customer"] = \
    {"Health":       "simple", \
     "AQL":          "medium", \
     "Transactions": "advanced", \
     "Statistics":   "simple", \
     "Replication":  "medium", \
     "Disk":         "simple", \
     "Errors":       "medium", \
     "RocksDB":      "none", \
     "Backup":       "simple", \
     "k8s":          "none", \
     "Connectivity": "none", \
     "Network":      "simple", \
     "Agency":       "none", \
     "Scheduler":    "none", \
     "Maintenance":  "none", \
     "kubeserenedb": "none", \
     "Search":       "simple", \
    }
PERSONAINTERESTS["appdeveloper"] = \
    {"Health":       "medium", \
     "AQL":          "advanced", \
     "Transactions": "advanced", \
     "Statistics":   "medium", \
     "Replication":  "simple", \
     "Disk":         "simple", \
     "Errors":       "advanced", \
     "RocksDB":      "simple", \
     "Backup":    "none", \
     "k8s":          "simple", \
     "Connectivity": "none", \
     "Network":      "medium", \
     "V8":           "advanced", \
     "Agency":       "none", \
     "Scheduler":    "simple", \
     "Maintenance":  "none", \
     "kubeserenedb": "none", \
     "Search":       "simple", \
    }
PERSONAINTERESTS["support"] = \
    {"Health":       "advanced", \
     "AQL":          "medium", \
     "Transactions": "medium", \
     "Statistics":   "medium", \
     "Replication":  "medium", \
     "Disk":         "advanced", \
     "Errors":       "medium", \
     "RocksDB":      "medium", \
     "Backup":       "medium", \
     "k8s":          "medium", \
     "Connectivity": "medium", \
     "Network":      "medium", \
     "Agency":       "simple", \
     "Scheduler":    "medium", \
     "Maintenance":  "simple", \
     "kubeserenedb": "medium", \
     "Search":       "simple", \
    }
PERSONAINTERESTS["serenedbdeveloper"] = \
    {"Health":       "advanced", \
     "AQL":          "advanced", \
     "Transactions": "advanced", \
     "Statistics":   "advanced", \
     "Replication":  "advanced", \
     "Disk":         "advanced", \
     "Errors":       "advanced", \
     "RocksDB":      "advanced", \
     "Backup":    "advanced", \
     "k8s":          "advanced", \
     "Connectivity": "advanced", \
     "Network":      "advanced", \
     "Agency":       "advanced", \
     "Scheduler":    "advanced", \
     "Maintenance":  "advanced", \
     "kubeserenedb": "advanced", \
     "Search":       "advanced", \
    }

# Parse command line options:

PERSONA = "all"
i = 1
while i < len(sys.argv):
    if sys.argv[i] == "-h" or sys.argv[i] == "--help":
        print_usage()
        sys.exit(0)
    if sys.argv[i] == "-p":
        if i+1 < len(sys.argv):
            P = sys.argv[i+1]
            if P in PERSONAINTERESTS:
                PERSONA = P
            else:
                print("Warning: persona " + P + " not known.", file=sys.stderr)
                sys.exit(1)
            i += 2
        else:
            i += 1

# Now read all metrics from the `allMetrics.yaml` file:

try:
    ALLMETRICSFILE = open(YAMLFILE)
except FileNotFoundError:
    print("Could not open file '" + YAMLFILE + "'!", file=sys.stderr)
    sys.exit(1)

try:
    ALLMETRICS = load(ALLMETRICSFILE, Loader=Loader)
except YAMLError as err:
    print("Could not parse YAML file '" + YAMLFILE + "', error:\n" + str(err), file=sys.stderr)
    sys.exit(2)

ALLMETRICSFILE.close()

# And generate the output:

CATEGORIES = {}
METRICS = {}
for m in ALLMETRICS:
    c = m["category"]
    if not c in CATEGORYNAMES:
        print("Warning: Found category", c, "which is not in our current list!", file=sys.stderr)
    if not c in CATEGORIES:
        CATEGORIES[c] = []
    CATEGORIES[c].append(m["name"])
    METRICS[m["name"]] = m

POSX = 0
POSY = 0

OUT = {"panels" : []}
PANELS = OUT["panels"]

def incxy(x, y):
    """Increment coordinates."""
    x += 12
    if x >= 24:
        x = 0
        y += 8
    return x, y

def make_panel(x, y, mett):
    """Make a panel."""
    title = mett["help"]
    while title[-1:] == "." or title[-1:] == "\n":
        title = title[:-1]
    return {"gridPos": {"h": 8, "w": 12, "x": x, "y": y}, \
            "description": mett["description"], \
            "title": title}

for c in CATEGORYNAMES:
    if c in CATEGORIES:
        ca = CATEGORIES[c]
        if len(ca) > 0:
            # Find complexity interest of persona:
            complexitylimit = COMPLEXITIES.index(PERSONAINTERESTS[PERSONA][c])
            # Make row:
            row = {"gridPos": {"h":1, "w": 24, "x": 0, "y": POSY}, \
                   "panels": [], "title": c, "type": "row"}
            POSX = 0
            POSY += 1
            PANELS.append(row)
            for name in ca:
                met = METRICS[name]
                if COMPLEXITIES.index(met["complexity"]) <= complexitylimit:
                    panel = make_panel(POSX, POSY, met)
                    if met["type"] == "counter":
                        panel["type"] = "graph"
                        panel["targets"] = [{"expr": "rate(" + met["name"] + \
                                                     "[3m])", \
                            "legendFormat": "{{instance}}:{{shortname}}"}]
                        POSX, POSY = incxy(POSX, POSY)
                        PANELS.append(panel)
                    elif met["type"] == "gauge":
                        panel["type"] = "graph"
                        panel["targets"] = [{"expr": met["name"], \
                            "legendFormat": "{{instance}}:{{shortname}}"}]
                        POSX, POSY = incxy(POSX, POSY)
                        PANELS.append(panel)
                    elif met["type"] == "histogram":
                        panel["type"] = "heatmap"
                        panel["legend"] = {"show": False}
                        panel["targets"] = [\
                            {"expr": "histogram_quantile(0.95, sum(rate(" + \
                             met["name"] + "_bucket[3m])) by (le))", \
                             "format": "heatmap", \
                             "legendFormat": ""}]
                        POSX, POSY = incxy(POSX, POSY)
                        PANELS.append(panel)
                        panel = make_panel(POSX, POSY, met)
                        panel["title"] = panel["title"] + " (count of events per second)"
                        panel["type"] = "graph"
                        panel["targets"] = [\
                            {"expr": "rate(" + met["name"] + "_count[3m])", \
                             "legendFormat": "{{instance}}:{{shortname}}"}]
                        POSX, POSY = incxy(POSX, POSY)
                        PANELS.append(panel)
                        panel = make_panel(POSX, POSY, met)
                        panel["title"] = panel["title"] + " (average per second)"
                        panel["type"] = "graph"
                        panel["targets"] = [\
                            {"expr": "rate(" + met["name"] + "_sum[3m])" + \
                                      " / rate(" + met["name"] + "_count[3m])", \
                              "legendFormat": "{{instance}}:{{shortname}}"}]
                        POSX, POSY = incxy(POSX, POSY)
                        PANELS.append(panel)
                    else:
                        print("Strange metrics type:", met["type"], file=sys.stderr)

json.dump(OUT, sys.stdout)

import ops as ops_mod
import scenarios as scenarios_mod
from rng import derive

PROBE_ITERATIONS = 20000
MIN_WINDOW_SECONDS = 5.0


def reachable_op_kinds(scenario, iterations=PROBE_ITERATIONS, seed=1):
    pick = scenarios_mod.resolve(scenario)
    state = scenarios_mod.WorkerState(ops_mod.NameGen("probe", 0))
    stream = derive(seed, 0)
    kinds = set()
    for _ in range(iterations):
        op = pick(stream, state)
        kinds.add(op.kind)
        state.note_created(op)
        state.note_rows(op)
        state.note_dropped(op)
    return kinds


def report(scenario, attempted_kinds, windows, faults_available, faults_used,
           committed, quiesces):
    reachable = reachable_op_kinds(scenario)
    never = sorted(reachable - set(attempted_kinds))
    unexpected = sorted(set(attempted_kinds) - reachable)

    # Windows are (committed_ops, seconds). Compare RATES, and only across
    # windows long enough to mean anything: the tail window after the last
    # quiesce is a sliver, and comparing its raw count against a full window
    # reports a collapse that never happened.
    usable = [(n, secs) for (n, secs) in windows if secs >= MIN_WINDOW_SECONDS]
    rates = [n / secs for (n, secs) in usable if secs > 0]
    first_rate = rates[0] if rates else None
    last_rate = rates[-1] if len(rates) >= 2 else None

    data = {
        "scenario": scenario,
        "op_kinds_reachable": sorted(reachable),
        "op_kinds_attempted": sorted(set(attempted_kinds)),
        "op_kinds_never_attempted": never,
        "op_kinds_unexpected": unexpected,
        "committed_ops": committed,
        "quiesces": quiesces,
        "windows": [{"ops": n, "seconds": round(secs, 1),
                     "ops_per_s": round(n / secs, 1) if secs > 0 else None}
                    for (n, secs) in windows],
        "windows_usable": len(usable),
        "rate_first": round(first_rate, 1) if first_rate else None,
        "rate_last": round(last_rate, 1) if last_rate else None,
        "faults_available": len(faults_available or ()),
        "faults_used": sorted(set(faults_used or ())),
    }

    findings = []
    if never:
        findings.append({
            "kind": "coverage_op_family_never_attempted",
            "key": None,
            "detail": f"scenario '{scenario}' can emit {sorted(never)} but this run "
                      f"never did; coverage shrank without anything going red",
            "candidates": None, "observed": None,
        })
    if unexpected:
        findings.append({
            "kind": "coverage_unexpected_op_kind",
            "key": None,
            "detail": f"this run emitted {unexpected}, which the scenario's own "
                      f"generator does not produce; the two have drifted apart",
            "candidates": None, "observed": None,
        })
    if first_rate and last_rate is not None and last_rate * 10 < first_rate:
        findings.append({
            "kind": "insufficient_pressure",
            "key": None,
            "detail": f"committed-op rate collapsed from {first_rate:.0f}/s in the "
                      f"first window to {last_rate:.0f}/s in the last; the run "
                      f"stopped doing work without failing. Measured against this "
                      f"run's own first window, never an absolute rate, and only "
                      f"across windows of at least {MIN_WINDOW_SECONDS}s.",
            "candidates": None, "observed": round(last_rate, 1),
        })
    return data, findings


def render(data):
    lines = ["", "===== coverage ====="]
    lines.append(f"  scenario            {data['scenario']}")
    lines.append(f"  committed ops       {data['committed_ops']}")
    lines.append(f"  windows             {data['windows']}")
    if data.get("rate_first") and data.get("rate_last"):
        lines.append(f"  rate first/last     {data['rate_first']}/s -> "
                     f"{data['rate_last']}/s")
    lines.append(f"  op kinds            {len(data['op_kinds_attempted'])}"
                 f"/{len(data['op_kinds_reachable'])} reachable attempted")
    if data["op_kinds_never_attempted"]:
        lines.append(f"  NEVER attempted     {data['op_kinds_never_attempted']}")
    lines.append(f"  fault points        {len(data['faults_used'])} used of "
                 f"{data['faults_available']} defined in the tree")
    if data["faults_used"]:
        lines.append(f"  faults used         {data['faults_used']}")
    return "\n".join(lines)

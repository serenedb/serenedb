import dataclasses


@dataclasses.dataclass
class Profile:
    name: str
    seconds: int = 60
    workers: int = 4
    scenario: str = "ddl_churn"
    quiesce_every: float = 20.0
    op_deadline_s: float = 30.0
    probe_interval_s: float = 2.0
    probe_timeout_s: float = 10.0
    wedge_confirmations: int = 2
    freeze_sample_gap_s: float = 4.0
    faults_enabled: bool = False
    restarts: int = 0
    table_cap: int = 12
    other_cap: int = 8
    cancels: int = 0
    compaction_windows: int = 0
    parks: int = 0
    graceful_restarts: int = 0
    conflict_ceiling: float = 0.35
    max_retries: int = 5
    livelock_retry_limit: int = 20
    datadir_root: str = "/dev/shm"


PROFILES = {
    "smoke": Profile(
        name="smoke", seconds=45, workers=4, quiesce_every=15.0,
    ),
    "soak": Profile(
        name="soak", seconds=900, workers=8, quiesce_every=60.0,
        faults_enabled=True, restarts=3, parks=2, graceful_restarts=1,
    ),
    "soak-tsan": Profile(
        name="soak-tsan", seconds=900, workers=4, quiesce_every=90.0,
        op_deadline_s=120.0, probe_timeout_s=30.0, faults_enabled=True,
    ),
    "scale": Profile(
        name="scale", seconds=600, workers=4, quiesce_every=120.0,
        table_cap=2500, other_cap=400, op_deadline_s=180.0,
        probe_timeout_s=60.0,
    ),
    "cancel": Profile(
        name="cancel", seconds=180, workers=4, quiesce_every=45.0, cancels=12,
    ),
    "iceberg": Profile(
        name="iceberg", seconds=120, workers=3, scenario="iceberg_views",
        quiesce_every=40.0, other_cap=6, op_deadline_s=120.0,
    ),
    "remote": Profile(
        name="remote", seconds=120, workers=3, scenario="foreign_servers",
        quiesce_every=40.0,
    ),
    "attach": Profile(
        name="attach", seconds=120, workers=3, scenario="attach_churn",
        quiesce_every=40.0, op_deadline_s=90.0,
    ),
    "server-race": Profile(
        name="server-race", seconds=120, workers=4, scenario="server_race",
        quiesce_every=40.0, op_deadline_s=90.0,
    ),
    # Everything at once, every chaos knob armed. The point is to break the
    # server, so a clean run here is the surprising outcome.
    "break-everything": Profile(
        name="break-everything", seconds=300, workers=4,
        scenario="break_everything", quiesce_every=75.0, op_deadline_s=120.0,
        faults_enabled=True, restarts=2, parks=1, graceful_restarts=1,
        cancels=6, compaction_windows=1, other_cap=6,
    ),
    "compaction-probe": Profile(
        name="compaction-probe", seconds=120, workers=4, quiesce_every=60.0,
        compaction_windows=2, scenario="ddl_churn",
    ),
    "wedge-probe": Profile(
        name="wedge-probe", seconds=180, workers=4, scenario="serial_churn",
        quiesce_every=60.0,
    ),
}


SCENARIO_CONFLICT_CEILING = {
    "shared_arena": 0.97,
    "name_reuse": 0.60,
}


def conflict_ceiling_for(profile, scenario):
    return SCENARIO_CONFLICT_CEILING.get(scenario, profile.conflict_ceiling)


ICEBERG_FIXTURE_DIR = "resources/tests/iceberg"
ICEBERG_PREFERRED = ("plain_v1", "part_v1", "plain_v2", "part_v2")


def iceberg_fixtures(repo):
    import os
    root = os.path.join(repo, ICEBERG_FIXTURE_DIR)
    if not os.path.isdir(root):
        return []
    have = set(os.listdir(root))
    picked = [os.path.join(root, n) for n in ICEBERG_PREFERRED if n in have]
    if picked:
        return picked
    return [os.path.join(root, n) for n in sorted(have)
            if os.path.isdir(os.path.join(root, n))][:4]


SCENARIOS_NEEDING_ICEBERG = frozenset({"iceberg_views"})


def resolve(name, **overrides):
    base = PROFILES.get(name)
    if base is None:
        raise SystemExit(f"unknown profile '{name}'; have {sorted(PROFILES)}")
    fields = dataclasses.asdict(base)
    for k, v in overrides.items():
        if v is not None and k in fields:
            fields[k] = v
    return Profile(**fields)

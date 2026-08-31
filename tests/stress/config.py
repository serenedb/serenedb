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
    max_retries: int = 5
    livelock_retry_limit: int = 20
    datadir_root: str = "/dev/shm"


PROFILES = {
    "smoke": Profile(
        name="smoke", seconds=45, workers=4, quiesce_every=15.0,
    ),
    "soak": Profile(
        name="soak", seconds=900, workers=8, quiesce_every=60.0,
        faults_enabled=True, restarts=3,
    ),
    "soak-tsan": Profile(
        name="soak-tsan", seconds=900, workers=4, quiesce_every=90.0,
        op_deadline_s=120.0, probe_timeout_s=30.0, faults_enabled=True,
    ),
    "wedge-probe": Profile(
        name="wedge-probe", seconds=180, workers=4, scenario="serial_churn",
        quiesce_every=60.0,
    ),
}


def resolve(name, **overrides):
    base = PROFILES.get(name)
    if base is None:
        raise SystemExit(f"unknown profile '{name}'; have {sorted(PROFILES)}")
    fields = dataclasses.asdict(base)
    for k, v in overrides.items():
        if v is not None and k in fields:
            fields[k] = v
    return Profile(**fields)

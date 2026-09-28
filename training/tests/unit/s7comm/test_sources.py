"""The capture pins against the spec and the shipped config (final review I3): every capture the
config names is pinned, and the QUT attack run is the spec's master.pcap, whose HMI connection
(258,790 records) and attacker (10.10.10.66) the hmi.pcap vantage point lacks."""
from pathlib import Path

from netsec_ml.s7comm import data as D

TRAINING = Path(__file__).resolve().parents[3]


def pins():
    """capture -> (sha256, kind, location), from training/s7comm/sources.sha256."""
    out = {}
    for line in (TRAINING / "s7comm" / "sources.sha256").read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#"):
            capture, sha, kind, location = line.split()
            out[capture] = (sha, kind, location)
    return out


def test_every_configured_capture_is_pinned():
    cfg = D.load_config(TRAINING / "configs" / "s7comm-v2.yaml")
    named = {c for s in D.sources_of(cfg) for c in s.captures}
    assert named <= set(pins()), sorted(named - set(pins()))


def test_the_qut_attack_run_is_the_spec_named_master_capture():
    sha, kind, location = pins()["qut-attack"]
    assert (kind, location) == ("qut-zip", "LabelledDataset/20161215163606_s7_process_attacks/master.pcap.zip")
    assert sha == "c5d520e61fb5d20d20201afc80abd40090b0bc6ce7d5b07384e43060ace30279"

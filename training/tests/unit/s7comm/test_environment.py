"""The training environment itself: the pinned CPU PyTorch, and the package importable."""
import torch

import netsec_ml.s7comm


def test_torch_is_the_pinned_cpu_build():
    # Ruling A3: 2.5.1, CPU only (spec section 6).
    assert torch.__version__.startswith("2.5.1")
    assert not torch.cuda.is_available()


def test_the_package_is_importable():
    assert netsec_ml.s7comm.__doc__

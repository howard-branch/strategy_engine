"""Run all signals combined with equal (or custom) weights."""

from experiments.runner import run_experiment
from signals import make_signal, list_signals
from signals.combiner import CombinedSignal, CombinedSignalConfig


def main(weights: dict[str, float] | None = None) -> None:
    if weights is None:
        weights = {name: 1.0 for name in list_signals()}

    signals = {name: make_signal(name) for name in weights}
    combined = CombinedSignal(signals=signals, config=CombinedSignalConfig(weights=weights))

    label = "Combined (" + ", ".join(f"{k}={v:.2f}" for k, v in weights.items()) + ")"
    run_experiment(combined, label=label)


if __name__ == "__main__":
    main()


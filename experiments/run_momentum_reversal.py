from experiments.runner import run_experiment
from signals import make_signal
from signals.combiner import CombinedSignal, CombinedSignalConfig


def main() -> None:

    combined = CombinedSignal(
        signals={
            "momentum": make_signal("momentum"),
            "reversal": make_signal("reversal"),
        },
        config=CombinedSignalConfig(weights={"momentum": 1.0, "reversal": 0.3}),
    )
    run_experiment(combined, label="Momentum + Reversal (0.3)")


if __name__ == "__main__":
    main()
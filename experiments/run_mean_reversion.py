from experiments.runner import run_experiment
from signals import make_signal


def main() -> None:
    signal = make_signal("mean_reversion")
    run_experiment(signal, label="Mean Reversion")


if __name__ == "__main__":
    main()

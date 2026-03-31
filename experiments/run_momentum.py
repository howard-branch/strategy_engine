from experiments.runner import run_experiment
from signals import make_signal


def main() -> None:
    signal = make_signal("momentum")
    run_experiment(signal, label="Momentum")


if __name__ == "__main__":
    main()
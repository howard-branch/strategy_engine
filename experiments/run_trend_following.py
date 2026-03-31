from experiments.runner import run_experiment
from signals import make_signal


def main() -> None:
    signal = make_signal("trend_following")
    run_experiment(signal, label="Trend Following")


if __name__ == "__main__":
    main()

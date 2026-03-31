from experiments.runner import run_experiment
from signals import make_signal


def main() -> None:
    signal = make_signal("low_volatility")
    run_experiment(signal, label="Low Volatility")


if __name__ == "__main__":
    main()

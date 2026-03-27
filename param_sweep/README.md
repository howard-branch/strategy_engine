# Parameter Sweep
Optimize strategy parameters through systematic backtesting.
## Scripts
### `sweep_reversal_ratio.py`
Simple parameter sweep that tests different `reversal_ratio` values (0.0 to 1.0) and identifies the optimal ratio based on Sharpe ratio.
**Usage:**
```bash
python -m param_sweep.sweep_reversal_ratio
```
**Output:** `reversal_ratio_sweep_results.csv` - Results for each ratio tested
---
### `advanced_sweep_reversal_ratio.py`
Advanced version with customizable sweep parameters and optimization metrics.
**Features:**
- Custom ratio ranges and step sizes
- Multiple optimization metrics (sharpe, total_return, annualised_return)
- Detailed output and analysis
**Usage:**
```bash
python -m param_sweep.advanced_sweep_reversal_ratio
```
To customize, edit the `SweepConfig` in the script:
```python
config = SweepConfig(
    ratio_min=0.0,
    ratio_max=1.0,
    ratio_step=0.1,
    optimization_metric="sharpe",
)
```
---
### `apply_optimal_ratio.py`
Apply the best-performing ratio from sweep results back to your strategy.
**Usage:**
```bash
python -m param_sweep.apply_optimal_ratio
```
---
## How It Works
1. Loads market data and calculates momentum & reversal signals
2. For each parameter value, combines signals and runs backtest
3. Calculates performance metrics (Sharpe, return, volatility, drawdown)
4. Saves results and identifies optimal parameters
## Results
Results are saved to `reversal_ratio_sweep_results.csv` containing:
- `reversal_ratio` - Parameter value tested
- `total_return` - Total strategy return
- `annualised_return` - Annualized return
- `annualised_vol` - Annualized volatility
- `sharpe` - Sharpe ratio
- `max_drawdown` - Maximum drawdown
- `avg_daily_turnover` - Average daily portfolio turnover

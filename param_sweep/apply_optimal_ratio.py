"""
Utility to apply optimized reversal_ratio back to the main strategy script.

This script reads the parameter sweep results and updates run_momentum_reversal.py
with the optimal reversal_ratio value.
"""

import pandas as pd
import argparse
from pathlib import Path


def apply_optimal_ratio(
    results_csv: str,
    target_file: str = "run_momentum_reversal.py",
    metric: str = "sharpe",
) -> None:
    """
    Read the best reversal_ratio from sweep results and update the target file.
    
    Args:
        results_csv: Path to the CSV file with sweep results
        target_file: Path to the script to update
        metric: The metric to optimize by (default: sharpe)
    """
    # Read results
    results_df = pd.read_csv(results_csv)
    
    if results_df.empty:
        print("Error: Results file is empty")
        return
    
    # Find best by metric
    if metric not in results_df.columns:
        print(f"Error: Metric '{metric}' not found in results")
        print(f"Available metrics: {', '.join(results_df.columns)}")
        return
    
    best_idx = results_df[metric].idxmax()
    best_row = results_df.iloc[best_idx]
    best_ratio = best_row["reversal_ratio"]
    best_metric_value = best_row[metric]
    
    print(f"Optimal reversal_ratio: {best_ratio:.2f}")
    print(f"  {metric.upper()}: {best_metric_value:.6f}")
    print(f"  Annualised Return: {best_row.get('annualised_return', 'N/A')}")
    print(f"  Annualised Vol: {best_row.get('annualised_vol', 'N/A')}")
    
    # Read target file
    target_path = Path(target_file)
    if not target_path.exists():
        print(f"Error: File {target_file} not found")
        return
    
    with open(target_path, 'r') as f:
        content = f.read()
    
    # Update the reversal_ratio line
    old_line = "    reversal_ratio = 0.1"
    new_line = f"    reversal_ratio = {best_ratio:.1f}"
    
    if old_line not in content:
        print(f"Warning: Could not find '{old_line}' in {target_file}")
        print("Manual update may be needed.")
        return
    
    updated_content = content.replace(old_line, new_line)
    
    # Write back
    with open(target_path, 'w') as f:
        f.write(updated_content)
    
    print(f"\n✓ Successfully updated {target_file}")
    print(f"  Changed: {old_line} → {new_line}")


def main():
    parser = argparse.ArgumentParser(
        description="Apply optimal reversal_ratio to strategy script"
    )
    parser.add_argument(
        "--results",
        default="reversal_ratio_sweep_results.csv",
        help="Path to sweep results CSV (default: reversal_ratio_sweep_results.csv)"
    )
    parser.add_argument(
        "--target",
        default="run_momentum_reversal.py",
        help="Path to script to update (default: run_momentum_reversal.py)"
    )
    parser.add_argument(
        "--metric",
        default="sharpe",
        choices=["sharpe", "total_return", "annualised_return"],
        help="Metric to optimize by (default: sharpe)"
    )
    
    args = parser.parse_args()
    
    apply_optimal_ratio(
        results_csv=args.results,
        target_file=args.target,
        metric=args.metric,
    )


if __name__ == "__main__":
    main()


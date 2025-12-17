#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Generate random run combinations for power analysis
# Outputs: A text file with 10 lines, each containing a random selection of runs
# -----------------------------------------------------------------------------

import random
import argparse


def main():
    parser = argparse.ArgumentParser(
        description='Generate 10 random run combinations for GLM power analysis'
    )
    parser.add_argument(
        '-num_of_runs',
        type=int,
        required=True,
        help='Number of runs per combination (e.g., 1, 2, 5, 10)'
    )
    parser.add_argument(
        '-total_runs',
        type=int,
        default=10,
        help='Total number of available runs (default: 10)'
    )
    parser.add_argument(
        '-output',
        type=str,
        default='run_combinations.txt',
        help='Output file name (default: run_combinations.txt)'
    )
    parser.add_argument(
        '-seed',
        type=int,
        default=None,
        help='Random seed for reproducibility (optional)'
    )
    
    args = parser.parse_args()
    
    # Always generate 10 combinations
    n_iterations = 10
    
    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
    
    # Generate available runs (1 to total_runs)
    available_runs = list(range(1, args.total_runs + 1))
    
    # Generate 10 random combinations
    combinations = []
    for i in range(n_iterations):
        selected = random.sample(available_runs, args.num_of_runs)
        #selected.sort()
        combinations.append(selected)
    
    # Write to file
    with open(args.output, 'w') as f:
        for combo in combinations:
            # Write as space-separated numbers
            f.write(' '.join(map(str, combo)) + '\n')
    
    # Print summary
    print(f"Generated 10 random combinations of {args.num_of_runs} run(s)")
    print(f"Output saved to: {args.output}")
    print("\nAll 10 combinations:")
    for i, combo in enumerate(combinations, 1):
        print(f"  {i}: {combo}")


if __name__ == '__main__':
    main()
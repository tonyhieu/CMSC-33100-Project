#!/usr/bin/env python3
import argparse
import copy
import csv
import io
import math
import os
import random
from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np

from src.AlgoFIFO import AlgoFIFO
from src.AlgoPCS import AlgoPCS
from src.AlgoPreemptivePriorityQueue import AlgoPreemptivePriorityQueue, PriorityType
from src.Scheduler import Scheduler
from src.SimulatedJob import SimulatedJob
from src.PCSSearchAlgo import PCSParameterSearch


@dataclass(frozen=True)
class AlgorithmConfig:
    name: str
    family: str
    w: float | None = None
    zeta: float | None = None


def parse_float_list(raw_value):
    return [float(item.strip()) for item in raw_value.split(",") if item.strip()]


def build_algorithm_configs(w_values, zeta_values):
    configs = [
        AlgorithmConfig(name="FIFO", family="FIFO"),
        AlgorithmConfig(name="PPQ", family="PPQ"),
    ]
    for w_value in w_values:
        for zeta_value in zeta_values:
            configs.append(
                AlgorithmConfig(
                    name=f"PCS[w={w_value:.2f},z={zeta_value:.2f}]",
                    family="PCS",
                    w=w_value,
                    zeta=zeta_value,
                )
            )
    return configs


def override_demand_function(job, demand_exponent):
    total_expected_length = job.getTotalExpectedLength()
    job.demandFunction = {}
    for allocation in range(1, job.nThreads + 1):
        # Calculate the baseline theoretical demand
        base_demand = total_expected_length / (allocation ** demand_exponent)
        
        # Inject random noise (e.g., +/- 15%) so the curve isn't perfectly smooth
        noise_factor = random.uniform(0.85, 1.15)
        
        job.demandFunction[allocation] = max(1e-6, base_demand * noise_factor)

def generate_workload(args, seed, demand_exponent):
    random.seed(seed)
    np.random.seed(seed)

    inter_arrival_times = np.random.exponential(scale=args.submission_time / args.jobs, size=args.jobs)
    sampled_submission_times = np.cumsum(inter_arrival_times)
    sampled_thread_numbers = np.random.poisson(lam=args.avg_threads - 1, size=args.jobs) + 1

    job_variance_factor = args.jobs_ld ** 2
    sigma_jobs = np.sqrt(np.log(1.0 + job_variance_factor))
    mu_jobs = np.log(args.avg_length) - 0.5 * (sigma_jobs**2)
    job_thread_lengths = np.random.lognormal(mean=mu_jobs, sigma=sigma_jobs, size=args.jobs)

    jobs = []
    global_semaphores = []
    for job_id in range(args.jobs):
        # Correct lognormal generation for threads
        thread_variance_factor = args.threads_ld ** 2
        sigma_threads = np.sqrt(np.log(1.0 + thread_variance_factor))
        mu_threads = np.log(job_thread_lengths[job_id]) - 0.5 * (sigma_threads**2)
        expected_lengths = np.random.lognormal(
            mean=mu_threads,
            sigma=sigma_threads,
            size=int(sampled_thread_numbers[job_id]),
        )
        job = SimulatedJob(
            job_id,
            float(sampled_submission_times[job_id]),
            int(sampled_thread_numbers[job_id]),
            expected_lengths,
            args.uncertainty,
            args.sem_prob,
            args.mutex_prob,
            global_semaphores,
        )
        override_demand_function(job, demand_exponent)
        jobs.append(job)
    return jobs, global_semaphores


def run_single_configuration(config, jobs, global_semaphores, args):
    job_copy = copy.deepcopy(jobs)
    semaphore_copy = copy.deepcopy(global_semaphores)

    if config.family == "FIFO":
        algorithm = AlgoFIFO(args.cores, semaphore_copy)
    elif config.family == "PPQ":
        algorithm = AlgoPreemptivePriorityQueue(
            args.cores,
            PriorityType.expectedLength,
            semaphore_copy,
            args.minimum_running_time,
        )
    elif config.family == "PCS":
        algorithm = AlgoPCS(
            args.cores,
            semaphore_copy,
            nQueues=args.nqueues,
            W=config.w,
            zetaMin=config.zeta,
            thresholds=[1, 28, 32, 36] 
        )
    else:
        raise ValueError(f"Unsupported algorithm family: {config.family}")

    scheduler = Scheduler(algorithm, job_copy)
    with redirect_stdout(io.StringIO()):
        scheduler.createSchedule()
        schedule_performance = scheduler.evaluateSchedule(verbose=False)

    return {
        "efficiency": float(schedule_performance.efficiency),
        "predictability": float(schedule_performance.predictability),
        "fairness": float(schedule_performance.fairness),
        "avg_jct": float(schedule_performance.AvgJCT),
    }


def aggregate_results(raw_rows):
    grouped = {}
    for row in raw_rows:
        if row.get("status") != "ok":
            continue
        key = (row["config_name"], row["family"], row["demand_exponent"], row["w"], row["zeta"])
        grouped.setdefault(key, []).append(row)

    summary_rows = []
    for key, rows in grouped.items():
        config_name, family, demand_exponent, w_value, zeta_value = key
        summary = {
            "config_name": config_name,
            "family": family,
            "demand_exponent": demand_exponent,
            "w": w_value,
            "zeta": zeta_value,
            "trials": len(rows),
        }
        for metric_name in ("efficiency", "predictability", "fairness", "avg_jct"):
            values = [row[metric_name] for row in rows]
            summary[f"{metric_name}_mean"] = float(np.mean(values))
            summary[f"{metric_name}_std"] = float(np.std(values))
        summary_rows.append(summary)

    summary_rows.sort(key=lambda row: (row["demand_exponent"], row["family"], row["w"] or -1, row["zeta"] or -1))
    return summary_rows


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def make_metric_grid(summary_rows, metric_name, demand_exponent, w_values, zeta_values):
    grid = np.full((len(w_values), len(zeta_values)), np.nan)
    for row in summary_rows:
        if row["family"] != "PCS" or row["demand_exponent"] != demand_exponent:
            continue
        w_index = w_values.index(row["w"])
        z_index = zeta_values.index(row["zeta"])
        grid[w_index, z_index] = row[f"{metric_name}_mean"]
    return grid


def dominates(left_row, right_row, pareto_metrics):
    direction = {
        "efficiency": "max",
        "predictability": "min",
        "fairness": "min",
        "avg_jct": "min",
    }
    left_better_or_equal = True
    left_strictly_better = False

    for metric_name in pareto_metrics:
        left_value = left_row[f"{metric_name}_mean"]
        right_value = right_row[f"{metric_name}_mean"]
        if direction[metric_name] == "min":
            if left_value > right_value:
                left_better_or_equal = False
                break
            if left_value < right_value:
                left_strictly_better = True
        else:
            if left_value < right_value:
                left_better_or_equal = False
                break
            if left_value > right_value:
                left_strictly_better = True

    return left_better_or_equal and left_strictly_better


def compute_pareto_ranks(summary_rows, pareto_metrics):
    ranked_rows = []
    for demand_exponent in sorted({row["demand_exponent"] for row in summary_rows}):
        demand_rows = [dict(row) for row in summary_rows if row["demand_exponent"] == demand_exponent]
        remaining = demand_rows[:]
        rank = 1

        while remaining:
            frontier = []
            for candidate in remaining:
                dominated = False
                for other in remaining:
                    if other is candidate:
                        continue
                    if dominates(other, candidate, pareto_metrics):
                        dominated = True
                        break
                if not dominated:
                    frontier.append(candidate)

            for candidate in frontier:
                candidate["pareto_rank"] = rank
                candidate["pareto_metrics"] = ",".join(pareto_metrics)
                ranked_rows.append(candidate)

            frontier_ids = {id(candidate) for candidate in frontier}
            remaining = [candidate for candidate in remaining if id(candidate) not in frontier_ids]
            rank += 1

    ranked_rows.sort(key=lambda row: (row["demand_exponent"], row["pareto_rank"], row["family"], row["w"] or -1, row["zeta"] or -1))
    return ranked_rows


def plot_all_configs(summary_rows, output_dir):
    metric_specs = [
        ("efficiency", "Efficiency", False),
        ("predictability", "Predictability", False),
        ("fairness", "Fairness", False),
        ("avg_jct", "Avg JCT", False),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    axes = axes.flatten()

    configs = sorted({row["config_name"] for row in summary_rows})
    x_values = sorted({row["demand_exponent"] for row in summary_rows})
    cmap = plt.get_cmap("tab20")

    for idx, config_name in enumerate(configs):
        config_rows = [row for row in summary_rows if row["config_name"] == config_name]
        config_rows.sort(key=lambda row: row["demand_exponent"])
        for axis, (metric_name, title, log_scale) in zip(axes, metric_specs):
            y_values = [row[f"{metric_name}_mean"] for row in config_rows]
            axis.plot(
                x_values,
                y_values,
                marker="o",
                linewidth=2,
                label=config_name,
                color=cmap(idx % 20),
            )
            if log_scale:
                axis.set_yscale("log")
            axis.set_title(title)
            axis.set_xlabel("Demand exponent")
            axis.grid(True, alpha=0.3)

    axes[0].legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=9)
    fig.suptitle("Algorithm Comparison Across Demand Models", fontsize=16, fontweight="bold")
    plt.tight_layout()
    fig.savefig(os.path.join(output_dir, "all_configs_by_demand.png"), dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_baseline_comparison(summary_rows, output_dir, baseline_pcs_name):
    keep_names = {"FIFO", "PPQ", baseline_pcs_name}
    filtered_rows = [row for row in summary_rows if row["config_name"] in keep_names]
    metric_specs = [
        ("efficiency", "Efficiency"),
        ("predictability", "Predictability"),
        ("fairness", "Fairness"),
        ("avg_jct", "Avg JCT"),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    axes = axes.flatten()
    color_map = {"FIFO": "#1f77b4", "PPQ": "#ff7f0e", baseline_pcs_name: "#2ca02c"}

    for axis, (metric_name, title) in zip(axes, metric_specs):
        for config_name in keep_names:
            rows = [row for row in filtered_rows if row["config_name"] == config_name]
            rows.sort(key=lambda row: row["demand_exponent"])
            axis.plot(
                [row["demand_exponent"] for row in rows],
                [row[f"{metric_name}_mean"] for row in rows],
                marker="o",
                linewidth=2,
                label=config_name,
                color=color_map[config_name],
            )
        axis.set_title(title)
        axis.set_xlabel("Demand exponent")
        axis.grid(True, alpha=0.3)

    axes[0].legend()
    fig.suptitle("Baseline Algorithm Comparison", fontsize=16, fontweight="bold")
    plt.tight_layout()
    fig.savefig(os.path.join(output_dir, "baseline_comparison.png"), dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_pcs_heatmaps(summary_rows, output_dir, demand_exponents, w_values, zeta_values):
    metric_specs = [
        ("efficiency", "Efficiency"),
        ("predictability", "Predictability"),
        ("fairness", "Fairness"),
        ("avg_jct", "Avg JCT"),
    ]
    for demand_exponent in demand_exponents:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.flatten()
        for axis, (metric_name, title) in zip(axes, metric_specs):
            grid = make_metric_grid(summary_rows, metric_name, demand_exponent, w_values, zeta_values)
            image = axis.imshow(grid, aspect="auto", cmap="viridis")
            axis.set_title(f"{title} @ demand={demand_exponent:.2f}")
            axis.set_xlabel("zetaMin")
            axis.set_ylabel("W")
            axis.set_xticks(range(len(zeta_values)))
            axis.set_xticklabels([f"{value:.2f}" for value in zeta_values])
            axis.set_yticks(range(len(w_values)))
            axis.set_yticklabels([f"{value:.2f}" for value in w_values])
            for row_idx in range(len(w_values)):
                for col_idx in range(len(zeta_values)):
                    value = grid[row_idx, col_idx]
                    if math.isnan(value):
                        continue
                    axis.text(col_idx, row_idx, f"{value:.2f}", ha="center", va="center", color="white", fontsize=8)
            fig.colorbar(image, ax=axis, fraction=0.046, pad=0.04)

        fig.suptitle(f"PCS Sweep Heatmaps for demand exponent {demand_exponent:.2f}", fontsize=16, fontweight="bold")
        plt.tight_layout()
        safe_demand = str(demand_exponent).replace(".", "_")
        fig.savefig(os.path.join(output_dir, f"pcs_heatmaps_demand_{safe_demand}.png"), dpi=220, bbox_inches="tight")
        plt.close(fig)


def plot_pareto_fronts(ranked_rows, output_dir, pareto_metrics):
    if len(pareto_metrics) < 2:
        return

    x_metric = pareto_metrics[0]
    y_metric = pareto_metrics[1]
    color_map = {1: "#d62728", 2: "#ff7f0e", 3: "#1f77b4"}

    for demand_exponent in sorted({row["demand_exponent"] for row in ranked_rows}):
        rows = [row for row in ranked_rows if row["demand_exponent"] == demand_exponent]
        fig, ax = plt.subplots(figsize=(10, 7))

        for row in rows:
            rank = row["pareto_rank"]
            color = color_map.get(rank, "#7f7f7f")
            ax.scatter(
                row[f"{x_metric}_mean"],
                row[f"{y_metric}_mean"],
                color=color,
                s=90,
                alpha=0.9,
            )
            if rank == 1:
                ax.annotate(
                    row["config_name"],
                    (row[f"{x_metric}_mean"], row[f"{y_metric}_mean"]),
                    textcoords="offset points",
                    xytext=(6, 6),
                    fontsize=8,
                )

        rank_one_rows = [row for row in rows if row["pareto_rank"] == 1]
        rank_one_rows.sort(key=lambda row: row[f"{x_metric}_mean"])
        if rank_one_rows:
            ax.plot(
                [row[f"{x_metric}_mean"] for row in rank_one_rows],
                [row[f"{y_metric}_mean"] for row in rank_one_rows],
                color="#d62728",
                linewidth=1.5,
                alpha=0.8,
            )

        ax.set_title(f"Pareto ranking at demand exponent {demand_exponent:.2f}")
        ax.set_xlabel(f"{x_metric} mean")
        ax.set_ylabel(f"{y_metric} mean")
        ax.grid(True, alpha=0.3)
        handles = [
            plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=color_map[1], markersize=10, label="Rank 1"),
            plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=color_map[2], markersize=10, label="Rank 2"),
            plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=color_map[3], markersize=10, label="Rank 3"),
            plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="#7f7f7f", markersize=10, label="Rank 4+"),
        ]
        ax.legend(handles=handles)

        safe_demand = str(demand_exponent).replace(".", "_")
        fig.savefig(os.path.join(output_dir, f"pareto_front_demand_{safe_demand}.png"), dpi=220, bbox_inches="tight")
        plt.close(fig)


def write_summary_report(summary_rows, ranked_rows, output_path, demand_exponents):
    lines = ["Algorithm comparison summary", ""]
    metric_preferences = {
        "efficiency_mean": max,
        "predictability_mean": min,
        "fairness_mean": min,
        "avg_jct_mean": min,
    }
    labels = {
        "efficiency_mean": "best efficiency",
        "predictability_mean": "best predictability",
        "fairness_mean": "best fairness",
        "avg_jct_mean": "best AvgJCT",
    }

    for demand_exponent in demand_exponents:
        lines.append(f"Demand exponent {demand_exponent:.2f}")
        rows = [row for row in summary_rows if row["demand_exponent"] == demand_exponent]
        for metric_name, selector in metric_preferences.items():
            winner = selector(rows, key=lambda row: row[metric_name])
            lines.append(f"  {labels[metric_name]}: {winner['config_name']} ({winner[metric_name]:.4f})")
        frontier = [row["config_name"] for row in ranked_rows if row["demand_exponent"] == demand_exponent and row["pareto_rank"] == 1]
        lines.append(f"  pareto front: {', '.join(frontier)}")
        lines.append("")

    with open(output_path, "w") as handle:
        handle.write("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description="Compare FIFO, PPQ, and PCS across demand and zeta sweeps")
    parser.add_argument("--output-dir", default=None, help="Directory for CSVs, graphs, and summary text")
    parser.add_argument("--trials", type=int, default=3, help="Number of random workloads per demand exponent")
    parser.add_argument("--seed", type=int, default=20260311, help="Base seed for reproducible workloads")
    parser.add_argument("--jobs", type=int, default=350, help="Jobs per workload")
    parser.add_argument("--cores", type=int, default=8, help="Core count for all algorithms")
    parser.add_argument("--submission-time", type=float, default=200.0, help="Latest submission time")
    parser.add_argument("--avg-length", type=float, default=20.0, help="Average job length")
    parser.add_argument("--jobs-ld", type=float, default=1, help="Job length dispersion")
    parser.add_argument("--threads-ld", type=float, default=1, help="Thread length dispersion")
    parser.add_argument("--uncertainty", type=float, default=5.0, help="Per-thread actual length uncertainty")
    parser.add_argument("--avg-threads", type=float, default=4.0, help="Average thread count")
    parser.add_argument("--mutex-prob", type=float, default=0.1, help="Mutex probability")
    parser.add_argument("--sem-prob", type=float, default=0.1, help="Semaphore probability")
    parser.add_argument("--minimum-running-time", type=float, default=5.0, help="PPQ subthread slice length")
    parser.add_argument("--nqueues", type=int, default=4, help="Queue count for PCS")
    parser.add_argument("--demand-exponents", default="0.5,0.7,0.9", help="Comma-separated demand scaling exponents")
    parser.add_argument("--w-values", default="0.0,1.0,3.0", help="Comma-separated PCS W values")
    parser.add_argument("--zeta-values", default="0.0,0.4,0.8", help="Comma-separated PCS zetaMin values")
    parser.add_argument(
        "--pareto-metrics",
        default="avg_jct,predictability,fairness",
        help="Comma-separated metrics used for Pareto ranking. Supported: efficiency,predictability,fairness,avg_jct",
    )
    parser.add_argument("--pcs-search", action="store_true", help="Run NSGA2 parameter search for PCS instead of grid sweep")
    args = parser.parse_args()

    demand_exponents = parse_float_list(args.demand_exponents)
    w_values = parse_float_list(args.w_values)
    zeta_values = parse_float_list(args.zeta_values)
    pareto_metrics = [item.strip() for item in args.pareto_metrics.split(",") if item.strip()]
    supported_metrics = {"efficiency", "predictability", "fairness", "avg_jct"}
    invalid_metrics = [item for item in pareto_metrics if item not in supported_metrics]
    if invalid_metrics:
        raise ValueError(f"Unsupported pareto metrics: {invalid_metrics}")

    if args.pcs_search:
        # For demonstration, use the first demand_exponent
        demand_exponent = demand_exponents[0]
        seed = args.seed
        jobs, global_semaphores = generate_workload(args, seed, demand_exponent)
        print("Running PCSParameterSearch (NSGA2) for PCS parameters...")
        search = PCSParameterSearch(jobList=jobs, globalSemaphoreList=global_semaphores, nCores=args.cores)
        X, F = search.findParetoFrontier(pop_size=20, n_gen=20, verbose=True)
        print("Pareto-optimal PCS configs (nQueues, W):")
        for params, metrics in zip(X, F):
            print(f"nQueues={int(round(params[0]))}, W={params[1]:.3f} | AvgJCT={metrics[0]:.3f}, Predictability={metrics[1]:.3f}")
        if args.output_dir is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            args.output_dir = timestamp  # <--- This is the crucial line you need!

        os.makedirs(args.output_dir, exist_ok=True)
        pareto_png = os.path.join(args.output_dir, "pcs_nsga2_pareto.png")
        pareto_csv = os.path.join(args.output_dir, "pcs_nsga2_pareto.csv")
        search.plotParetoFrontier(save_path=pareto_png)
        search.saveParetoCSV(X, F, pareto_csv)
        print(f"Saved NSGA2 Pareto plot to {pareto_png}")
        print(f"Saved NSGA2 Pareto CSV to {pareto_csv}")
    if args.output_dir is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_dir = os.path.join("comparison_output", timestamp)
    os.makedirs(args.output_dir, exist_ok=True)
    graphs_dir = os.path.join(args.output_dir, "graphs")
    os.makedirs(graphs_dir, exist_ok=True)

    configs = build_algorithm_configs(w_values, zeta_values)
    raw_rows = []

    for demand_exponent in demand_exponents:
        print(f"Running demand exponent {demand_exponent:.2f}")
        for trial_index in range(args.trials):
            seed = args.seed + int(demand_exponent * 1000) + trial_index
            print(f"  Trial {trial_index + 1}/{args.trials} with seed {seed}")
            jobs, global_semaphores = generate_workload(args, seed, demand_exponent)

            for config in configs:
                print(f"    {config.name}")
                row = {
                    "trial": trial_index,
                    "seed": seed,
                    "config_name": config.name,
                    "family": config.family,
                    "demand_exponent": demand_exponent,
                    "w": config.w,
                    "zeta": config.zeta,
                    "status": "ok",
                    "error": "",
                }
                try:
                    row.update(run_single_configuration(config, jobs, global_semaphores, args))
                except Exception as exc:  # noqa: BLE001
                    row.update(
                        {
                            "efficiency": "",
                            "predictability": "",
                            "fairness": "",
                            "avg_jct": "",
                            "status": "failed",
                            "error": str(exc),
                        }
                    )
                raw_rows.append(row)

    raw_csv_path = os.path.join(args.output_dir, "raw_results.csv")
    write_csv(
        raw_csv_path,
        raw_rows,
        [
            "trial",
            "seed",
            "config_name",
            "family",
            "demand_exponent",
            "w",
            "zeta",
            "status",
            "error",
            "efficiency",
            "predictability",
            "fairness",
            "avg_jct",
        ],
    )

    summary_rows = aggregate_results(raw_rows)
    summary_csv_path = os.path.join(args.output_dir, "summary_results.csv")
    write_csv(
        summary_csv_path,
        summary_rows,
        [
            "config_name",
            "family",
            "demand_exponent",
            "w",
            "zeta",
            "trials",
            "efficiency_mean",
            "efficiency_std",
            "predictability_mean",
            "predictability_std",
            "fairness_mean",
            "fairness_std",
            "avg_jct_mean",
            "avg_jct_std",
        ],
    )

    ranked_rows = compute_pareto_ranks(summary_rows, pareto_metrics)
    pareto_csv_path = os.path.join(args.output_dir, "pareto_rankings.csv")
    write_csv(
        pareto_csv_path,
        ranked_rows,
        [
            "config_name",
            "family",
            "demand_exponent",
            "w",
            "zeta",
            "trials",
            "pareto_rank",
            "pareto_metrics",
            "efficiency_mean",
            "efficiency_std",
            "predictability_mean",
            "predictability_std",
            "fairness_mean",
            "fairness_std",
            "avg_jct_mean",
            "avg_jct_std",
        ],
    )

    baseline_pcs_name = f"PCS[w={w_values[min(len(w_values) - 1, 1)]:.2f},z={zeta_values[0]:.2f}]"
    plot_all_configs(summary_rows, graphs_dir)
    plot_baseline_comparison(summary_rows, graphs_dir, baseline_pcs_name)
    plot_pcs_heatmaps(summary_rows, graphs_dir, demand_exponents, w_values, zeta_values)
    plot_pareto_fronts(ranked_rows, graphs_dir, pareto_metrics)
    summary_report_path = os.path.join(args.output_dir, "summary.txt")
    write_summary_report(summary_rows, ranked_rows, summary_report_path, demand_exponents)

    print(f"Saved raw results to {raw_csv_path}")
    print(f"Saved summary results to {summary_csv_path}")
    print(f"Saved pareto rankings to {pareto_csv_path}")
    print(f"Saved graphs to {graphs_dir}")
    print(f"Saved text summary to {summary_report_path}")


if __name__ == "__main__":
    main()
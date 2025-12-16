import random
import csv
from collections import deque
import math


def simulate_mm1(X, Y, sim_time, seed):
    """
    Simple M/M/1 queue simulator.
    X = mean inter-arrival time
    Y = mean service time
    """

    random.seed(seed)

    run_id = seed
    lam = 1.0 / X   # arrival rate
    mu = 1.0 / Y    # service rate

    t = 0.0             # current time
    q = deque()         # FIFO queue
    busy = False

    t_next_arr = random.expovariate(lam)
    t_next_dep = math.inf

    jobs = []           # per-job statistics
    timeline = []       # queue length over time (only first 60s)

    last_arrival = 0.0

    while t < sim_time:
        if t_next_arr < t_next_dep:
            # arrival
            t = t_next_arr
            iat = t - last_arrival
            last_arrival = t

            job = {
                "arrival_time": t,
                "service_start_time": None,
                "departure_time": None,
                "inter_arrival_time": iat,
                "service_time": None,
            }

            q.append(job)

            if t <= 60:
                timeline.append((run_id, seed, t, len(q)))

            t_next_arr = t + random.expovariate(lam)

            if not busy:
                busy = True
                current = q.popleft()
                current["service_start_time"] = t
                st = random.expovariate(mu)
                current["service_time"] = st
                t_next_dep = t + st

        else:
            # departure
            t = t_next_dep
            current["departure_time"] = t
            wait = current["service_start_time"] - current["arrival_time"]

            jobs.append(
                (
                    run_id,
                    seed,
                    current["arrival_time"],
                    current["service_start_time"],
                    current["departure_time"],
                    wait,
                    current["inter_arrival_time"],
                    current["service_time"],
                )
            )

            if t <= 60:
                timeline.append((run_id, seed, t, len(q)))

            if len(q) > 0:
                current = q.popleft()
                current["service_start_time"] = t
                st = random.expovariate(mu)
                current["service_time"] = st
                t_next_dep = t + st
            else:
                busy = False
                t_next_dep = math.inf

    return jobs, timeline


def save_job_data_to_csv(filename, rows):
    with open(filename, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "run_id",
                "seed",
                "arrival_time",
                "service_start_time",
                "departure_time",
                "waiting_time",
                "inter_arrival_time",
                "service_time",
            ]
        )
        w.writerows(rows)


def save_timeline_to_csv(filename, rows):
    with open(filename, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "seed", "time", "queue_length"])
        w.writerows(rows)


def main():
    sim_time = 6000.0
    Y = 1.0
    X_values = [1.1, 1.5, 2.0]
    num_runs = 10
    base_seed = 1

    for X in X_values:
        print(f"\nRunning simulations for X = {X}")
        all_jobs = []
        all_timeline = []

        for r in range(num_runs):
            seed = base_seed + r
            print(f"  run {r + 1}/{num_runs}, seed={seed}")
            jobs, timeline = simulate_mm1(X, Y, sim_time, seed)
            all_jobs.extend(jobs)
            all_timeline.extend(timeline)

        job_file = f"mm1_results_X_{X}_AGG.csv"
        tl_file = f"mm1_timeline_X_{X}.csv"
        save_job_data_to_csv(job_file, all_jobs)
        save_timeline_to_csv(tl_file, all_timeline)
        print(f"Saved {job_file} and {tl_file}")


if __name__ == "__main__":
    main()

import redis
import time
import random
import json
import os
from pprint import pprint
from utility import DebugPopulateSeeder

# Connect to clients and flush them
r1 = redis.Redis(port=6380, decode_responses=True)
r2 = redis.Redis(port=6381, decode_responses=True)


# Get usec p99 latency from dfly_bench
def measure_latency(qps=1000, t=10):
    cmd = f"./dfly_bench  -c 2 --test_time={t} --ratio=0:10 --qps={qps} --proactor_threads 2 | sed -n -e 's/^.*P99 lat: //p'"
    return int(float(os.popen(cmd).read()[:-3]))


# Flush both instances and fill N1
def flush_and_fill(key_target, data_size):
    r1.flushall()
    r2.flushall()

    # Fill instance N1 with data
    seeder = DebugPopulateSeeder(
        key_target=key_target, data_size=data_size, variance=2, samples=100
    )
    seeder.run(r1)

    print("Filled: ", r1.info("memory")["used_memory_human"])


def run_replication(measure_time=10):
    r2.replicaof("localhost", "6380")
    start = time.time()

    if measure_time > 0:
        time.sleep(0.1)
        latency = measure_latency(t=measure_time)
    else:
        latency = 0

    while True:
        role = r2.role()
        if role[3] == "online":
            break
        time.sleep(0.05)

    took = time.time() - start

    r2.replicaof("no", "one")
    r2.flushall()
    time.sleep(0.5)

    print(f"  took: {took}, latency: {latency}")
    return (took, latency)


def run(times=2):
    # check how much replication takes without load
    took, _ = run_replication(measure_time=0)
    measure_time = int(took * 0.8)

    total_took = 0
    total_latency = 0
    for _ in range(times):
        took, latency = run_replication(measure_time=measure_time)
        total_took += took
        total_latency += latency
    return (total_took / times, total_latency / times)


def run_old():
    print(f">> OLD")
    r1.execute_command("CONFIG", "SET", "background_snapshotting", "false")
    r1.execute_command("CONFIG", "SET", "background_heartbeat", "false")
    return run()


def run_new(budget_us, sleep_freq):
    print(f">> budget: {budget_us}, sleep_freq: {sleep_freq}")
    r1.execute_command("CONFIG", "SET", "background_snapshotting", "true")
    r1.execute_command("CONFIG", "SET", "background_heartbeat", "true")
    r1.execute_command("CONFIG", "SET", "sched_budget_background_fib", budget_us * 1000)
    r1.execute_command("CONFIG", "SET", "sched_background_sleep_freq", sleep_freq)
    return run()


BASE_KEYC = 50_000
DATA_SIZES = [50, 100, 1000, 2000, 10_0000]
BUDGETS = [1, 3, 5, 10, 50, 100]
SLEEP_FREQS = [10, 30, 50, 80]

data_points = [list() for _ in DATA_SIZES]


def save():
    with open("data-points.json", "w") as f:
        json.dump(data_points, f)
        f.flush()


for data_i, data_size in enumerate(DATA_SIZES):
    flush_and_fill(int(BASE_KEYC * (200 / data_size**0.5)), data_size)

    took, latency = run_old()
    data_points[data_i].append((latency, took, ()))

    for budget in BUDGETS:
        for sleep_freq in SLEEP_FREQS:
            took, latency = run_new(budget, sleep_freq)
            data_points[data_i].append((latency, took, (budget, sleep_freq)))

        save()

f.close()

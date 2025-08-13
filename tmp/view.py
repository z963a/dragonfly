import matplotlib.pyplot as plt
import json
import sys
from collections import defaultdict

with open(sys.argv[1]) as f:
    all_data = json.load(f)


def plot_lat_time(p, data):
    base_latency = data[0][0]
    x_points = []
    y_points = []
    for latency, took, _ in data[1:]:
        x_points.append(took)
        y_points.append(latency)

    p.scatter(x_points, y_points, marker=".", s=70)
    p.scatter([[data[0][1]]], [[data[0][0]]], marker="*", s=100)
    p.set_xlabel("Replication time")
    p.set_ylabel("Latency")


def plot_heatmap(p, data):
    arr = list(list())
    last_budet = -1
    budgets = list()
    freqs = list()
    for latency, took, (budget, freq) in data[1:]:
        if budget != last_budet:
            arr.append(list())
            last_budet = budget
        arr[-1].append(latency)

        if budget not in budgets:
            budgets.append(budget)
        if freq not in freqs:
            freqs.append(freq)

    p.set_xlabel("Frequencies")
    p.set_ylabel("Budget")

    p.set_xticks(list(range(len(freqs))))
    p.set_yticks(list(range(len(budgets))))
    p.set_xticklabels(list(freqs))
    p.set_yticklabels(list(budgets))
    p.imshow(arr, cmap="jet", aspect="auto")


def plot_budget_ranges(p, data, xi, yi):
    XNAMES = ["Budget", "Freq"]
    YNAMES = ["Latency", "Replication time"]
    bins = defaultdict(list)
    for dp in data:
        param = dp[2]
        if param is None or len(param) < 1:
            bins[-1].append(dp[yi])
        else:
            bins[param[xi]].append(dp[yi])

    x_points = []
    y_dif = []
    y_bottom = []
    for bud, vals in bins.items():
        if bud > 0:
            x_points.append(bud)
            y_dif.append(max(vals) - min(vals))
            y_bottom.append(min(vals))

    p.bar(x_points, y_dif, bottom=y_bottom, width=3)
    p.scatter([-1], [max(bins[-1])], s=50)
    p.set_xlabel(XNAMES[xi])
    p.set_ylabel(YNAMES[yi])


for data in all_data:
    if len(data) == 0:
        continue

    fig, axs = plt.subplots(nrows=2, ncols=2, figsize=(10, 8))
    plot_lat_time(axs[0, 0], data)
    plot_heatmap(axs[0, 1], data)
    plot_budget_ranges(axs[1, 0], data, 0, 0)
    plot_budget_ranges(axs[1, 1], data, 0, 1)

    fig.tight_layout()
    plt.show()

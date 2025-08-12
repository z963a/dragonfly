import matplotlib.pyplot as plt
import json

with open("data-points.json") as f:
    all_data = json.load(f)

for data in all_data:
    if len(data) == 0:
        continue

    x_points = [dp[1] for dp in data[1:]]
    y_points = [dp[0] for dp in data[1:]]

    plt.scatter(x_points, y_points, marker=".", s=70)
    plt.scatter([[data[0][1]]], [[data[0][0]]], marker="*", s=100)
    plt.xlabel("Replication time")
    plt.ylabel("Latency")
    plt.show()

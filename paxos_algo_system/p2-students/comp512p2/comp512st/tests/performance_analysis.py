import matplotlib.pyplot as plt
import pandas as pd

# EXPERIMENT 1: measuring acceptance latency and acceptance throughput (acceptances processed by
# the system per second) as a function of the number of players in the game

df = pd.read_csv("metrics_varying_players.csv")

# Group by number of players
grouped_by_player_count = df.groupby("num_players", as_index=False).agg({
    "acceptance_latency": "mean",
    "acceptance_throughput": "sum"
})

# x1: num players in game
x1 = grouped_by_player_count["num_players"]

# average latency (y1)
avg_latency = grouped_by_player_count["acceptance_latency"]

# average throughput among players (y2)
avg_throughput = grouped_by_player_count["acceptance_throughput"]

# Plot 1: latency
plt.plot(x1, avg_latency, marker="o")
plt.xlabel("Number of Players")
plt.ylabel("Acceptance Latency (ms)")
plt.title("Acceptance Latency vs. Number of Players")
plt.grid(True)
plt.savefig("player_count_vs_avg_latency.png")
plt.close()

# Plot 2: throughput
plt.plot(x1, avg_throughput, marker="o")
plt.xlabel("Number of Players")
plt.ylabel("Acceptance Throughput (# of moves per second)")
plt.title("Acceptance Throughput vs. Number of Players")
plt.grid(True)
plt.savefig("player_count_vs_avg_throughput.png")
plt.close()

# EXPERIMENT 2: measuring acceptance latency and acceptance throughput (acceptances processed by
# the system per second) as a function of the time interval between player moves (3 players)

df = pd.read_csv("metrics_varying_interval.csv")

# Group by number of players
grouped_by_interval_size = df.groupby("interval_size", as_index=False).agg({
    "acceptance_latency": "mean",
    "acceptance_throughput": "sum"
})

# x2: num players in game
x2 = grouped_by_interval_size["interval_size"]

# average latency (y3)
avg_latency2 = grouped_by_interval_size["acceptance_latency"]

# average throughput among players (y4)
avg_throughput2 = grouped_by_interval_size["acceptance_throughput"]

# Plot 1: latency
plt.plot(x2, avg_latency2, marker="o")
plt.xlabel("Time Interval Between Moves (ms)")
plt.ylabel("Acceptance Latency (ms)")
plt.title("Acceptance Latency vs. Time Interval Size")
plt.grid(True)
plt.savefig("interval_size_vs_avg_latency.png")
plt.close()

# Plot 2: throughput
plt.plot(x2, avg_throughput2, marker="o")
plt.xlabel("Time Interval Between Moves (ms)")
plt.ylabel("Acceptance Throughput (# of moves per second)")
plt.title("Acceptance Throughput vs. Time Interval Size")
plt.grid(True)
plt.savefig("interval_size_vs_avg_throughput.png")
plt.close()

# EXPERIMENT 3: plotting the acceptance throughput per process for varying interval sizes

df = pd.read_csv("metrics_varying_interval.csv")

# Group by number of players
grouped_by_player_name = df.groupby("process_name", as_index=False)

# Plot throughput per player across intervals
for name, group in grouped_by_player_name:
    plt.plot(group["interval_size"], group["acceptance_throughput"], marker="o", label=name)

plt.xlabel("Time Interval Between Moves (ms)")
plt.ylabel("Acceptance Throughput (# of moves per second)")
plt.title("Acceptance Throughput per Player vs. Time Interval")
plt.legend(title="Process")
plt.grid(True)
plt.tight_layout()
plt.savefig("acceptance_throughput_per_player.png")
plt.close()

# EXPERIMENT 4: plotting the acceptance throughput per process for varying number of players

df = pd.read_csv("metrics_varying_players.csv")

# Group by number of players
grouped_by_player_name = df.groupby("process_name", as_index=False)

# Plot throughput per player across intervals
for name, group in grouped_by_player_name:
    plt.plot(group["num_players"], group["acceptance_throughput"], marker="o", label=name)

plt.xlabel("Number of Players")
plt.ylabel("Acceptance Throughput (# of moves per second)")
plt.title("Acceptance Throughput per Player vs. Number of Players")
plt.legend(title="Process")
plt.grid(True)
plt.tight_layout()
plt.savefig("acceptance_throughput_per_player_varying_players.png")
plt.close()

# EXPERIMENT 5: measuring acceptance latency and acceptance throughput (acceptances processed by
# the system per second) as a function of the time interval between player moves (5 players)

df = pd.read_csv("metrics_varying_interval_2.csv")

# Group by number of players
grouped_by_interval_size = df.groupby("interval_size", as_index=False).agg({
    #"acceptance_latency": "mean",
    "acceptance_throughput": "sum"
})

# x: num players in game
x = grouped_by_interval_size["interval_size"]

# average throughput among players (y4)
avg_throughput = grouped_by_interval_size["acceptance_throughput"]

# Plot 1: throughput
plt.plot(x, avg_throughput, marker="o")
plt.xlabel("Time Interval Between Moves (ms)")
plt.ylabel("Acceptance Throughput (# of moves per second)")
plt.title("Acceptance Throughput vs. Time Interval Size")
plt.grid(True)
plt.savefig("interval_size_vs_avg_throughput_2.png")
plt.close()
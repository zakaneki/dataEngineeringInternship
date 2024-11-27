import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file
file_path = "data.csv"  # Replace with your CSV file path
df = pd.read_csv(file_path)

# Create the plot
fig, ax1 = plt.subplots(figsize=(12, 6))

# Plot measurements on the primary y-axis
color_measurement = "blue"
ax1.plot(df["date"], df["_col1"], label="Measurement (PM100)", marker="o", linestyle="-", color=color_measurement)
ax1.set_xlabel("Date", fontsize=12)
ax1.set_ylabel("Measurement (PM100)", color=color_measurement, fontsize=12)
ax1.tick_params(axis="y", labelcolor=color_measurement)
ax1.tick_params(axis="x", rotation=45)
ax1.grid(alpha=0.3)

# Create a secondary y-axis for tourist estimation
ax2 = ax1.twinx()
color_tourist = "green"
ax2.plot(df["date"], df["tourist_estimation"], label="Tourist Estimation", marker="x", linestyle="--", color=color_tourist)
ax2.set_ylabel("Tourist Estimation", color=color_tourist, fontsize=12)
ax2.tick_params(axis="y", labelcolor=color_tourist)

# Add a title
plt.title("Daily Measurement (PM100) vs Tourist Estimation", fontsize=16)

# Add a legend
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
plt.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left", fontsize=12)

# Adjust layout and show the plot
plt.tight_layout()
plt.show()

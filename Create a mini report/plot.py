import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file into a DataFrame
df = pd.read_csv('pollution_data.csv')

# Convert the 'date' column to datetime format for proper sorting
df['date'] = pd.to_datetime(df['date'])

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(df['date'], df['pollution_total_visitor'], marker='o', label="Pollution Total Visitor")
plt.title("Pollution Total Visitor Over Time (Kyiv, Ukraine)", fontsize=16)
plt.xlabel("Date", fontsize=14)
plt.ylabel("Pollution Total Visitor", fontsize=14)
plt.xticks(rotation=45)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=12)
plt.tight_layout()

# Show the plot
plt.show()

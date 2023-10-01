from sklearn.datasets import make_blobs
from matplotlib import pyplot
from pandas import DataFrame
import seaborn as sns

# Set the parameters for data generation
n = 1000  # Number of data points
d = 2  # Number of dimensions
k = 4  # Number of clusters

# Generate synthetic data using make_blobs function
points, y = make_blobs(n_samples=n, centers=k, n_features=d)

# Save the generated dataset to a file
with open(f"dataset_{d}_dim_{k}_clusters.txt", "w") as file:
    for point in points:
        for value in range(d):
            if value == (d - 1):
                file.write(str(round(point[value], 4)))
            else:
                file.write(str(round(point[value], 4)) + " ")
        file.write("\n")

# Create a DataFrame for further visualization with labels
df = DataFrame(dict(x=points[:, 0], y=points[:, 1], label=y))

# Define colors for different labels
palette = sns.color_palette("hsv", n_colors = k)

# Create a scatterplot of the data points with different colors for each label
fig, ax = pyplot.subplots()
grouped = df.groupby('label')
for key, group in grouped:
    group.plot(ax=ax, kind='scatter', x='x', y='y', label=key, color=palette[key])

# Show the scatterplot
pyplot.show()

import os
from sklearn.datasets import make_blobs
import matplotlib.pyplot as plt
from pandas import DataFrame
import seaborn as sns

# Set the parameters for data generation
n = 100000  # Number of data points
d = 7  # Number of dimensions
k = 10  # Number of clusters

# Generate synthetic data using make_blobs function
points, y = make_blobs(n_samples=n, centers=k, n_features=d, random_state=17, cluster_std=0.5)

# Create 'datasets' and 'plots' folders if they don't exist
os.makedirs('datasets', exist_ok=True)
os.makedirs('plots', exist_ok=True)

# Define the file path inside the 'datasets' folder
file_path = os.path.join('datasets', f"n_{n}_d_{d}_k_{k}.txt")

# Save the generated dataset to a file inside the 'datasets' folder 
with open(file_path, "w") as file:
    for point in points:
        for value in range(d):
            if value == (d - 1):
                file.write(str(round(point[value], 4)))
            else:
                file.write(str(round(point[value], 4)) + " ")
        file.write("\n")


# Define colors for different labels
palette = sns.color_palette("hsv", n_colors=k)

if d == 2:
    # Create a DataFrame
    df = DataFrame(dict(x=points[:, 0], y=points[:, 1], label=y))

     # Create a 2D scatterplot of the data points with different colors for each label
    fig, ax = plt.subplots()
    grouped = df.groupby('label')
    
    for key, group in grouped:
        group.plot(ax=ax, kind='scatter', x='x', y='y', label=key+1, color=palette[key])

elif d == 3:
    # Create a DataFrame
    df = DataFrame(dict(x=points[:, 0], y=points[:, 1], z=points[:, 2], label=y))

    # Create a 3D scatterplot of the data points with different colors for each label
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    grouped = df.groupby('label')
    
    for key, group in grouped:
        ax.scatter(group['x'], group['y'], group['z'], label=key+1, color=[palette[key]], alpha=0.25)
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('Z')

else:
    exit(0)

# Set labels and legend
plt.legend()
plt.title(f"Results (n={n}, d={d}, k={k})")

# Save the plot as an image file
plt.savefig(f"plots/dataset_n_{n}_d_{d}_k_{k}.png")

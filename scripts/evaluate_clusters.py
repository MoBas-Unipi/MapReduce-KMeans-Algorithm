import glob
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import silhouette_score, pairwise_distances_argmin_min

# Parameters definition
n = 1000 # Number of data points
d = 2      # Number of dimensions
k = 3      # Number of clusters
r = 1      # Number of reducers

# Function to read points from a file and return them as a list of tuples
def read_points_from_file(filename, num_coords, skip_first=False):
    points = []
    try:
        with open(filename, "r") as file:
            for line in file:
                parts = line.strip().split()
                if skip_first:
                    parts = parts[1:]  # Skip the first element (centroidID)
                if len(parts) == num_coords:
                    coords = [float(part) for part in parts]
                    points.append(tuple(coords))
                else:
                    raise ValueError(f"Invalid number of coordinates in line: {line}. Expected {num_coords}, got {len(parts)}")
                          
    except ValueError as e:
        print(e)
        sys.exit(1)    
        
    return points

# File paths for the dataset and the output files matching the pattern
dataset_file = f"../datasets/n_{n}_d_{d}_k_{k}.txt"
output_files = glob.glob(f"../results/n_{n}_d_{d}_k_{k}/{r}reducers/part-r-*")

# Read points from the dataset file
dataset_points = read_points_from_file(dataset_file, d)

# Initialize the computed centroids list
computed_centroids = []

# Read points from all matching output files and append them to computed_centroids
for output_file in output_files:
    computed_centroids.extend(read_points_from_file(output_file, d, skip_first=True))

# Compute centroids using KMeans clustering only on the dataset points
# kmeans = KMeans(n_clusters=k, init='random', n_init=5, max_iter=30) 
# kmeans.fit(dataset_points)
# centroids = kmeans.cluster_centers_

# Create the 'plots' folder if it doesn't exist
os.makedirs('../plots/results', exist_ok=True)

if d == 2:
    # Create a scatterplot for the dataset points
    dataset_x, dataset_y = zip(*dataset_points)
    plt.scatter(dataset_x, dataset_y, label="Dataset Points", alpha=0.2)
    plt.xlabel("X")
    plt.ylabel("Y")

    # Create a scatterplot for the computed centroids from the output file (in red)
    centroid_x, centroid_y = zip(*computed_centroids)
    plt.scatter(centroid_x, centroid_y, label="Computed Centroids", color="red", marker="x")

    # Create a scatterplot for the centroids computed with KMeans (in yellow)
    # centroid_x, centroid_y = zip(*centroids)
    # plt.scatter(centroid_x, centroid_y, label="sklearn KMeans centroids", color="yellow")

elif d == 3:
    # Create a scatterplot for the dataset points
    fig = plt.figure()
    # ax = fig.add_subplot(111, projection='3d')
    ax = plt.axes(projection='3d', computed_zorder=False)

    dataset_x, dataset_y, dataset_z = zip(*dataset_points)
    ax.scatter(dataset_x, dataset_y, dataset_z, label="Dataset Points", alpha=0.2, zorder=1)
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.set_zlabel("Z")

    # Create a scatterplot for the computed centroids from the output file (in red)
    centroid_x, centroid_y, centroid_z = zip(*computed_centroids)
    ax.scatter(centroid_x, centroid_y, centroid_z, label="Computed Centroids", color="red", marker="x", alpha=0.9, zorder=2)

    # Create a scatterplot for the centroids computed with KMeans (in black)
    # centroid_x, centroid_y, centroid_z = zip(*centroids)
    # ax.scatter(centroid_x, centroid_y, centroid_z, label="sklearn KMeans centroids", color="black")
    
if d in [2, 3]:
    # Set labels and legend
    plt.legend()
    plt.title(f"Results (n={n}, d={d}, k={k})")

    # Save the plot as an image file
    plt.savefig(f"../plots/results/results_n_{n}_d_{d}_k_{k}.png")


# Convert dataset_points and computed_centroids to numpy arrays
dataset = np.array(dataset_points)
centroids = np.array(computed_centroids)

# Use pairwise_distances_argmin_min to get the closest centroid indices
cluster_labels, _ = pairwise_distances_argmin_min(dataset, centroids)

# Compute the silhouette score
silhouette_avg = silhouette_score(dataset, cluster_labels)

print(f"Silhouette Score: {silhouette_avg}")

# Save the silhouette score to a file
with open(f"../results/n_{n}_d_{d}_k_{k}/{r}reducers/silhouette_score.txt", "w") as score_file:
    score_file.write(f"Silhouette Score: {silhouette_avg}")

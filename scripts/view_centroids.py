import os
import sys
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans

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
                    x, y = float(parts[0]), float(parts[1])
                    if num_coords == 3:
                        z = float(parts[2])
                        points.append((x, y, z))
                else:
                    raise ValueError  
                          
    except ValueError as e:
        print(f"Invalid number of coordinates in line: {line}. Expected {num_coords}, got {len(parts)}")                
        sys.exit(1)
        
    return points

# Parameters definition
n = 1000
d = 3  # Set d to 2 or 3
k = 4

# Check if d is valid (either 2 or 3)
try:
    if d not in [2, 3]:
        raise ValueError
except ValueError as e:
    print(f"Invalid value for 'd'. It should be either 2 or 3. It is {d} instead.")    
    sys.exit(1)

# File paths for the dataset and the output file
dataset_file = f"datasets/n_{n}_d_{d}_k_{k}.txt"
output_file = "../K-means-mapreduce/output/part-r-00000"

# Read points from the dataset and output files
dataset_points = read_points_from_file(dataset_file, d)
computed_centroids = read_points_from_file(output_file, d, skip_first=True)

# Compute centroids using KMeans clustering only on the dataset points
kmeans = KMeans(n_clusters=k, init='random', n_init=5, max_iter=30) 
kmeans.fit(dataset_points)
centroids = kmeans.cluster_centers_

# Create the 'plots' folder if it doesn't exist
os.makedirs('plots', exist_ok=True)

if d == 2:
    # Create a scatterplot for the dataset points
    dataset_x, dataset_y = zip(*dataset_points)
    plt.scatter(dataset_x, dataset_y, label="Dataset Points")
    plt.xlabel("X")
    plt.ylabel("Y")

    # Create a scatterplot for the computed centroids from the output file (in red)
    centroid_x, centroid_y = zip(*computed_centroids)
    plt.scatter(centroid_x, centroid_y, label="Computed Centroids", color="red", marker="x")

    # Create a scatterplot for the centroids computed with KMeans (in yellow)
    centroid_x, centroid_y = zip(*centroids)
    plt.scatter(centroid_x, centroid_y, label="sklearn KMeans centroids", color="yellow")

    # Save the plot as an image file
    plt.savefig(f"plots/results_n_{n}_d_{d}_k_{k}.png")

elif d == 3:
    # Create a scatterplot for the dataset points
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    dataset_x, dataset_y, dataset_z = zip(*dataset_points)
    ax.scatter(dataset_x, dataset_y, dataset_z, label="Dataset Points", alpha=0.02)
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.set_zlabel("Z")

    # Create a scatterplot for the computed centroids from the output file (in red)
    centroid_x, centroid_y, centroid_z = zip(*computed_centroids)
    ax.scatter(centroid_x, centroid_y, centroid_z, label="Computed Centroids", color="red", marker="x")
    
    # Create a scatterplot for the centroids computed with KMeans (in yellow)
    centroid_x, centroid_y, centroid_z = zip(*centroids)
    ax.scatter(centroid_x, centroid_y, centroid_z, label="sklearn KMeans centroids", color="yellow")

    # Save the plot as an image file
    plt.savefig(f"plots/results_n_{n}_d_{d}_k_{k}.png")

# Set labels and legend
plt.legend()
plt.title(f"Results (n={n}, d={d}, k={k})")

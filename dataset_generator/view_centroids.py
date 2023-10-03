import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import numpy as np

# Function to read points from a file and return them as a list of tuples
def read_points_from_file(filename):
    points = []
    with open(filename, "r") as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) == 2:
                x, y = float(parts[0]), float(parts[1])
            elif len(parts) == 3:
                x, y = float(parts[1]), float(parts[2])
            else:
                continue  
            points.append((x, y))
    return points

# File paths for the dataset and the output file
dataset_file = "datasets/n_1000_d_2_k_4.txt"
output_file = "../K-means-mapreduce/output/part-r-00000"

# Number of clusters
n_clusters = 4

# Read points from the two files
dataset_points = read_points_from_file(dataset_file)
computed_centroids = read_points_from_file(output_file)

# Create a scatterplot for the dataset points
dataset_x, dataset_y = zip(*dataset_points)
plt.scatter(dataset_x, dataset_y, label="Dataset Points")

# Create a scatterplot for the computed centroids from the output file (in red)
centroid_x, centroid_y = zip(*computed_centroids)
plt.scatter(centroid_x, centroid_y, label="Computed Centroids", color="red", marker="x")

# Compute centroids using KMeans clustering only on the dataset points
kmeans = KMeans(n_clusters, init='random', max_iter=30) 
kmeans.fit(dataset_points)
centroids = kmeans.cluster_centers_

# Create a scatterplot for the centroids computed with KMeans (in blue)
centroid_x, centroid_y = zip(*centroids)
plt.scatter(centroid_x, centroid_y, label="sklearn KMean centroids", color="yellow", marker="o")

# Set labels and legend
plt.xlabel("X")
plt.ylabel("Y")
plt.legend()

# Show the plot
plt.show()

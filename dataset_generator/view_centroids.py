import matplotlib.pyplot as plt

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
                continue  # Skip lines with less than 2 or 3 values
            points.append((x, y))
    return points

# File paths for your dataset and the other file
dataset_file = "datasets/n_1000_d_2_k_4.txt"
output_file = "../K-means-mapreduce/output/part-r-00000"  # Replace with the actual path to your other file

# Read points from the two files
dataset_points = read_points_from_file(dataset_file)
computed_centroids = read_points_from_file(output_file)

# Create a scatterplot for the dataset points (all of the same color)
dataset_x, dataset_y = zip(*dataset_points)
plt.scatter(dataset_x, dataset_y, label="Dataset Points")

# Create a scatterplot for the other points (in a different color)
other_x, other_y = zip(*computed_centroids)
plt.scatter(other_x, other_y, label="Other Points", color="red")

# Set labels and legend
plt.xlabel("X")
plt.ylabel("Y")
plt.legend()

# Show the plot
plt.show()

package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.mapreduce.KMeansCombiner;
import it.unipi.dii.hadoop.mapreduce.KMeansMapper;
import it.unipi.dii.hadoop.mapreduce.KMeansReducer;
import it.unipi.dii.hadoop.model.Centroid;
import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.sql.Timestamp;
import java.util.*;


public class KMeans {

    /**
     * Generate a list of Centroid (size k = clusterNumber) taken randomly from input file
     *
     * @return List of initial centroids
     */
    public static List<Centroid> generateInitialCentroids(Configuration conf, int clustersNumber, int pointsNumber, Path inputPath) {
        Set<Integer> initialCentroidPositions = new TreeSet<>();
        List<Centroid> initialCentroids = new ArrayList<>();

        Random random = new Random(1);

        // Generate random line numbers as initial centroid positions
        while (initialCentroidPositions.size() != clustersNumber) {
            initialCentroidPositions.add(random.nextInt(pointsNumber));
        }

        try {
            // Access the Hadoop FileSystem and open the input file
            FileSystem hdfs = FileSystem.get(conf);
            FSDataInputStream inputStream = hdfs.open(inputPath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            int lineNumber = 0;
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                if (initialCentroidPositions.contains(lineNumber)) {
                    // The current line number matches one of the initial centroid positions
                    // Process the line and add it to the initialCentroids list
                    List<Double> coordinates = splitInCoordinates(line);
                    Centroid initialCentroid = new Centroid();
                    initialCentroid.setPoint(new Point(coordinates));
                    initialCentroids.add(initialCentroid);
                }
                lineNumber++;
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return initialCentroids;
    }

    /**
     * Set the initial centroids in the Hadoop Configuration (storage of initial centroids)
     *
     * @param initialCentroids initial centroids
     */
    public static void setCentroidsInConfiguration(List<Centroid> initialCentroids, Configuration conf) {
        // Create an array to store the centroid points as strings
        String[] centroidStrings = new String[initialCentroids.size()];

        // Convert each centroid's point to a string and store it in the array
        for (int i = 0; i < initialCentroids.size(); i++) {
            centroidStrings[i] = initialCentroids.get(i).getPoint().toString();
        }
        // Set the configuration property specified by `key` to the array of centroid strings
        conf.setStrings("centroids", centroidStrings);
    }

    /**
     * Configures a job for a specific iteration.
     *
     * @param iteration The iteration number for the job.
     * @return A configured Hadoop MapReduce job for K-Means clustering.
     */
    public static Job configureJob(int iteration, Configuration conf, int reducersNumber, Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "K-Means-Job-n-" + iteration);
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);
        job.setNumReduceTasks(reducersNumber);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Point.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    /**
     * Calculates the shift (change) in centroids between the previous centroids stored in the
     * configuration and the current centroids.
     *
     * @param computedCentroids The list of current centroids.
     * @return The total shift (change) in centroids.
     */
    public static double computeCentroidsShift(List<Centroid> computedCentroids, Configuration conf) {
        // Load previous centroids from the configuration
        List<Centroid> previousCentroids = readCentroidsInConfiguration(conf);
        // Initialize the variable to store the total shift of all centroids
        double centroidShift = 0.0;
        // Iterate over all centroids
        for (int i = 0; i < computedCentroids.size(); i++) {
            // Calculate the Euclidean distance between the computed and previous centroids
            double distance = computeEuclideanDistance(computedCentroids.get(i).getPoint(),
                                                            previousCentroids.get(i).getPoint());
            // Add the distance to the total shift
            centroidShift += distance;
        }
        // Return the total shift of all centroids
        System.out.println("KMeans() - Shift computed : " + centroidShift);
        return centroidShift;
    }

    /**
     * Reads computed centroids from files and returns them as a list.
     *
     * @return A list of computed centroids.
     */
    public static List<Centroid> readComputedCentroids(Configuration conf, Path outputPath) throws IOException {
        List<Centroid> computedCentroids = new ArrayList<>();
        FileSystem hdfs = FileSystem.get(conf);
        try {
            // List the status of files in the specified output path.
            FileStatus[] fileStatus = hdfs.listStatus(outputPath);
            for (FileStatus status : fileStatus) {
                Path path = status.getPath();
                // Skip success files in the output folder
                if (path.getName().endsWith("_SUCCESS")) {
                    continue;
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)))) {
                    // Read each line from the file.
                    for (String line; (line = br.readLine()) != null;) {
                        //split line by spaces
                        String[] fields = line.split("\\s");
                        Centroid centroid = new Centroid();
                        // Set the centroid ID from the first field.
                        centroid.setCentroidID(new IntWritable(Integer.parseInt(fields[0])));
                        List<Double> coordinates = new ArrayList<>();
                        // Parse and add the remaining fields as coordinates.
                        for (int i = 1; i < fields.length; i++) {
                            coordinates.add(Double.parseDouble(fields[i]));
                        }
                        // Create a Point object with the parsed coordinates.
                        Point point = new Point(coordinates);
                        centroid.setPoint(point);
                        // Add the computed centroid to the list.
                        computedCentroids.add(centroid);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Sort the computed centroids.
        Collections.sort(computedCentroids);
        return computedCentroids;
    }

    /**
     * Read the set of centroid stored in the Hadoop configuration
     * @return a list of centroids
     */
    public static List<Centroid> readCentroidsInConfiguration(Configuration conf) {
        //create a list to store the read centroids set
        List<Centroid> centroids = new ArrayList<>();

        //get string representation of the centroids
        String[] centroidStrings = conf.getStrings("centroids");

        // Convert each centroid's point to a string and store it in the array
        for (int i = 0; i < centroidStrings.length; i++) {
            //get the coordinates by splitting the i-th centroid string
            List<Double> coordinates = splitInCoordinates(centroidStrings[i]);
            //add to centroids list a new Centroid object along with the extracted coordinates
            centroids.add(new Centroid(i,new Point(coordinates)));
        }
        //return the list of centroids
        return centroids;
    }

    /**
     * Split a string by spaces and creates a list of doubles representing the coordinates
     * @param text text representing the point's coordinates to split
     * @return coordinates in List<Double> format
     */
    public static List<Double> splitInCoordinates(String text){
        List<Double> coordinates = new ArrayList<>();
        String[] line = text.split(" ");
        for (String coordinate : line) {
            coordinates.add(Double.parseDouble(coordinate));
        }
        return coordinates;
    }

    /**
     * Compute the Euclidean distance between two points
     * Math explanation: square root of the sum of the squares of the differences for each coordinate
     * @param point point to compare against the centroid
     * @param centroid current centroid
     * @return distance between the centroid and the point
     */
    public static double computeEuclideanDistance(Point point, Point centroid) {
        double sum = 0.0;
        List<Double> centroidCoordinates = centroid.getCoordinates();
        for (int i = 0; i < centroidCoordinates.size(); i++) {
            double diff = centroidCoordinates.get(i) - point.getCoordinates().get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Clears the specified output path in the Hadoop Distributed FileSystem (HDFS).
     * If the output path exists, it is recursively deleted.
     *
     * @param conf The Hadoop Configuration object.
     * @param outputPath The Hadoop Path object representing the output path to be cleared.
     * @throws IOException If an I/O error occurs while attempting to delete the output path.
     */
    public static void clearOutputPath(Configuration conf, Path outputPath) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        try {
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
        } catch (IOException e) {
            System.err.println("Error in deleting the output path");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Checks if the convergence criterion for the K-Means algorithm is satisfied.
     * The convergence criterion is satisfied if one of the following conditions is met:
     * 1. The total difference (shift) of all centroids is less than the specified threshold.
     * 2. The current iteration has reached the maximum number of iterations.
     *
     * @param shift The total difference of all centroids.
     * @param currentIteration The current iteration number.
     * @param threshold The convergence threshold to determine when to stop iterating.
     * @param maxIterations The maximum number of iterations allowed for the algorithm.
     * @return {@code true} if the convergence criterion is satisfied, {@code false} otherwise.
     */
    public static boolean isConverged(double shift, int currentIteration, Float threshold, int maxIterations) {
        return shift < threshold || currentIteration == maxIterations;
    }


    /**
     * Append to k-means-log.txt file the iteration and shift information or the final centroids computed
     *
     * @param currentIteration  number of the current iteration
     * @param centroidShift     value of the current shift
     * @param computedCentroids list of the final centroids computed
     * @param isIteration       boolean to discriminate if the iterations are terminated or not
     * @param executionTime
     */
    public static void addLogInfo(int currentIteration, double centroidShift, List<Centroid> computedCentroids, boolean isIteration, double executionTime) {
        // Use try-with-resources to automatically close the FileWriter, BufferedWriter and PrintWriter
        try (FileWriter fw = new FileWriter("k-means-log.txt", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            if (isIteration) {
                // Write the iteration information to the log file
                out.println("Iteration " + currentIteration + ", Shift Value: " + centroidShift);
            }
            else {
                out.println("\nFINAL CENTROIDS:");
                // write the final centroids computed to the log file
                for (Centroid centroid : computedCentroids) {
                    out.println(centroid.getPoint().toString());
                }
                // write the timestamp in the log file
                out.println("\nTimestamp: " + new Timestamp(System.currentTimeMillis()));
                // write the execution time in the log file
                out.println("\nExecution Time: " + executionTime + " s");
                // write the average iteration time in the log file
                out.println("\nAverage Iteration Time: " + executionTime/currentIteration + " s");
            }
        } catch (IOException e) {
            System.err.println("Error during the write operation on the log file");
            e.printStackTrace();
        }
    }
}

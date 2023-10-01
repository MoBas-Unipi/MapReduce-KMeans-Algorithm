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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class KMeans {

    private Path inputPath;
    private Path outputPath;
    private int pointsNumber;
    private int clustersNumber;
    private int reducersNumber;
    private float threshold;
    private int maxIterations;
    private FileSystem hdfs;


    //TODO provare a fare le funzioni "public static"
    public void setParameters(String[] args , Configuration conf){
        this.inputPath = new Path(args[0]);
        this.outputPath = new Path(args[1]);
        this.pointsNumber = conf.getInt("points_number",1000); //n
        this.clustersNumber = conf.getInt("clusters_number", 4); //k
        this.reducersNumber = conf.getInt("reducers_number", 1);
        this.threshold = conf.getFloat("threshold", 0.0001F);
        this.maxIterations = conf.getInt("max_iterations", 30);
    }

    /**
     * Generate a list of Centroid (size k = clusterNumber) taken randomly from input file
     *
     * @return List of initial centroids
     */
    public List<Centroid> generateInitialCentroids(Configuration conf) {
        Set<Integer> initialCentroidPositions = new TreeSet<>();
        List<Centroid> initialCentroids = new ArrayList<>();

        Random random = new Random();

        // Generate random line numbers as initial centroid positions
        while (initialCentroidPositions.size() != this.clustersNumber) {
            initialCentroidPositions.add(random.nextInt(this.pointsNumber));
        }

        try {
            // Access the Hadoop FileSystem and open the input file
            this.hdfs = FileSystem.get(conf);
            FSDataInputStream inputStream = this.hdfs.open(this.inputPath);
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
    public void setCentroidsInConfiguration(List<Centroid> initialCentroids, Configuration conf) {
        // Create an array to store the centroid points as strings
        String[] centroidStrings = new String[initialCentroids.size()];

        // Convert each centroid's point to a string and store it in the array
        for (int i = 0; i < initialCentroids.size(); i++) {
            centroidStrings[i] = initialCentroids.get(i).getPoint().toString();
        }
        // Set the configuration property specified by `key` to the array of centroid strings
        conf.setStrings("centroids", centroidStrings);
        //System.out.println(Arrays.toString(conf.getStrings("centroids")));
    }

    /**
     * Configures a job for a specific iteration.
     *
     * @param iteration The iteration number for the job.
     * @return A configured Hadoop MapReduce job for K-Means clustering.
     */
    public Job configureJob(int iteration, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println(Arrays.toString(conf.getStrings("centroids")));
        Job job = Job.getInstance(conf, "K-Means-Job-n-" + iteration);
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Point.class);
        job.setNumReduceTasks(reducersNumber);
        job.setReducerClass(KMeansReducer.class);
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
    public double computeCentroidsShift(List<Centroid> computedCentroids, Configuration conf) {
        // Load previous centroids from the configuration
        List<Centroid> previousCentroids = readCentroidsInConfiguration(conf);
        // Initialize the variable to store the total shift of all centroids
        double centroidShift = 0.00000;
        // Iterate over all centroids
        for (int i = 0; i < computedCentroids.size(); i++) {
            // Calculate the Euclidean distance between the computed and previous centroids
            double distance = this.computeEuclideanDistance(computedCentroids.get(i).getPoint(),
                                                            previousCentroids.get(i).getPoint());
            // Add the distance to the total shift
            centroidShift += distance;
        }
        // Return the total shift of all centroids
        System.out.println("SHIFT :" + centroidShift);
        return centroidShift;
    }

    /**
     * Reads computed centroids from files and returns them as a list.
     *
     * @return A list of computed centroids.
     */
    public List<Centroid> readComputedCentroids() {
        List<Centroid> computedCentroids = new ArrayList<>();

        try {
            // List the status of files in the specified output path.
            FileStatus[] fileStatus = this.hdfs.listStatus(this.outputPath);
            for (FileStatus status : fileStatus) {
                Path path = status.getPath();
                // Skip success files in the output folder
                if (path.getName().endsWith("_SUCCESS")) {
                    continue;
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader(this.hdfs.open(path)))) {
                    // Read each line from the file.
                    for (String line; (line = br.readLine()) != null;) {
                        String[] fields = line.split(" ");
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
    public List<Centroid> readCentroidsInConfiguration(Configuration conf) {
        //create a list to store the read centroids set
        List<Centroid> centroids = new ArrayList<>();

        //get string representation of the centroids
        String[] centroidStrings = conf.getStrings("centroids");

        // Convert each centroid's point to a string and store it in the array
        for (int i = 0; i < centroidStrings.length; i++) {
            //get the coordinates by splitting the i-th centroid string
            List<Double> coordinates = splitInCoordinates(centroidStrings[i]);
            //add to centroids list a new Centroid object along with the extracted coordinates
            centroids.add(new Centroid(new IntWritable(i),new Point(coordinates)));
        }
        //return the list of centroids
        return centroids;
    }

    //TODO test
    /**
     * Split a string by "," characters and creates a list of doubles representing the coordinates
     * @param text text representing the point's coordinates to split
     * @return coordinates in List<Double> format
     */
    public List<Double> splitInCoordinates(String text){
        List<Double> coordinates = new ArrayList<>();
        String[] line = text.split(" ");
        for (String coordinate : line) {
            coordinates.add(Double.parseDouble(coordinate));
        }
        return coordinates;
    }


    //TODO test
    /**
     * Compute the Euclidean distance between two points
     * Math explanation: square root of the sum of the squares of the differences for each coordinate
     * @param point point to compare against the centroid
     * @param centroid current centroid
     * @return distance between the centroid and the point
     */
    public double computeEuclideanDistance(Point point, Point centroid) {
        double sum = 0.000000;
        List<Double> centroidCoordinates = centroid.getCoordinates();
        for (int i = 0; i < centroidCoordinates.size(); i++) {
            double diff = centroidCoordinates.get(i) - point.getCoordinates().get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public void clearOutputPath() {
        try {
            if (this.hdfs.exists(this.outputPath)) {
                this.hdfs.delete(this.outputPath, true);
            }
        } catch (IOException e) {
            System.err.println("Error in deleting the output path");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     *
     * @param shift total difference of all centroids
     * @param currentIteration current iteration
     * @return true if the convergence criterion is satisfied
     */
    public boolean isConverged(double shift, int currentIteration) {
        return shift < this.threshold || currentIteration == maxIterations;
    }

    public int getMaxIterations() {
        return maxIterations;
    }
}

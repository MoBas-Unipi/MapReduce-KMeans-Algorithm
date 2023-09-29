package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.mapreduce.KMeansCombiner;
import it.unipi.dii.hadoop.mapreduce.KMeansMapper;
import it.unipi.dii.hadoop.mapreduce.KMeansReducer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import it.unipi.dii.hadoop.model.Centroid;
import it.unipi.dii.hadoop.model.Point;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class Utils {

    private Configuration conf;
    private Path inputPath;
    private Path outputPath;
    private int pointsNumber;
    private int clustersNumber;
    private int distance;
    private int reducersNumber;
    private float threshold;
    private int maxIterations;

    public void setParameters(String[] args){
        this.conf = new Configuration();
        this.conf.addResource(new Path("config.xml"));
        this.inputPath = new Path(args[0]);
        this.outputPath = new Path(args[1]);
        this.pointsNumber = conf.getInt("points_number",100); //n
        this.clustersNumber = conf.getInt("clusters_number", 2); //k
        this.distance = conf.getInt("distance", 2); //d
        this.reducersNumber = conf.getInt("reducers_number", 1);
        this.threshold = conf.getFloat("threshold", 0.0001f);
        this.maxIterations = conf.getInt("max_iterations", 50);
    }

    /**
     * Generate a list of Centroid (size k = clusterNumber) taken randomly from input file
     *
     * @return List of initial centroids
     */
    public List<Point> generateInitialCentroids() {
        Set<Integer> initialCentroidPositions = new TreeSet<>();
        List<Point> initialCentroids = new ArrayList<>();

        Random random = new Random();

        // Generate random line numbers as initial centroid positions
        while (initialCentroidPositions.size() != this.clustersNumber) {
            initialCentroidPositions.add(random.nextInt(this.pointsNumber));
        }

        try {
            // Access the Hadoop FileSystem and open the input file
            FileSystem hdfs = FileSystem.get(this.conf);
            FSDataInputStream inputStream = hdfs.open(this.inputPath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            int lineNumber = 0;
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                if (initialCentroidPositions.contains(lineNumber)) {
                    // The current line number matches one of the initial centroid positions
                    // Process the line and add it to the initialCentroids list
                    List<Double> coordinates = splitInCoordinates(line);
                    Point initialCentroid = new Point(coordinates);
                    initialCentroids.add(initialCentroid);
                }
                lineNumber++;
            }

            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return initialCentroids;
    }


    /**
     * Set the initial centroids in the Hadoop Configuration (storage of initial centroids)
     *
     * @param initialCentroids initial centroids
     */
    public void setCentroidsInConfiguration(List<Point> initialCentroids) {
        // Create an array to store the centroid points as strings
        String[] centroidStrings = new String[initialCentroids.size()];

        // Convert each centroid's point to a string and store it in the array
        for (int i = 0; i < initialCentroids.size(); i++) {
            centroidStrings[i] = initialCentroids.get(i).toString();
        }
        // Set the configuration property specified by `key` to the array of centroid strings
        this.conf.setStrings("centroids", centroidStrings);
    }


    public Job configureJob(int iteration) {
        Job job;
        try {
            job = Job.getInstance(this.conf, "K-Means Job n. " + iteration);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(this.reducersNumber);
            FileInputFormat.addInputPath(job, this.inputPath);
            FileOutputFormat.setOutputPath(job, this.outputPath);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return job;
    }


    /**
     * Read the set of centroid stored in the Hadoop configuration
     * @return a list of centroids
     */
    public List<Centroid> readCentroidsInConfiguration() {
        //create a list to store the read centroids set
        List<Centroid> centroids = new ArrayList<>();

        //get string representation of the centroids
        String[] centroidStrings = this.conf.getStrings("centroids");

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
        double sum = 0;
        List<Double> centroidCoordinates = centroid.getCoordinates();
        for (int i = 0; i < centroidCoordinates.size(); i++) {
            double diff = centroidCoordinates.get(i) - point.getCoordinates().get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }
}

package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Centroid;
import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Utils {
    private Path inputPath;
    private Path outputPath;
    private int pointsNumber;
    private int clustersNumber;
    private int distance;
    private int reducersNumber;
    private float threshold;
    private int maxIterations;

    public void setParameters(Configuration conf, String[] args){
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
     * @param conf Hadoop configuration
     * @return List of initial centroids
     */
    public List<Centroid> generateInitialCentroidSet(Configuration conf) {
        Set<Integer> initialCentroidPositions = new TreeSet<>();
        List<Centroid> initialCentroids = new ArrayList<>();

        Random random = new Random();

        // Generate random line numbers as initial centroid positions
        while (initialCentroidPositions.size() != this.clustersNumber) {
            initialCentroidPositions.add(random.nextInt(this.pointsNumber));
        }

        try {
            // Access the Hadoop FileSystem and open the input file
            FileSystem hdfs = FileSystem.get(conf);
            FSDataInputStream inputStream = hdfs.open(this.inputPath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            int lineNumber = 0;
            int centroidId = 0;
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                if (initialCentroidPositions.contains(lineNumber)) {
                    // The current line number matches one of the initial centroid positions
                    // Process the line and add it to the initialCentroids list
                    List<Double> coordinates = splitInCoordinates(line);
                    Centroid centroid = new Centroid(new IntWritable(centroidId) , new Point(coordinates));
                    initialCentroids.add(centroid);
                    centroidId++;
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
     * @param conf Hadoop configuration
     * @param initialCentroidSet initial centroids set
     */
    public void setCentroidsSetInConfiguration(Configuration conf, List<Centroid> initialCentroidSet) {

    }

    /**
     * Split a string by a "," and creates a list of doubles representing the coordinates
     * @param text
     * @return coordinates in List<Double> format
     */
    public List<Double> splitInCoordinates(String text){
        List<Double> coordinates = new ArrayList<>();
        String[] line = text.split(",");
        for (String coordinate : line) {
            coordinates.add(Double.parseDouble(coordinate));
        }
        return coordinates;
    }
}

package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Centroid;
import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

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
     * @return
     */
    public List<Centroid> generateInitialCentroidSet(Configuration conf) {
        return null;
    }

    /**
     * Set the initial centroids in the Hadoop Configuration (storage of initial centroids)
     * @param conf Hadoop configuration
     * @param initialCentroidSet initial centroids set
     */
    public void setCentroidsSetInConfiguration(Configuration conf, List<Centroid> initialCentroidSet) {

    }


    //TODO togliere la config come parametro e prenderla da locale
    /**
     * Read the set of centroid stored in the Hadoop configuration
     * @param conf TODO remove
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
            centroids.add(new Centroid(i,coordinates));
        }
        //return the list of centroids
        return centroids;
    }

    //TODO test
    /**
     * Split a string by "," characters and creates a list of doubles representing the coordinates
     * @param text string to split
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

package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.KMeans;
import it.unipi.dii.hadoop.model.Centroid;
import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Point> {

    private List<Centroid> centroids;
    private KMeans utils = new KMeans();


    /**
     *Setup function of the KMeansMapper class.
     * initializes and loads the list of centroids saved in the Hadoop configuration to be used in the Mapper function
     * @param context context object to interact with Hadoop
     */
    @Override
    public void setup(Context context) {
        System.out.println("FUNZIONA SETUP");
        //Load centroids set from Hadoop configuration
        this.centroids = utils.readCentroidsInConfiguration(context.getConfiguration());
    }

    /**
     * Map function of the KMeansMapper class.
     * 1.create a Point object from the text received
     * 2.find the centroid closest to him
     * 3.emits a pair containing the chosen centroid along with the object created
     * @param key input key
     * @param value input value (point coordinates)
     * @param context context object to interact with Hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //1.create the Point object
        List<Double> coordinates = utils.splitInCoordinates(String.valueOf(value));
        Point point = new Point(coordinates);

        //initialize variables to write/emit
        IntWritable centroidID = null;
        double distanceFromCentroid = Double.MAX_VALUE ;

        //2.find the nearest centroid to the point
        for (Centroid centroid : centroids) {
            System.out.println("READ CENTROIDS");
            //get the current centroid (for use its coordinates)
            Point currentCentroid = centroid.getPoint();

            //compute the distance between the point and the current centroid
            double currentDistance = utils.computeEuclideanDistance(point, currentCentroid);
            System.out.println(currentDistance);

            //check and update the nearest centroid
            if (currentDistance < distanceFromCentroid) {
                centroidID = centroid.getCentroidID();
                distanceFromCentroid = currentDistance;
            }
        }

        //3.emit/write the pair (centroid_id, nearest point)
        context.write(centroidID, point);
        System.out.println("last distance: "+distanceFromCentroid);
        System.out.println("FUNZIONA MAPPER");
    }

}

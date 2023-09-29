package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Text> {

    /**
     * Reduce function of the KMeansReducer class.
     * Receives a list of Points (also points with partial sums) associated to a specific centroid and then:
     1.creates the new centroid object that contains the sum of the coordinates of all points in the list (each dimension separated)
     2.computes the position of the new centroid by averaging the sum just calculated
     3.emits a pair containing the centroidID and a new Centroid object with its new coordinates
     * @param centroidID centroid id
     * @param partialSumPointsList list containing all points associated to this centroid (included points with partial sums)
     * @param context context object to interact with Hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce (IntWritable centroidID, Iterable<Point> partialSumPointsList, Context context) throws IOException, InterruptedException {
        Iterator<Point> pointsIterator = partialSumPointsList.iterator();
        Point nextCentroid = pointsIterator.next();

        //1.sum the coordinates of the points associated for each centroidID
        while (pointsIterator.hasNext()) {
            nextCentroid.sumCoordinates(pointsIterator.next());
        }
        //2.compute the new centroid position (average of the coordinates sum)
        nextCentroid.averageCoordinates();

        //3.emit/write of the pair (centroidID, updated centroid)
        context.write(centroidID, new Text(nextCentroid.toString()));
    }


    /*
    ??CLENAUP
        1. chiusura file oppure liberare spazio

     */



}

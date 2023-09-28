package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

    /**
     * Combine function (mini-reducer) of the KMeansCombiner class.
     * Receives a list of Points associated to a specific centroid and then:
     * 1.creates a new point that contains the sum of the coordinates of all points in the list (each dimension separated)
     * 2.emits a pair containing the cetroidID along with the newly created point containing the computed sum
     * @param centroidID id of the centroid
     * @param pointsList list containing all points associated to this centroid
     * @param context context object to interact with Hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(IntWritable centroidID, Iterable<Point> pointsList, Context context) throws IOException, InterruptedException {
        //definition of an iterator for the points list
        Iterator<Point> pointsIterator = pointsList.iterator();

        //definition of the point that holds the coordinates partial sum
        Point partialSumPoint = pointsIterator.next();

        //iteration and sum over the whole list of points
        while (pointsIterator.hasNext()) {
            //1.sum the coordinates of the current point with the other points coordinates in list
            partialSumPoint.sumCoordinates(pointsIterator.next());
        }

        //2. emit/write of the pair (centroidID, partialSumPoint)
        context.write(centroidID,partialSumPoint);
    }

}

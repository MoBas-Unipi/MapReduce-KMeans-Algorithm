package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends Reducer <IntWritable, Point, IntWritable, Point> {

    /* REDUCE (MINI-REDUCER) (centroid_id, Iterable<Point>, context)
        1.somma parziale dei punti associati allo stesso centroid_id
        2.emit coppia (centroid_id, somma parziale)
     */

}

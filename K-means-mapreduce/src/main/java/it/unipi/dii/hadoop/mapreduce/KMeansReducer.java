package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer <IntWritable, Point, IntWritable, Text> {

    /* REDUCE (centroid_id, Iterable<Point>, context)
        1. somma totale dei punti associati allo stesso centroid_id
        2. calcolo posizione nuovo centroide (media dei punti per ogni centroid_id)
        3. emit/write (centroid_id, posizione nuovo centroide)


    ??CLENAUP
        1. chiusura file oppure liberare spazio

     */



}

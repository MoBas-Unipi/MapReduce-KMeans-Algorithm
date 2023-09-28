package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Point> {


    /*SETUP FUNCTION (context)
        1. read centroids from configuration (HDFS)
     */


    /*MAP FUNCTION (key, punto, context)
        1.Creare oggetto punto
        2.Trovare il centroide più vicino al punto e salvare il centroid_id
        3.Emit/write (centroid_id, punto)



     */



}

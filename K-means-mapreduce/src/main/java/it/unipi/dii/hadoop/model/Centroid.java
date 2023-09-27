package it.unipi.dii.hadoop.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class Centroid implements WritableComparable<Centroid> {

    private IntWritable centroidID;
    private Point point;


    public Centroid(int centroidID, List<Double> coordinates) {
        this.centroidID = new IntWritable(centroidID);
        this.point = new Point(coordinates);
    }

    public IntWritable getCentroidID() {
        return centroidID;
    }

    public Point getPoint() {
        return point;
    }

    @Override
    public int compareTo(Centroid centroid) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}

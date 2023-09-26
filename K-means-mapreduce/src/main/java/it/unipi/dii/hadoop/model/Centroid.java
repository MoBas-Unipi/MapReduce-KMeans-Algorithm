package it.unipi.dii.hadoop.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid implements WritableComparable<Centroid> {

    private IntWritable centroidID;
    private Point point;

    public Centroid(IntWritable centroidID, Point point) {
        this.centroidID = centroidID;
        this.point = point;
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

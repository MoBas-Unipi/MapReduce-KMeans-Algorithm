package it.unipi.dii.hadoop.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid implements WritableComparable<Centroid> {

    private IntWritable centroidID;
    private Point point;

    public Centroid() {
    }

    public Centroid(IntWritable centroidID, Point point) {
        this.centroidID = centroidID;
        this.point = point;
    }

    public IntWritable getCentroidID() {
        return centroidID;
    }

    public Point getPoint() {
        return point;
    }

    /**
     * Override IntWritable class compareTo function.
     * function used for the shuffle and sort operation.
     * compares two centroid ids and returns the following values based on the result:
     *  -1: if the currentCentroidID is less than the centroidID passed as parameter
     *   0: if they are equal
     *   1: if the currentCentroidID is greater than the centroidID passed as parameter
     * @param centroid Centroid object to compare against the current centroid
     * @return an integer based on the comparison result (-1, 0 or 1)
     */

    public void setCentroidID(IntWritable centroidID) {
        this.centroidID = centroidID;
    }

    public void setPoint(Point point) {
        this.point = point;
    }

    @Override
    public int compareTo(Centroid centroid) {
        //initialize the centroid IDs to compare
        int currentCentroidID = Integer.parseInt(String.valueOf(this.centroidID));
        int centroidIDtoCompare = Integer.parseInt(String.valueOf(centroid.getCentroidID()));

        //compare the centroid IDs and return a value as following
        return (currentCentroidID < centroidIDtoCompare ? -1 : (currentCentroidID == centroidIDtoCompare ? 0 : 1));

    }

    /**
     * Override IntWritable class write function.
     * serialize in bytes the centroidID and the point object (having the centroid coordinates) attributes of the centroid
     * @param dataOutput output stream object
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.centroidID.write(dataOutput);
        this.point.write(dataOutput);
    }

    /**
     * Override Writable class readFields function
     * deserialize from bytes into a Centroid object attributes the centroidID and the point object (having the centroid coordinates)
     * @param dataInput input stream object
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //deserialize the centroid id and the point object
        this.centroidID.readFields(dataInput);
        this.point.readFields(dataInput);
    }
    
}

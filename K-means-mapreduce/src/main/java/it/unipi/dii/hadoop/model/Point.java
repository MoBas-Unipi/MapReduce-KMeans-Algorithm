package it.unipi.dii.hadoop.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Point implements Writable {
    private List<Double> coordinates;
    private int partialPointsCounter;


    /**
     * Point class constructor
     * @param coordinates list of the coordinates for that point (list of doubles)
     */
    public Point(List<Double> coordinates) {
        this.coordinates = coordinates;
        this.partialPointsCounter = 1;
    }

    /**
     * Sum the point coordinates with the coordinates of the point passed as parameter
     * @param point point with the coordinates to sum with the
     */
    public void sumCoordinates(Point point) {
        //iterate a number of times equal to coordinates dimensions
        for ( int i = 0; i < this.coordinates.size(); i++) {
            //sum the i-th coordinate value with the i-th point coordinate passed as parameter
            this.coordinates.set(i, this.coordinates.get(i) + point.getCoordinates().get(i));
        }
        //update the number of points used to sum the coordinates
        this.partialPointsCounter += point.getPartialPointsCounter();
    }

    /**
     * Compute the average value of each coordinate based on the number of points used to make the
     * sum (partial points counter)
     */
    public void averageCoordinates () {
        //iterate a number of times equal to coordinates dimensions
        for ( int i = 0; i < this.coordinates.size(); i++) {
            //make the average for each coordinate of the point
            this.coordinates.set(i, this.coordinates.get(i) / this.partialPointsCounter);
        }
        //update the number of points to 1 (is a new centroid, not anymore a sum)
        this.partialPointsCounter = 1;
    }

    public List<Double> getCoordinates() {
        return coordinates;
    }

    public int getPartialPointsCounter() {
        return partialPointsCounter;
    }


    /**
     * Override Writable class write function
     * serialize in bytes the coordinates attribute, its size and the partial sum counter
     * @param dataOutput output stream object
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //serialize the dimensions number of the coordinates
        dataOutput.writeInt(this.coordinates.size());
        //serialize coordinates attribute
        for (Double coordinate : this.coordinates) {
            dataOutput.writeDouble(coordinate);
        }
        //serialize partial sums counter attribute
        dataOutput.writeInt(this.partialPointsCounter);
    }

    /**
     * Override Writable class readFields function
     * deserialize from bytes into a Point object attributes the coordinates, its size and the partial sum counter
     * @param dataInput input stream object
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.coordinates = new ArrayList<>();
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            double value = dataInput.readDouble();
            this.coordinates.add(value);
        }
        this.partialPointsCounter = dataInput.readInt();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Double coordinate : coordinates) {
            sb.append(coordinate).append(" ");
        }
        return sb.toString().trim();
    }
}

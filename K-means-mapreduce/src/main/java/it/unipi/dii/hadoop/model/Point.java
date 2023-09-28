package it.unipi.dii.hadoop.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Point implements Writable {
    private List<Double> coordinates = new ArrayList<>();
    private int partialPointsCounter;


    public Point(List<Double> coordinates) {
        this.coordinates = coordinates;
        this.partialPointsCounter = 1;
    }

    
    public List<Double> getCoordinates() {
        return coordinates;
    }

    public int getPartialPointsCounter() {
        return partialPointsCounter;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

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

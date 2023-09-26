package it.unipi.dii.hadoop.model;

import java.util.ArrayList;
import java.util.List;

public class Point {
    private List<Double> coordinates = new ArrayList<>();
    private int partialPointsCounter;


    public Point(List<Double> coordinates) {
        this.coordinates = coordinates;
    }

    
    public List<Double> getCoordinates() {
        return coordinates;
    }

    public int getPartialPointsCounter() {
        return partialPointsCounter;
    }


}

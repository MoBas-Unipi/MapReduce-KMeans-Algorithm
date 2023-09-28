package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Point;

import java.util.List;


public class KMeans {
    private final static Utils utils = new Utils();

    public static void main (String[] args) {
        //check number of arguments
        if (args.length != 2) {
            System.err.println("Error! Usage: <input path> <output path>");
            System.exit(1);
        }
        //create configuration object and load config file and set parameters
        utils.setParameters(args);

        //centroids set generation
        List<Point> initialCentroids = utils.generateInitialCentroids();

        //add centroids set to Hadoop Configuration
        utils.setCentroidsInConfiguration(initialCentroids);



        /*MapReduce Execution
            1. Jobs configuration and submission
            2. Old and new centroids difference
            3. Check threshold OR max iterations
                3.1 write the results in the output file
                3.2 update the new centroids set
        */


        //Record execution time


    }


}

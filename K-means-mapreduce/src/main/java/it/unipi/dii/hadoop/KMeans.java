package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Centroid;
import it.unipi.dii.hadoop.model.Point;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
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
        // check if the number of iterations is set correctly
        if (utils.getMaxIterations() < 1) {
            System.err.println("Error! Define value 'max_iterations' as >= 1");
            System.exit(1);
        }

        //centroids set generation
        List<Centroid> initialCentroids = utils.generateInitialCentroids();

        //add centroids set to Hadoop Configuration
        utils.setCentroidsInConfiguration(initialCentroids);

        int currentIteration = 1;
        boolean convergenceCondition = false;

        while (!convergenceCondition){
            // Delete output path files if exist
            utils.clearOutputPath();
            // Configure and execute Job
            Job job = utils.configureJob(currentIteration);
            try {
                if (!job.waitForCompletion(true)){
                    System.out.println("Error in executing the job");
                    System.exit(1);
                }
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            // Read computed centroids list from the output files
            List<Centroid> computedCentroids = utils.readComputedCentroids();
            // Compute the centroids shift
            double centroidsShift = utils.computeCentroidsShift(computedCentroids);
            // Check the convergence condition
            convergenceCondition = utils.isConverged(centroidsShift, currentIteration);
            if (!convergenceCondition){
                // Set the current computed centroids in configuration
                utils.setCentroidsInConfiguration(computedCentroids);
                currentIteration++;
            }
        }
    }
}

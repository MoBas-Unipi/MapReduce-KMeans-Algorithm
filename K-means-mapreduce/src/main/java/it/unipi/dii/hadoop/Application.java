package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Centroid;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;


public class Application {
    private final static KMeans utils = new KMeans();

    public static void main (String[] args) throws IOException {
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
            //utils.clearOutputPath();
            // Configure and execute Job

            Job job = utils.configureJob(currentIteration);
            /*
            try{
                if (!job.waitForCompletion(true)){
                    System.err.println("Error in the configuration or execution of the job");
                    //System.exit(1);
                }
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                System.err.println("Error in the configuration or execution of the job");
                e.printStackTrace();
            }
            */
            // Read computed centroids list from the output files
            List<Centroid> computedCentroids = utils.readComputedCentroids();
            // Compute the centroids shift
            double centroidsShift = utils.computeCentroidsShift(computedCentroids);
            // Check the convergence condition
            convergenceCondition = utils.isConverged(centroidsShift, currentIteration);
            if (!convergenceCondition){
                // Set the current computed centroids in configuration
                utils.setCentroidsInConfiguration(computedCentroids);
                System.out.println("ITERATION: " + currentIteration);
                currentIteration++;
            }
            else {
                for (Centroid centroid : computedCentroids){
                    System.out.println("CENTROID ID: "+ centroid.getCentroidID());
                    System.out.println("COORDINATES: "+centroid.getPoint().getCoordinates() + "\n");
                }
            }
        }
    }
}

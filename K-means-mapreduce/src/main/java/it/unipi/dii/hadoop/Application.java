package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Application {

    public static void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //check number of arguments
        if (args.length != 2) {
            System.err.println("Error! Usage: <input path> <output path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml"));

        //set parameters loaded from config.xml
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);
        final int pointsNumber = conf.getInt("points_number",1000); //n
        final int clustersNumber = conf.getInt("clusters_number", 4); //k
        final int reducersNumber = conf.getInt("reducers_number", 1);
        final Float threshold = conf.getFloat("threshold", 0.0001F);
        final int maxIterations = conf.getInt("max_iterations", 30);


        // check if the number of iterations is set correctly
        if (maxIterations < 1) {
            System.err.println("Error! Define value 'max_iterations' as >= 1");
            System.exit(1);
        }

        //centroids set generation
        List<Centroid> initialCentroids = KMeans.generateInitialCentroids(conf, clustersNumber, pointsNumber, inputPath);

        //add centroids set to Hadoop Configuration
        KMeans.setCentroidsInConfiguration(initialCentroids,conf);

        //start map reduce execution iterations
        int currentIteration = 1;
        boolean convergenceCondition = false;

        while (!convergenceCondition){
            System.out.println("Application() - ITERATION: " + currentIteration);
            // Delete output path files if exist
            KMeans.clearOutputPath(conf, outputPath);


            // Configure and execute Job
            try(Job job = KMeans.configureJob(currentIteration,conf, reducersNumber, inputPath, outputPath);){
                if (!job.waitForCompletion(true)) {
                    System.err.println("Error in the execution of the job");
                    System.exit(1);
                }
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                System.err.println("Error in the configuration of the job");
                e.printStackTrace();
            }

            // Read computed centroids list from the output files
            List<Centroid> computedCentroids = KMeans.readComputedCentroids(conf, outputPath);
            // Compute the centroids shift
            double centroidsShift = KMeans.computeCentroidsShift(computedCentroids, conf);

            // Check the convergence condition
            convergenceCondition = KMeans.isConverged(centroidsShift, currentIteration, threshold, maxIterations);
            if (!convergenceCondition){
                // Set the current computed centroids in configuration
                KMeans.setCentroidsInConfiguration(computedCentroids,conf);
                currentIteration++;
            } else {
                System.out.println("Application() - FINAL CENTROIDS : ");
                for (Centroid centroid : computedCentroids) {
                    System.out.println(centroid.getPoint().toString());
                }
            }
        }
    }
}

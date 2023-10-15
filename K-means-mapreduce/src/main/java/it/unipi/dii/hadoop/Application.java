package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;


public class Application {

    public static void main (String[] args) throws IOException {
        // Check number of arguments
        if (args.length != 2) {
            System.err.println("Error! Usage: <input path> <output path>");
            System.exit(1);
        }

        // Create Configuration object and load configuration from the specified XML file.
        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml"));

        // Set parameters loaded from config.xml
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);
        final int pointsNumber = conf.getInt("pointsNumber", 1000); // n
        final int clustersNumber = conf.getInt("clustersNumber", 4); // k
        final int reducersNumber = conf.getInt("reducersNumber", 1);
        final Float threshold = conf.getFloat("threshold", 0.0001F);
        final int maxIterations = conf.getInt("maxIterations", 2);

        // Check if the number of iterations is set correctly
        if (maxIterations < 1) {
            System.err.println("Error! Define value 'max_iterations' as >= 1");
            System.exit(1);
        }

        // Centroids set generation
        List<Centroid> initialCentroids = KMeans.generateInitialCentroids(conf, clustersNumber, pointsNumber, inputPath);

        // Add centroids set to Hadoop Configuration
        KMeans.setCentroidsInConfiguration(initialCentroids,conf);

        // Start map reduce execution iterations
        int currentIteration = 1;
        boolean convergenceCondition = false;

        while (!convergenceCondition){
            System.out.println("Application() - ITERATION: " + currentIteration);
            // Delete output path files if exists
            KMeans.clearOutputPath(conf, outputPath);

            // Configure and execute Job
            try(Job job = KMeans.configureJob(currentIteration, conf, reducersNumber, inputPath, outputPath)){
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

            //append the iteration number and the shift value to the log file
            KMeans.addLogInfo(currentIteration,centroidsShift,computedCentroids,true);

            // Check the convergence condition
            convergenceCondition = KMeans.isConverged(centroidsShift, currentIteration, threshold, maxIterations);
            if (!convergenceCondition){
                // Set the current computed centroids in configuration
                KMeans.setCentroidsInConfiguration(computedCentroids, conf);
                currentIteration++;
            } else {
                // print final centroids
                System.out.println("Application() - FINAL CENTROIDS : ");
                for (Centroid centroid : computedCentroids) {
                    System.out.println(centroid.getPoint().toString());
                }
                //append the final centroids to the log file
                KMeans.addLogInfo(currentIteration,centroidsShift,computedCentroids,false);
            }
        }
    }
}

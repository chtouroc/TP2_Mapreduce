package cs.Lab2.pagerank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class PageRankDriver {
    
	  
    public static Double damping_factor = 0.85;
    public static int nbr_iterations = 2;
    public static String input = "TP2_Mapreduce/PLP/exercice_5_2_Pagerank/soc-Epinions1.txt";
    public static String output = "TP2_Mapreduce/PLP/exercice_5_2_Pagerank/result";
    
    public static Set<String> noeuds = new HashSet<String>();
    public static String séparateur = "|";

  
    public static void main(String[] args) throws Exception {
        
        String result1 = null;;
        String final_result = null;
        PageRankDriver pagerank = new PageRankDriver();
        
        System.out.println("Execution de l'Etape1");
        boolean isCompleted = pagerank.Etape1(input, output + "/result0");
        if (!isCompleted) {
            System.exit(1);
        }
        for (int nbr_runs = 0; nbr_runs < nbr_iterations; nbr_runs++) {
            result1 = output + "/result" + Integer.toString(nbr_runs);
            final_result = output + "/result" + Integer.toString(nbr_runs + 1);
            System.out.println("Execution de l'Etape2[" + (nbr_runs + 1) + "/" + PageRankDriver.nbr_iterations + "] (PageRank calculation) ...");
            isCompleted = pagerank.Etape2(result1, final_result);
            if (!isCompleted) {
                System.exit(1);
            }
        }

        System.exit(0);
    }
    

    public boolean Etape1(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Etape1");
        job.setJarByClass(PageRankDriver.class);
        
        // Input Mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Etape1Mapper.class);
        
        // Output Reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Etape1Reducer.class);
        
        return job.waitForCompletion(true);
     
    }
    

    public boolean Etape2(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Etape2");
        job.setJarByClass(PageRankDriver.class);
        
        // Input Mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Etape2Mapper.class);
        
        // Output Reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Etape2Reducer.class);

        return job.waitForCompletion(true);
        
    }

    
}

    


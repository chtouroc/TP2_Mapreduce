package cs.Lab2.TfIdf;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TfIdfDriver extends Configured implements Tool {

    private static final String output = "TP2_Mapreduce/PLP/exercice_5_1_TfIdf/output/etape3";
 

    private static final String input = "TP2_Mapreduce/PLP/exercice_5_1_TfIdf/output/etape2";
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Fréquence d'un mot dans le corpus, TF-IDF");
 
        job.setJarByClass(TfIdfDriver.class);
        job.setMapperClass(TfIdfMapper.class);
        job.setReducerClass(TfIdfReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
 
        //on récupère le nombre de documents du corpus
        Path inputPath = new Path("/home/cloudera/workspace/TfIdf/input/");
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
        //on stocke le nombre de documents dans le nom du job
        job.setJobName(String.valueOf(stat.length));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TfIdfDriver(), args);
        System.exit(res);
    }
}

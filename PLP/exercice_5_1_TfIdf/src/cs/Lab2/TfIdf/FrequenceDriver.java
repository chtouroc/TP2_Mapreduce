package cs.Lab2.TfIdf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class FrequenceDriver extends Configured implements Tool {
	//chemin de l'output 
    private static final String output = "output/etape1";
    //chemin du texte en entrée
    private static final String input = "/home/cloudera/workspace/TfIdf/input/";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Fréquence d'un mot dans le doc");
 
        job.setJarByClass(FrequenceDriver.class);
        job.setMapperClass(FrequenceMapper.class);
        job.setReducerClass(FrequenceReducer.class);
        job.setCombinerClass(FrequenceReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FrequenceDriver(), args);
        System.exit(res);
    }
}



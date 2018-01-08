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




public class NombreTotalMotsDriver extends Configured implements Tool {
    private static final String output = "/home/cloudera/workspace/TfIdf/output/etape2";
 
    private static final String input = "/home/cloudera/workspace/TfIdf/output/etape1";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Nombre total de mots");
 
        job.setJarByClass(NombreTotalMotsDriver.class);
        job.setMapperClass(NombreTotalMotsMapper.class);
        job.setReducerClass(NombreTotalMotsReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new NombreTotalMotsDriver(), args);
        System.exit(res);
    }
}
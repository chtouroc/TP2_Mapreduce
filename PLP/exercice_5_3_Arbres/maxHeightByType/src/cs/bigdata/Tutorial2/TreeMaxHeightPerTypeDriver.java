package cs.bigdata.Tutorial2;



import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TreeMaxHeightPerTypeDriver {
	public static void main(String[] args) {
		JobClient client = new JobClient();
		// Configuring the Job
		JobConf conf = new JobConf(TreeMaxHeightPerTypeDriver.class);
		conf.setJobName("TreeMaxHeightPerType");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// Setting Mapper and reducer
		conf.setMapperClass(TreeMaxHeightPerTypeMapper.class);
		conf.setReducerClass(TreeMaxHeightPerTypeReducer.class);

		// Formats of the Data Type of Input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// input and output directories
		FileInputFormat.setInputPaths(conf, new Path("TP2_Mapreduce/PLP/exercice_5_3_Arbres/output/arbres.csv"));
		FileOutputFormat.setOutputPath(conf, new Path("TP2_Mapreduce/PLP/exercice_5_3_Arbres/output/output_Max_Height"));
	
		client.setConf(conf);
		try {
			// Run the job
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

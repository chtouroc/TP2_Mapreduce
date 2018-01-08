package cs.bigdata.Tutorial2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;

public class TreeMaxHeightPerTypeMapper extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable _key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String Temp= value.toString();
		String[] SplitData = Temp.split(";");
		double maxHeight = 0 ;
		if ( !SplitData[6].isEmpty() )  {
			if (!(SplitData[6].equals("HAUTEUR"))) {
			    maxHeight = Double.parseDouble(SplitData[6]);
			}
		}
		int maxValuePartint = (int)maxHeight ;
		output.collect(new Text(SplitData[2]), new IntWritable(maxValuePartint));
	}
}
package cs.bigdata.Tutorial2;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TreeMaxHeightPerTypeReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text _key,
			Iterator<IntWritable> values,
			OutputCollector<Text,IntWritable> output, 
			Reporter reporter)
			throws IOException {
		Text key = _key;
		// initialize the maximum per type
		int maximum = 0;
		//update the maximum per type using the mapper's output
		while (values.hasNext()) {
			IntWritable val = (IntWritable) values.next();
			if (val.get() > maximum) {
				maximum = val.get();
			}
		}
		output.collect(key, new IntWritable(maximum));
	}
}
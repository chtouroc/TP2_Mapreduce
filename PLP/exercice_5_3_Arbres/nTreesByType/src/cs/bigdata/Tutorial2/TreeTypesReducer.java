package cs.bigdata.Tutorial2;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TreeTypesReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {


	@Override
	public void reduce(Text _key,
			Iterator<IntWritable> values,
			OutputCollector<Text,IntWritable> output, 
			Reporter reporter)
			throws IOException {
		Text key = _key;
		//initialize the tree frequency per type
		int freq = 0;
		//increment tree frequency whenever the same type is encountered
		while (values.hasNext()) {
			IntWritable val = (IntWritable) values.next();
			freq += val.get();
		}
		output.collect(key, new IntWritable(freq));
	}
}
package cs.Lab2.TfIdf;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class TfIdfMapper extends Mapper<LongWritable, Text, Text, Text> {

	public TfIdfMapper() {
	}

 
   
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        //on prend en entrée les couples <(terme " dans " nom_doc), nbOccurences n / nbre total de mots dans le doc N>
			 	//on retourne <terme, nom_doc = n/N>
			 	String[] wordAndCounters = value.toString().split("\t");
		        String[] wordAndDoc = wordAndCounters[0].split(" dans ");                
		        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
		    }
		}
		
		
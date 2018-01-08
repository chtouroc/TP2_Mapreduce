package cs.Lab2.TfIdf;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class NombreTotalMotsMapper extends Mapper<LongWritable, Text, Text, Text> {

	public NombreTotalMotsMapper() {
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	//on prend en entrée <terme " dans " nom_doc, nbOccurences> et on retourne <nom_doc, terme=nbOccurences>
		 	//on sépare le nombre d'occurence 
			String[] ch1 = value.toString().split("\t");
			//on sépare le mot du nom du doc
	        String[] ch2 = ch1[0].split(" dans ");
	        context.write (new Text(ch2[1]), new Text (ch2[0] + "=" + ch1[1]));
	    }
         
      }

   
		
		
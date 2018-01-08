package cs.Lab2.TfIdf;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class NombreTotalMotsReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
//le reducer retourne le couple <(terme " dans " nom_doc),(nbre d'occurences, nombre total de mots dans le doc>
	  int total_mots = 0;
      Map<String, Integer> occurence_mot = new HashMap<String, Integer>();
      for (Text val : values) {
          String[] ch1 = val.toString().split("=");
          occurence_mot.put(ch1[0], Integer.valueOf(ch1[1]));
          total_mots += Integer.parseInt(val.toString().split("=")[1]);
      }
      for (String word : occurence_mot.keySet()) {
          context.write(new Text(word + " dans " + key.toString()), new Text(occurence_mot.get(word) + "/"
                  + total_mots));
      }
  }

  
}

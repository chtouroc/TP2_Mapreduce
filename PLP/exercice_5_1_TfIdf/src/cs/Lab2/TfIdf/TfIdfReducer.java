package cs.Lab2.TfIdf;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TfIdfReducer extends Reducer<Text, Text, Text, Text> {
	 

		 
	    private static final DecimalFormat DF = new DecimalFormat("###.########");
	 
	    public TfIdfReducer() {
	    }
	 

	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        // on récupère le nombre total de documents qui est stocké dans le nom du job (voir TfIdfDriver
	        int nbDocCorpus = Integer.parseInt(context.getJobName());
	        // nombre de documents dans lesquels le mot apparait
	        int nbDocContenantMot = 0;
	        Map<String, String> frequences = new HashMap<String, String>();
	        for (Text val : values) {
	            String[] frequencesDoc = val.toString().split("=");
	            nbDocContenantMot++;
	            frequences.put(frequencesDoc[0], frequencesDoc[1]);
	        }
	        for (String document : frequences.keySet()) {
	            String[] nbOccurences_nbTotalMots = frequences.get(document).split("/");
	 
	            //calcul de tf
	            double tf = Double.valueOf(Double.valueOf(nbOccurences_nbTotalMots[0])
	                    / Double.valueOf(nbOccurences_nbTotalMots[1]));
	 
	            //calcul de idf
	            double idf = (double) nbDocCorpus / (double) nbDocContenantMot;
	 
	            //calcul de TfIdf
	            double tfIdf = nbDocCorpus == nbDocContenantMot ?
	                    tf : tf * Math.log10(idf);
	 
	            context.write(new Text(key + " dans " + document), new Text("TF = " + nbOccurences_nbTotalMots[0] + " / "+ nbOccurences_nbTotalMots[1] +
	            		" IDF = log("+ nbDocContenantMot + "/"+ nbDocCorpus + ") " + "TfIdf = " + DF.format(tfIdf) + "]"));
	        }
	    }
	}
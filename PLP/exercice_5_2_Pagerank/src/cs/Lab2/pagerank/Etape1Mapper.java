package cs.Lab2.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


	public class Etape1Mapper  extends Mapper<LongWritable, Text, Text, Text> {
	    /* Input : un nœud (un utilisateur A) 
	     * Output : pour chaque lien de l’utilisateur A vers l’utilisateur B, on émet : 
		(utilisateur A, utilisateur B)*/

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        
	    	 if (value.charAt(0) != '#') {

	            int tabulation = value.find("\t");
	            String noeudA = Text.decode(value.getBytes(), 0, tabulation);
	            String noeudB = Text.decode(value.getBytes(), tabulation + 1, value.getLength() - (tabulation + 1));
	            context.write(new Text(noeudA), new Text(noeudB));
	            
	            //on ajoute le noeud source à la liste de noeuds pour pouvoir calculer le nombre total de noeuds à l'étape 2
	            
	            PageRankDriver.noeuds.add(noeudA);
	            	            PageRankDriver.noeuds.add(noeudB);            
	        }
	    }}
	    





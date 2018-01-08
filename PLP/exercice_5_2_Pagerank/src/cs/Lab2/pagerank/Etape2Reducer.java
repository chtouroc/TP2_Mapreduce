package cs.Lab2.pagerank;


	import java.io.IOException;

	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;

	public class Etape2Reducer extends Reducer<Text, Text, Text, Text> {
	    
	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
	                                                                                InterruptedException {
	    	  /* Calcul du PageRank :
	         * REDUCER :
	         * - Input : 
	         * 1.	(utilisateur A, [utilisateur B1…]) 
	         * 2.	(utilisateur Bi ; « PageRank ; nb total de liens sortant de l’utilisateur A)
	         * Output : on calcule le PageRank par itération et on retourne : 
	         * (utilisateur A , « PageRank ; [utilisateur B1…] »)
	         */
	        
	        String liens = "";
	        double somme_PR_autres_utilisateurs = 0.0;
	        
	        for (Text value : values) {
	 
	            String content = value.toString();
	            
	            if (content.startsWith(PageRankDriver.séparateur)) {
	                
	                liens += content.substring(PageRankDriver.séparateur.length());
	            } else {
	                
	                String[] split = content.split("\\t");
	              
	                double pageRank = Double.parseDouble(split[0]);
	                int totalLinks = Integer.parseInt(split[1]);
	                //on ajoute la contribution de tous les utilisateurs qui ont un lien vers l'utilisateur source
	                
	                somme_PR_autres_utilisateurs += (pageRank / totalLinks);
	            }

	        }
	        
	        double PR = PageRankDriver.damping_factor * somme_PR_autres_utilisateurs + (1 - PageRankDriver.damping_factor);
	        context.write(key, new Text(PR + "\t" + liens));
	        
	    }

	}




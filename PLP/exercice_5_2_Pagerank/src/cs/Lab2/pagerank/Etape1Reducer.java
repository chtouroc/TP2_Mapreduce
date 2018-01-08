package cs.Lab2.pagerank;



	import java.io.IOException;

	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;

	public class Etape1Reducer extends Reducer<Text, Text, Text, Text> {
	    
	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        
	        /* Input : (utilisateur A, [utilisateur B1…])
	         * Output : (utilisateur A, « PageRank ; [utilisateurs Bi] »)
	         * La valeur initiale du PageRank pour tous les utilisateurs est : 
	         * damping factor = 0,85 / nombre total de noeuds
	         */	        
	    	String liens = (PageRankDriver.damping_factor / PageRankDriver.noeuds.size()) + "\t";
	    	boolean source = true;
	    
	        for (Text value : values) {
	            if (!source) 
	                liens += ",";
	            liens += value.toString();
	            source = false;
	        }

	        context.write(key, new Text(liens));
	    }

	}


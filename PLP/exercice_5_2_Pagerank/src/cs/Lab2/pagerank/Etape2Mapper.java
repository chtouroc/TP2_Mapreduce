package cs.Lab2.pagerank;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class Etape2Mapper  extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Calcul du PageRank :
         * MAPPER :
         * Input : (utilisateur A, « PageRank ; [utilisateurs Bi] »)
         * 
         * Output : on émet 2 sorties 
         * 1.	La liste des liens de chaque utilisateur : 
         * (utilisateur A, [utilisateur B1…])
         * 2.	pour chaque utilisateur Bi, on émet le PageRank de l’utilisateur source et le nombre total : 
         * (utilisateur Bi ; « PageRank ; nb total de liens sortant de l’utilisateur A)
         */
        
        int indice1 = value.find("\t");
        int indice2 = value.find("\t", indice1 + 1);
        
       
        String page = Text.decode(value.getBytes(), 0, indice1);
        String pageRank = Text.decode(value.getBytes(), indice1 + 1, indice2 - (indice1 + 1));
        String links = Text.decode(value.getBytes(), indice2 + 1, value.getLength() - (indice2 + 1));
        
        String[] autres_utilisateurs = links.split(",");
        for (String utilisateur : autres_utilisateurs) { 
            Text pageRankFinal = new Text(pageRank + "\t" + autres_utilisateurs.length);
            context.write(new Text(utilisateur), pageRankFinal); 
        }
    
        context.write(new Text(page), new Text(PageRankDriver.séparateur + links));
        
    }
    
}



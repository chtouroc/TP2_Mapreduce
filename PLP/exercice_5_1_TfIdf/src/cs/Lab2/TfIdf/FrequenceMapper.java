package cs.Lab2.TfIdf;
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;




public class FrequenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public FrequenceMapper() {
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Text word = new Text();
	
		// on récupère le nom du document
		String fileName = ((FileSplit) context.getInputSplit()).getPath()
				.getName();

		
		for (String token: value.toString().split("\\s+")) {
			token  = token.toLowerCase(); 
			int l = token.length();
			//on vérifie que la chaine de caractère n'est pas vide
			if (token == null || token.isEmpty())  {}
			else {
			//on vérifie que la chaine de caractère commence par une lettre ou un chiffre
			if (Character.isLetter(token.charAt(0)) ||  Character.isDigit(token.charAt(0)))
			{
			/*si la chaine de caractère finit par une ponctuation ou une parenthèse, on enlève le dernier caractère
			 de la chaine et on ne prend que la partie qui concerne le mot */
			if (Character.isLetter(token.charAt(l-1)) == false && Character.isDigit(token.charAt(l-1))==false){
					token = token.substring(0,l-1);
					if (Character.isLetter(token.charAt(l-2)) == false && Character.isDigit(token.charAt(l-2))==false){
						token = token.substring(0,l-2);
						if (Character.isLetter(token.charAt(l-3)) == false && Character.isDigit(token.charAt(l-3))==false){
			
			}}}
			// on renvoie les couples <terme " dans " nom_doc, 1>
			word.set(token+ " dans " +fileName.toString());
            context.write(word, new IntWritable(1));}
         }
      }
	   }  }

		
		
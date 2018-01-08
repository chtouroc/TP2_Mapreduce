package cs.bigdata.Tutorial2;


import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		PrintWriter writer = new PrintWriter("/home/cloudera/ex27/YearHeightTreeDisplay.txt", "UTF-8");
		String localSrc = "/home/cloudera/ex27/arbres.csv" ;
	
		
		int lineCount=0;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			while (line !=null){
				// Process of the current line
				//System.out.println(line);
				// go to the next line
				line = br.readLine();
				lineCount=lineCount+1;	
				writer.println(lineCount +")  "+ Tree.setName(line)+"___Year: "+ Tree.setYear(line)+"___Height: "+Tree.setHeight(line));
				}
			
			System.out.println("Number of lines in the file" );
			System.out.println(lineCount);
			
		br.close();	
		}
		
		finally{
			//close the file
			writer.close();
			in.close();
			fs.close();
		}
 
		
		
	}

	

}

package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class question8 {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "/home/cloudera/Downloads/isdhistory.txt";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			int i=0;
			while (line != null){
				// Process of the current line
				i=i+1;
				if (i>22) {
				Station.setUSAF(line);
				Station.setName(line);
				Station.setCountry(line);
				Station.setElevation(line);
				System.out.println(Station.getUSAF()+" "+Station.getName()+" "+Station.getCountry()+" "+Station.getElevation());
				}
				// go to the next line
				line = br.readLine();
		}}
		finally{
			//close the file
			in.close();
			fs.close();
		}
 
		
		
	}

}

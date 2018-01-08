package cs.bigdata.Tutorial2;
public abstract class Tree {
	
	static String GEOPOINT;
	static String ARRONDISSEMENT;
	static String GENRE;
	static String ESPECE;
	static String FAMILLE;
	static String ANNEE_PLANTATION;
	static String HAUTEUR;
	static String NOMCOMMUN;
	static String VARIETE;
	static String OBJECTID;
	static String NOM_EV;
	
	public static String setYear(String line) {
		String[] split = line.split(";");
		ANNEE_PLANTATION=split[5];
		return ANNEE_PLANTATION;	
	}
	public static  void printYear(String line) {     
	        System.out.println(setYear(line));    	
	  }
	public static String setName(String line) {
		String[] split = line.split(";");
		NOMCOMMUN=split[9];
		return NOMCOMMUN;	
	}
	public static  void printName(String line) {     
	        System.out.println(setName(line));    	
	}
	public static String setHeight(String line) {
		String[] split = line.split(";");
		HAUTEUR=split[6];
		return HAUTEUR;	
	}
	public static  void printHeight(String line) {     
	        System.out.println(setHeight(line));    	
	}
	       
}
		
		


package cs.bigdata.Tutorial2;

public abstract class Station {
	static String USAF = new String();
	static String line = new String();
	static String name = new String();
	static String country = new String();
	static String elevation = new String();
	
	static void setUSAF(String line) {
		USAF = line.substring(1,6);}
		
	static void setName(String line) {
		name = line.substring(13,42);
	}
	static void setCountry(String line) {
		country = line.substring(43, 45);
	}
	static void setElevation(String line) {
		elevation = line.substring(74, 81);
	}
	static String getUSAF() {
		return USAF;}
		
	static String getName() {
		return name;
	}
	static String getCountry() {
		return country;
	}
	static String getElevation() {
		return elevation;
	}
	
	
}
	
	
	
	
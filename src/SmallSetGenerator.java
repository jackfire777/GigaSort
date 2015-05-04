package gigasort;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class SmallSetGenerator {
	
	private FileWriter fwOut;
	private Random rand;
	private ArrayList<Long> sorter = new ArrayList<>();
	
	public SmallSetGenerator(){
		rand = new Random();
	}
	
	private void generate(){
		try {
			File file = new File("/s/bach/k/under/jmess/CurrSem/cs455/p3/samplesort.txt");
			if(!file.exists()) {
			    file.createNewFile();
			   }
			fwOut = new FileWriter(file);
			Long val;
			for (int i =0; i<1000; i++){
				val = Math.abs(rand.nextLong());
				fwOut.write(val +"\n");
				sorter.add(val);
			}
			fwOut.close();
			
			file = new File("/s/bach/k/under/jmess/CurrSem/cs455/p3/samplesortansw.txt");
			fwOut = new FileWriter(file);
			Collections.sort(sorter);
			for (Long vals : sorter){
				fwOut.write(vals.toString() + "\n");
			}
			
		} catch (IOException e) {
			System.out.println(e.getMessage() + ": Could not create file or FileWriter");
			System.exit(1);
		}
		
	}

	public static void main (String args[]){
		SmallSetGenerator gen = new SmallSetGenerator();
		gen.generate();
	}
}

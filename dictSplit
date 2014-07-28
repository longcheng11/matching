import x10.io.File;
import x10.io.FileWriter;
import x10.util.ArrayList;
import x10.util.List;
import x10.array.Array;

import x10.regionarray.Region;
import x10.regionarray.DistArray;
import x10.regionarray.Dist;
import x10.io.FileReader;

public class dictSplit {
	public static def main(args: Rail[String]) {
		// TODO auto-generated stub
		
		val FILE=Int.parse(args(0)); //the number of files to be read
		
		var data_path:String;
		var out_path:String;
		var temp:File;
		var outFile:FileWriter;
		var counter:Long=0;
		var fread:FileReader;
		val N:Long=96;    //number of output files  
		var e:Long=0;
		var e2:Long=-1;
		
		out_path="/data/match_data/dict_data/"+0.toString()+".dict";
		outFile=new FileWriter(new File(out_path),true);
		
		var file_start:Long=System.currentTimeMillis();
		for(var e1:Int=0 as Int;e1<FILE;e1++){
			data_path="/data/match_data/dict/"+e1.toString();
			temp=new File(data_path);	
			
			if(temp.exists()){
				fread=new FileReader(temp);
				val iter=fread.lines();
				while(iter.hasNext()){
					e=counter/1000; //write to the e%N-th file every 1000 lines
					if(e>e2){
						outFile.flush();
						outFile.close();
						out_path="/data/match_data/dict_data/"+(e%N).toString()+".dict";
						outFile=new FileWriter(new File(out_path),true);
					}
					outFile.write(iter.next());
					outFile.write("\n");
					counter++;
					e2=e;					
				}
			}
		}	
		
		var file_end:Long=System.currentTimeMillis();
		Console.OUT.println("split Dict time is "+(file_end-file_start)+ "ms");
	}
}

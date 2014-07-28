import x10.io.File;
import x10.io.FileWriter;
import x10.util.ArrayList;
import x10.util.List;
import x10.array.Array;
import x10.util.HashMap;
import x10.io.ReaderIterator;
import x10.util.StringBuilder;
import x10.util.HashSet;
import x10.compiler.Native;
import x10.compiler.NativeCPPInclude;
import x10.compiler.NativeCPPCompilationUnit;

import x10.regionarray.Region;
import x10.regionarray.DistArray;
import x10.regionarray.Dist;

@NativeCPPInclude("gzRead.h")
@NativeCPPCompilationUnit("gzRead.cc")

public class kvBuild {
	
	@Native("c++","gzRead(#1->c_str())")
	static native def gzRead(file:String):String;
	
	public static class Pair {
		
		private var key:String;
		private var payload:ArrayList[String];
		
		public def this(){
			this.key=null;			
			this.payload=new ArrayList[String]();		
		}
		
		public def this(k:String,p:ArrayList[String]){
			this.key=k;			
			this.payload=p;		
		}
	}
	
	
	public static def buildPair(pairList:ArrayList[Pair]):ArrayList[Pair]{
		var size:Long=pairList.size();
		var key:String;
		var value:ArrayList[String];
		var kvPair:ArrayList[Pair]=new ArrayList[Pair]();
		
		for(i in 0..(size-2)){
			for(j in (i+1)..(size-1)){
				key=pairList(i).key+"\t"+pairList(j).key;  //build the key -- <s1,s2>
				value=new ArrayList[String]();
				
				for(p1 in pairList(i).payload){
					for(p2 in pairList(j).payload){
						value.add(p1+" "+p2);         //build the values -- <l1,l2>						   
					}					
				}
				
				kvPair.add(new Pair(key,value));         //build 1->n mapping				
			}			
		}
		return kvPair;		
	}
	
	
	public static def parseKey(ks:String):ArrayList[String]{
		var KeyList:ArrayList[String]=new ArrayList[String]();
		var len:Int=ks.length();
		var token:Int=0 as Int;
		var check:Int=0 as Int;
		var key:String;
		
		while(token<len){
			check=ks.indexOf('|',token);
			if(check==-1 as Int){
				key=ks.substring(token, len-1 as Int);
				KeyList.add(key);
				break;
			}
			else{
				key=ks.substring(token, check);
				KeyList.add(key);
				token=check+1 as Int;
			}
		}
		return KeyList;		
	}
	
	
	//Parsing a sentence
	public static def Parsing(s:String):ArrayList[Pair]{ 		
		var len:Int=s.length();
		var start:Int=0 as Int;
		var end:Int=0 as Int;
		
		var line:String;
		var check:Int=0 as Int;
		
		var subString:String;
		var keyString:String;
		
		var tmpPair:Pair;
		var tmpList:ArrayList[String];
		var pairList:ArrayList[Pair]=new ArrayList[Pair]();
				
		while(start<len){
			end=s.indexOf('\n',start);
			line=s.substring(start,end+1 as Int); // a line with \n 
			check=line.indexOf('|');
			if(check==-1 as Int){
				start=end+1 as Int; //next line
				continue;
			}
			else{ //parsing the keys
				subString=line.substring(0 as Int,check);
				keyString=line.substring(check+1 as Int, line.length());
				tmpList=parseKey(keyString);
				tmpPair=new Pair(subString, tmpList);
				pairList.add(tmpPair);
				start=end+1 as Int;	 //next line			
			}
		}	
		return buildPair(pairList);
	} 
	
	
	public static def main(args: Rail[String]) {
		// TODO auto-generated stub
		
		val N:Long=Place.MAX_PLACES;
		Console.OUT.println("the number of places is "+N);
		
		val FILE=Int.parse(args(0));
		//val InPath:String=args(1);
		
		var file_start:Long=System.currentTimeMillis();
		
		finish for( p in Place.places()){
			at (p) async {
				val f_start:Int=here.id as Int*FILE;
				val f_end:Int=(here.id+1) as Int*FILE;
				
				var data_path:String;
				var lstring:String;
				var temp:File;
				var pairList:ArrayList[Pair];
				var outFile:FileWriter;
				
				for(var e1:Int=f_start;e1<f_end;e1++){					
					data_path="/data/match_data/gz_data/"+e1.toString()+".gz";
					temp=new File(data_path);					
					if(temp.exists()){
						outFile=new FileWriter(new File("/data/match_data/pair_data/"+e1.toString()+".pair"), true);
						lstring=gzRead(data_path);
						var len:Int=lstring.length();
						//Console.OUT.println("string length is "+len);
						var start:Int=0 as Int;
						var end:Int=0 as Int;
						var sentence:String;
						while(start<len) {
							end=lstring.indexOf("\n\n",start);
							sentence=lstring.substring(start,end+1 as Int); // a sentence
							//Console.OUT.println("sentence is \n"+sentence);
							pairList=Parsing(sentence);
							
							//print out the kv Pairs
							for (term in pairList) {
								outFile.write(term.key+"|");
								for(pay in term.payload){
									outFile.write(pay+"\t");
								}
								outFile.write("\n");
							}
							
							start=end+2 as Int; //next sentence
						}
						outFile.flush();
						outFile.close();
						
					} //end if  
				} //end for
			} //end async
		}
		
		var file_end:Long=System.currentTimeMillis();
		Console.OUT.println("build Pair time is "+(file_end-file_start));
	}
}

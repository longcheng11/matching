import x10.io.File;
import x10.io.FileWriter;
import x10.util.ArrayList;
import x10.util.List;
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
import x10.regionarray.RemoteArray;
import x10.regionarray.Array;
import x10.util.RailBuilder;

@NativeCPPInclude("gzRead.h")
@NativeCPPCompilationUnit("gzRead.cc")

public class match {
	
	@Native("c++","gzRead(#1->c_str())")
	static native def gzRead(file:String):String;
	
	
	public static class kvPair {
		
		private var key:String;
		private var payload:ArrayList[String];
		
		public def this(){
			this.key="";			
			this.payload=new ArrayList[String]();		
		}
		
		public def this(k:String,p:ArrayList[String]){
			this.key=k;			
			this.payload=p;		
		}
	}
	
	
	//Parsing the dict_data
	public static def ParsDict(line:String):Array[Long]{ 		
		var value:Array[Long]=new Array[Long](2);
		var token:Int=line.indexOf(',');
		
		try{
			var k:String=line.substring(0 as Int,token); 
			value(0)=Long.parse(k);
			
			var v:String=line.substring(token+1 as Int);
			value(1)=Long.parse(v);
			//Console.OUT.println(value(0)+" "+value(1));
		}
		catch (Exception){
			value(0)=0;
			value(1)=1;
		}
		
		return value;
	} 
	
	
	//Parsing the test_data
	public static def ParsTest(line:String):kvPair{ 		
		var pair:kvPair;
		var key:String;
		var value:String;
		var valueList:ArrayList[String]=new ArrayList[String]();
		var len:Int=line.length();
		var token:Int=0 as Int;
		var check:Int;
		
		try{
			//key
			token=line.indexOf('|');
			key=line.substring(0 as Int,token); 
			token++;
			
			//value-list
			while(token<len){
				check=line.indexOf('\t',token);
				if(check==-1 as Int){
					break;
				}
				else{
					value=line.substring(token, check);
					valueList.add(value);
					token=check+1 as Int;
				}
			}
			pair=new kvPair(key, valueList);     
		}
		catch (Exception){
			pair=new kvPair();
		}	
		return pair;
	}  
	
	public static def hash_3(key:Long,size:Long):Long {
		var mod:long=key%size;	
		return mod;
	} 
	
	public static def Serialize(A:Array[String],B:Array[Char]){
		var size:Long=A.size;
		var num1:Long=0;
		for (i in 0..(size-1)){
			val b=A(i);
			val c=b.length();
			for(j in 0..(c-1)){
				B(num1)=b.charAt(j as Int);
				num1++;
			}
			B(num1)='\n';
			num1++;
		}
	}
	
	public static def DeSerialize(A:RemoteArray[Char],B:Array[String]){
		var size:Long=A.size;
		var tmp:StringBuilder=new StringBuilder();
		var num1:Long=0;
		for(i in 0..(size-1)){
			if(A.operator()(i as Int)!='\n') {
				tmp.add(A.operator()(i as Int));
			}
			else {
				B(num1)=tmp.toString();
				num1++;
				tmp=new StringBuilder();
			}
		}
		tmp=null;
	}
	
	public static def main(args: Rail[String]) {
		// TODO auto-generated stub
		
		val N:Long=Place.MAX_PLACES;
		Console.OUT.println("the number of places is "+N);
		
		val FILE1=Int.parse(args(0));
		val FILE2=Int.parse(args(1));
		val r <: Region=Region.make(0..(N-1));
		val d <: Dist=Dist.makeBlock(r);
		
		//read in
		val dict=DistArray.make[ArrayList[Array[Long]]](d);        // the dictionary data	
		val test=DistArray.make[ArrayList[kvPair]](d);             // the test data
		
		//local dict table
		val table=DistArray.make[HashMap[Long,HashSet[Long]]](d);  //local hash tables
		
		//remote receive
		val dict_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val dict_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);		
		val test_receive=DistArray.make[Array[RemoteArray[Char]]](d);	
		
		finish for (p in dict.dist.places()){
			at (p) async {
				//read in
				dict(here.id)=new ArrayList[Array[Long]]();
				test(here.id)=new ArrayList[kvPair]();
				
				//local dict table
				table(here.id)= new HashMap[Long,HashSet[Long]]();
				
				//receive dict_data
				dict_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				dict_payload_receive(here.id)=new Array[RemoteArray[Long]](N);
				
				//receive test_data
				test_receive(here.id)=new Array[RemoteArray[Char]](N);
			}
		}
		
		//Console.OUT.println("///////////////// Start Reading ////////////////");
		var time_1:Long=System.currentTimeMillis();
		
		//read the dict into dict(here.id) at each place
		finish for( p in Place.places()){
			at (p) async {
				val f_start:Int=here.id as Int*FILE1;
				val f_end:Int=(here.id+1) as Int*FILE1;
				var dict_path:String;
				var lstring:String;
				var temp:File;
				var value:Array[Long];
				var check:HashSet[Long];
				
				for(var e1:Int=f_start;e1<f_end;e1++){	
					dict_path="/data/match_data/dict_data/"+e1.toString()+".dict.gz";
					//dict_path="/data/sample_test/"+e1.toString()+".dict.gz";
					temp=new File(dict_path);
					if(temp.exists()){
						lstring=gzRead(dict_path);
						var len:Int=lstring.length();
						var start:Int=0 as Int;
						var end:Int=0 as Int;
						var line:String;
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end); // a kv from dict, remove the '\n'
							value=ParsDict(line);
							dict(here.id).add(value);
							start=end+1 as Int; //next line							
						}					
					} //end if
				}
				//Console.OUT.println(" dict number is "+dict(here.id).size());
			}
		}
		
		var time_2:Long=System.currentTimeMillis();
		Console.OUT.println(" reading dict_data "+(time_2-time_1)+" ms");        
		
		//read the Test into test(here.id) at each place
		finish for( p in Place.places()){
			at (p) async {
				val f_start:Int=here.id as Int*FILE2;
				val f_end:Int=(here.id+1) as Int*FILE2;
				var test_path:String;
				var lstring:String;
				var temp:File;
				var pair:kvPair;
				
				for(var e1:Int=f_start;e1<f_end;e1++){	
					test_path="/data/match_data/test_data/"+e1.toString()+".pair.gz";
					//test_path="/data/sample_test/"+e1.toString()+".pair.gz";
					temp=new File(test_path);
					if(temp.exists()){
						lstring=gzRead(test_path);
						var len:Int=lstring.length();
						var start:Int=0 as Int;
						var end:Int=0 as Int;
						var line:String;
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end+1 as Int); // a kv from dict
							pair=ParsTest(line);
							test(here.id).add(pair);
							start=end+1 as Int; //next line							
						}					
					} //end if
				}
				//Console.OUT.println(" pair number is"+test(here.id).size());
			}
		}
		var time_3:Long=System.currentTimeMillis();
		Console.OUT.println(" reading test_data "+(time_3-time_2)+" ms");
		
		
		//hash the dict data
		finish for( p in Place.places()){
			at (p) async {
				val pn=here.id;					
				var dict_key_collector:Array[RailBuilder[Long]]=new Array[RailBuilder[Long]](N);
				var dict_payload_collector:Array[RailBuilder[Long]]=new Array[RailBuilder[Long]](N);
				for(j in (0..(N-1))){
					dict_key_collector(j)=new RailBuilder[Long]();
					dict_payload_collector(j)=new RailBuilder[Long]();
				}
				
				//hash distribution
				var des:Long;
				for(value in dict(here.id)){
					des=hash_3(value(0),N);
					dict_key_collector(des).add(value(0));
					//Console.OUT.println(" key is "+value(0));
					dict_payload_collector(des).add(value(1));
				}
				
				//push the dict-pair to remote places
				var keys_array:Array[long];
				var payload_array:Array[long];
				for( k in 0..(N-1)) {
					keys_array=new Array[Long](dict_key_collector(k).result());
					payload_array=new Array[Long](dict_payload_collector(k).result());
					val kk=k;
					val pk=Place.place(k);
					val s1=keys_array.size;	
					//Console.OUT.println(" transfer size is "+s1);
					at(pk){
						dict_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						dict_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) dict_keys_receive(here.id)(pn));
					Array.asyncCopy( payload_array, at (pk) dict_payload_receive(here.id)(pn));
				}  //end pushing	
				
				//empty the read in data
				dict(here.id).clear();
				dict(here.id)=null;					
			} //end async at place
		} 
		var time_4:Long=System.currentTimeMillis();
		Console.OUT.println(" hash dict_data "+(time_4-time_3)+" ms");
		
		//build local dict hash table
		finish for( p in Place.places()){
			at (p) async {
				var check:HashSet[Long];
				var key:Long;
				var value:Long;
				var size:Long;
				
				for( i in 0..(N-1)) {			
					size=dict_keys_receive(here.id)(i).size;
					//Console.OUT.println(" received size is "+size);
					for(j in 0..(size-1)){	
						key=dict_keys_receive(here.id)(i).operator()(j as Int);
						value=dict_payload_receive(here.id)(i).operator()(j as Int);
						check=table(here.id).getOrElse(key,null);	
						if(check==null){
							check=new HashSet[Long]();
							check.add(value);
							table(here.id).put(key,check);
						}
						else{
							check.add(value);
						}
					} //end for1					
				}
				dict_keys_receive(here.id)=null;
				dict_payload_receive(here.id)=null;			
				Console.OUT.println(" table number is "+table(here.id).size());
			}					
		}
		var time_5:Long=System.currentTimeMillis();
		Console.OUT.println(" build local table "+(time_5-time_4)+" ms");
		
		//hash-partitioning the test data and transfer to remote nodes as Chars
		finish for( p in Place.places()){
			at (p) async {
				val pn=here.id;					
				var test_collector:Array[RailBuilder[String]]=new Array[RailBuilder[String]](N);
				
				for(j in 0..(N-1)){
					test_collector(j)=new RailBuilder[String]();
				}
				
				//hash distribution
				var des:Long;
				var kstring:String;
				var token:Int;
				var transtring:String; //to be sent strings
				for(pair in test(here.id)){
					for(vstring in pair.payload){
						token=vstring.indexOf(' ');
						kstring=vstring.substring(0 as Int, token);
						des=hash_3(Long.parse(kstring),N);
						transtring=pair.key+"|"+vstring;  //in the form of s11[\t]s21[|]v11[_]v21
						test_collector(des).add(transtring);
					}	
				}
				
				//push the test-string to remote places
				var tmp:Array[String];
				var Ser_1:Array[Char];
				for( k in 0..(N-1)) {
					val kk=k;
					val pk=Place.place(k);
					tmp=new Array[String](test_collector(k).result());
					val size=tmp.size;
					var num:Long=0;
					var a:Long;
					for (i in 0..(size-1)){
						a=tmp(i).length()+1;
						num+=a;
					}
					Ser_1=new Array[Char](num);
					Serialize(tmp,Ser_1);
					val SIZE=Ser_1.size;
					val local=here.id;
					at(pk){
						test_receive(here.id)(local)= new RemoteArray(new Array[Char](SIZE));
					}
					Array.asyncCopy( Ser_1, at (pk) test_receive(here.id)(local));
				}
				
				//empty the read in data
				test(here.id).clear();
				test(here.id)=null;					
			} //end async at place
		} 
		var time_6:Long=System.currentTimeMillis();
		Console.OUT.println(" hash test_data "+(time_6-time_5)+" ms");
		
		//local searching and build local semantics
		finish for( p in Place.places()){
			at (p) async {
				val pn=here.id;	
				var Deser_1:Array[String];
				var lstring:String;
				var key:String;
				var payload:String;
				var v1:Long;
				var v2:Long;
				var token:Int;
				var check:HashSet[Long];
				var counter:Long=0;
				var pa_num:Long=0;
				for( k in 0..(N-1)) {
					val size=test_receive(here.id)(k).size;
					var num:Long=0;
					for(i in 0..(size-1)){
						if(test_receive(here.id)(k).operator()(i as Int)=='\n') {
							num++;
						}
					}
					Deser_1=new Array[String](num);
					Console.OUT.println("debug at place "+pn+" string size is "+num);
					DeSerialize(test_receive(here.id)(k),Deser_1);
					
					//check the matching
					for(i in 0..(num-1)){
						lstring=Deser_1(i);
						pa_num++;
						//Console.OUT.println(i+"\t"+lstring);
						token=lstring.indexOf('|');
						key=lstring.substring(0 as Int, token);
						payload=lstring.substring(token+1 as Int);
						token=payload.indexOf(' ');
						v1=Long.parse(payload.substring(0 as Int, token));
						v2=Long.parse(payload.substring(token+1 as Int));
						check=table(here.id).getOrElse(v1,null);
						
						if(check!=null && check.contains(v2)){
							counter++;
							//then formulate the results
						}
					} //end for i				
				} //end for k
				Console.OUT.println("debug at place "+pn+" match/whole is "+counter+"/"+pa_num);
			}
		}
		var time_7:Long=System.currentTimeMillis();
		
		Console.OUT.println(" MATCHING TAKES "+(time_7-time_1)+" ms///////////////");               		
	}	
}

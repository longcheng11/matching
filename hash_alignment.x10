import x10.io.File;
import x10.util.ArrayList;
import x10.util.List;
import x10.array.Array;
import x10.util.HashMap;
import x10.io.ReaderIterator;
import x10.util.Map.Entry;
import x10.util.Random;
import x10.util.concurrent.AtomicBoolean;
import x10.util.concurrent.AtomicLong;
import x10.util.concurrent.AtomicInteger;
import x10.compiler.Native;
import x10.compiler.NativeCPPInclude;
import x10.compiler.NativeCPPCompilationUnit;
import x10.util.StringBuilder;
import x10.util.concurrent.AtomicInteger;
import x10.util.HashSet;
import x10.util.ArrayBuilder;

@NativeCPPInclude("gzRead.h")
@NativeCPPCompilationUnit("gzRead.cc")

public class hash_match {
	
	@Native("c++","gzRead(#1->c_str())")
	static native def gzRead(file:String):String;
	
	public static class Pair {
		
		private var key:Long;
		private var payload:Long;
		
		public def this(){
			this.key=0L;			
			this.payload=0L;
		}
		
		public def this(k:Long,p:Long){
			this.key=k;			
			this.payload=p;		
		}		
	}
	
	public static class Tuple {
		
		private var key1:String;
		private var key2:String;
		private var payload:String;
		
		public def this(){
			this.key1="";	
			this.key2="";
			this.payload="";		
		}
		
		public def this(k1:String,k2:String,p:String){
			this.key1=k1;
			this.key2=k2;
			this.payload=p;		
		}		
	}
	
	public static def Serialize(A:Array[String],B:Array[Char]){
		var size:Int=A.size;
		var num1:Int=0;
		for (i in (0..(size-1))){
			val b=A(i);
			val c=b.length();
			for(j in (0..(c-1))){
				B(num1)=b(j);
				num1++;
			}
			B(num1)='\n';
			num1++;
		}
	}
	
	public static def DeSerialize(A:RemoteArray[Char],B:Array[String]){
		var size:Int=A.size;
		var tmp:StringBuilder=new StringBuilder();
		var num1:Int=0;
		for(i in (0..(size-1))){
			if(A(i)!='\n') {
				tmp.add(A(i));
			}
			else {
				B(num1)=tmp.toString();
				num1++;
				tmp=new StringBuilder();
			}	  	
		}
		tmp=null;
	}
	
	public static def Parsing(line:String):Tuple{ 
		var token:Int=line.indexOf('|');
		var key1:String=line.substring(0,token);
		
		var v:String=line.substring(token+1);
		token=v.indexOf('|');
		
		var key2:String=v.substring(0,token);
		var payload:String=v.substring(token+1);
		
		var t:Tuple=new Tuple(key1,key2,payload);
		
		return t;
	}
	
	public static def hash_3(key:Long,size:Int):Int {
		var s:Long=size as Long;
		var mod:long=key%s;	
		return mod as Int;
	} 
	
	
	public static def main(args: Array[String]) {
		// TODO auto-generated stub
		
		val N:Int=Place.MAX_PLACES;
		val FILE=Int.parse(args(0));
		Console.OUT.println("<#places> "+N+" <chunk/thread> "+FILE);
		val path_dict=args(1);
		val path_test=args(2);
		
		val region:Region=0..(N-1);
		val d:Dist=Dist.makeBlock(region);
		
		//read in
		val dict_list=DistArray.make[ArrayList[Tuple]](d);        // the dictionary data	
		val test_list=DistArray.make[ArrayList[Tuple]](d);        // the test data
		
		//local dict table
		val table=DistArray.make[HashMap[Long,Pair]](d);  //local hash tables
		
		//remote receive
		val dict_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val dict_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);		
		val dict_relation_receive=DistArray.make[Array[RemoteArray[Long]]](d);		
		val test_key_receive=DistArray.make[Array[RemoteArray[Long]]](d);		
		val test_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);		
		val test_string_receive=DistArray.make[Array[RemoteArray[Char]]](d);	
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				//read
				dict_list(here.id)=new ArrayList[Tuple]();
				test_list(here.id)=new ArrayList[Tuple]();
				
				//receive -  the remote arrays
				dict_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				dict_payload_receive(here.id)=new Array[RemoteArray[Long]](N);
				dict_relation_receive(here.id)=new Array[RemoteArray[Long]](N);
				test_key_receive(here.id)=new Array[RemoteArray[Long]](N);
				test_payload_receive(here.id)=new Array[RemoteArray[Long]](N);
				test_string_receive(here.id)=new Array[RemoteArray[Char]](N);
				
				//dict hash table
				table(here.id)=new HashMap[Long,Pair]();
			}
		}
		
		//Console.OUT.println("///////////////// Start Reading ////////////////");
		var time_1:Long=System.currentTimeMillis();
		
		//read the dict into dict(here.id) at each place		
		finish for (p in Place.places()){
			at (p) async {		
				val f_start:Int=here.id*FILE;
				val f_end:Int=(here.id+1)*FILE;
				for(var e3:Int=f_start;e3<f_end;e3++){
					
					//read dict
					var dict_file:String=path_dict+e3.toString()+".dict.gz";
					var temp_r:File=new File(dict_file);
					if(temp_r.exists()){
						var lstring:String=gzRead(dict_file);
						var len:Int=lstring.length();
						var start:Int=0;
						var end:Int=0;
						var line:String;						
						var value:Tuple;
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							value=Parsing(line);
							dict_list(here.id).add(value);
							start=end+1;							
						}	
					}	 //end if	
					
					//read test
					var test_file:String=path_test+e3.toString()+".pair.gz";
					var temp_s:File=new File(test_file);
					if(temp_s.exists()){
						var lstring:String=gzRead(test_file);
						var len:Int=lstring.length();
						var start:Int=0;
						var end:Int=0;
						var line:String;
						var value:Tuple;
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							value=Parsing(line);
							test_list(here.id).add(value);
							start=end+1;							
						}	
					}	 //end if						
				} //end for e3
				//Console.OUT.println("place "+here.id+" dict is "+dict_list(here.id).size()+" test is "+test_list(here.id).size());		
				System.gc();
			} //end async at place 
		} //end finish place
		var time_2:Long=System.currentTimeMillis();
		Console.OUT.println(" reading data "+(time_2-time_1)+" ms");   
		
		//hash the dict data	
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;					
				var dict_key1_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var dict_key2_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var dict_payload_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				
				for(j in (0..(N-1))){
					dict_key1_collector(j)=new ArrayList[Long]();
					dict_key2_collector(j)=new ArrayList[Long]();
					dict_payload_collector(j)=new ArrayList[Long]();
				}
				
				//hash distribution
				var des:Int;
				var k1:Long;
				var k2:Long;
				var payload:Long;
				for(v in dict_list(here.id)){
					k1=Long.parse(v.key1);
					k2=Long.parse(v.key2);
					payload=Long.parse(v.payload);
					des=hash_3(k1,N);
					dict_key1_collector(des).add(k1);
					dict_key2_collector(des).add(k2);
					dict_payload_collector(des).add(payload);
				}
				
				//push the dict to remote places
				var key1_array:Array[long];
				var key2_array:Array[long];
				var payload_array:Array[long];
				for( k in (0..(N-1))) {
					key1_array=dict_key1_collector(k).toArray();
					key2_array=dict_key2_collector(k).toArray();
					payload_array=dict_payload_collector(k).toArray();
					val kk=k;
					val pk=Place.place(k);
					val s1=key1_array.size;	
					at(pk){
						dict_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						dict_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						dict_relation_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( key1_array, at (pk) dict_keys_receive(here.id)(pn));
					Array.asyncCopy( key2_array, at (pk) dict_payload_receive(here.id)(pn));
					Array.asyncCopy( payload_array, at (pk) dict_relation_receive(here.id)(pn));
				}  //end pushing	
				
				//empty the read in dict
				dict_list(here.id)=null;				
			} //end async at place
		} 
		var time_3:Long=System.currentTimeMillis();
		Console.OUT.println(" redistribute dict_data "+(time_3-time_2)+" ms");
		
		//build local dict hash table
		finish for( p in Place.places()){
			at (p) async {
				var check:Pair;
				var key:Long;
				var value:Long;
				var relation:Long;
				var size:Long;
				
				for( i in 0..(N-1)) {			
					size=dict_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<size;j++){
						key=dict_keys_receive(here.id)(i)(j);
						value=dict_payload_receive(here.id)(i)(j);
						relation=dict_relation_receive(here.id)(i)(j);
						check=table(here.id).getOrElse(key,null);
						if(check==null){
							check=new Pair(value,relation);
							table(here.id).put(key,check);
						}
					}
				}
				dict_keys_receive(here.id)=null;
				dict_payload_receive(here.id)=null;		
				dict_relation_receive(here.id)=null;
			}
		}
		var time_4:Long=System.currentTimeMillis();
		Console.OUT.println(" build table "+(time_4-time_3)+" ms");
		
		
		//hash-partitioning the test data and transfer to remote nodes as Chars
		finish for( p in Place.places()){
			at (p) async {
				val pn=here.id;		
				
				var test_key1_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var test_key2_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var test_payload_collector:Array[ArrayList[String]]=new Array[ArrayList[String]](N);
				
				for(j in (0..(N-1))){
					test_key1_collector(j)=new ArrayList[Long]();
					test_key2_collector(j)=new ArrayList[Long]();
					test_payload_collector(j)=new ArrayList[String]();
				}
				
				//hash distribution
				var des:Int;
				var k1:Long;
				var k2:Long;
				var payload:String;
				for(v in test_list(here.id)){
					k1=Long.parse(v.key1);
					k2=Long.parse(v.key2);
					payload=v.payload;
					des=hash_3(k1,N);
					test_key1_collector(des).add(k1);
					test_key2_collector(des).add(k2);
					test_payload_collector(des).add(payload);
				}
				
				//put the keys
				var key1_array:Array[long];
				var key2_array:Array[long];
				for( k in (0..(N-1))) {
					key1_array=test_key1_collector(k).toArray();
					key2_array=test_key2_collector(k).toArray();
					val kk=k;
					val pk=Place.place(k);
					val s1=key1_array.size;	
					at(pk){
						test_key_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						test_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( key1_array, at (pk) test_key_receive(here.id)(pn));
					Array.asyncCopy( key2_array, at (pk) test_payload_receive(here.id)(pn));
				}  //end pushing keys
				
				//put the strings
				var tmp:Array[String];
				var Ser_1:Array[Char];
				for( k in 0..(N-1)) {
					val kk=k;
					val pk=Place.place(k);
					tmp=test_payload_collector(k).toArray();
					val size=tmp.size;
					var num:Int=0;
					var a:Long;
					for (i in 0..(size-1)){
						a=tmp(i).length()+1;
						num+=a;
					}
					Ser_1=new Array[Char](num);
					Serialize(tmp,Ser_1);
					val SIZE=Ser_1.size;
					at(pk){
						test_string_receive(here.id)(pn)= new RemoteArray(new Array[Char](SIZE));
					}
					Array.asyncCopy( Ser_1, at (pk) test_string_receive(here.id)(pn));
				}
				
				//empty the read in data
				test_list(here.id)=null;
			}
		}
		var time_5:Long=System.currentTimeMillis();
		Console.OUT.println(" redistribute test_data "+(time_5-time_4)+" ms");
		
		//local searching and results output
		finish for( p in Place.places()){
			at (p) async {
				val pn=here.id;
				
				var Deser_1:Array[String];
				var key1:Long;
				var key2:Long;
				var payload:String;
				var check:Pair;
				var counter:Int=0;
				var pa_num:Int=0;
				
				for( i in 0..(N-1)) {
					val size=test_string_receive(here.id)(i).size;
					var num:Int=0;
					for(j in 0..(size-1)){
						if(test_string_receive(here.id)(i)(j)=='\n') {
							num++;
						}
					}
					Deser_1=new Array[String](num);
					DeSerialize(test_string_receive(here.id)(i),Deser_1);
					
					//check the matching
					for(j in 0..(num-1)){
						pa_num++;
						key1=test_key_receive(here.id)(i)(j);
						key2=test_payload_receive(here.id)(i)(j);
						payload=Deser_1(j);
						check=table(here.id).getOrElse(key1,null);
						if(check!=null && key2==check.key){
							counter++;
							//then formulate the results here
						}
					}
				} //end for i
				Console.OUT.println("debug at place "+pn+" match/whole is "+counter+"/"+pa_num);
			}
		}
		var time_6:Long=System.currentTimeMillis();
		Console.OUT.println(" local matching "+(time_6-time_5)+" ms");               	
		
		Console.OUT.println(" WHOLE without I/O TAKES "+(time_6-time_2)+" ms///////////////"); 
		
	}
}

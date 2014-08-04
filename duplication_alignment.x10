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

public class duplication_alignment {
	
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
		
		//broadcast the dict_data to all the places
		finish for( p in Place.places()){
			at (p) async {
				val pn=here.id;
				
				val s1:Int=dict_list(here.id).size();
				var dict_key_collector:Array[Long]=new Array[Long](s1);
				var dict_payload_collector:Array[Long]=new Array[Long](s1);
				var dict_relation_collector:Array[Long]=new Array[Long](s1);
				var i:Int=0;
				var k1:Long;
				var k2:Long;
				var payload:Long;
				for(v in dict_list(here.id)){
					k1=Long.parse(v.key1);
					k2=Long.parse(v.key2);
					payload=Long.parse(v.payload);
					dict_key_collector(i)=k1;
					dict_payload_collector(i)=k2;
					dict_relation_collector(i)=payload;
					i++;
				}
				
				//duplicate the dict-pair to remote places
				for( k in 0..(N-1)) {
					val kk=k;
					val pk=Place.place(k);
					at(pk){
						dict_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						dict_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						dict_relation_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( dict_key_collector, at (pk) dict_keys_receive(here.id)(pn));
					Array.asyncCopy( dict_payload_collector, at (pk) dict_payload_receive(here.id)(pn));
					Array.asyncCopy( dict_relation_collector, at (pk) dict_relation_receive(here.id)(pn));
				}  //end pushing	
				
				//empty the read in data
				dict_list(here.id)=null;
			}
		}
		var time_3:Long=System.currentTimeMillis();
		Console.OUT.println(" duplicate dict_data "+(time_3-time_2)+" ms");
		
		
		//build local dict hash table and searching
		finish for( p in Place.places()){
			at (p) async {
				val pn=here.id;	
				var check:Pair;
				var key:Long;
				var value:Long;
				var relation:Long;
				var size:Long;
				
				//building
				for( i in 0..(N-1)) {			
					size=dict_keys_receive(here.id)(i).size;
					for(j in 0..(size-1)){	
						key=dict_keys_receive(here.id)(i).operator()(j as Int);
						value=dict_payload_receive(here.id)(i).operator()(j as Int);
						relation=dict_relation_receive(here.id)(i).operator()(j as Int);
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
				
				//searching
				var k1:Long;
				var k2:Long;
				var payload:String;
				var counter:Int=0;
				var pa_num:Int=0;
				for(v in test_list(here.id)){
					pa_num++;
					k1=Long.parse(v.key1);
					k2=Long.parse(v.key2);
					payload=v.payload;
					check=table(here.id).getOrElse(k1,null);
					if(check!=null && k2==check.key){
						counter++;
						//then formulate the results here
					}	
				}
				Console.OUT.println("debug at place "+pn+" match/whole is "+counter+"/"+pa_num);
			}
		}
		var time_4:Long=System.currentTimeMillis();
		Console.OUT.println(" building and local matching "+(time_4-time_3)+" ms");               	
		
		Console.OUT.println(" WHOLE without I/O TAKES "+(time_4-time_2)+" ms///////////////"); 
		
	}
}

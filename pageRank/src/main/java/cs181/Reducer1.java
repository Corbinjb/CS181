package cs181;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
 
public class Reducer1 extends Reducer<Text, Text, Text, Text> {

	/* TODO - Implement the reduce function. 
	 * 
	 * 
	 * Input :    Adjacency Matrix Format       ->	( j   ,   M  \t  i	\t value 
	 * 			  Vector Format					->	( j   ,   V  \t   value )
	 * 
	 * Output :   Key-Value Pairs               
	 * 			  Key ->   	i
	 * 			  Value -> 	M_ij * V_j  
	 * 						
	 */

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double vVal = 0;
		ArrayList<String> mList = new ArrayList<String> ();
		
		for(Text val: values) {
			String input  = val.toString();
			String[] valueList = input.split("\t"); 
			
			if(valueList.length == 2) {
				vVal += Double.parseDouble(valueList[1]);
			}else {
				mList.add(valueList[1] + "\t" + valueList[2]);
			}
		}
		
		for(String mval: mList) {
			String[] mvalList = mval.split("\t");
			Double product = Double.parseDouble(mvalList[1]) * vVal;
			
			context.write(new Text(mvalList[0]), new Text(Double.toString(product)));
		}
	}

}
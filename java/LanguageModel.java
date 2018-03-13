import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//By default, when use context.write(), MapReduce output key and value in one line
			//and key and value are separated by \t
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//this is --> cool = 20
			String outputKey = "";
			String outputValue = "";
			for(int i=0; i<words.length-1; i++)
				outputKey += words[i] + " ";
			outputKey = outputKey.trim();
			outputValue = words[words.length-1] + "=" + String.valueOf(count);

			context.write(new Text(outputKey), new Text(outputValue));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
		//only output topK following word
		int topK;
		// get the topK parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//key: is
			//value: {cool=20, big=50, cold=20, a=5};
			//use TreeMap to sort count in reverse order
			//TreeMap: {[50,("big")],[20,("cool","cold")],[5,"a"]};
			TreeMap<Integer,List<String>> myMap = new TreeMap<Integer,List<String>>(Collections.<Integer>reverseOrder());
			for(Text value : values){
				String[] curVal = value.toString().trim().split("=");
				String following_word = curVal[0];
				int count = Integer.parseInt(curVal[1]);
				if(myMap.containsKey(count)){
					myMap.get(count).add(following_word);
				}
				else{
					List<String> followingWordList = new ArrayList<String>();
					followingWordList.add(following_word);
					myMap.put(count,followingWordList);
				}
			}
			Iterator<Integer> iter = myMap.keySet().iterator();
			for(int j=0; iter.hasNext() && j<topK;) {
				int keyCount = iter.next();
				List<String> words = myMap.get(keyCount);
				for(String curWord: words) {
					//DBOutputWritable: refer to DBOutputWritable.java
					//in map reduce, all input are key value pair
					//key is new DBoutputWritable(....)
					//so what is value!?
					//value is NullWritable.get(), means value is Null;
					context.write(new DBOutputWritable(key.toString(), curWord, keyCount),NullWritable.get());
					j++;
				}
			}
		}
	}
}

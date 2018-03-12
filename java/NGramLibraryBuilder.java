import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			//setup n-gram from command line
			Configuration confg = context.getConfiguration();
			//if parameter in command line is missing, default value is 5
			noGram = confg.getInt("noGram",5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			line = line.trim().toLowerCase();
			//preserve ' because I'm, don't contains '
			line = line.replaceAll("[^a-z']"," ");
			System.out.println(line);
			String[] words = line.split("\\s+");

			//if the length of this sentence is less than two
			//which means it contains only one word
			//we ignore it
			if(words.length < 2)
				return;
			for(int i=0; i <= words.length - noGram; i++){
				String phrases = words[i];
				for(int j=1; j<noGram; j++){
					phrases += " " + words[i+j];
					context.write(new Text(phrases),new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for(IntWritable value : values)
				sum += value.get();
			context.write(key, new IntWritable(sum));
		}
	}

}
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		String inputDir = args[0];	//input path
		String nGramLib = args[1];	//output path of job1
		String numberOfNGram = args[2];	//how many gram
		String threshold = args[3];  //the word with frequency under threshold will be discarded
		String numberOfFollowingWords = args[4];	//topK

		//job1
		Configuration conf1 = new Configuration();

		//By default, mapreduce program accepts text file and it reads line by line. 
		//But we want to read text file sentence by sentence
		//So we have to override one property textinputformat.record.delimiter
		//In the future, I will figure out how to set delimiter by regular expression
		conf1.set("textinputformat.record.delimiter", ".");
		conf1.set("noGram", numberOfNGram);
		
		Job job1 = Job.getInstance(conf1);
		job1.setJobName("NGram");
		job1.setJarByClass(Driver.class);
		
		job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
		job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job1, new Path(inputDir));
		TextOutputFormat.setOutputPath(job1, new Path(nGramLib));
		job1.waitForCompletion(true);
		
		//job1's output is job2's input
		
		//2nd job
		Configuration conf2 = new Configuration();
		conf2.set("threshold", threshold);
		conf2.set("n", numberOfFollowingWords);

		//Use dbConfiguration to configure all the jdbcDriver, db user, db password, database
		DBConfiguration.configureDB(conf2, 
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://ip address:port/database name",
				"root",
				"password");
		
		Job job2 = Job.getInstance(conf2);
		job2.setJobName("Model");
		job2.setJarByClass(Driver.class);

		//How to add external dependency to current project?
		/*
		  1. upload dependency to hdfs
		  2. use this "addArchiveToClassPath" method to define the dependency path on hdfs
		 */
		//mysql-connector-java-5.1.39-bin.jar: connect to MySQL
		job2.addArchiveToClassPath(new Path("connector's path"));

		//Why do we add setMapOutputKeyClass() and setMapOutputValueClass()?
		//Because mapper's output key and value are inconsistent with reducer's output key and value
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(DBOutputWritable.class);
		job2.setOutputValueClass(NullWritable.class);
		
		job2.setMapperClass(LanguageModel.Map.class);
		job2.setReducerClass(LanguageModel.Reduce.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(DBOutputFormat.class);

		//use dbOutputformat to define the table name and columns
		DBOutputFormat.setOutput(job2, "output", 
				new String[] {"starting_phrase", "following_word", "count"});

		//job1's output is job2's input
		TextInputFormat.setInputPaths(job2, nGramLib);
		job2.waitForCompletion(true);
	}

}

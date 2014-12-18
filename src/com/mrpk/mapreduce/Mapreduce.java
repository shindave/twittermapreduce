package com.mrpk.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Mapreduce {
	private static SentimentParser sentimentParser = new SentimentParser();
	private static String dohaveKey(String string) {
		if (string.toLowerCase().contains(" nba ")) {
			return "nba";
		} else if (string.contains(" mlb ")) {
			return "mlb";
		} else if (string.contains(" soccer ")){
			return "soccer";
		} else if (string.contains(" sport ")) {
			return "sport";
		}
		return null;
	}
		static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        		int score = 0;
        		String tmpkey = dohaveKey(value.toString());
        		if (tmpkey != null) {
        			score += sentimentParser.countScore(value);
        			word.set(tmpkey);
        			output.collect(word, new IntWritable(score));
        		}
        }
    }
    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
    	@Override
        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
    		int sum = 0;
    		while (values.hasNext()) {
    			sum += values.next().get();
    		}
    		output.collect(key, new IntWritable(sum));
    	}      
    }
    public static void main(String[] args) throws Exception {
        sentimentParser.initpos();
        sentimentParser.initnega();
    	
    	JobConf conf = new JobConf(Mapreduce.class);
        conf.setJobName("mapreduce");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
package mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordAcount {
	static final String URL = "hdfs://localhost:9000";
	static final String INPUT_PATH = "hdfs://localhost:9000/d1/hello";
	static final String OUT_PATH = "hdfs://localhost:9000/d1/out";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(URL), conf);
		if (fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		Job job = new Job(conf, WordAcount.class.getName());

		// 1.1 input
		FileInputFormat.setInputPaths(job, INPUT_PATH);
//		job.setInputFormatClass(TextInputFormat.class);

		// 1.2 mapper
		job.setMapperClass(MyMapper.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(LongWritable.class);

		// 1.3 partition
//		job.setPartitionerClass(HashPartitioner.class);
//		job.setNumReduceTasks(1);

		// 1.4 sort, group

		// 1.5 combiner

		// 2.2 shuffle and sort, reducer
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 2.3 output
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
//		job.setOutputFormatClass(TextOutputFormat.class);

		// submit to JobTracker
		job.waitForCompletion(true);
	}

	static class MyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] splited = value.toString().split("\\s+");
			for (String word : splited) {
				context.write(new Text(word), new LongWritable(1L));
			}
		}
	}

	static class MyReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long count = 0L;
			for (LongWritable value : values) {
				count += value.get();
			}
			context.write(key, new LongWritable(count));
		}
	}
}

package suanfa;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopK {
	static final String URL = "hdfs://localhost:9000";
	static final String INPUT_PATH = "hdfs://localhost:9000/topk/in";
	static final String OUT_PATH = "hdfs://localhost:9000/topk/out";

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		FileSystem fileSystem = FileSystem.get(new URI(URL), conf);
		if (fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}

		Job job = new Job(conf, TopK.class.getName());

		// 1.1 input
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// job.setInputFormatClass(TextInputFormat.class);

		// 1.2 mapper
		job.setMapperClass(MyMapper.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(LongWritable.class);

		// 1.3 partition
		// job.setPartitionerClass(HashPartitioner.class);
		// job.setNumReduceTasks(1);

		// 1.4 sort, group

		// 1.5 combiner

		// 2.2 shuffle and sort, reducer
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// 2.3 output
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// job.setOutputFormatClass(TextOutputFormat.class);

		// submit to JobTracker
		job.waitForCompletion(true);
	}

	static class MyMapper extends
			Mapper<LongWritable, Text, LongWritable, NullWritable> {
		Long max = Long.MIN_VALUE;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Long temp = Long.parseLong(value.toString());
			if (temp > max)
				max = temp;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
			context.write(new LongWritable(max), NullWritable.get());
		}
	}

	static class MyReducer extends
			Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
		Long max = Long.MIN_VALUE;
		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			Long temp = key.get();
			if (temp > max)
				max = temp;
		}
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
			context.write(new LongWritable(max), NullWritable.get());
		}
	}
}

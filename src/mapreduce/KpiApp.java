package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class KpiApp {
	static final String INPUT_PATH = "hdfs://localhost:9000/wlan";
	static final String OUT_PATH = "hdfs://localhost:9000/wlan_out";

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();
		Job job = new Job(configuration, KpiApp.class.getSimpleName());

		// 1.1
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		job.setInputFormatClass(TextInputFormat.class);

		// 1.2
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KpiWritable.class);

		// 1.3
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);

		// 1.4 sort, group
		// 1.5 combine

		// 2.2
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);

		// 2.3
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.waitForCompletion(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] splited = value.toString().split("\t");
			Text k2 = new Text(splited[1]);
			KpiWritable v2 = new KpiWritable(splited[6], splited[7],
					splited[8], splited[9]);
			context.write(k2, v2);
		}
	}

	static class MyReducer extends
			Reducer<Text, KpiWritable, Text, KpiWritable> {
		@Override
		protected void reduce(Text k2, Iterable<KpiWritable> v2s,
				Context context) throws IOException, InterruptedException {
			long upPackNum = 0L;
			long downPackNum = 0L;
			long upPayLoad = 0L;
			long downPayLoad = 0L;
			for (KpiWritable kpiWritable : v2s) {
				upPackNum += kpiWritable.upPackNum;
				downPackNum += kpiWritable.downPackNum;
				upPayLoad += kpiWritable.upPayLoad;
				downPayLoad += kpiWritable.downPayLoad;
			}
			KpiWritable v3 = new KpiWritable(upPackNum + "", downPackNum + "",
					upPayLoad + "", downPayLoad + "");
			context.write(k2, v3);
		}
	}
}

class KpiWritable implements Writable {
	long upPackNum;
	long downPackNum;
	long upPayLoad;
	long downPayLoad;

	public KpiWritable() {
	}

	public KpiWritable(String upPackNum, String downPackNum, String upPayLoad,
			String downPayLoad) {
		this.upPackNum = Long.parseLong(upPackNum);
		this.downPackNum = Long.parseLong(downPackNum);
		this.upPayLoad = Long.parseLong(upPayLoad);
		this.downPayLoad = Long.parseLong(downPayLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readLong();
		this.downPackNum = in.readLong();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upPackNum);
		out.writeLong(downPackNum);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
	}

	@Override
	public String toString() {
		return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t"
				+ downPayLoad;
	}
}
package mtds.alicaldam.powermonitoring;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PowerMonitoringJob extends Configured implements Tool {

	public static final String INPUT_SEPARATOR = ",";

	public static final String FIRST_TIMESTAMP = "first_timestamp";

	public static final int MEASURE_ID_INDEX = 0;
	public static final int TIMESTAMP_INDEX = 1;
	public static final int PLUG_ID_INDEX = 2;
	public static final int HOUSEHOLD_ID_INDEX = 3;
	public static final int HOUSE_ID_INDEX = 4;
	public static final int MEASURE_INDEX = 5;

	public static final int SECONDS_IN_A_HOUR = 3600;

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		InputStream in = null;
		try {
			in = fs.open(inputPath);
			BufferedReader r = new BufferedReader(new InputStreamReader(in));
			long first_timestamp =Long.parseLong( r.readLine().split(INPUT_SEPARATOR)[1]);
			System.out.println("First timestamp: "+first_timestamp);
			config.setLong(FIRST_TIMESTAMP, first_timestamp);
		} catch (Exception e) {
			
			return -1;
		}
		
		Job job = Job.getInstance(config, "Power Monitoring");
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		/*
		 * Map Reduce configuration: the output classes for map and reduce are
		 * different. it is needed to specify them for both the operation.
		 */
		// MAP configuration
		job.setMapperClass(PowerMonitoringMap.class);
		job.setMapOutputKeyClass(HouseIdHourKey.class);
		job.setMapOutputValueClass(MeasurementRecord.class);
		// REDUCE configuration
		job.setReducerClass(PowerMonitoringReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PowerMonitoringJob(), args);
		System.exit(exitCode);
	}

}

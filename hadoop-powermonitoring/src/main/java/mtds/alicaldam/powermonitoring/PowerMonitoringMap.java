package mtds.alicaldam.powermonitoring;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PowerMonitoringMap extends
		Mapper<LongWritable, Text, HouseIdHourKey, MeasurementRecord> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] values = value.toString().split(PowerMonitoringJob.INPUT_SEPARATOR);

		long timestamp = Long.parseLong(values[PowerMonitoringJob.TIMESTAMP_INDEX]);
		int houseId = Integer.parseInt(values[PowerMonitoringJob.HOUSE_ID_INDEX]);

		int householdId = Integer.parseInt(values[PowerMonitoringJob.HOUSEHOLD_ID_INDEX]);
		int plugId = Integer.parseInt(values[PowerMonitoringJob.PLUG_ID_INDEX]);
		int measure = Integer.parseInt(values[PowerMonitoringJob.MEASURE_INDEX]);

		int hourIndex = 0;
		Configuration configuration = context.getConfiguration();
		long first = configuration.getLong(PowerMonitoringJob.FIRST_TIMESTAMP, 0);
		hourIndex = (int) ((timestamp - first) / PowerMonitoringJob.MILLIS_IN_A_HOUR);

		HouseIdHourKey mapKey = new HouseIdHourKey(houseId, hourIndex);
		MeasurementRecord record = new MeasurementRecord(householdId, plugId,
				measure);
		context.write(mapKey, record);

	}

}

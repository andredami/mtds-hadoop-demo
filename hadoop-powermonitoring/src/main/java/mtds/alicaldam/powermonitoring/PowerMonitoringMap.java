package mtds.alicaldam.powermonitoring;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PowerMonitoringMap extends
		Mapper<LongWritable, Text, HouseIdHourKey, MeasurementRecord> {

}

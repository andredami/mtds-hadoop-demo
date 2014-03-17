package mtds.alicaldam.powermonitoring;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PowerMonitoringReduce extends
		Reducer<HouseIdHourKey, MeasurementRecord, HouseIdHourKey, DoubleWritable> {

}

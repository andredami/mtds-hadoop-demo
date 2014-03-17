package mtds.alicaldam.powermonitoring;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class MeasurementRecord implements WritableComparable<MeasurementRecord> {
	
	IntWritable householdId;
	IntWritable plugId;
	IntWritable measure;
		
	public MeasurementRecord(IntWritable householdId, IntWritable plugId,
			IntWritable measure) {
		super();
		this.householdId = householdId;
		this.plugId = plugId;
		this.measure = measure;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(MeasurementRecord o) {
		// TODO Auto-generated method stub
		return 0;
	}

}

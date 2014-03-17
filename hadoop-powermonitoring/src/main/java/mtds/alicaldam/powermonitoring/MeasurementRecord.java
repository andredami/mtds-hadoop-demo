package mtds.alicaldam.powermonitoring;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MeasurementRecord implements WritableComparable<MeasurementRecord> {
	
	Integer householdId;
	Integer plugId;
	Integer measure;

	public MeasurementRecord(int householdId2, int plugId2,
			int measure2) {
		super();
		this.householdId = householdId2;
		this.plugId = plugId2;
		this.measure = measure2;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.householdId=arg0.readInt();
		this.plugId=arg0.readInt();
		this.measure=arg0.readInt();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(householdId);
		arg0.writeInt(plugId);
		arg0.writeInt(measure);
		
	}

	@Override
	public int compareTo(MeasurementRecord o) {
		return this.measure.compareTo(o.measure);
	}
}

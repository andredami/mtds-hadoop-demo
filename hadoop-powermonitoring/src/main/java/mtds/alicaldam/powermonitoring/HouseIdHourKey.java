package mtds.alicaldam.powermonitoring;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class HouseIdHourKey implements WritableComparable<HouseIdHourKey> {

	
	Integer houseId;
	Integer hourIndex;
	
	
	
	public HouseIdHourKey(int householdId, int hourIndex) {
		super();
		this.houseId = householdId;
		this.hourIndex = hourIndex;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.houseId = arg0.readInt();
		this.hourIndex = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(houseId);
		arg0.writeInt(hourIndex);
	}

	@Override
	public int compareTo(HouseIdHourKey arg0) {
		int dHouseholdId = this.houseId.compareTo(arg0.houseId);
		int dHourIndex = this.hourIndex.compareTo(arg0.hourIndex);
		
		if(dHouseholdId != 0){
			return dHouseholdId;
		}
		
		return dHourIndex;
	}
}

package mtds.alicaldam.powermonitoring;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class HouseIdHourKey implements WritableComparable<HouseIdHourKey> {

	
	Integer houseId;
	@Override
	public String toString() {
		return "HouseIdHourKey [houseId=" + houseId + ", hourIndex="
				+ hourIndex + "]";
	}

	Integer hourIndex;
	
	
	public HouseIdHourKey() {
		// This is needed for hadoop to use reflection 
		//http://stackoverflow.com/questions/11446635/no-such-method-exception-hadoop-init
	}
	public HouseIdHourKey(int houseId, int hourIndex) {
		super();
		this.houseId = houseId;
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
		int dHouseId = this.houseId.compareTo(arg0.houseId);
		int dHourIndex = this.hourIndex.compareTo(arg0.hourIndex);
		
		if(dHouseId != 0){
			return dHouseId;
		}
		
		return dHourIndex;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((hourIndex == null) ? 0 : hourIndex.hashCode());
		result = prime * result + ((houseId == null) ? 0 : houseId.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HouseIdHourKey other = (HouseIdHourKey) obj;
		if (hourIndex == null) {
			if (other.hourIndex != null)
				return false;
		} else if (!hourIndex.equals(other.hourIndex))
			return false;
		if (houseId == null) {
			if (other.houseId != null)
				return false;
		} else if (!houseId.equals(other.houseId))
			return false;
		return true;
	}
	
}

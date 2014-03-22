package mtds.alicaldam.powermonitoring;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PowerMonitoringReduce
		extends
		Reducer<HouseIdHourKey, MeasurementRecord, Text, DoubleWritable> {

	@Override
	protected void reduce(HouseIdHourKey key,
			Iterable<MeasurementRecord> values, Context context)
			throws IOException, InterruptedException {
		//System.out.println("Starting reduce for "+key);
		LinkedList<MeasurementRecord> list = new LinkedList<MeasurementRecord>();
		for (MeasurementRecord r : values) {
			//System.out.println();
			list.add(new MeasurementRecord(r.householdId, r.plugId, r.measure));
			
		}
		Collections.sort(list);

		Map<PlugId, LinkedList<MeasurementRecord>> map = new HashMap<PlugId, LinkedList<MeasurementRecord>>();
		for (MeasurementRecord rec : list) {
			//System.out.println("Structuring: "+rec);
			PlugId id = new PlugId(rec.householdId, rec.plugId);
			if (map.containsKey(id)) {
				//System.out.println("registering measure:"+rec);
				map.get(id).addLast(rec);
			} else {
				//System.out.println("NEW plug: "+rec.householdId+","+rec.plugId);
				//System.out.println("registering measure:"+rec);
				LinkedList<MeasurementRecord> newlist = new LinkedList<MeasurementRecord>();
				newlist.addLast(rec);
				map.put(id, newlist);
			}
		}

		int overallMedianValue = computeMedianValue(list);
		System.out.printf("OVERALL MEDIAN VALUE IS:%d\n", overallMedianValue);
		int numOfPlugs = map.entrySet().size();
		System.out.printf("NUMBER OF PLUGS IS:%d\n",numOfPlugs);
		int numOfOutliers = 0;
		for (LinkedList<MeasurementRecord> pluglist : map.values()) {
			int medianplug = computeMedianValue(pluglist);
			//System.out.println("medianplug:"+medianplug);
			if (medianplug > overallMedianValue) {
				//System.out.println("incrementing outliers...");
				numOfOutliers++;
			}
		}

		long timestamp = context.getConfiguration().getLong(PowerMonitoringJob.FIRST_TIMESTAMP, 0) + key.hourIndex * PowerMonitoringJob.SECONDS_IN_A_HOUR;
		Date date = new Date(timestamp*1000);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		
		context.write(new Text(key.houseId + PowerMonitoringJob.INPUT_SEPARATOR + df.format(date)), new DoubleWritable((double)numOfOutliers / numOfPlugs * 100));

	}

	private int computeMedianValue(LinkedList<MeasurementRecord> values) {
		return values.size() % 2 != 0 
				? values.get(values.size() / 2).measure
				: ((values.get(values.size() / 2).measure + 
						values.get((values.size() / 2) - 1).measure) 
						/ 2);
	}

	public class PlugId {
		public PlugId(int householdId, int plugId) {
			super();
			this.householdId = householdId;
			this.plugId = plugId;
		}

		int householdId;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + householdId;
			result = prime * result + plugId;
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
			PlugId other = (PlugId) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (householdId != other.householdId)
				return false;
			if (plugId != other.plugId)
				return false;
			return true;
		}

		int plugId;

		private PowerMonitoringReduce getOuterType() {
			return PowerMonitoringReduce.this;
		}
	}
}

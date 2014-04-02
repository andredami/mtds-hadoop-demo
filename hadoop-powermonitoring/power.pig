--registro la UDF per calcolare la mediana
REGISTER hadoop-powermonitoring-0.0.1-SNAPSHOT.jar;
DEFINE MEDIAN powermonitoring.udf.Median();
--carico i dati
datarecords = LOAD '/user/hduser/data.csv' USING PigStorage(',') AS (mid:long,timestamp:long,plugid:int,householdid:int,houseid:int,measurement:int);
--setto il campo hourindex
top_bag = GROUP datarecords ALL;
start= FOREACH top_bag GENERATE MIN(datarecords.timestamp) AS timestamp;
parsed_datarecord = FOREACH datarecords GENERATE (timestamp-start.timestamp)/(long)3600 AS hourindex:long, plugid, householdid, houseid, measurement;
--calcolo la mediana per ogni edificio e per ogni ora
data_per_househour= GROUP parsed_datarecord BY (houseid, hourindex);
house_medians = FOREACH data_per_househour GENERATE group, MEDIAN(parsed_datarecord.measurement) AS house_median:double;
--calcolo la mediana per ogni plug in ogni appartamento
data_per_plughour=  GROUP parsed_datarecord BY (houseid, hourindex, householdid, plugid);
plug_medians = FOREACH data_per_plughour GENERATE group, MEDIAN(parsed_datarecord.measurement) AS plug_median:double;
--cerco gli outliers
house_median_flat= FOREACH house_medians GENERATE FLATTEN(group) AS (houseid:int, hourindex:long), house_median;
plug_median_flat= FOREACH plug_medians GENERATE FLATTEN(group) AS (houseid:int, hourindex:long, householdid:int, plugid:int), plug_median;
pre_outliers = JOIN house_median_flat BY (hourindex, houseid), plug_median_flat BY (hourindex, houseid);
outliers = FOREACH pre_outliers GENERATE house_median_flat::hourindex AS hourindex,house_median_flat::houseid AS houseid, (plug_median_flat::plug_median - house_median_flat::house_median) AS test:double;
outliers_only = FILTER outliers BY test > (double)0;
--calcolo il numero di outliers in ogni edificio per ogni ora 
pre_outliers_count = GROUP outliers_only BY (houseid, hourindex);
outliers_count = FOREACH pre_outliers_count GENERATE group, (int)COUNT(outliers_only) AS outliers_num:int;
--calcolo il numero totale di plug in ogni edificio per ogni ora
pre_plug_count = GROUP plug_median_flat BY (houseid, hourindex);
plugs_count =FOREACH pre_plug_count GENERATE group,(int)COUNT(plug_median_flat) AS plugs_num:int;
--calcolo la percentuale di outliers
perc_eval = JOIN outliers_count BY group, plugs_count BY group;
perc_val = FOREACH perc_eval GENERATE outliers_count::group, ((double)outliers_count::outliers_num/(double)plugs_count::plugs_num)*(double) 100 AS perc;
--ripristino timestamp
val = FOREACH perc_val GENERATE outliers_count::group.houseid AS house, ((long)3600 * outliers_count::group.hourindex) + start.timestamp AS time, perc AS percentage;
DUMP val;

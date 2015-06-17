rmf /user/ddobryak/sensor/res;

register 'hdfs://hadoop1/user/ddobryak/sensor/lib/piggybank-0.12.0.jar'
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

src = load '/user/ddobryak/sensor/data' using PigStorage('\t') as (str:chararray);

data = foreach src generate FLATTEN(STRSPLIT(str, '","', 6)) as
(
    id:chararray
  , sensor_id: chararray
  , device_id: chararray
  , last_seen: chararray
  , rssi: chararray
  , dt: chararray
);

tmp = foreach data generate 
  sensor_id
, device_id
, rssi
, ToDate(REPLACE(last_seen, ' UTC', ''), 'EEE MMM dd HH:mm:ss.SSS yyyy') as dt;


tmp1 = foreach tmp generate
  sensor_id
, device_id
, rssi
, dt
, ToString(dt,'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ') as dt_str;
  
tmp2 = foreach tmp1 generate
  sensor_id
, device_id
, (float)rssi as rssi
, dt
, 3600 * 1000 * (ISOToUnix(dt_str) / (3600 * 1000)) as dt_rounded_long;

grouped = foreach (group tmp2 by (sensor_id, device_id, dt_rounded_long)) generate
  group.sensor_id as sensor_id
, group.device_id as device_id
, MIN(tmp2.dt) as min_dt
, MAX(tmp2.dt) as max_dt
, AVG(tmp2.rssi) as avg_rssi
, COUNT(tmp2) as cnt;

res = foreach grouped generate
  sensor_id
, device_id
, (MilliSecondsBetween(max_dt, min_dt) == (long)0 ? 1 : SecondsBetween(max_dt, min_dt)) as seen_for
, avg_rssi
, ToString(min_dt,'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ') as dt
, cnt;

store res into '/user/ddobryak/sensor/res' using PigStorage('\t');
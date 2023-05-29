CREATE EXTERNAL TABLE IF NOT EXISTS `tharso`.`accelerometer_landing` (
  `user` string,
  `x` float,
  `y` float,
  `z` float,
  `timeStamp` bigint
) COMMENT "Landing accelerometer data."
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tharsos-bucket/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
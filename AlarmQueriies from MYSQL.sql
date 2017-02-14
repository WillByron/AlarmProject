
Create View dbo.bmdi_alarm_category as 
SELECT *  FROM OPENQUERY([BMDI64],'select ''alarm category id'',manual_alarm,manual_category from BMDI_test.alarm_category')
SELECT  *  FROM OPENQUERY([BMDI64],'SELECT*FROM BMDI_test.map_categories ') 
alter View dbo.bmdi_alarm_events as 
SELECT  *  FROM OPENQUERY([BMDI64],'SELECT alarm_index_id, timestamp, alarm_source, msh_start_datetime, alarm_start_datetime, monitor_label, unit, bed, 
duration_sec, channel, alarm_text
FROM BMDI_test.alarm_events where bed = ''9-244'' and alarm_start_datetime between ''2015-05-18 17:00'' and ''2015-05-18 23:00'' order by alarm_start_datetime ') 

alter View dbo.bmdi_alarm_events as 
SELECT  *  FROM OPENQUERY([BMDI64],'SELECT*
FROM BMDI_test.map_categories ') 

Create View dbo.bmdi_raw_data as 
SELECT *  FROM OPENQUERY([BMDI64],'SELECT raw_index_id, timestamp, alarm_source, msh_datetime, report_datetime, monitor_label, unit, bed, channel, alarm_text
FROM BMDI_test.raw_data') 

SELECT *  FROM OPENQUERY([BMDI64],'select ''alarm category id'',manual_alarm,manual_category from BMDI_test.alarm_category')
SELECT  *  FROM OPENQUERY([BMDI64],'SELECT*
FROM BMDI_test.map_categories ') 
SELECT  *  FROM OPENQUERY([BMDI64],'
SELECT alarm_events.alarm_index_id AS alarm_index_id,

  alarm_events.alarm_source AS alarm_source,
  alarm_events.msh_start_datetime AS msh_start_datetime,
  alarm_events.alarm_start_datetime AS alarm_start_datetime,
  alarm_events.monitor_label AS monitor_label,
  alarm_events.unit AS unit,
  alarm_events.bed AS bed,
  alarm_events.duration_sec AS duration_sec,
  alarm_events.channel AS channel,
  alarm_events.alarm_text AS alarm_text,

  alarm_categories.manual_alarm AS manual_alarm,
  alarm_categories.manual_category AS manual_category,
  map_categories_new.map_categories_id AS map_categories_id,
  map_categories_new.map_category AS map_category,
  map_categories_new.report_category AS report_category
  
  FROM BMDI_test.alarm_events alarm_events
  JOIN BMDI_test.alarm_category alarm_categories ON (alarm_events.alarm_text = alarm_categories.manual_alarm)
  Left Join  BMDI_test.map_categories map_categories_new ON (case when alarm_categories.manual_category = 'Soft Inop'= map_categories_new.map_category) 
  where map_category is not null')

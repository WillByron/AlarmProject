select  distinct filename from   [HelixReportStage].[dbo].HadoopRawAlarms	 where	day(dateadd(s, cast([alarm_ts] as int), '19700101')) = 5
select  filename from   [HelixReportStage].[dbo].[HadoopRawAlarmsStage2]	 where	dateadd(s, cast([alarm_ts] as int), '19700101') = (select 
 max(dateadd(s, cast([alarm_ts] as int), '19700101')) from [HelixReportStage].[dbo].[HadoopRawAlarmsStage2]s	)

 select dateadd(s, cast([alarm_ts] as int), '19700101'),* from  [HelixReportStage].[dbo].[HadoopRawAlarmsStage2] order by alarm_ts desc
select count(distinct filename) from   [HelixReportStage].[dbo].HadoopRawAlarms		
select count(*) from [HelixReportStage].[dbo].HadoopRawAlarms

select min(dateadd(s, cast([alarm_ts] as int), '19700101')),max(dateadd(s, cast([alarm_ts] as int), '19700101'))
from [HelixReportStage].[dbo].HadoopRawAlarms

select top 1000 * from [HelixReportStage].[dbo].HadoopRawAlarms 
select Source, TimeStamp,Unit, Bed, Channel,Text, count(*)  from [HelixReportStage].[dbo].[HadoopAlarmsDuration]	group by Source, TimeStamp,Unit, Bed, Channel,Text having count(*) > 1
/data/bmdi/signals/avro/2017/01/1483588046007.avro
/data/bmdi/signals/avro/2017/01/1484145892850.avro
--Create HadoopAlarmsDuration
USE [HelixReportStage]
GO

INSERT INTO [dbo].[HadoopRawAlarmsStage2]
           ([msh_ts]
           ,[alarm_ts]
           ,[source]
           ,[unit]
           ,[bed]
           ,[channel]
           ,[text]
           ,[msg_id]
           ,[helix_rcv_ts]
           ,[tz_offset]
           ,[filename]
          )
SELECT [msh_ts] ,[alarm_ts] ,[source] ,[unit] ,[bed] ,[channel] ,[text]  ,[msg_id] ,[helix_rcv_ts]   ,[tz_offset] ,[filename]  
FROM [dbo].[HadoopRawAlarms]
GO
truncate table HadoopRawAlarms


select LAG(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text Order by dateadd(s, cast([alarm_ts] as int), '19700101')) PreviousValue,
		dateadd(s, cast([alarm_ts] as int), '19700101') TIMESTAMP,
		LEAD(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text Order by dateadd(s, cast([alarm_ts] as int), '19700101')) NextValue,
		datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),lead(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ) DURATION,
  *   from   [HelixReportStage].[dbo].[HadoopRawAlarmsStage2]  where	text like '%161%' and   bed = '9-224'  order by timestamp asc
  select max(dateadd(s, cast([alarm_ts] as int), '19700101')) from [HelixReportStage].[dbo].[HadoopRawAlarmsStage2] where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N303' and channel = '286' and text = 'awRR   31  >30'
--Start key select statements from Prod Table
select * from [HelixReport].[dbo].HadoopAlarmEvents order by AlarmSource, AlarmUnit, AlarmBedLabel, AlarmChannel,AlarmDescription
/data/bmdi/signals/avro/2017/01/1484071379029.avro
/data/bmdi/signals/avro/2017/01/1484074549496.avro
Select  Count(*) Count, sum( AlarmDurationSeconds )DurationSeconds, sum( AlarmDurationSeconds )/60 DurationMin, AlarmSource
		  ,AlarmUnit
		  ,AlarmBedLabel
		  ,AlarmChannel
		  ,AlarmDescription 
		  ,min(AlarmDateTime) MinTs
		  ,max(AlarmDateTime) MaxTs
from [HelixReport].[dbo].HadoopAlarmEvents  where AlarmDurationSeconds >=0 
group by AlarmSource, AlarmUnit, AlarmBedLabel, AlarmChannel,AlarmDescription
order by AlarmSource, AlarmUnit, AlarmBedLabel, AlarmChannel,AlarmDescription
--End Start key select statements from Prod Table


Select * from [HelixReportStage].[dbo].[HadoopAlarmsDuration] 
  where  source = 'YSCPHGPRI01' and unit = 'MICU'and bed = '10-204'and channel = '291'and text = 'ART   Change Scale'
Select * from [HelixReportStage].[dbo].[HadoopAlarmsDuration] 
  where  source = 'HSRIXSERV5' and unit = 'V3N'and bed = 'V3N303'and channel = '291'and text = 'SpO2   Searching'

select distinct text from [HelixReportStage].[dbo].[HadoopAlarmsDuration] 
 
 
 SELECT 
		      
      a.[source] AlarmSource
      ,a.[unit] AlarmUnit
      ,a.[bed] AlarmBedLabel
      ,a.[channel] AlarmChannel
      ,a.[text] AlarmDescription
      ,a.[DURATION] AlarmDurationSeconds
	  ,c.[AlarmCategory]
      ,c.[ReportCategory]
	  ,d.[PAT_MRN_ID] PatMrnId
      ,d.[PAT_ENC_CSN_ID] PatEncCSNId
      ,d.[DEPARTMENT_ID] DepartmentId
      ,d.[DEPARTMENT_NAME] DepartmentName
      ,d.[ROOM_ID] ClarityRoomId
      ,d.[ROOM_NAME] ClarityRoomName
      ,d.[ROOM_NUMBER] ClarityRoomNumber
      ,d.[BED_ID]  ClarityBedId
      ,d.[BED_LABEL] ClarityBedLabel
 	  ,d.transfertime ClarityBedTimeStart
      ,a.[timestamp] AlarmDateTime
	  ,d.nextvalue ClarityBedTimeEnd
      ,d.[EventTypeName] ClarityEventType
     ,d.[EVENT_ID] ClarityEventId
      ,d.[Date_Inserted]
	  ,a.[filename] HadoopFileName
	  ,a.[msg_id] AlarmMsgId 
	  into HelixReport.[dbo].HadoopAlarmEvents
 from   [HelixReportStage].[dbo].HadoopRawAlarmsStage3 a 
 left outer join   [HelixReportStage].[dbo].HadoopAlarmBedLabelXRef b on a.bed = b.AlarmDataBedLabel
 left outer join [HelixReportStage].[dbo].[HadoopAlarmCategories] c on a.text = c.alarmtext
 left outer join   HelixReport.dbo.Hadoop_BedTransfers_Pivoted d on b.ClarityBedLabel = d.bed_label
 and  a.TIMESTAMP between d.transfertime and d.nextvalue
 where a.nextvalue is not null 
 order by a.bed,a.channel,a.timestamp


 select * from helixReport.dbo.Hadoop_BedTransfers_Pivoted where bed_label = '10204-A' order by transfertime

 select * from [HelixReportStage].[dbo].HadoopAlarmsDuration where msg_id = '7107224148846cc'
 
 select * from [HelixReportStage].[dbo].HadoopRawAlarms		 where msg_id = '710722414422144'
 
 select * from [HelixReportStage].[dbo].[HadoopAlarmsDuration] 
 where  source = 'YSCPHGPRI01' and unit = 'MICU'and bed = '10-204'and channel = '291'and text = 'ART   Change Scale'

  select * from [HelixReportStage].[dbo].HadoopAlarmsDuration where filename = '1.avro' and source = 'YSCPHGPRI01' and unit = 'MICU'and
  bed = '9-254'and channel = '290'

  select * from [HelixReportStage].[dbo].HadoopAlarmsDuration where filename = '/tmp/avro/2.avro' and source = 'YSCPHGPRI01' and unit = 'MICU'and
  bed = '9-254'and channel = '290'


  truncate table [dbo].[Hadoop_BedTransfers_Pivoted]

INSERT INTO [dbo].[Hadoop_BedTransfers_Pivoted]
           ([PAT_MRN_ID]
           ,[PreviousValue]
           ,[TransferTime]
           ,[NextValue]
           ,[EVENT_ID]
           ,[DEPARTMENT_ID]
           ,[DEPARTMENT_NAME]
           ,[ROOM_ID]
           ,[ROOM_NAME]
           ,[ROOM_NUMBER]
           ,[BED_ID]
           ,[BED_LABEL]
           ,[EVENT_SUBTYPE_C]
           ,[PAT_ENC_CSN_ID]
           ,[EventTypeName]
           ,[Date_Inserted])

SELECT [PAT_MRN_ID]
	  ,LAG(Transfertime) OVER (partition BY [PAT_MRN_ID], [PAT_ENC_CSN_ID] Order by transfertime) PreviousValue
      ,[TransferTime]
      ,lead(Transfertime) OVER (partition BY [PAT_MRN_ID], [PAT_ENC_CSN_ID] Order by transfertime) PreviousValue
	  ,[EVENT_ID]
      ,[DEPARTMENT_ID]
      ,[DEPARTMENT_NAME]
      ,[ROOM_ID]
      ,[ROOM_NAME]
      ,[ROOM_NUMBER]
      ,[BED_ID]
      ,[BED_LABEL]
      ,[EVENT_SUBTYPE_C]
      ,[PAT_ENC_CSN_ID]
      ,[EventTypeName]
      ,[Date_Inserted]
  FROM [dbo].[Hadoop_BedTransfers]
GO


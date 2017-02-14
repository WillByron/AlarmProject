



---- Clear stage Tables
--Delete From  HelixReportStage.[dbo].[HadoopRawAlarmsStage] --Do a delete so we can keep identity seed so that records inserted to final fact table are unique


----Below two lines only for first load
--truncate table HelixReport.[dbo].HadoopAlarmEvents
--truncate table HelixReportStage.[dbo].HadoopRawAlarmsUnion
set nocount on

truncate table HelixReportStage.[dbo].HadoopRawAlarmsStage
INSERT INTO HelixReportStage.[dbo].HadoopRawAlarmsStage
           (
		    LoopId
		   ,[source]
           ,[unit]
           ,[bed]
           ,[channel]
		   ,Duration
           ,[text]
           ,[msh_ts]
           ,[alarm_ts]
           ,[msg_id]
           ,[TimeStamp]
           ,[filename]
			,AlarmEnded )
    SELECT 

	    ROW_NUMBER() OVER(order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101'))  
	   ,[source]
      ,[unit]
      ,[bed]
      ,[channel]
	  ,Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
		LEAD(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text 
		Order by dateadd(s, cast([alarm_ts] as int), '19700101')))
	  ,[text]
	  ,[msh_ts]
      ,[alarm_ts]
      ,[msg_id]
       ,dateadd(s, cast([alarm_ts] as int), '19700101')
      ,[filename]
	  ,0
  FROM HelixReportStage.[dbo].[HadoopRawAlarms] 
  where Day(dateadd(s, cast([alarm_ts] as int), '19700101')) in (26)
--where  source = 'PICSERV05' and unit = '5-100' and bed = '5-118' and channel = '286' and text = 'PAPd 23 > 20'
  order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')
GO


----Get Clarity Bed Transfer records

--truncate table HelixReport.[dbo].[Hadoop_BedTransfers_Pivoted]

--INSERT INTO HelixReport.[dbo].[Hadoop_BedTransfers_Pivoted]
--           ([PAT_MRN_ID]
--           ,[PreviousValue]
--           ,[TransferTime]
--           ,[NextValue]
--           ,[EVENT_ID]
--           ,[DEPARTMENT_ID]
--           ,[DEPARTMENT_NAME]
--           ,[ROOM_ID]
--           ,[ROOM_NAME]
--           ,[ROOM_NUMBER]
--           ,[BED_ID]
--           ,[BED_LABEL]
--           ,[EVENT_SUBTYPE_C]
--           ,[PAT_ENC_CSN_ID]
--           ,[EventTypeName]
--           ,[Date_Inserted])
--SELECT	[PAT_MRN_ID]
--	 ,LAG(Transfertime) OVER (partition BY [BED_LABEL] Order by [BED_LABEL],transfertime ) PreviousValue
--	 ,[TransferTime]
--	 ,lead(Transfertime,1,'12/31/2079') OVER (partition BY [BED_LABEL] Order by [BED_LABEL],transfertime ) NextValue
--     ,[EVENT_ID]
--     ,[DEPARTMENT_ID]
--     ,[DEPARTMENT_NAME]
--     ,[ROOM_ID]
--     ,[ROOM_NAME]
--     ,[ROOM_NUMBER]
--     ,[BED_ID]
--     ,[BED_LABEL]
--     ,[EVENT_SUBTYPE_C]
--     ,[PAT_ENC_CSN_ID]
--     ,[EventTypeName]
--     ,[Date_Inserted]
--  FROM HelixReport.[dbo].[Hadoop_BedTransfers] where [TransferTime] > (select dateadd(d,-1,min([timestamp])) from [HelixReportStage].[dbo].HadoopRawAlarmsStage )
--GO

truncate table helixreportstage.dbo.HadoopActiveAlarms

DECLARE
    @counter			INT ,
    @max				INT = 0,
	@min				INT = 0

	
-- Initialize the @max variable. We'll use this variable in the next WHILE loop.
SELECT @counter = Min(LoopId) ,@max = Max(LoopId) FROM HelixReportStage.[dbo].HadoopRawAlarmsStage
-- Loop 
WHILE @counter <= @max
--WHILE @counter <= 3
BEGIN
;with cte1 as 
(SELECT 
 Loopid ,AuditId, source, unit,Bed,Channel,Duration,TEXT,msh_ts,alarm_ts,msg_id,TimeStamp,filename,DateInserted ,alarmended 
FROM  HelixReportStage.[dbo].HadoopRawAlarmsStage 
)
MERGE INTO helixreportstage.dbo.HadoopActiveAlarms AS Target
USING (SELECT AuditId, source, unit,Bed,Channel,Duration,TEXT,msh_ts,alarm_ts,msg_id,TimeStamp,filename,DateInserted ,alarmended 
FROM cte1 WHERE LoopId = @counter) AS Source
--using (select * from cte1) as source
ON (Target.Source = Source.Source AND Target.unit = Source.unit AND TARGET.channel = source.channel AND TARGET.bed = source.bed 
AND TARGET.text = source.text and source.alarmended = TARGET.alarmended )
WHEN MATCHED THEN
	
    UPDATE SET Target.Duration =  
	case	when Source.Duration > 15 then isnull(Target.Duration,0) + 5  
			Else  isnull(Target.Duration,0) + Source.Duration 
			End,
	AlarmEnded = Case When Source.Duration > 15  then 1 Else target.AlarmEnded End,
    Previoustimestamp = Source.Timestamp

WHEN NOT MATCHED BY TARGET THEN
    INSERT (AuditId, source, unit,Bed,Channel,Duration,TEXT,msh_ts,alarm_ts,msg_id,TimeStamp,previousTimeStamp,filename,DateInserted ,alarmended)
    VALUES (source.AuditId, source.source, source.unit,	source.Bed,source.Channel,case when source.duration > 15 then 5 else source.duration end
	,source.TEXT,source.msh_ts,source.alarm_ts,
	source.msg_id,source.TimeStamp,	source.timestamp,source.filename,source.DateInserted ,
	case when Source.Duration > 15  then 1 Else Source.AlarmEnded End);
	NextRecord:
    SET @counter = @counter + 1
END
--Populate fact table
--INSERT INTO [HelixReport].Dbo.[HadoopAlarmEvents]
--           (AlarmKey
--		   ,[AlarmSource]
--		   ,[AlarmDateTime]
--           ,[AlarmUnit]
--           ,[AlarmBedLabel]
--           ,[AlarmChannel]
--           ,[AlarmDescription]
--           ,[AlarmDurationSeconds]
--           ,[AlarmCategory]
--           ,[ReportCategory]
--           ,[PatMrnId]
--           ,[PatEncCSNId]
--           ,[DepartmentId]
--           ,[DepartmentName]
--           ,[ClarityRoomId]
--           ,[ClarityRoomName]
--           ,[ClarityRoomNumber]
--           ,[ClarityBedId]
--           ,[ClarityBedLabel]
--           ,[ClarityBedTimeStart]
--           ,[ClarityBedTimeEnd]
--           ,[ClarityEventType]
--           ,[ClarityEventId]
--           ,[Date_Inserted]
--           ,[HadoopFileName]
--           ,[AlarmMsgId])
     
-- SELECT a.AuditId
--       ,a.[source] AlarmSource
--	  ,a.[timestamp] AlarmDateTime
--      ,a.[unit] AlarmUnit
--      ,a.[bed] AlarmBedLabel 
--      ,a.[channel] AlarmChannel
--      ,a.[text] AlarmDescription
--      ,a.[DURATION] AlarmDurationSeconds
--	  ,b.[AlarmCategory]
--      ,b.[ReportCategory]
--	  ,d.[PAT_MRN_ID] PatMrnId
--      ,d.[PAT_ENC_CSN_ID] PatEncCSNId
--      ,d.[DEPARTMENT_ID] DepartmentId
--      ,d.[DEPARTMENT_NAME] DepartmentName
--      ,d.[ROOM_ID] ClarityRoomId
--      ,d.[ROOM_NAME] ClarityRoomName
--      ,d.[ROOM_NUMBER] ClarityRoomNumber
--      ,d.[BED_ID]  ClarityBedId
--      ,d.[BED_LABEL] ClarityBedLabel
-- 	  ,d.transfertime ClarityBedTimeStart
-- 	  ,d.nextvalue ClarityBedTimeEnd
--      ,d.[EventTypeName] ClarityEventType
--      ,d.[EVENT_ID] ClarityEventId
--      ,d.[Date_Inserted]
--	  ,a.[filename] HadoopFileName
--	  ,a.[msg_id] AlarmMsgId 

--from   [HelixReportStage].[dbo].HadoopRawAlarmsStage3 a  
--inner join [HelixReportStage].[dbo].HadoopAlarmCategories b on a.text = b.alarmtext
--inner join [HelixReportStage].[dbo].HadoopAlarmBedLabelXRef c on a.unit = c.AlarmUnit and a.bed = c.AlarmBedLabel
--left outer join 
--(select * from HelixReport.dbo.Hadoop_BedTransfers_Pivoted   
--	where   transfertime >=(Select DateAdd(month,-1,min(TIMESTAMP) )
--	from [HelixReportStage].[dbo].HadoopRawAlarmsStage3) 
--	and nextvalue is not null) d
--on
--	a.TIMESTAMP >= d.transfertime and a.timestamp < d.nextvalue and c.ClaritybedLabel = d.bed_label  


----Insert New AlarmTexts to ReportCategory Table
--INSERT INTO [HelixReportStage].[dbo].[HadoopAlarmCategories] ([AlarmText],[AlarmCategory] ,[ReportCategory])
--Select distinct text,'Unknown','Unknown' from [HelixReportStage].[dbo].HadoopRawAlarmsStage2 a 
--where  not exists
--(select * from [HelixReportStage].[dbo].[HadoopAlarmCategories] where AlarmText = a.text)

----Insert new bed labels into bed label dimension

--INSERT INTO [HelixReportStage].[dbo].[HadoopAlarmBedLabelXRef]  ([AlarmUnit] ,[AlarmBedLabel],[ClarityBedLabel],[ClarityRoomId],[ClarityRoomName],[ClarityDepartmentId],[ClarityDepartmentName])

--Select distinct unit, bed,'Unknown','Unknown','Unknown','Unknown','Unknown'  from [HelixReportStage].[dbo].HadoopRawAlarmsStage2 a where not exists
--(select * from [HelixReportStage].[dbo].HadoopAlarmBedLabelXRef where AlarmBedLabel = a.bed and Alarmunit = a.unit)


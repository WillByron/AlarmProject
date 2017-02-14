
---- Clear previous runs alarms that ended 
Delete From  HelixReportStage.[dbo].[HadoopRawAlarmsStage] --Do a delete so we can keep identity seed so that records inserted to final fact table are unique

Delete From  helixreportstage.dbo.HadoopActiveAlarms where AlarmEnded = 1
----Below  line only for first load
--truncate table HelixReport.[dbo].HadoopAlarmEvents

set nocount on
Set Statistics Time On

INSERT INTO HelixReportStage.[dbo].HadoopRawAlarmsStage
           (
		    LoopId
			,[Rank]
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
	  ,DENSE_RANK() over ( order by source, unit, bed, channel,text) as rank
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
  where Day(dateadd(s, cast([alarm_ts] as int), '19700101')) in (27)
    order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')
GO


--Get Clarity Bed Transfer records

truncate table HelixReport.[dbo].[Hadoop_BedTransfers_Pivoted]

INSERT INTO HelixReport.[dbo].[Hadoop_BedTransfers_Pivoted]
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
SELECT	[PAT_MRN_ID]
	 ,LAG(Transfertime) OVER (partition BY [BED_LABEL] Order by [BED_LABEL],transfertime ) PreviousValue
	 ,[TransferTime]
	 ,lead(Transfertime,1,'12/31/2079') OVER (partition BY [BED_LABEL] Order by [BED_LABEL],transfertime ) NextValue
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
  FROM HelixReport.[dbo].[Hadoop_BedTransfers] where [TransferTime] > (select dateadd(d,-1,min([timestamp])) from [HelixReportStage].[dbo].HadoopRawAlarmsStage )
GO

DECLARE @StartTime datetime,@EndTime datetime   
SELECT @StartTime=GETDATE() 
--Your Query to be run goes here--  



DECLARE
    @counter			INT ,
    @max				INT = 0,
	@min				INT = 0,
	@Filename           varchar(255)


-- Initialize the @max variable. We'll use this variable in the next WHILE loop.
SELECT @counter = Min(LoopId) ,@max = Max(LoopId),@filename = max(filename) FROM HelixReportStage.[dbo].HadoopRawAlarmsStage 

-- Loop through alarm events and save actual alarms based on criteria that an alarm ends if the next alarm is over 15 seconds away. If only one event it is assumed it happened for 5 seconds (no event after it so assume 5 seconds)
WHILE @counter <= @max
--WHILE @counter <= 4
BEGIN
--;with cte1 as 
--(SELECT 
-- Rank,Loopid ,AuditId, source, unit,Bed,Channel,Duration,TEXT,msh_ts,alarm_ts,msg_id,TimeStamp,filename,DateInserted ,alarmended 
--FROM  HelixReportStage.[dbo].HadoopRawAlarmsStage where rank in (1,39) 
--)
MERGE  helixreportstage.dbo.HadoopActiveAlarms AS Target
--MERGE  @HadoopActiveAlarms AS Target
USING (SELECT Rank,AuditId, source, unit,Bed,Channel,isnull(Duration,0) Duration,TEXT,msh_ts,alarm_ts,msg_id,TimeStamp,filename,DateInserted , alarmended
---FROM  cte1 WHERE LoopId = @counter ) AS Source
FROM  HelixReportStage.[dbo].HadoopRawAlarmsStage WHERE LoopId = @counter ) AS Source
ON (Target.RANK = Source.rank and source.alarmended = TARGET.alarmended )
WHEN MATCHED THEN
    UPDATE SET Target.Duration =  
	case	when isnull(Source.Duration,0) > 15 then isnull(Target.Duration,0) + 5 when source.Duration = 0 and source.filename <> @filename  then isnull(Target.Duration,0)  
			Else  isnull(Target.Duration,0) + isnull(Source.Duration,0)
			End,
	AlarmEnded = Case When Source.Duration > 15  or (isnull(Source.Duration,0) =0 and source.filename <> @filename) then 1 Else target.AlarmEnded End,
	EventCount = EventCount + 1
   
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Rank,AuditId, source, unit,Bed,Channel,Duration,TEXT,msh_ts,alarm_ts,msg_id,TimeStamp,filename,DateInserted ,alarmended,EventCount)
	--VALUES (Rank,source.AuditId, source.source, source.unit,	source.Bed,source.Channel, isnull(source.duration ,0)
	--,source.TEXT,source.msh_ts,source.alarm_ts,
	--source.msg_id,source.TimeStamp,	source.filename,source.DateInserted ,
	-- Source.AlarmEnded ,1);
    VALUES (Rank,source.AuditId, source.source, source.unit,	source.Bed,source.Channel,case when source.duration > 15 then 5 else source.duration end
	,source.TEXT,source.msh_ts,source.alarm_ts,
	source.msg_id,source.TimeStamp,	source.filename,source.DateInserted ,
	case when Source.Duration > 15  then 1 Else Source.AlarmEnded End,1);
NextRecord:
    SET @counter = @counter + 1
END

SELECT @EndTime=GETDATE()   
SELECT DATEDIFF(ms,@StartTime,@EndTime) AS [Duration in milliseconds]   

;with cte4 as 
(Select rank from HelixReportStage.[dbo].HadoopRawAlarmsStage  where  datediff(s,timestamp,(Select max(timestamp) from HelixReportStage.[dbo].HadoopRawAlarmsStage)) > 15
group by rank having count(*)=1)


Update helixreportstage.dbo.HadoopActiveAlarms  set Duration = 5, AlarmEnded = 1
from  helixreportstage.dbo.HadoopActiveAlarms  a join cte4 b on a.rank = b.rank


--Insert New AlarmTexts to ReportCategory Table
INSERT INTO [HelixReportStage].[dbo].[HadoopAlarmCategories] ([AlarmText],[AlarmCategory] ,[ReportCategory])
Select distinct text,'Unknown','Unknown' from [HelixReportStage].[dbo].HadoopRawAlarmsStage a 
where  not exists
(select * from [HelixReportStage].[dbo].[HadoopAlarmCategories] where AlarmText = a.text)

--Insert new bed labels into bed label dimension

INSERT INTO [HelixReportStage].[dbo].[HadoopAlarmBedLabelXRef]  ([AlarmUnit] ,[AlarmBedLabel],[ClarityBedLabel],[ClarityRoomId],[ClarityRoomName],[ClarityDepartmentId],[ClarityDepartmentName])

Select distinct unit, bed,'Unknown','Unknown','Unknown','Unknown','Unknown'  from [HelixReportStage].[dbo].HadoopRawAlarmsStage a where not exists
(select * from [HelixReportStage].[dbo].HadoopAlarmBedLabelXRef where AlarmBedLabel = a.bed and Alarmunit = a.unit)





--Populate fact table
INSERT INTO [HelixReport].Dbo.[HadoopAlarmEvents]
           (AlarmKey
		   ,[AlarmSource]
		   ,[AlarmDateTime]
           ,[AlarmUnit]
           ,[AlarmBedLabel]
           ,[AlarmChannel]
           ,[AlarmDescription]
           ,[AlarmDurationSeconds]
           ,[AlarmCategory]
           ,[ReportCategory]
           ,[PatMrnId]
           ,[PatEncCSNId]
           ,[DepartmentId]
           ,[DepartmentName]
           ,[ClarityRoomId]
           ,[ClarityRoomName]
           ,[ClarityRoomNumber]
           ,[ClarityBedId]
           ,[ClarityBedLabel]
           ,[ClarityBedTimeStart]
           ,[ClarityBedTimeEnd]
           ,[ClarityEventType]
           ,[ClarityEventId]
           ,[Date_Inserted]
           ,[HadoopFileName]
           ,[AlarmMsgId])
     
 SELECT a.AuditId
       ,a.[source] AlarmSource
	  ,a.[timestamp] AlarmDateTime
      ,a.[unit] AlarmUnit
      ,a.[bed] AlarmBedLabel 
      ,a.[channel] AlarmChannel
      ,a.[text] AlarmDescription
      ,a.[DURATION] AlarmDurationSeconds
	  ,b.[AlarmCategory]
      ,b.[ReportCategory]
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
 	  ,d.nextvalue ClarityBedTimeEnd
      ,d.[EventTypeName] ClarityEventType
      ,d.[EVENT_ID] ClarityEventId
      ,d.[Date_Inserted]
	  ,a.[filename] HadoopFileName
	  ,a.[msg_id] AlarmMsgId 

from   helixreportstage.dbo.HadoopActiveAlarms a  
inner join [HelixReportStage].[dbo].HadoopAlarmCategories b on a.text = b.alarmtext
inner join [HelixReportStage].[dbo].HadoopAlarmBedLabelXRef c on a.unit = c.AlarmUnit and a.bed = c.AlarmBedLabel
left outer join 
(select * from HelixReport.dbo.Hadoop_BedTransfers_Pivoted   
	where   transfertime >=(Select DateAdd(month,-1,min(TIMESTAMP) )
	from [HelixReportStage].[dbo].HadoopRawAlarmsStage) 
	) d
on
	a.TIMESTAMP >= d.transfertime and a.timestamp < d.nextvalue and c.ClaritybedLabel = d.bed_label  
where AlarmEnded = 1

Set Statistics Time Off
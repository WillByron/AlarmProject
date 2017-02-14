--Clear previous runs alarms that ended 
--Delete From  HelixReportStage.[dbo].[HadoopRawAlarmsStage] --Do a delete so we can keep identity seed so that records inserted to final fact table are unique
--Delete From  helixreportstage.dbo.HadoopActiveAlarms where AlarmEnded = 1
--Below  line only for first load

truncate table HelixReport.[dbo].HadoopAlarmEvents
truncate table helixreportstage.dbo.HadoopActiveAlarms
truncate table  HelixReportStage.[dbo].[HadoopRawAlarmsStage]
set nocount on
Declare	@LastFilename       varchar(255)
SELECT @LastFilename = max(filename) FROM HelixReportStage.[dbo].HadoopRawAlarmsStage 

INSERT INTO HelixReportStage.[dbo].HadoopRawAlarmsStage
           (
		    LoopId
			,[Rank]
		   ,[source]
           ,[unit]
           ,[bed]
           ,[channel]
		   ,Duration
		   ,NextDuration
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
		,NextDuration = LEAD(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text 
		Order by dateadd(s, cast([alarm_ts] as int), '19700101'))
	  ,[text]
	  ,[msh_ts]
      ,[alarm_ts]
      ,[msg_id]
      ,dateadd(s, cast([alarm_ts] as int), '19700101')
      ,[filename]
	  ,0
FROM HelixReportStage.[dbo].[HadoopRawAlarms] 
where Day(dateadd(s, cast([alarm_ts] as int), '19700101')) in (29)
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

-- First lets loop through the alarms that we know stop before the last file becaue there is o next record that is in the last file



















-- Now lets loop through alarms that we know may still be active because they either started in the last file or Start before the last file and move into the last file

DECLARE @StartTime datetime,@EndTime datetime   
SELECT @StartTime=GETDATE() 

DECLARE
    @counter			INT =0,
    @max				INT = 0,
	@min				INT = 0

declare 
	   @AuditId bigint
      ,@LoopId bigint
      ,@Rank int
      ,@source varchar(50)
      ,@unit varchar(50)
      ,@bed varchar(50)
      ,@channel varchar(50)
      ,@Duration int
      ,@text varchar(50)
      ,@msh_ts varchar(50)
      ,@alarm_ts varchar(50)
      ,@msg_id varchar(50)
      ,@TIMESTAMP DateTime
      ,@filename varchar(50)
      ,@DateInserted DateTime
      ,@AlarmEnded smallint

declare Records cursor for


SELECT [AuditId]

      ,[Rank]
      ,[source]
      ,[unit]
      ,[bed]
      ,[channel]
      ,[Duration]
      ,[text]
      ,[msh_ts]
      ,[alarm_ts]
      ,[msg_id]
      ,[TIMESTAMP]
      ,[filename]
      ,[DateInserted]
      ,[AlarmEnded]
  FROM [dbo].[HadoopRawAlarmsStage] order by loopid

  open Records
     fetch next from Records into @AuditId,@Rank, @source ,@unit ,@bed ,@channel ,@Duration ,@text ,@msh_ts ,@alarm_ts ,@msg_id ,@TIMESTAMP ,@filename ,@DateInserted ,@AlarmEnded
while @@fetch_status = 0
BEGIN


MERGE  helixreportstage.dbo.HadoopActiveAlarms AS Target
--MERGE  @HadoopActiveAlarms AS Target
USING (SELECT auditid=@AuditId,rank=@Rank ,source=@source ,Unit=@unit ,Bed=@bed ,Channel=@channel ,Duration=isnull(@Duration,0) ,Text=@text ,msh_ts=@msh_ts 
,alarm_ts=@alarm_ts ,msg_id=@msg_id ,TIMESTAMP=@TIMESTAMP ,filename=@filename ,DateInserted=@DateInserted ,AlarmEnded=@AlarmEnded) AS Source
ON (Target.RANK = Source.rank and source.alarmended = TARGET.alarmended )
WHEN MATCHED THEN
    UPDATE SET Target.Duration =  
	case	when isnull(Source.Duration,0) > 15 then isnull(Target.Duration,0) + 5 when source.Duration = 0 and source.filename <> @LastFilename  then isnull(Target.Duration,0)  
			Else  isnull(Target.Duration,0) + isnull(Source.Duration,0)
			End,
	AlarmEnded = Case When Source.Duration > 15  or (source.Duration = 0 and source.filename <> @LastFilename) then 1 Else target.AlarmEnded End,
	EventCount = EventCount + 1
   
WHEN NOT MATCHED BY TARGET THEN
    INSERT (AuditId,Rank, source, unit,Bed,Channel,Duration,TEXT,msh_ts,alarm_ts,msg_id,TimeStamp,filename,DateInserted ,alarmended,EventCount)

    VALUES (source.AuditId, Source.Rank,source.source, source.unit,	source.Bed,source.Channel,case when source.duration > 15 then 5 else source.duration end
	,source.TEXT,source.msh_ts,source.alarm_ts,
	source.msg_id,source.TimeStamp,	source.filename,source.DateInserted ,
	case when Source.Duration > 15  then 1 Else Source.AlarmEnded End,1);
   fetch next from Records into @AuditId,@Rank, @source ,@unit ,@bed ,@channel ,@Duration ,@text ,@msh_ts ,@alarm_ts ,@msg_id ,@TIMESTAMP ,@filename ,@DateInserted ,@AlarmEnded

END
close Records 
deallocate Records

SELECT @EndTime=GETDATE()   
SELECT DATEDIFF(s,@StartTime,@EndTime) AS [Duration in seconds]   

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
           ,[AlarmMsgId]
		   ,EventCount)
     
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
	  ,a.eventCount
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


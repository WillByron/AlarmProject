-- Clear stage Tables
Delete From  HelixReportStage.[dbo].[HadoopRawAlarmsStage2] --Do a delete so we can keep identity seed so that records inserted to final fact table are unique
Truncate Table  HelixReportStage.[dbo].[HadoopRawAlarmsStage3] 

--Below two lines only for first load
--truncate table HelixReport.[dbo].HadoopAlarmEvents
--truncate table HadoopRawAlarmsUnion

--Pivot Timestamps so we can calculate Duration from row to row. Union with previous runs active alarms
INSERT INTO HelixReportStage.[dbo].[HadoopRawAlarmsStage2]
           ([source],[unit],[bed],[channel] ,[text],[msh_ts],[alarm_ts],[msg_id],[helix_rcv_ts],[tz_offset],[TIMESTAMP],[NextValue],[filename])

SELECT [source],[unit],[bed],[channel],[text],[msh_ts],[alarm_ts],[msg_id],[helix_rcv_ts],[tz_offset]
	 
	  ,dateadd(s, cast([alarm_ts] as int), '19700101') TIMESTAMP
      ,LEAD(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text Order by  Source, Unit, Bed, Channel,[Text],dateadd(s, cast([alarm_ts] as int), '19700101')) NextValue
      ,[filename]
 from (	

 Select [msh_ts],[alarm_ts],[source],[unit],[bed],[channel],[text],[msg_id],[helix_rcv_ts],[tz_offset],[filename] from 
 HelixReportStage.[dbo].HadoopRawAlarms
 union All 
 select [msh_ts],[alarm_ts],[source],[unit],[bed],[channel],[text],[msg_id],[helix_rcv_ts],[tz_offset],[filename] 
 from HelixReportStage.[dbo].HadoopRawAlarmsUnion)a


--End the alarm for the last alarm in the file unless it is the last file. For the last file in this batch, When the next file comes in we can possibly close without a random 300 sec update

Update HelixReportStage.[dbo].[HadoopRawAlarmsStage2] set NextValue = DateAdd(s,0,timestamp),AlarmEnded = 1 where nextvalue is null and substring(filename,33,13) <> 
 (Select max(substring(filename,33,13) ) from 
 HelixReportStage.[dbo].[HadoopRawAlarmsStage2])

--Update Duration between raw alarm records based on Source, unit, bed, channel and text
Update [HelixReportStage].[dbo].HadoopRawAlarmsStage2 set  Duration = datediff(s,Timestamp,NextValue) where nextvalue is not null 
Update [HelixReportStage].[dbo].HadoopRawAlarmsStage2 set Duration = 5, AlarmEnded = 1 where Duration > 15
--Insert New AlarmTexts to ReportCategory Table
INSERT INTO [HelixReportStage].[dbo].[HadoopAlarmCategories] ([AlarmText],[AlarmCategory] ,[ReportCategory])
Select distinct text,'Unknown','Unknown' from [HelixReportStage].[dbo].HadoopRawAlarmsStage2 a 
where  not exists
(select * from [HelixReportStage].[dbo].[HadoopAlarmCategories] where AlarmText = a.text)

--Insert new bed labels into bed label dimension

INSERT INTO [HelixReportStage].[dbo].[HadoopAlarmBedLabelXRef]  ([AlarmUnit] ,[AlarmBedLabel],[ClarityBedLabel],[ClarityRoomId],[ClarityRoomName],[ClarityDepartmentId],[ClarityDepartmentName])

Select distinct unit, bed,'Unknown','Unknown','Unknown','Unknown','Unknown'  from [HelixReportStage].[dbo].HadoopRawAlarmsStage2 a where not exists
(select * from [HelixReportStage].[dbo].HadoopAlarmBedLabelXRef where AlarmBedLabel = a.bed and Alarmunit = a.unit)
--Get Clarity Bed Transfer records

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
--  FROM HelixReport.[dbo].[Hadoop_BedTransfers] where [TransferTime] > (select dateadd(d,-1,min([timestamp])) from [HelixReportStage].[dbo].HadoopRawAlarmsStage2 )
--GO

--reduce records based on grouping logic
--Accumulate duration seconds until next row is greater than 60 secinds way. Add those seconds and  insert record

Truncate Table [HelixReportStage].[dbo].[HadoopRawAlarmsUnion]


-- capture entire group of active alarms from the last file and stage for the next run. (source,Unit, Bed, Channel,Text, Alarm_ts) of alarms in the last file. We need to stage so that we can wait for next files processing
;With CTE as (
SELECT 
      
      [source]
      ,[unit]
      ,[bed]
      ,[channel]
      ,[text]
 
	
  FROM [HelixReportStage].[dbo].[HadoopRawAlarmsStage2] 

	where nextvalue is null
	 group by source ,unit,bed, channel,text 
  ),
 cte2 as 
 (Select Max(AuditId)Auditid, a.[source],a.[unit],a.[bed],a.[channel],a.[text] from [HelixReportStage].[dbo].[HadoopRawAlarmsStage2] a join cte b
 on a.source = b.source and a.unit = b.unit and a.bed = b.bed and a.channel = b.channel and a.text = b.text
 where alarmended = 1
 group by a.source ,a.unit,a.bed, a.channel,a.text 
 
 ),
 cte3 as (
 select min(a.auditid)-1 Auditid,a.[source],a.[unit], a.[bed],a.[channel],a.[text] from [HelixReportStage].[dbo].[HadoopRawAlarmsStage2]  a join cte b 
 on   a.source = b.source and a.unit = b.unit and a.bed = b.bed and a.channel = b.channel and a.text = b.text 
 left outer join cte2 c 
 on b.source = c.source and b.unit = c.unit and b.bed = c.bed and b.channel = c.channel and b.text = c.text 
 where c.source is null
  group by a.source ,a.unit,a.bed, a.channel,a.text )

  INSERT INTO [HelixReportStage].[dbo].[HadoopRawAlarmsUnion]
           (AuditId,
		   [msh_ts]
           ,[alarm_ts]
           ,[source]
           ,[unit]
           ,[bed]
           ,[channel]
           ,[text]
           ,[msg_id]
           ,[helix_rcv_ts]
           ,[tz_offset]
           ,[filename])

 SELECT a.AuditId, 
       a.[msh_ts]
      ,a.[alarm_ts]
      ,a.[source]
      ,a.[unit]
      ,a.[bed]
      ,a.[channel]
      ,a.[text]
    
      ,a.[msg_id]
      ,a.[helix_rcv_ts]
      ,a.[tz_offset]
      ,a.[filename] 
from [HelixReportStage].[dbo].[HadoopRawAlarmsStage2]  a join cte3 b 
on a.source = b.source and a.unit = b.unit and a.bed = b.bed and a.channel = b.channel and a.text = b.text
where a.auditid > b.auditid
order by a.source ,a.unit,a.bed, a.channel,a.text ,dateadd(s, cast([alarm_ts] as int), '19700101')

--Delete alarms that are still active and did not end by the end of the last file of todays processing

Delete from [HelixReportStage].[dbo].[HadoopRawAlarmsStage2] 
from [HelixReportStage].[dbo].[HadoopRawAlarmsUnion] a join [HelixReportStage].[dbo].[HadoopRawAlarmsStage2] b
on a.auditid = b.auditid



DECLARE
    @counter			INT ,
    @max				INT = 0,
	@min				INT = 0,
	@rank				INT,
	@NextDurationSum	INT,
	@Duration			INT,
	@TotalDuration		INT = 0,
	@Id					INT,
	@AlarmEnded			Bit,
	@AuditId			BigInt
	
	


IF OBJECT_ID('tempdb..#MyTable', 'U') IS NOT NULL
 DROP TABLE #MyTable;



;with cte as (

SELECT  
	   
	   AuditId
	   ,DENSE_RANK() over ( order by source, unit, bed, channel,text) as rank
      
      ,[source]
      ,[unit]
      ,[bed]
      ,[channel]
      ,[DURATION]
      ,[text]
      ,[msh_ts]
      ,[alarm_ts]
      ,[msg_id]
      ,[helix_rcv_ts]
      ,[tz_offset]
      ,[TIMESTAMP]
      ,[NextValue]
      ,[filename]
      ,[DateInserted]
	  ,AlarmEnded
      
FROM [HelixReportStage].[dbo].[HadoopRawAlarmsStage2] where nextvalue is not null)

-- Declare a variable of type TABLE. It will be used as a temporary table.
-- Insert your required data in the variable of type TABLE

SELECT 
		ROW_NUMBER() OVER(ORDER BY Auditid ASC)   as ID
      ,AuditId 
       ,[source]
      ,[unit]
      ,[bed]
      ,[channel]
      ,[DURATION]
      ,[text]
      ,[msh_ts]
      ,[alarm_ts]
      ,[msg_id]
      ,[helix_rcv_ts]
      ,[tz_offset]

      ,[TIMESTAMP]
      ,[NextValue]
      ,[filename]
      ,[DateInserted]
      ,AlarmEnded
	  Into #MyTable
FROM cte order by rank, timestamp
--select alarmended,* from #mytable 
--where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N303' and channel = '286' and text = 'NBPm   131 >110'	order by rank,timestamp

CREATE CLUSTERED INDEX IDX_ID ON #MyTable(ID)
-- Initialize the @max variable. We'll use thie variable in the next WHILE loop.
SELECT @counter = Min(id) ,@max = Max(ID) FROM #MyTable

-- Loop 

WHILE @counter <= @max
BEGIN

    SELECT	 @AuditId= AuditId,@NextDurationSum =NextDurationSum, @AlarmEnded = AlarmEnded  , @DURATION = Duration
    FROM #MyTable
    WHERE Id = @counter

	if @NextDurationSum is null or @AlarmEnded = 1 
		Begin
		   set @TotalDURATION = @TotalDURATION + @DURATION 
		    Goto InsertRecord
	    End 
		
	If @NextDurationSum is not null and @DURATION < 15
	Begin
		set @TotalDURATION = @TotalDURATION + @DURATION 
		Goto NextRecord
	End 
	InsertRecord:
	Begin

	INSERT INTO HelixReportStage.[dbo].HadoopRawAlarmsStage3
    		SELECT  AuditId
			  ,[source]
			  ,[unit]
			  ,[bed]
			  ,[channel]
			  ,[DURATION] = @TotalDURATION
			  ,[text]
			  ,[msh_ts]
			  ,[alarm_ts]
			  ,[msg_id]
			  ,[helix_rcv_ts]
			  ,[tz_offset]
			  ,[TIMESTAMP]
			  ,[NextValue]
			  ,[filename]
			  ,[DateInserted]
		  FROM #MyTable Where Id = @counter
	 set @TotalDURATION = 0
	End
	NextRecord:
    SET @counter = @counter + 1

END
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

from   [HelixReportStage].[dbo].HadoopRawAlarmsStage3 a  
inner join [HelixReportStage].[dbo].HadoopAlarmCategories b on a.text = b.alarmtext
inner join [HelixReportStage].[dbo].HadoopAlarmBedLabelXRef c on a.unit = c.AlarmUnit and a.bed = c.AlarmBedLabel
left outer join 
(select * from HelixReport.dbo.Hadoop_BedTransfers_Pivoted   
	where   transfertime >=(Select DateAdd(month,-1,min(TIMESTAMP) )
	from [HelixReportStage].[dbo].HadoopRawAlarmsStage3) 
	and nextvalue is not null) d
on
	a.TIMESTAMP >= d.transfertime and a.timestamp < d.nextvalue and c.ClaritybedLabel = d.bed_label  






	
--Clear previous runs alarms that ended 
--Delete From  HelixReportStage.[dbo].[HadoopRawAlarmsStage] --Do a delete so we can keep identity seed so that records inserted to final fact table are unique
--Delete From  helixreportstage.dbo.HadoopActiveAlarms where AlarmEnded = 1
--Below  line only for first load

truncate table HelixReport.[dbo].HadoopAlarmEvents
truncate table helixreportstage.dbo.HadoopActiveAlarms
truncate table  HelixReportStage.[dbo].[HadoopRawAlarmsStage]

set nocount on


INSERT INTO HelixReportStage.[dbo].HadoopRawAlarmsStage
           (
		    
		    [Rank]
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
	   DENSE_RANK() over ( order by source, unit, bed, channel,text) as rank
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
DECLARE    @counter			INT 
      ,@max				INT = 0
	  ,@min				INT = 0
      ,@AuditId bigint
      ,@LoopId bigint
      ,@Rank int
      ,@source varchar(50)
      ,@unit varchar(50)
      ,@bed varchar(50)
      ,@channel varchar(50)
      ,@Duration int
	  ,@PreviousDuration int
	  ,@NextDuration int
	  ,@TotalDuration int=0
      ,@text varchar(50)
      ,@msh_ts varchar(50)
      ,@alarm_ts varchar(50)
      ,@msg_id varchar(50)
      ,@TIMESTAMP DateTime
      ,@filename varchar(50)
      ,@DateInserted DateTime
      ,@AlarmEnded smallint
	  ,@EventCount int = 0
	  ,@FirstTimeStamp DateTime
	  ,@i smallint = 1


While @i < 2
Begin
Set @Counter = 0
Set @max = 0

		IF OBJECT_ID('tempdb..#MyTable', 'U') IS NOT NULL
		DROP TABLE #MyTable;

		If @i = 1 
		Begin

		SELECT [AuditId]
				  ,ROW_NUMBER() OVER(order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101'))  Loopid
				  ,a.[Rank]
				  ,[source]
				  ,[unit]
				  ,[bed]
				  ,[channel]
				  ,[Duration]
				  ,lag(Duration)OVER (partition BY Rank  Order by timestamp) PreviousDuration
				  ,lead(Duration)OVER (partition BY Rank  Order by timestamp) NextDuration
				  ,[text]
				  ,[msh_ts]
				  ,[alarm_ts]
				  ,[msg_id]
				  ,[TIMESTAMP]
				  ,[filename]
				  ,[DateInserted]
				  ,[AlarmEnded]
				  , EventCount=0
					into #MyTable
				  FROM HelixReportStage.[dbo].HadoopRawAlarmsStage a 
				left outer join (select distinct rank FROM HelixReportStage.[dbo].HadoopRawAlarmsStage where filename   in  (select max(filename) FROM HelixReportStage.[dbo].HadoopRawAlarmsStage))b
				on a.rank = b.rank where b.rank is not null order by a.rank,filename 

				CREATE Unique CLUSTERED INDEX IDX_ID ON #MyTable(Loopid)
				SELECT @counter = Min(Loopid) ,@max = Max(Loopid) FROM #MyTable
		End



		If @i=2
		
		IF OBJECT_ID('tempdb..#MyTable', 'U') IS NOT NULL
		DROP TABLE #MyTable2;

		Begin
		
			SELECT [AuditId]
				  ,ROW_NUMBER() OVER(order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101'))  Loopid
				  ,[Rank]
				  ,[source]
				  ,[unit]
				  ,[bed]
				  ,[channel]
				  ,[Duration]
				  ,lag(Duration)OVER (partition BY Rank  Order by timestamp) PreviousDuration
				  ,lead(Duration)OVER (partition BY Rank  Order by timestamp) NextDuration
				  ,[text]
				  ,[msh_ts]
				  ,[alarm_ts]
				  ,[msg_id]
				  ,[TIMESTAMP]
				  ,[filename]
				  ,[DateInserted]
				  ,[AlarmEnded]
				   ,EventCount
				  into #MyTable2
			from (	

				SELECT [AuditId],[Rank] ,[source],[unit],[bed],[channel],[Duration],[text],[msh_ts],[alarm_ts],[msg_id],[TimeStamp],[filename],[DateInserted],[AlarmEnded],[EventCount]
				FROM HelixReportStage.[dbo].[HadoopActiveAlarms] 
				where alarmended = 0 --active

				union All 

				(select AuditId,a.[Rank],[source],[unit],[bed],[channel],Duration,[text],[msh_ts],[alarm_ts],[msg_id],[TimeStamp],[filename],[DateInserted],[AlarmEnded],0
				FROM HelixReportStage.[dbo].HadoopRawAlarmsStage a 
				left outer join (select distinct rank FROM HelixReportStage.[dbo].HadoopRawAlarmsStage where filename    in  (select max(filename) FROM HelixReportStage.[dbo].HadoopRawAlarmsStage))b
				on a.rank = b.rank where b.rank is null) ) UnionAll

				CREATE Unique CLUSTERED INDEX IDX_ID ON #MyTable2(Loopid)
				SELECT @counter = Min(Loopid) ,@max = Max(Loopid) FROM #MyTable2
		End

		--select * from #MYTable where NextDuration = 0
		--select count(*) from #MYTable order by loopid
		-- Initialize the @max variable. We'll use thie variable in the next WHILE loop.




					WHILE @counter <= @max
					BEGIN
							If @i = 1
							Begin
    							SELECT 
									@AuditId =Auditid,@Rank=Rank, @source=Source ,@unit=Unit ,@bed=Bed ,@channel=Channel ,@Duration=Duration ,@text=Text ,
									@msh_ts=msh_ts ,@alarm_ts=alarm_ts ,@msg_id=msg_id ,@TIMESTAMP=TIMESTAMP ,@filename=filename ,@DateInserted=DateInserted ,@AlarmEnded=AlarmEnded,@EventCount=EventCount,
									@PreviousDuration = isnull(PreviousDuration,-1),@Duration=isnull(Duration,-1), @NextDuration = isnull(NextDuration,-1), @TimeStamp = TimeStamp
								FROM #MyTable
								WHERE Loopid = @counter
							End

							If @i = 2
							Begin
    							SELECT 
									@AuditId =Auditid,@Rank=Rank, @source=Source ,@unit=Unit ,@bed=Bed ,@channel=Channel ,@Duration=Duration ,@text=Text ,
									@msh_ts=msh_ts ,@alarm_ts=alarm_ts ,@msg_id=msg_id ,@TIMESTAMP=TIMESTAMP ,@filename=filename ,@DateInserted=DateInserted ,@AlarmEnded=AlarmEnded,@EventCount=EventCount,
									@PreviousDuration = isnull(PreviousDuration,-1),@Duration=isnull(Duration,-1), @NextDuration = isnull(NextDuration,-1), @TimeStamp = TimeStamp
								FROM #MyTable2
								WHERE Loopid = @counter
							End

								Set @EventCount = @EventCount + 1
		
								If @FirstTimeStamp is null 
									Begin
											Set @FirstTimeStamp = @TimeStamp
									End

								If (@DURATION <= 15 and @Duration >= 0 )
									Begin
						
											if @NextDuration = -1
												Begin		
													Set @EventCount = @EventCount + 1
													Goto InsertAlarmThatHasEnded
												End
											Else
												--Set @EventCount = @EventCount + 1
												set @TotalDURATION = @TotalDURATION + @DURATION 
												Goto NextRecord
					

									End
								If  (@Duration > 15 and @PreviousDuration = -1)  or (@Duration > 15 and @PreviousDuration > 15 )--First Alarm and is over 15 seconds so should be its own alarm event or 
																																--consecutive events are > 15 so counted as single events with its actual duration
									Begin

											if @NextDuration = -1
											--Begin
												Set @EventCount = @EventCount + 1
											--End 
											Goto InsertAlarmThatHasEnded
									End	

		
								If  (@Duration > 15 and @PreviousDuration >= 0 and @PreviousDuration <= 15) --if an alarm is > 15 end it and start a new alarm
									Begin
											if @NextDuration = -1
											--Begin
												set @EventCount = @EventCount + 1
											--End 
											Set @Duration = 5
						
											Goto InsertAlarmThatHasEnded
									End	

								if	(@Duration = -1 and @NextDuration   = -1  and @PreviousDuration = -1)   --Only event in Alarm Group efault it to 5 seconds
								Begin
											Set @Duration = 5
						
											Goto InsertAlarmThatHasEnded
								End

								if	(@Duration = -1 and @NextDuration   = -1 and @PreviousDuration <> -1)  --This was the last record in an alarm group, we already added the event to event count ... skip it
									Begin
											Set @FirstTimeStamp = null
											Set @EventCount =0
											set @TotalDURATION = 0
									End
											Goto nextrecord
					

								InsertAlarmThatHasEnded:

								Begin
									--Set @EventCount = @EventCount + 1
									set @AlarmEnded = 1
									set @TotalDURATION = @TotalDURATION + @Duration 
										INSERT INTO HelixReportStage.[dbo].HadoopEndedlarms
    											Values  (@AuditId,@Rank, @source ,@unit ,@bed ,@channel ,@TotalDuration ,@text ,@msh_ts ,@alarm_ts ,
												@msg_id ,@FirstTimeStamp ,@filename ,@DateInserted ,@AlarmEnded,@EventCount)
							
										 set @TotalDURATION = 0
										 Set @EventCount =0
										 Set @FirstTimeStamp = NUll
										 GoTo NextRecord
								End

								InsertAlarmThatIsStillActive:

								Begin
									--Set @EventCount = @EventCount + 1
									set @AlarmEnded = 0
									set @TotalDURATION = @TotalDURATION + @Duration 
										INSERT INTO HelixReportStage.[dbo].HadoopActiveAlarms
    											Values  (@AuditId,@Rank, @source ,@unit ,@bed ,@channel ,@TotalDuration ,@text ,@msh_ts ,@alarm_ts ,
												@msg_id ,@FirstTimeStamp ,@filename ,@DateInserted ,@AlarmEnded,@EventCount)
							
										 set @TotalDURATION = 0
										 Set @EventCount =0
										 Set @FirstTimeStamp = NUll
										 GoTo NextRecord
								End




								NextRecord:
								SET @counter = @counter + 1

					END
Set @i = @i+1
End






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


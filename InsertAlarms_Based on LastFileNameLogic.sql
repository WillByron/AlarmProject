--Clear previous runs alarms that ended 

Declare @FirstLoad bit = 0

If @FirstLoad = 1
		Begin
			truncate table HelixReport.[dbo].HadoopAlarmEvents
			truncate table helixreportstage.dbo.HadoopActiveAlarms
			truncate table helixreportstage.dbo.HadoopEndedAlarms
			truncate table  HelixReportStage.[dbo].[HadoopRawAlarmsStage]
		End
Else
		Begin
			delete From  HelixReportStage.[dbo].[HadoopRawAlarmsStage] --Do a delete so we can keep identity seed so that records inserted to final fact table are unique
			truncate table    helixreportstage.dbo.HadoopEndedAlarms 
		End
--Below  line only for first load


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
	  ,0  --0 = new alarm  1 = active alarm 2 = ended alarm
FROM HelixReportStage.[dbo].[HadoopRawAlarms] 
 where  Day(dateadd(s, cast([alarm_ts] as int), '19700101')) in (27) -- load just one day when testing



order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')
GO


--Get Clarity Bed Transfer records

--truncate table HelixReport.[dbo].[Hadoop_BedTransfers_Pivoted]

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
      
      ,@DateInserted DateTime
      ,@AlarmEnded smallint
	  ,@EventCount int = 0
	  ,@EventCountCumulative int =0
	  ,@AlarmEventStartTime DateTime
	  ,@AlarmEventEndTime DateTime
	  ,@MaxFileName varchar(50)
	  ,@HadoopAlarmStartFileName varchar(50)
	  ,@HadoopAlarmEndFileName varchar(50)
	  ,@HadoopOrigFileName varchar(50)
	  ,@i smallint = 1
	  ,@ActiveAlarmDuration int
	  ,@FirstFilename varchar(50)
	  ,@FirstEventDate DateTime
	  --,@SingleEvent bit 

Set @Counter = 0
Set @max = 0



		Begin
		    IF OBJECT_ID('tempdb..#MyTable', 'U') IS NOT NULL
		     DROP TABLE #MyTable;

		
		
			SELECT [AuditId]
				  ,ROW_NUMBER() OVER(order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101'))  Loopid
				  ,[Rank]
				  ,[source]
				  ,[unit]
				  ,[bed]
				  ,[channel]
				  ,[Duration]
				  ,ActiveAlarmDuration
				  ,lag(Duration)OVER (partition BY UnionAll.Rank  Order by AlarmEventStartTime) PreviousDuration
				  ,lead(Duration)OVER (partition BY UnionAll.Rank  Order by AlarmEventStartTime) NextDuration
				  ,[text]
				  ,[msh_ts]
				  ,[alarm_ts]
				  ,[timestamp]=dateadd(s, cast([alarm_ts] as int), '19700101')
				  ,[msg_id]
				  ,AlarmEventStartTime
				 
				  ,HadoopAlarmStartFileName
				  ,HadoopOrigFileName
				  ,[DateInserted]
				  ,[AlarmEnded]
				  ,EventCount

				  into #MyTable
			from (	

				SELECT [AuditId],[Rank] ,[source],[unit],[bed],[channel],[Duration],ActiveAlarmDuration,[text],[msh_ts],[alarm_ts],[msg_id],
				AlarmEventStartTime,HadoopAlarmStartFileName,HadoopOrigFileName,[DateInserted],[AlarmEnded],[EventCount]
				FROM HelixReportStage.[dbo].[HadoopActiveAlarms] 
				

				union All 

				(select AuditId,[Rank],[source],[unit],[bed],[channel],Duration,ActiveAlarmDuration=0,[text],[msh_ts],[alarm_ts],[msg_id],[TimeStamp],'',[filename],[DateInserted],[AlarmEnded],0
				 FROM HelixReportStage.[dbo].HadoopRawAlarmsStage  ) ) UnionAll
				 --where  source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT12' and channel = '290' and text = 'Rem.AlarmDev.Malf.'
				 order by AlarmEventStartTime

				CREATE Unique CLUSTERED INDEX IDX_ID ON #MyTable(Loopid)
				SELECT @counter = Min(Loopid) ,@max = Max(Loopid),@MaxFileName =max(HadoopOrigFileName) FROM #MyTable
		End


		Truncate Table  HelixReportStage.[dbo].[HadoopActiveAlarms] 


		--select * from #MYTable where  source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT12' and channel = '290' and text = 'Rem.AlarmDev.Malf.'
		--select * from #MYTable order by loopid
		--select * from #MYTable order by loopid
		--SELECT  Min(Loopid) , Max(Loopid) FROM #MyTable
		-- Initialize the @max variable. We'll use thie variable in the next WHILE loop.




					WHILE @counter <= @max
					BEGIN
							
							Begin
    							SELECT 
									@AuditId =Auditid,@Rank=Rank, @source=Source ,@unit=Unit ,@bed=Bed ,@channel=Channel ,@Duration=Duration ,@ActiveAlarmDuration = ActiveAlarmDuration,@text=Text ,
									@msh_ts=msh_ts ,@alarm_ts=alarm_ts ,@msg_id=msg_id ,@AlarmEventStartTime= AlarmEventStartTime ,@HadoopAlarmStartFileName = HadoopAlarmStartFileName, @HadoopOrigFileName=HadoopOrigFileName ,@DateInserted=DateInserted ,@AlarmEnded=AlarmEnded,
									@PreviousDuration = isnull(PreviousDuration,-1),@Duration=isnull(Duration,-1), @NextDuration = isnull(NextDuration,-1), @TimeStamp = [timestamp],@EventCount = EventCount
								FROM #MyTable
								WHERE Loopid = @counter
							End


							If @AlarmEnded = 1 
								Begin
									Set @EventCountCumulative = @EventCountCumulative + @EventCount 
								End
							Else
								Begin
									Set @EventCountCumulative = @EventCountCumulative + @EventCount +1
								End


								If @FirstEventDate is null and @AlarmEnded =0--capture first time stamp of alarm group because that is the one that needs to be saved upon insert
									Begin
											Set @FirstEventDate = @TimeStamp
											
									End
								If @FirstEventDate is null and @AlarmEnded =1--capture first time stamp of alarm group because that is the one that needs to be saved upon insert
									Begin
											Set @FirstEventDate = @AlarmEventStartTime 
											
									End
								
								
								If @FirstFileNAme is null and @AlarmEnded = 0
									Begin
											Set @FirstFileName = @HadoopOrigFileName
											
									End
								If @FirstFileNAme is null and @AlarmEnded = 1
									Begin
											Set @FirstFileName = @HadoopAlarmStartFileName
											
									End
								If @AlarmEnded = 1 -- If an event was previously saved as an active alarm in HadoopActiveAlarms table, we need to capture the accumulated duration and no save it as an alarm
									Begin
											set @TotalDURATION = @ActiveAlarmDuration 
											Goto NextRecord
									End

								If (@DURATION <= 15 and @Duration >= 0 ) -- This is to capture the normal 5 second alarms and accumulate the duration until it comes to an end.
									Begin
						
											if @NextDuration = -1 and @MaxFileName <> @HadoopOrigFileName
												Begin	
														
													Set @EventCountCumulative = @EventCountCumulative + 1
													Goto InsertAlarmThatHasEnded
												End
											else if @NextDuration = -1 and @MaxFileName = @HadoopOrigFileName
											Begin
													Set @EventCountCumulative = @EventCountCumulative + 1
													Goto InsertAlarmThatIsStillActive
											End
											Else
												--Set @EventCount = @EventCount + 1
												set @TotalDURATION = @TotalDURATION + @DURATION 
												Goto NextRecord
					

									End
								If  (@Duration > 15 and @PreviousDuration = -1)  or (@Duration > 15 and @PreviousDuration > 15 )--First Alarm and is over 15 seconds so should be its own alarm event or 
																															--consecutive events are > 15 so counted as single events with its actual duration
									Begin
											--Set @SingleEvent =1	--If a row is over 15 seconds and so was pevious it is not the end of an alarm but an alarm in itself 
											if @NextDuration = -1
											--Begin
												Set @EventCountCumulative = @EventCountCumulative + 1
											--End 
											Goto InsertAlarmThatHasEnded
									End	

		
								If  (@Duration > 15 and @PreviousDuration >= 0 and @PreviousDuration <= 15 ) --if an alarm is > 15 end it and start a new alarm unless it was a saved active alarm
									Begin	
											--Set @SingleEvent =1																						--0 = new alarm  1 = active alarm 2 = ended alarm
											if @NextDuration = -1
											--Begin
												set @EventCountCumulative = @EventCountCumulative + 1
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
										 Set @TotalDURATION = 0
										 Set @EventCountCumulative =0
										 Set @AlarmEventStartTime = NUll
										 Set @AlarmEventEndTime = null 
										 Set @FirstEventDate= NUll
										 Set @FirstFileName = NUll
										 Set @HadoopAlarmStartFileName = ''
									End
											Goto nextrecord
					

InsertAlarmThatHasEnded:

								Begin
									--Set @EventCount = @EventCount + 1
									set @AlarmEnded = 2 --0 = new alarm  1 = active alarm 2 = ended alarm
									set @TotalDURATION = @TotalDURATION + @Duration 
									set @HadoopAlarmEndFileName = @HadoopOrigFileName
									--If @SingleEvent = 1 --If a row is over 15 seconds and so was pevious it is not the end of an alarm but an alarm in itself so need to calculate end time since dont have  next records time
											--Begin
												Set @AlarmEventEndTime = Dateadd(s,@TotalDURATION,@FirstEventDate )
											--End
									--Else
											--Begin
												--Set @AlarmEventEndTime = @TimeStamp
											--End
										
										
										INSERT INTO HelixReportStage.[dbo].HadoopEndedAlarms
    											Values  (@AuditId,@Rank, @source ,@unit ,@bed ,@channel ,@TotalDuration ,@text ,@msh_ts ,@alarm_ts ,
												@msg_id ,@FirstEventDate ,@AlarmEventEndTime,@FirstFileName,@HadoopAlarmEndFileName,@HadoopOrigFileName ,@DateInserted ,@AlarmEnded,@EventCountCumulative)
							
										 Set @TotalDURATION = 0
										 Set @EventCountCumulative =0
										 Set @AlarmEventStartTime = NUll
										 Set @FirstEventDate= NUll
										 --Set @SingleEvent= 0
										 Set @AlarmEventEndTime = null 
										 Set @FirstFileName = NUll
										 Set @HadoopAlarmStartFileName = ''
										 GoTo NextRecord
								End

InsertAlarmThatIsStillActive:

								Begin
									--Set @EventCount = @EventCount + 1
									set @AlarmEnded = 1 --0 = new alarm  1 = active alarm 2 = ended alarm
									set @TotalDURATION = @TotalDURATION + @Duration
									Set @ActiveAlarmDuration =  @TotalDURATION
										INSERT INTO HelixReportStage.[dbo].HadoopActiveAlarms
    											Values  (@AuditId,@Rank, @source ,@unit ,@bed ,@channel ,@TotalDuration ,@ActiveAlarmDuration ,@text ,@msh_ts ,@alarm_ts ,
												@msg_id ,@FirstEventDate  , @FirstFileName,'',@HadoopOrigFileName ,@DateInserted ,@AlarmEnded,@EventCountCumulative)
							
										 Set @TotalDURATION = 0
										 Set @EventCountCumulative =0
										 Set @AlarmEventStartTime = NUll
										 Set @AlarmEventEndTime = null 
										 Set @FirstEventDate= NUll
										 Set @FirstFileName = NUll
										 Set @HadoopAlarmStartFileName = ''

										 GoTo NextRecord
								End




								NextRecord:
								SET @counter = @counter + 1

					END







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
		   ,[AlarmEventStartTime]
           ,[AlarmEventEndTime]
		   ,AlarmUnit
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
           ,HadoopAlarmStartFileName
		   ,HadoopAlarmEndFileName
           ,[AlarmMsgId]
		   ,EventCount)
     
 SELECT a.AuditId
       ,a.[source] AlarmSource
	  ,a.[AlarmEventStartTime] 
	  ,a.[AlarmEventEndTime]
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
	  ,a.HadoopAlarmStartFileName 
	  ,a.HadoopAlarmEndFileName
	  ,a.[msg_id] AlarmMsgId 
	  ,a.eventCount
from   helixreportstage.dbo.HadoopEndedAlarms a  
inner join [HelixReportStage].[dbo].HadoopAlarmCategories b on a.text = b.alarmtext
inner join [HelixReportStage].[dbo].HadoopAlarmBedLabelXRef c on a.unit = c.AlarmUnit and a.bed = c.AlarmBedLabel
left outer join 
(select * from HelixReport.dbo.Hadoop_BedTransfers_Pivoted   
	where   transfertime >=(Select DateAdd(month,-1,min(TIMESTAMP) )
	from #MyTable) 
	) d
on
	a.[AlarmEventStartTime] >= d.transfertime and a.[AlarmEventStartTime] < d.nextvalue and c.ClaritybedLabel = d.bed_label  



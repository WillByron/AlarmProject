SELECT [source]
      ,[unit]
      ,[bed]
      ,[channel]
	  ,[text]
      ,[msh_ts]
      ,[alarm_ts]
      ,[msg_id]
      ,[helix_rcv_ts]
      ,[tz_offset]
	  ,LAG(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text Order by dateadd(s, cast([alarm_ts] as int), '19700101')) PreviousValue
	  ,dateadd(s, cast([alarm_ts] as int), '19700101') TIMESTAMP
      ,LEAD(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text Order by dateadd(s, cast([alarm_ts] as int), '19700101')) NextValue
      ,[filename]
 from (	Select [msh_ts],[alarm_ts],[source],[unit],[bed],[channel],[text],[msg_id],[helix_rcv_ts],[tz_offset],[filename] from  HelixReportStage.[dbo].HadoopRawAlarms) a

 order by Source, Unit, Bed, Channel,Text
 select * from     HelixReportstage.[dbo].HadoopRawAlarmsStage2  
 where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N303' and channel = '285' and text = 'xTachy 145>140'	
 order by source,unit,bed,channel,text,timestamp

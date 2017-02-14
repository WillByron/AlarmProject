select dateadd(s, cast([alarm_ts] as int), '19700101'),* from HelixReportstage.[dbo].HadoopRawAlarms
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N303' and channel = '288' and text = 'Missed Beat'
and Day(dateadd(s, cast([alarm_ts] as int), '19700101')) in (29)
order by dateadd(s, cast([alarm_ts] as int), '19700101')

SELECT auditid,rank,loopid, alarmended,dateadd(s, cast([alarm_ts] as int), '19700101')	,
Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
LEAD(dateadd(s, cast([alarm_ts] as int), '19700101'))OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101'))) ,[source],[unit],[bed],[channel],[text],[filename] 
FROM HelixReportstage.[dbo].HadoopRawAlarmsStage 
where source = 'HSRIXSERV5' and unit = 'V3S' and bed = 'V3S3007' and channel = '286' and text = '!! ECG Leads Off'
order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')

SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration ,[source],[unit],[bed],[channel],[text],timestamp,[filename] 
FROM HelixReportstage.[dbo].HadoopActiveAlarms 
where source = 'HSRIXSERV5' and unit = 'V3S' and bed = 'V3S3007' and channel = '286' and text = '!! ECG Leads Off'
order by source,unit,bed,channel,text,timestamp
select * from HelixReportstage.[dbo].HadoopRawAlarmsStage  order by text


select  rank FROM HelixReportStage.[dbo].HadoopRawAlarmsStage where filename  not in  (select max(filename) FROM HelixReportStage.[dbo].HadoopRawAlarmsStage)

select  A.*,b.rank  FROM HelixReportStage.[dbo].HadoopRawAlarmsStage a 
left outer join (select distinct rank FROM HelixReportStage.[dbo].HadoopRawAlarmsStage where filename   in  (select max(filename) FROM HelixReportStage.[dbo].HadoopRawAlarmsStage))b
on a.rank = b.rank where b.rank is not null order by a.rank,filename

select A.*,b.rank FROM HelixReportStage.[dbo].HadoopRawAlarmsStage a 
left outer join (select distinct rank FROM HelixReportStage.[dbo].HadoopRawAlarmsStage where filename    in  (select max(filename) FROM HelixReportStage.[dbo].HadoopRawAlarmsStage))b
on a.rank = b.rank where b.rank is null order by a.rank,filename

select 424614+91928

SELECT * FROM HelixReport.[dbo].HadoopAlarmevents
where alarmsource = 'HSRIXSERV5' and alarmunit = 'V3N' and alarmbedLabel = 'V3N303' and alarmchannel = '288' and AlarmDescription = 'Missed Beat'
order by alarmsource,alarmunit,alarmbedLabel,alarmchannel,AlarmDescription,AlarmDateTime

select sum(eventcount) from HelixReport.[dbo].HadoopAlarmevents where day(AlarmDateTime) = 29
select sum(eventcount) from  HelixReportstage.[dbo].HadoopActiveAlarms  516542
select count(*) from  HelixReportstage.[dbo].HadoopActiveAlarms 
select Count(*) from HelixReportstage.[dbo].HadoopRawAlarms where Day(dateadd(s, cast([alarm_ts] as int), '19700101')) in (29)
select Count(*) FROM HelixReportstage.[dbo].HadoopRawAlarmsStage where Day(dateadd(s, cast([alarm_ts] as int), '19700101')) in (29)
select sum(eventcount) from  HelixReportstage.[dbo].HadoopActiveAlarms
select * from  HelixReportstage.[dbo].HadoopActiveAlarms
select Day(dateadd(s, cast(AlarmDateTime as int), '19700101')),* from HelixReport.[dbo].HadoopAlarmevents order by eventcount desc
select Day(dateadd(s, cast(AlarmDateTime as int), '19700101')),* FROM HelixReportstage.[dbo].HadoopRawAlarmsStage
select Day(dateadd(s, cast(AlarmDateTime as int), '19700101')),* from  HelixReportstage.[dbo].HadoopActiveAlarms


select eventcount,alarmended,timestamp,* from HelixReportstage.[dbo].HadoopActiveAlarms where alarmended = 0

select filename from HelixReportstage.[dbo].HadoopRawAlarmsStage  where substring(filename,33,13) =  (Select max(substring(filename,33,13) ) from  HelixReportstage.[dbo].HadoopRawAlarmsStage )

;with cte4 as 
(Select rank from HelixReportStage.[dbo].HadoopRawAlarmsStage  where  datediff(s,timestamp,(Select max(timestamp) from HelixReportStage.[dbo].HadoopRawAlarmsStage)) < 15
group by rank having count(*)=1)

select * from HelixReportStage.[dbo].HadoopRawAlarmsStage  where rank = 1691
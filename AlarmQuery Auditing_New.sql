--Testing of differetn situations
--First Alarm > 15 so should be inserted

SELECT auditid,rank,loopid, alarmended,dateadd(s, cast([alarm_ts] as int), '19700101')	,
Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
LEAD(dateadd(s, cast([alarm_ts] as int), '19700101')) OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101'))) ,
PreviousDuration=lag(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
NextDuration=Lead(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
[source],[unit],[bed],[channel],[text],[filename] 
FROM HelixReportstage.[dbo].HadoopRawAlarmsStage 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N306' and channel = '285' and text = 'Apnea 0:20'
order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')


SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, [source],[unit],[bed],[channel],[text],AlarmEventStartTime,AlarmEventEndTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName
FROM HelixReportstage.[dbo].HadoopEndedAlarms 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N306' and channel = '285' and text = 'Apnea 0:20'
order by auditid
SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, activealarmduration,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName 
FROM HelixReportstage.[dbo].HadoopactiveAlarms 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N306' and channel = '285' and text = 'Apnea 0:20'
order by auditid
---

SELECT auditid,rank,loopid, alarmended,dateadd(s, cast([alarm_ts] as int), '19700101')	,
Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
LEAD(dateadd(s, cast([alarm_ts] as int), '19700101')) OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101'))) ,
PreviousDuration=lag(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
NextDuration=Lead(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
[source],[unit],[bed],[channel],[text],[filename] 
FROM HelixReportstage.[dbo].HadoopRawAlarmsStage 
where source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT12' and channel = '290' and text = 'Rem.AlarmDev.Malf.'
order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')

SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, activealarmduration,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName 
FROM HelixReportstage.[dbo].HadoopactiveAlarms 
where source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT12' and channel = '290' and text = 'Rem.AlarmDev.Malf.'
order by auditid

SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, [source],[unit],[bed],[channel],[text],AlarmEventStartTime,AlarmEventEndTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName 
FROM HelixReportstage.[dbo].HadoopEndedAlarms 
where source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT12' and channel = '290' and text = 'Rem.AlarmDev.Malf.'
order by auditid
SELECT Eventcount, alarmkey,AlarmDurationSeconds, [alarmsource],[alarmunit],AlarmBedLabel,AlarmChannel,AlarmDescription,AlarmEventStartTime,AlarmEventEndTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName 
FROM HelixReport.[dbo].HadoopAlarmEvents
where alarmsource = 'PICSERV05' and alarmunit = 'CTICU' and alarmbedlabel = 'CT12' and alarmchannel = '290' and AlarmDescription = 'Rem.AlarmDev.Malf.'
order by alarmkey

2017-01-25 20:31:00.000	/data/bmdi/signals/avro/2017/01/1485375241847.avro

select * into HelixReportstage.[dbo].HadoopactiveAlarms25 from HelixReportstage.[dbo].HadoopactiveAlarms 

SELECT auditid,rank,loopid, alarmended,dateadd(s, cast([alarm_ts] as int), '19700101')	,

Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
LEAD(dateadd(s, cast([alarm_ts] as int), '19700101')) OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101'))) ,
PreviousDuration=lag(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
NextDuration=Lead(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) 
,[source],[unit],[bed],[channel],[text],[filename] 
FROM HelixReportstage.[dbo].HadoopRawAlarmsStage 
where source = 'HSRIXSERV5' and unit = 'V3S' and bed = 'V3S3007' and channel = '286' and text = '!! ECG Leads Off'
order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')

SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration ,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,AlarmEventEndTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName 
FROM HelixReportstage.[dbo].HadoopEndedAlarms 
where source = 'HSRIXSERV5' and unit = 'V3S' and bed = 'V3S3007' and channel = '286' and text = '!! ECG Leads Off'
order by auditid
SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, activealarmduration,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName 
FROM HelixReportstage.[dbo].HadoopactiveAlarms 
where source = 'HSRIXSERV5' and unit = 'V3S' and bed = 'V3S3007' and channel = '286' and text = '!! ECG Leads Off'
order by auditid


---Starts with under 15 seconds many records and has an active alarm

SELECT auditid,rank,loopid, alarmended,dateadd(s, cast([alarm_ts] as int), '19700101')	,
Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
LEAD(dateadd(s, cast([alarm_ts] as int), '19700101')) OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101'))) ,
PreviousDuration=lag(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
NextDuration=Lead(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
[source],[unit],[bed],[channel],[text],[filename] 
FROM HelixReportstage.[dbo].HadoopRawAlarmsStage 
where source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT13' and channel = '290' and text = 'SpO2   No Sensor'
order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')

SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration ,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,AlarmEventEndTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName  
FROM HelixReportstage.[dbo].HadoopEndedAlarms 
where source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT13' and channel = '290' and text = 'SpO2   No Sensor'
order by auditid
SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, activealarmduration,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName  
FROM HelixReportstage.[dbo].HadoopactiveAlarms 
where source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT13' and channel = '290' and text = 'SpO2   No Sensor'
order by auditid

--Inserts into HadoopAlarmEnded and has just one event that becomes 5 seconds

SELECT auditid,rank,loopid, alarmended,dateadd(s, cast([alarm_ts] as int), '19700101')	,
Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
LEAD(dateadd(s, cast([alarm_ts] as int), '19700101')) OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101'))) ,
PreviousDuration=lag(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
NextDuration=Lead(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
[source],[unit],[bed],[channel],[text],[filename] 
FROM HelixReportstage.[dbo].HadoopRawAlarmsStage 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N305' and channel = '285' and text = 'Desat 58 < 85'
order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')

SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration ,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,AlarmEventEndTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName  
FROM HelixReportstage.[dbo].HadoopEndedAlarms 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N305' and channel = '285' and text = 'Desat 58 < 85'
order by auditid

SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, activealarmduration,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName  
FROM HelixReportstage.[dbo].HadoopactiveAlarms 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N305' and channel = '285' and text = 'Desat 58 < 85'
order by auditid

--Insert into HadoopAlarm Ended -- Starts with a long duration alarm that needs inserting

SELECT auditid,rank,loopid, alarmended,dateadd(s, cast([alarm_ts] as int), '19700101')	,
Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
LEAD(dateadd(s, cast([alarm_ts] as int), '19700101')) OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101'))) ,
PreviousDuration=lag(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
NextDuration=Lead(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
[source],[unit],[bed],[channel],[text],[filename] 
FROM HelixReportstage.[dbo].HadoopRawAlarmsStage 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N305' and channel = '291' and text = 'SpO2   Low Perf'
order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')

SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration ,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,AlarmEventEndTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName  
FROM HelixReportstage.[dbo].HadoopEndedAlarms 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N305' and channel = '291' and text = 'SpO2   Low Perf'
order by auditid
SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, activealarmduration,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName  
FROM HelixReportstage.[dbo].HadoopactiveAlarms 
where source = 'HSRIXSERV5' and unit = 'V3N' and bed = 'V3N305' and channel = '291' and text = 'SpO2   Low Perf'
order by auditid

--Insert into Alarm Ended and ActiveAlarm
SELECT auditid,rank,loopid, alarmended,dateadd(s, cast([alarm_ts] as int), '19700101')	,
Duration = datediff(s,dateadd(s, cast([alarm_ts] as int), '19700101'),
LEAD(dateadd(s, cast([alarm_ts] as int), '19700101')) OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101'))) ,
PreviousDuration=lag(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
NextDuration=Lead(Duration)OVER (partition BY Source, Unit, Bed, Channel,Text 
Order by dateadd(s, cast([alarm_ts] as int), '19700101')) ,
[source],[unit],[bed],[channel],[text],[filename] 
FROM (select 'Day25' ImportDay,* from HelixReportstage.[dbo].HadoopRawAlarmsStage25 union all  
select 'Day26'ImportDay,* from HelixReportstage.[dbo].HadoopRawAlarmsStage26 union all
select 'Day27'ImportDay,* from HelixReportstage.[dbo].HadoopRawAlarmsStage27) a
where source = 'PICSERV05' and unit = 'CTICU' and bed = 'CT12' and channel = '290' and text = 'Rem.AlarmDev.Malf.'
order by source,unit,bed,channel,text,dateadd(s, cast([alarm_ts] as int), '19700101')

SELECT Eventcount, alarmkey,AlarmDurationSeconds, [alarmsource],[alarmunit],AlarmBedLabel,AlarmChannel,AlarmDescription,AlarmEventStartTime,AlarmEventEndTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName 
FROM HelixReport.[dbo].HadoopAlarmEvents
where alarmsource = 'PICSERV05' and alarmunit = 'CTICU' and alarmbedlabel = 'CT12' and alarmchannel = '290' and AlarmDescription = 'Rem.AlarmDev.Malf.'
order by alarmkey





SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration ,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,AlarmEventEndTime ,HadoopAlarmEndFileName,HadoopOrigFileName  
FROM HelixReportstage.[dbo].HadoopEndedAlarms 
where source = 'PICSERV02' and unit = '4NN' and bed = 'NN1-08' and channel = '291' and text = 'ARRHYTHMIA OFF'
order by auditid

select * from HelixReportstage.[dbo].HadoopactiveAlarms25 where source = 'PICSERV02' and unit = '4NN' and bed = 'NN1-08' and channel = '291' and text = 'ARRHYTHMIA OFF'
select * from HelixReportstage.[dbo].HadoopactiveAlarms26 where source = 'PICSERV02' and unit = '4NN' and bed = 'NN1-08' and channel = '291' and text = 'ARRHYTHMIA OFF'
SELECT Eventcount,AlarmEnded,rank, auditid,eventcount,alarmended	,Duration, activealarmduration,[source],[unit],[bed],[channel],[text],AlarmEventStartTime,HadoopAlarmStartFileName ,HadoopAlarmEndFileName,HadoopOrigFileName  
FROM HelixReportstage.[dbo].HadoopactiveAlarms 
where alarmsource = 'PICSERV05' and alarmunit = 'CTICU' and alarmbedlabel = 'CT12' and alarmchannel = '290' and AlarmDescription = 'Rem.AlarmDev.Malf.'
order by auditid





 
select count(*) from HelixReportstage.[dbo].HadoopRawAlarmsStage25 
select count(*) from HelixReportstage.[dbo].HadoopRawAlarmsStage26 
select count(*) from HelixReportstage.[dbo].HadoopRawAlarmsStage27
select sum(eventcount) from  HelixReportstage.[dbo].HadoopEndedAlarms 
select sum(eventcount) from  HelixReportstage.[dbo].HadoopactiveAlarms 

select * into HelixReportstage.[dbo].HadoopRawAlarmsStage27 from HelixReportstage.[dbo].HadoopRawAlarmsStage
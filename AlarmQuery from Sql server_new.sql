/****** Script for SelectTopNRows command from SSMS  ******/
SELECT  [AuditId]
      ,[source]
      ,[unit]
      ,[bed]
      ,[channel]
      ,[Duration]
      ,[text]
      ,[msh_ts]
      ,[alarm_ts]
      ,[msg_id]
      ,[TimeStamp]
      ,[PreviousTimeStamp]
      ,[filename]
      ,[DateInserted]
      ,[AlarmEnded]
      ,[AlarmCount]
  FROM [HelixReportStage].[dbo].[HadoopActiveAlarms]
 select count(*)  FROM [HelixReportStage].[dbo].[HadoopActiveAlarms]
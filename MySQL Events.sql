/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP 1000 [alarm_index_id]
      ,[timestamp]
      ,[alarm_source]
      ,[msh_start_datetime]
      ,[alarm_start_datetime]
      ,[monitor_label]
      ,[unit]
      ,[bed]
      ,[duration_sec]
      ,[channel]
      ,[alarm_text]
  FROM [bmdi].[dbo].[bmdi_alarm_events] order by duration_sec desc

  select distinct channel from [bmdi].[dbo].[bmdi_alarm_events]
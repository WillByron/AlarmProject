DECLARE
    @counter			INT = 1,
    @max				INT = 0,
	@rank				INT,
	@NextDurationSum	INT,
	@Duration			INT,
	@TotalDuration		INT = 0,
	@Id					INT

IF OBJECT_ID('tempdb..#MyTable', 'U') IS NOT NULL
 DROP TABLE #MyTable;


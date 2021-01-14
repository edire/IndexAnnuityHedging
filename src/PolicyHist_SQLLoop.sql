
SET NOCOUNT ON


DECLARE @dte DATE = '5/14/2018'

WHILE @dte <= '1/11/2021' BEGIN
	PRINT @dte
	EXEC [hdg].[stpHedgingPolicies] @dte
	EXEC hdg.stpHedgingPoliciesTrend @dte
	SET @dte = DATEADD(DAY, 1, @dte)
END

/*
TRUNCATE TABLE hdg.tblHedgingPolicies
TRUNCATE TABLE hdg.tblHedgingPoliciesTrend
*/
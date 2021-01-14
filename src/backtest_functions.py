

import pandas as pd
from mymodules import ODBC



def GetDateList():
    df_datelist = pd.read_csv(r"C:\Users\Eric.Di Re\Documents\Eric_Local\PerfectHedge\data\date_list.csv").dropna()
    return df_datelist.values.tolist()


def ChooseOption(current_dte, exp_dte, strike):
    sql_option = f"""
    SELECT TOP 1
         expiration_date = CONVERT(DATE, t.expiration)
         , t.strike
         , cost = t.ask_1545
         , sell = t.bid_1545
    FROM eggy.tblCBOEOptionsEODQuotes t
    WHERE t.option_type = 'C'
         AND t.quote_date = '{current_dte}'
         AND t.expiration = '{exp_dte}'
    ORDER BY ABS(t.strike - {strike}) ASC
    """
    return ODBC.ReadSQL(db='insurance', sql=sql_option)


def CombineOptions(qty_x, qty_y, cost_x, cost_y, Notional_x, Notional_y):
    if qty_x > qty_y:
        qty = qty_x - qty_y
        cost_per = cost_x / qty_x
        Notional = Notional_x - Notional_y
    elif qty_x < qty_y:
        qty = qty_y - qty_x
        cost_per = cost_y / qty_y
        Notional = Notional_y - Notional_x
    cost = qty * cost_per
    return qty, Notional, cost



def CalculateGains(df_spx, exp_dte, start_price, notional):
    if exp_dte <= df_spx['CalendarDate'].max():
            idx_price = df_spx[df_spx['CalendarDate'] == exp_dte]['Price'].iloc[0]
            gain = notional * max(idx_price / start_price - 1, 0)
    else:
        gain = 0
    return gain



def GetSPXHistory():
    sql_spx = """
        SELECT CalendarDate = CONVERT(DATE, [Date])
            , Price = [Adj Close]
        FROM eggy.tblSPXHistory
        WHERE [Date] BETWEEN '5/15/2018' AND '12/31/2020'
        ORDER BY [Date]
        """
    return ODBC.ReadSQL(db='Insurance', sql=sql_spx)


def GetPolicyList(strategy='new'):
    sql_policies = """
		SELECT h.PolNo
            , h.IndexAV
			, h.Notional
			, h.NotionalShort
			, h.IndexDate
			, h.IndexValue
			, h.IndexValueShort
			, anniversary_date = h.AnniversaryDate
			, expiration_date = h.ExpirationDate
			, h.RowStartDate
            , h.RowEndDate
			, h.StrikeBal
			, h.StrikeShortBal
			, RowNum = ROW_NUMBER() OVER (PARTITION BY h.PolNo, h.IndexDate ORDER BY h.RowStartDate ASC)
		FROM hdg.tblHedgingPolicies_BackTestTrend h
        --WHERE h.PolNo = '1000000453P1'
        """
    df_policies = ODBC.ReadSQL(db='Insurance', sql=sql_policies)
    if strategy=='current':
        df_policies = df_policies[df_policies['RowNum']==1]
    df_policies['is_hedged'] = 0
    return df_policies


def GetMonteCarloSPX():
    return pd.read_csv(r"C:\Users\Eric.Di Re\Documents\Eric_Local\PerfectHedge\data\MC_SPX.csv")


def GetPolicyListGains():
    sql_policies_gains = """
        SELECT t.PolNo
        	, IndexDate = CONVERT(DATE, t.IndexDate)
        	, t.IndexAV
        	, t.IndexValue
        	, t.IndexValueShort
        	, t.Notional
        	, t.NotionalShort
        	, AnniversaryDate = t.NewAnniversaryDate
        FROM hdg.vHedgingPoliciesGains t
        --WHERE t.PolNo = '1000000453P1'
        """
    return ODBC.ReadSQL(db='Insurance', sql=sql_policies_gains)


def InProgress(current_dte, idx_dte, row_start_dte, row_end_dte, start_date, end_date, is_hedged, strategy='new'):
    if strategy=='new':
        if row_start_dte <= current_dte and row_end_dte >= current_dte and idx_dte >= start_date and idx_dte <= end_date:
            return 1, 0
        else:
            return 0, 0
    else:
        if row_start_dte <= current_dte and is_hedged == 0 and idx_dte >= start_date and idx_dte <= end_date:
            return 1, 1
        else:
            return 0, is_hedged


def GetExpirationDateList():
    sql_expiration_dates = """
        SELECT DISTINCT quote_date = CONVERT(DATE, quote_date)
            , expiration
        FROM eggy.tblCBOEOptionsEODQuotes
        WHERE option_type = 'C'
        """
    return ODBC.ReadSQL(db='Insurance', sql=sql_expiration_dates)



def GetExpirationDate(df_expiration_dates, quote_date, anniversary_date):
    df_expiration_dates_today = df_expiration_dates[df_expiration_dates['quote_date']==quote_date].copy()
    df_expiration_dates_today['diff'] = df_expiration_dates_today.apply(lambda x: abs((x['expiration'] - anniversary_date).days), axis=1)
    df_expiration_dates_today.sort_values(['diff'], inplace=True)
    return df_expiration_dates_today['expiration'].iloc[0]



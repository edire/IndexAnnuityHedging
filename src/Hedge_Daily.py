

import pandas as pd
from mymodules import ODBC, MyLogging, SendEmail
import datetime as dt


try:

    logger = MyLogging.NewLogger(__file__, use_cd=True)

    logger.info("Begin SQL procs to update policy list.")
    ODBC.RunSQL(db='Insurance', sql='EXEC [hdg].[stpHedgingPolicies] NULL')
    ODBC.RunSQL(db='Insurance', sql='EXEC [hdg].[stpHedgingPoliciesTrend] NULL')

    logger.info("Begin balance comparison calculations.")


    #########################################################################################################################
    # Define functions needed
    #########################################################################################################################

    def ChooseOption(exp_dte, strike, qty):
        strike = strike
        sql_option_low = f"""
            SELECT TOP 1
            	expiration_date = Expiration_Date
                	, strike = Strike * 10.0
            FROM hdg.vOptionPrices_XSP
            WHERE Expiration_Date = '{exp_dte}'
                AND Strike * 10.0 <= {strike}
            ORDER BY ABS(Strike * 10.0 - {strike}) ASC
        """
        sql_option_high = f"""
            SELECT TOP 1
            	expiration_date = Expiration_Date
                	, strike = Strike * 10.0
            FROM hdg.vOptionPrices_XSP
            WHERE Expiration_Date = '{exp_dte}'
                AND Strike * 10.0 > {strike}
            ORDER BY ABS(Strike * 10.0 - {strike}) ASC
        """
        df_option_low = ODBC.ReadSQL(db='insurance', sql=sql_option_low)
        df_option_high = ODBC.ReadSQL(db='insurance', sql=sql_option_high)
        if df_option_low.empty:
            df_option_high['qty'] = qty
            df_option_all = df_option_high
        elif df_option_high.empty:
            df_option_low['qty'] = qty
            df_option_all = df_option_low
        else:
            strike_low = df_option_low['strike'].iloc[0]
            strike_high = df_option_high['strike'].iloc[0]
            x_high = round((strike - strike_low) * qty / (strike_high - strike_low), 1)
            x_low = qty - x_high
            df_option_low['qty'] = x_low
            df_option_high['qty'] = x_high
            df_option_all = pd.concat((df_option_high, df_option_low), axis=0)
            df_option_all = df_option_all[df_option_all['qty'] != 0]
        return df_option_all



    #########################################################################################################################
    # Gather SQL data
    #########################################################################################################################

    sql_policies = """
        SELECT h.IDHedgingPoliciesCurrent
              , h.PolNo
              , h.Notional
              , h.NotionalShort
              , h.IndexDate
              , h.IndexValue
              , h.IndexValueShort
              , h.AnniversaryDate
              , expiration_date = h.ExpirationDate
              , h.StrikeBal
              , h.StrikeShortBal
        FROM hdg.tblHedgingPolicies h
        WHERE h.IsLocked = 0
        """
    df_policies = ODBC.ReadSQL('Insurance', sql = sql_policies)
    df_policies['qty'] = df_policies['Notional'] / (df_policies['IndexValue'] * 100)
    df_policies['qty_short'] = df_policies['NotionalShort'] / (df_policies['IndexValueShort'] * 100)
    df_policies['NotionalBal'] = df_policies['StrikeBal'] * df_policies['qty'] * 100
    df_policies['NotionalShortBal'] = df_policies['StrikeShortBal'] * df_policies['qty_short'] * 100


    sql_options_hist = """
    SELECT c.purchase_date
         , c.expiration_date
         , strike = c.strike * t.IndexFactor
         , qty = os.OptionSign * c.qty / t.IndexFactor
         , Notional = os.OptionSign * c.Notional
         , c.category
    FROM   hdg.tblHedgingOptionsTrend c
    	LEFT JOIN hdg.tblDimOptionTicker t
    		ON t.Symbol = c.Symbol
        OUTER APPLY (SELECT OptionSign = CASE WHEN c.category = 'short' THEN -1 ELSE 1 END) os
    WHERE  c.expiration_date >= CONVERT(DATE, GETDATE())
        AND c.IsLocked = 0
        """
    df_options_hist = ODBC.ReadSQL(db='Insurance', sql = sql_options_hist)
    df_options = df_options_hist[df_options_hist['category'] == 'long']
    df_options_short = df_options_hist[df_options_hist['category'] == 'short']



    #########################################################################################################################
    # Run option purchasing logic
    #########################################################################################################################

    current_date = dt.date.today() #+ dt.timedelta(days=-1)

    df_pol_bal = df_policies.groupby(['expiration_date'])[['qty', 'NotionalBal', 'qty_short', 'NotionalShortBal']].sum()

    df_opt_bal = df_options.groupby(['expiration_date'])[['qty', 'Notional']].sum()
    df_opt_short_bal = df_options_short.groupby(['expiration_date'])[['qty', 'Notional']].sum()


    # Long Options
    if df_pol_bal.empty == False or df_opt_bal.empty == False:
        df_bal = pd.merge(df_pol_bal, df_opt_bal, how='outer', on='expiration_date')
        df_bal.reset_index(inplace=True)
        df_bal.fillna(0, inplace=True)

        for s in range(df_bal.shape[0]):
            buy_qty = df_bal.iloc[s]['qty_x'] - df_bal.iloc[s]['qty_y']
            buy_qty_rnd = round(buy_qty, 1)

            if buy_qty_rnd != 0:
                strike = (df_bal.iloc[s]['NotionalBal'] - df_bal.iloc[s]['Notional']) / (buy_qty * 100)
                df_opt_buy = ChooseOption(df_bal.iloc[s]['expiration_date'], strike, buy_qty_rnd)
                df_opt_buy['purchase_date'] = current_date
                df_opt_buy['Notional'] = df_opt_buy['strike'] * df_opt_buy['qty'] * 100
                df_options = df_options.append(df_opt_buy[['purchase_date', 'expiration_date', 'strike', 'qty', 'Notional']])


    # Short Options
    if df_pol_bal.empty == False or df_opt_short_bal.empty == False:
        df_bal = pd.merge(df_pol_bal, df_opt_short_bal, how='outer', on='expiration_date')
        df_bal.reset_index(inplace=True)
        df_bal.fillna(0, inplace=True)

        for s in range(df_bal.shape[0]):
            buy_qty = df_bal.iloc[s]['qty_short'] - df_bal.iloc[s]['qty_y']
            buy_qty_rnd = round(buy_qty, 1)

            if buy_qty_rnd != 0:
                strike = (df_bal.iloc[s]['NotionalShortBal'] - df_bal.iloc[s]['Notional']) / (buy_qty * 100)
                df_opt_buy = ChooseOption(df_bal.iloc[s]['expiration_date'], strike, buy_qty_rnd)
                df_opt_buy['purchase_date'] = current_date
                df_opt_buy['Notional'] = df_opt_buy['strike'] * df_opt_buy['qty'] * 100
                df_options_short = df_options_short.append(df_opt_buy[['purchase_date', 'expiration_date', 'strike', 'qty', 'Notional']])



    #########################################################################################################################
    # Run option cleanup and combine
    #########################################################################################################################

    df_options = df_options[df_options['purchase_date']==current_date]
    df_options['category'] = 'long'

    df_options_short = df_options_short[df_options_short['purchase_date']==current_date]
    df_options_short['category'] = 'short'
    df_options_short[['qty', 'Notional']] = -df_options_short[['qty', 'Notional']]

    df_options_final = pd.concat((df_options, df_options_short), axis=0)
    df_options_final.sort_values(by=['purchase_date', 'expiration_date', 'strike'], inplace=True)
    df_options_final.reset_index(drop=True, inplace=True)


    if df_options_final.empty == False:
        logger.info("Loading today's purchases to SQL")
        con = ODBC.CallODBC(db='Insurance')
        df_options_final.to_sql(name='tblHedgingOptions', schema='hdg', con=con, if_exists='replace', index=False)
        logger.info('Done, no problems!\n')
    else:
        logger.info('Done - No Updates\n')


except Exception as e:
    logger.info(str(e) + '\n')
    to_emails = ['eric.dire@puritanlife.com']
    body = 'Error running package Hedging_Daily\n \
        Error message: ' + str(e)
    SendEmail.SendEmail(to_emails, 'ETL Load Error', body)
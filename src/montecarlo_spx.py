


import pandas as pd
import datetime as dt
from mymodules import ODBC
import os



def SP_MC(start_date, forecast_days, iterations):
    sql_holidays = '''
        SELECT AsOfDate = CONVERT(DATE, CalendarDate)
            , IsHoliday = 1
        FROM dim.tblTime
        WHERE IsMarketHoliday = 1
                OR IsWeekend = 1
        '''
    df_holidays = ODBC.ReadSQL(db='Insurance', sql=sql_holidays)

    sql_spx = f"""
        SELECT AsOfDate
            , Price
            , DailyRate
        FROM hdg.vSPXHistory
        WHERE AsOfDate <= '{start_date}'
            AND DailyRate IS NOT NULL
        ORDER BY AsOfDate
        """
    df_spx = ODBC.ReadSQL(db='Insurance', sql=sql_spx)

    start_date = dt.datetime.strptime(start_date, '%m/%d/%Y').date()

    #Monte Carlo Simulation
    sim = pd.DataFrame()
    future_dates = []
    for i in range(forecast_days):
        future_dates.append(start_date + dt.timedelta(days=i+1))
    sim['AsOfDate'] = future_dates
    sim = pd.merge(sim, df_holidays, how='left', on='AsOfDate')
    sim = sim[sim.IsHoliday.isna() == True][['AsOfDate']]

    for c in range(iterations):
        monte_carlo = []
        sp = df_spx[df_spx.AsOfDate == start_date].iloc[0].Price
        for r in range(sim.shape[0]):
            sp = sp * (1 + df_spx.sample().DailyRate.iloc[0])
            monte_carlo.append(sp)
        sim[c] = monte_carlo

    sim.to_csv(os.path.join(os.getcwd(), "MC_SPX.csv"), index=False)



SP_MC('9/3/2019', 400, 200)

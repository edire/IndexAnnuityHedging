

import os
import pandas as pd
import datetime as dt
import backtest_functions as mf



run_montecarlo = False
strategy = 'current'


#########################################################################################################################
# Define SQL pulls
#########################################################################################################################

date_lst = mf.GetDateList()

df_spx_actuals = mf.GetSPXHistory()
mc_spx = mf.GetMonteCarloSPX()


df_policies = mf.GetPolicyList(strategy=strategy)

df_policies_gains = mf.GetPolicyListGains()

if strategy == 'current':
    df_expiration_dates = mf.GetExpirationDateList()


#########################################################################################################################
# Run monthly loop
#########################################################################################################################

for d in range(len(date_lst)):
    start_date = date_lst[d][0]
    start_date = dt.datetime.strptime(start_date, "%m/%d/%Y").date()
    end_date = date_lst[d][1]
    end_date = dt.datetime.strptime(end_date, "%m/%d/%Y").date()


    df_policies_gains_monthly = df_policies_gains.copy()
    df_policies_gains_monthly = df_policies_gains_monthly[(df_policies_gains_monthly['IndexDate'] >= start_date)
                                                  & (df_policies_gains_monthly['IndexDate'] <= end_date)]


    if run_montecarlo == True:
        loop_size = mc_spx.shape[1] - 1
    else:
        loop_size = 1


    result_set = {}


    #########################################################################################################################
    # Run simulation loop
    #########################################################################################################################

    for sim in range(loop_size):

        if run_montecarlo == True:
            df_spx = mc_spx[['AsOfDate', f'{sim}']].copy()
            df_spx.columns = ['CalendarDate', 'Price']
            df_spx['CalendarDate'] = pd.to_datetime(df_spx['CalendarDate'])
            df_spx['CalendarDate'] = df_spx['CalendarDate'].apply(lambda x: x.date())
        else:
            df_spx = pd.DataFrame(columns=['CalendarDate', 'Price'])

        df_spx = pd.concat((df_spx_actuals, df_spx), axis=0)
        df_spx.reset_index(drop=True, inplace=True)

        start_date_idx = df_spx[df_spx['CalendarDate'] == df_spx['CalendarDate'].min()].index.tolist()[0]
        end_date_idx = df_spx[df_spx['CalendarDate'] == df_spx['CalendarDate'].max()].index.tolist()[0]


        df_policies_gains_monthly_final = pd.DataFrame(columns=['PolNo', 'IndexDate', 'IndexAV', 'IndexValue', 'IndexValueShort'
                                                                , 'Notional', 'NotionalShort', 'AnniversaryDate', 'gain', 'gain_short'
                                                                , 'gain_total'])


        df_options_final_monthly = pd.DataFrame(columns=['purchase_date', 'expiration_date', 'strike', 'qty'
                                                         , 'Notional', 'cost', 'category', 'start_date', 'end_date'])


        df_options = pd.DataFrame(columns=['purchase_date', 'expiration_date', 'strike', 'qty', 'Notional', 'cost'])
        df_options_short = pd.DataFrame(columns=['purchase_date', 'expiration_date', 'strike', 'qty', 'Notional', 'cost'])


        #########################################################################################################################
        # Run option purchasing loop
        #########################################################################################################################

        for i in range(end_date_idx - start_date_idx + 1):
            current_dte = df_spx['CalendarDate'].loc[start_date_idx + i]


            df_policies_work = df_policies.copy()
            df_policies_work[['InProgress', 'is_hedged']] = df_policies_work.apply(lambda x: mf.InProgress(current_dte
                                                                                                        , x['IndexDate']
                                                                                                        , x['RowStartDate']
                                                                                                        , x['RowEndDate']
                                                                                                        , start_date
                                                                                                        , end_date
                                                                                                        , x['is_hedged']
                                                                                                        , strategy)
                                                                                   , axis=1, result_type='expand')
            df_policies_work = df_policies_work[df_policies_work['InProgress'] == 1]
            df_policies_work['qty'] = df_policies_work['Notional'] / (df_policies_work['IndexValue'] * 100)
            df_policies_work['qty_short'] = df_policies_work['NotionalShort'] / (df_policies_work['IndexValueShort'] * 100)
            df_policies_work['NotionalBal'] = df_policies_work['StrikeBal'] * df_policies_work['qty'] * 100
            df_policies_work['NotionalShortBal'] = df_policies_work['StrikeShortBal'] * df_policies_work['qty_short'] * 100
            if strategy == 'current':
                df_policies_work['expiration_date'] = df_policies_work.apply(lambda x: mf.GetExpirationDate(df_expiration_dates, current_dte, x['anniversary_date']), axis=1)
            df_pol_bal = df_policies_work.groupby(['expiration_date'])[['qty', 'NotionalBal', 'qty_short', 'NotionalShortBal']].sum()

            if strategy == 'new':
                df_opt_bal = df_options.groupby(['expiration_date'])[['qty', 'Notional']].sum()
                df_opt_short_bal = df_options_short.groupby(['expiration_date'])[['qty', 'Notional']].sum()
            else:
                df_opt_bal = pd.DataFrame(columns=['expiration_date', 'qty', 'Notional'])
                df_opt_short_bal = pd.DataFrame(columns=['expiration_date', 'qty', 'Notional'])



            # Long Options
            if df_pol_bal.empty == False or df_opt_bal.empty == False:
                df_bal = pd.merge(df_pol_bal, df_opt_bal, how='outer', on='expiration_date')
                df_bal.reset_index(inplace=True)
                df_bal = df_bal[df_bal['expiration_date'] >= current_dte]
                df_bal.fillna(0, inplace=True)


                for s in range(df_bal.shape[0]):
                    buy_qty = df_bal.iloc[s]['qty_x'] - df_bal.iloc[s]['qty_y']
                    buy_qty_rnd = round(buy_qty, 1)

                    if buy_qty_rnd != 0:
                        strike = (df_bal.iloc[s]['NotionalBal'] - df_bal.iloc[s]['Notional']) / (buy_qty * 100)
                        df_opt_buy = mf.ChooseOption(current_dte, df_bal.iloc[s]['expiration_date'], strike)
                        df_opt_buy['purchase_date'] = current_dte
                        df_opt_buy['qty'] = buy_qty_rnd
                        df_opt_buy['Notional'] = df_opt_buy['strike'] * buy_qty_rnd * 100
                        if buy_qty_rnd > 0:
                            df_opt_buy['cost'] = df_opt_buy['cost'] * buy_qty_rnd * 100
                        else:
                            df_opt_buy['cost'] = df_opt_buy['sell'] * buy_qty_rnd * 100
                        df_options = df_options.append(df_opt_buy[['purchase_date', 'expiration_date', 'strike', 'qty', 'Notional', 'cost']])


            # Short Options
            if df_pol_bal.empty == False or df_opt_short_bal.empty == False:
                df_bal = pd.merge(df_pol_bal, df_opt_short_bal, how='outer', on='expiration_date')
                df_bal.reset_index(inplace=True)
                df_bal = df_bal[df_bal['expiration_date'] >= current_dte]
                df_bal.fillna(0, inplace=True)

                for s in range(df_bal.shape[0]):
                    buy_qty = df_bal.iloc[s]['qty_short'] - df_bal.iloc[s]['qty_y']
                    buy_qty_rnd = round(buy_qty, 1)

                    if buy_qty_rnd != 0:
                        strike = (df_bal.iloc[s]['NotionalShortBal'] - df_bal.iloc[s]['Notional']) / (buy_qty * 100)
                        df_opt_buy = mf.ChooseOption(current_dte, df_bal.iloc[s]['expiration_date'], strike)
                        df_opt_buy['purchase_date'] = current_dte
                        df_opt_buy['qty'] = buy_qty_rnd
                        df_opt_buy['Notional'] = df_opt_buy['strike'] * buy_qty_rnd * 100
                        if buy_qty_rnd > 0:
                            df_opt_buy['cost'] = df_opt_buy['sell'] * buy_qty_rnd * 100
                        else:
                            df_opt_buy['cost'] = df_opt_buy['cost'] * buy_qty_rnd * 100
                        df_options_short = df_options_short.append(df_opt_buy[['purchase_date', 'expiration_date', 'strike', 'qty', 'Notional', 'cost']])



    #########################################################################################################################
    # Run option cleanup and combine
    #########################################################################################################################

        df_options.reset_index(drop=True, inplace=True)
        df_options_short.reset_index(drop=True, inplace=True)
        df_options['category'] = 'long'
        df_options_short['category'] = 'short'
        df_options_sql = pd.concat((df_options, df_options_short))

        df_comb = pd.merge(df_options, df_options_short, how='inner', on=['purchase_date', 'expiration_date', 'strike'])
        df_comb = df_comb[df_comb['qty_x'] != df_comb['qty_y']]
        if df_comb.empty == False:
            df_comb[['qty', 'Notional', 'cost']] = df_comb.apply(lambda x: mf.CombineOptions(x['qty_x'], x['qty_y'], x['cost_x'], x['cost_y'], x['Notional_x'], x['Notional_y']), axis=1, result_type='expand')
            df_comb['category'] = 'comb'
            df_comb = df_comb[df_options.columns]
        else:
            df_comb = pd.DataFrame(columns = df_options.columns)

        df_options_cln = pd.merge(df_options, df_options_short, how='left', on=['purchase_date', 'expiration_date', 'strike'], indicator=True)
        df_options_cln = df_options_cln[df_options_cln['_merge'] == 'left_only']
        df_options_cln = df_options_cln[['purchase_date', 'expiration_date', 'strike', 'qty_x', 'Notional_x', 'cost_x', 'category_x']]
        df_options_cln.columns = df_options.columns

        df_options_short_cln = pd.merge(df_options_short, df_options, how='left', on=['purchase_date', 'expiration_date', 'strike'], indicator=True)
        df_options_short_cln = df_options_short_cln[df_options_short_cln['_merge'] == 'left_only']
        df_options_short_cln = df_options_short_cln[['purchase_date', 'expiration_date', 'strike', 'qty_x', 'Notional_x', 'cost_x', 'category_x']]
        df_options_short_cln.columns = df_options_short.columns
        df_options_short_cln[['qty', 'cost', 'Notional']] = -df_options_short_cln[['qty', 'cost', 'Notional']]

        df_options_final = pd.concat((df_comb, df_options_cln, df_options_short_cln), axis=0)
        df_options_final.sort_values(by=['purchase_date', 'expiration_date', 'strike'], inplace=True)
        df_options_final.reset_index(drop=True, inplace=True)


        df_options_final['start_date'] = start_date
        df_options_final['end_date'] = end_date
        df_options_final['gain'] = df_options_final.apply(lambda x: mf.CalculateGains(df_spx, x['expiration_date'], x['strike'], x['Notional']), axis=1)
        df_options_final_monthly = df_options_final_monthly.append(df_options_final)



#########################################################################################################################
# Policy gains for summary
#########################################################################################################################

        df_policies_gains_monthly['gain'] = df_policies_gains_monthly.apply(lambda x: mf.CalculateGains(df_spx, x['AnniversaryDate'], x['IndexValue'], x['Notional']), axis=1)
        df_policies_gains_monthly['gain_short'] = df_policies_gains_monthly.apply(lambda x: mf.CalculateGains(df_spx, x['AnniversaryDate'], x['IndexValueShort'], x['NotionalShort']), axis=1)
        df_policies_gains_monthly['gain_total'] = df_policies_gains_monthly['gain'] - df_policies_gains_monthly['gain_short']
        df_policies_gains_monthly_final = df_policies_gains_monthly_final.append(df_policies_gains_monthly)



#########################################################################################################################
# Store Results
#########################################################################################################################

    result_set[sim] = {'gain_pol': df_policies_gains_monthly_final['gain_total'].sum()
                       , 'gain_opt': df_options_final_monthly['gain'].sum()}


df_result_set = pd.DataFrame.from_dict(result_set, orient='index')
# df_result_set.to_csv(os.path.join(r"C:\Users\Eric.Di Re\Documents\Eric_Local\PerfectHedge\temp", f'Backtest_{strategy}.csv'))



########################################################################################################


pol_gain, opt_gain, cost, acc_val = df_result_set['gain_pol'].sum(), df_result_set['gain_opt'].sum(), df_options_final_monthly['cost'].sum(), df_policies_gains_monthly_final['IndexAV'].sum()

summary = f'''
Policy gain:    $ {pol_gain:,.0f}
Option gain:    $ {opt_gain:,.0f}
$ Variance:     $ {opt_gain - pol_gain:,.0f}
% Variance:       {100 * (opt_gain - pol_gain) / pol_gain:.1f} %
Cost:           $ {cost:,.0f}
Acc Value:      $ {acc_val:,.0f}
Cost %:           {100 * cost / acc_val:.2f} %
Gain %:           {100 * (pol_gain - opt_gain) / acc_val:.2f} %
Net Cost:       $ {cost + pol_gain - opt_gain:,.0f}
Net %:            {100 * (cost + pol_gain - opt_gain) / acc_val:.2f} %
New Acc Val     $ {acc_val + pol_gain:,.0f}'''

print(summary)




df_options_final_monthly.to_csv(os.path.join(r"C:\Users\Eric.Di Re\Documents\Eric_Local\PerfectHedge\temp", "options.csv"))
df_policies_gains_monthly_final.to_csv(os.path.join(r"C:\Users\Eric.Di Re\Documents\Eric_Local\PerfectHedge\temp", "policies.csv"))


# df_options_final_monthly.T
# df_policies_gains_monthly_final.T

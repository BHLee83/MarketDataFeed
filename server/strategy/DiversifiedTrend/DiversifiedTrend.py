import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from database.db_manager import DatabaseManager

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import norm

import scipy.cluster.hierarchy as sch

from server.strategy.HRP import correlDist, getIVP, getQuasiDiag, getRecBipart

# from IPython.core.display import display, HTML
# display(HTML("<style>.container { width: 100% !important; }</style>"))


class DiversifiedTrend():

    def __init__(self, start_date):
        self.isRT = False
        self.start_date = start_date
        self.db_manager = DatabaseManager()
        self.current_path = os.path.dirname(os.path.abspath(__file__))
        self.target_path = os.path.dirname(os.path.dirname(self.current_path)) + '/static/lib/images/'
        self.name_heatmap = 'DT_heatmap.png'
        self.name_dendrogram = 'DT_dendrogram.png'

    """
    1. Process cleaning and normalizing data indepently in timely manner.
    """
    def norm(self, ticker, start):
        table_name = "marketdata_price_1d"
        ret = self.db_manager.load_close_by_symbol(table_name, ticker, start)
        if len(ret) == 0:
            return None
        
        df = pd.DataFrame(ret, columns = ['trd_date', 'close'])
        df['trd_date'] = pd.to_datetime(df['trd_date'])
        df = df.set_index('trd_date')
        df['daily_chg'] = df['close'] .pct_change()
        # df['daily_chg'].iloc[0] = 0
        df.loc[0, 'daily_chg'] = 0
        df['daily_vol'] = np.log(df['close']).diff().ewm(com = 32).std()
        
        df['vol_fast'] = np.log(df['close']).diff().ewm(halflife = 12).std()
        df['vol_slow'] = np.log(df['close']).diff().ewm(halflife = 252).std()
        
        df['normalized'] = 1000 + (np.log(df['close']).diff() / np.log(df['close']).diff().ewm(com = 32).std()).clip(-4.2,4.2).cumsum()
        df['diff'] = df['normalized'].diff()
        """
        with target risk 10%, calculate the notional exposure with dynamic scaling volatility
        """
        df['exposure'] = 0.06 / (df['daily_vol'] * np.sqrt(252))
        df['dynamic_exposure'] = 0.1/ (df[['vol_fast', 'vol_slow']].max(axis = 1) * np.sqrt(252))
        #print(time.time()-start)
        return df


    """
    2. Calculate momentum scores for every asset classes
    """
    def momentum(self, price_data):
        if price_data is None:
            return None

        df = price_data
        for j in [4,8,16,32,64]:
            f,g = 1.-1/(1.*j), 1.-1/(3.*j)
            l2_normalized = np.sqrt(1.0 / (1.-f*1.*f) - 2.0 / (1.-f*1.*g) + 1.0/(1.-g*1.*g))
            df['EWMA_'+str(j)] = (df['normalized'].ewm(span = 2*j -1).mean() - df['normalized'].ewm(span = 2*(3*j) -1).mean()) / l2_normalized
            df['EWMA_'+str(j)] = 5 * df['EWMA_'+str(j)] * np.exp((-df['EWMA_'+str(j)]**2)/4)/(np.sqrt(2)*np.exp(-1/2))

        for k in [20,40,80,160,320]:
            df['roll_max'+str(k)] = df['normalized'].rolling(window = k).max()
            df['roll_min'+str(k)] = df['normalized'].rolling(window = k).min()
            df['roll_mean'+str(k)] = (df['roll_max'+str(k)] + df['roll_min'+str(k)])*0.5
            df['breakout_'+str(k)] = 10*(df['normalized'] - df['roll_mean'+str(k)]) / (df['roll_max'+str(k)] - df['roll_min'+str(k)])
            df['breakout_'+str(k)] = df['breakout_'+str(k)].ewm(span = int(k/4)).mean()
            
        """
        Estimate momentum correlation across 8 variables, with weekly resampled data for last 104(2 years) data points
        Then, diversification multiplier = 1/sqrt(W*corr*W.T), capped at max 2.5
        """
        
        for i in ['EWMA', 'breakout']:
            corr_df = (df.filter(like = i).resample("W").first().iloc[-104:]).corr()
            weight_df = [1/5]*5
            df[i+"multiplier"] = min(1/((np.dot(np.dot(weight_df,corr_df),weight_df))**0.5),2.5)
            df[i] = (df.filter(like = i+"_").mean(axis = 1) * df[i+'multiplier']).clip(-5,5)
        
        cross_corr_df = (df[["EWMA", "breakout"]].resample("W").first()).iloc[-104:].corr()
        cross_weight_df = [1/2]*2
        df['div_factor'] = min(1/((np.dot(np.dot(cross_weight_df,cross_corr_df),cross_weight_df))**0.5),2.5)
        df['momentum'] = (df[["EWMA", "breakout"]].mean(axis = 1) * df['div_factor']).clip(-5,5)
        
        """
        To estimate portfolio diversification factor, we need volatility-adjusted forecast return, with target volatility 10%
        """
        df['forecast_ret_norm'] = df['daily_chg'] * ((df['exposure']* df['momentum']).shift()) #momentum normalized daily return
        #df['forecast_ret_norm'] = df['daily_chg'] * ((df['exposure']).shift())
        #df['risk_premium_norm'] = df['daily_chg'] * ((df['dynamic_exposure'] * 2.5).shift()) #long biased normalized daily return
        
        return df[['normalized','daily_vol','vol_slow','vol_fast','EWMA','breakout', "div_factor", 'momentum','forecast_ret_norm']]


    """
    3. Define the portfolio's universe
    """
    def universe(self):
        equities = ['101','DAM', 'ES', 'ESX', 'MHI', 'MND', 'MTW', 'NKD', 'NQ', 'RTY', 'SCN', 'SFID', 'SGP', 'VS', 'VX', 'YM']  # 16 entities
        
        bonds = ['BTP','BTS','GBL','GBM ','GBS', 'GBX', 'KRDRVFUBM3', 'KRDRVFUBM5', 'KRDRVFUBMA', 'KRDRVFUBML', 
                'OAT', 'SR3', 'TN', 'UB', 'Z3N', 'ZB', 'ZF', 'ZN', 'ZT']   # 19 entities
        
        fx = ['6A', '6B', '6C', '6E', '6J', '6L', '6M', '6N', '6R', '6S', '6Z', 'CUS', 'KRDRVFUUSD', 'SIU'] # 14 entities
        
        comdty = ['BZ ', 'CC ','CL', 'CT', 'GC', 'HE', 'HG', 'HO', 'KC', 'KE', 'LE', 'NG', 
                'OJ', 'PA', 'PL', 'RB', 'SB', 'SI', 'ZC', 'ZL', 'ZO', 'ZR', 'ZS', 'ZW']   # 24 entities

        return equities +bonds + fx + comdty
        #return equities


    """
    4. Final Portfolio's output.
    """
    def portfolio(self):
        entities = self.universe()
        indexes = entities.copy()
        dfs = []
        for i in entities:
            df = self.momentum(norm(i, self.start_date))
            if df is None:
                indexes.remove(i)
                continue
            dfs.append(df.iloc[-1])
            #print(i)
        portfolio_df = pd.DataFrame(pd.concat(dfs, axis = 1)).T
        portfolio_df.index = indexes
        
        return portfolio_df


    """
    5. Weight calculation through HRP algorithms, with returns & vol-adjusted returns
    """
    def time_series_return(self):
        entities = self.universe()
        indexes = entities.copy()
        dfs = []
        for i in entities:
            df = self.norm(i, self.start_date)
            if df is None:
                indexes.remove(i)
                continue
            # print(i)
            dfs.append(df['diff'])  # vol-adjusted returns
        ret_df = pd.DataFrame(pd.concat(dfs, axis = 1))
        ret_df.columns = indexes
        ret_df = ret_df.fillna(0)
        
        return ret_df


    """
    5. Weight Calculation and Diversification multiplier using Hierachical Risk Parity
    """
    def backtest(self):
        entities = self.universe()
        indexes = entities.copy()
        dfs = []
        for i in entities:
            df = self.momentum(norm(i, self.start_date))
            if df is None:
                indexes.remove(i)
                continue
            print(i)
            dfs.append(df.iloc[:,-1])   # forecast_ret_norm
        return_df = pd.DataFrame(pd.concat(dfs, axis = 1))
        return_df.columns = indexes
        return_df = return_df.fillna(0)
        
        corr_df = (return_df.resample("W").first().iloc[-104:].fillna(0).corr().clip(0,1))
        w = [1/len(corr_df.index)] * len(corr_df.index)
        
        div_factor = 1/(np.dot(np.dot(w,corr_df),w)**0.5)
        
        return return_df, div_factor


    def main(self):
        # start_date = '20250123'
        # obj = DiversifiedTrend(start_date)
        # print(portfolio())
        df = self.time_series_return()
        # print(df.iloc[-1000:].resample("W-MON").sum())


        corr = abs(df.iloc[-1000:].resample("W-MON").sum().corr())
        cov = df.iloc[-1000:].resample("W-MON").sum().cov()
        dist = correlDist(corr)
        link = sch.linkage(dist, 'ward')
        sortIx = getQuasiDiag(link)
        sortIx = corr.index[sortIx].tolist() #recover labels
        # df0 = corr.loc[sortIx, sortIx]
        # hrp = getRecBipart(cov,sortIx)

        plt.figure(figsize = (15,10))
        sns.heatmap(abs(df.iloc[-750:].resample("W-MON").sum().corr()), annot= True, fmt = '.1f', linewidth = 0.5, cmap = 'Blues', annot_kws={"fontsize": 6})
        if os.path.exists(self.target_path + self.name_heatmap):
            os.remove(self.target_path + self.name_heatmap)
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)
        plt.savefig(self.target_path + self.name_heatmap)
        # plt.show()
        
        plt.figure(figsize = (15,10))
        sch.dendrogram(link, labels = sortIx, leaf_rotation=90, leaf_font_size=8)
        if os.path.exists(self.target_path + self.name_dendrogram):
            os.remove(self.target_path + self.name_dendrogram)
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)
        plt.savefig(self.target_path + self.name_dendrogram)
        # plt.show()

        plt.close()
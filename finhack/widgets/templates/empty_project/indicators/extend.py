import pandas as pd
import numpy as np

class extend:

        # Pivot Points, Supports and Resistances
        def PPSR(df,p):
            PP = pd.Series((df['high'] + df['low'] + df['close']) / 3)
            R1 = pd.Series(2 * PP - df['low'])
            S1 = pd.Series(2 * PP - df['high'])
            R2 = pd.Series(PP + df['high'] - df['low'])
            S2 = pd.Series(PP - df['high'] + df['low'])
            R3 = pd.Series(df['high'] + 2 * (PP - df['low']))
            S3 = pd.Series(df['low'] - 2 * (df['high'] - PP))
            psr = {'PP':PP, 'R1':R1, 'S1':S1, 'R2':R2, 'S2':S2, 'R3':R3, 'S3':S3}
            PSR = pd.DataFrame(psr)
            
            if PSR.empty:
                return df
            
            df = df.join(PSR)

            df['PPSR']=df.PP
            df['PPR1']=df.R1
            df['PPS1']=df.S1
            df['PPR2']=df.R2
            df['PPS2']=df.S2
            df['PPR3']=df.R3
            df['PPS3']=df.S3
            return df
        
        
        # Stochastic oscillator %K
        def STOK(df):
            SOk = pd.Series((df['close'] - df['low']) / (df['high'] - df['low']), name = 'SO%k')
            df = df.join(SOk)
            return df
        
        
        # Stochastic Oscillator, EMA smoothing, nS = slowing (1 if no slowing)
        def STO(df,  nK, nD, nS=1):
            SOk = pd.Series((df['close'] - df['low'].rolling(nK).min()) / (df['high'].rolling(nK).max() - df['low'].rolling(nK).min()), name = 'SO%k'+str(nK))
            SOd = pd.Series(SOk.ewm(ignore_na=False, span=nD, min_periods=nD-1, adjust=True).mean(), name = 'SO%d'+str(nD))
            SOk = SOk.ewm(ignore_na=False, span=nS, min_periods=nS-1, adjust=True).mean()
            SOd = SOd.ewm(ignore_na=False, span=nS, min_periods=nS-1, adjust=True).mean()
            df = df.join(SOk)
            df = df.join(SOd)
            return df
        
        
        # Stochastic Oscillator, SMA smoothing, nS = slowing (1 if no slowing)
        def STO(df, nK, nD,  nS=1):
            SOk = pd.Series((df['close'] - df['low'].rolling(nK).min()) / (df['high'].rolling(nK).max() - df['low'].rolling(nK).min()), name = 'SO%k'+str(nK))
            SOd = pd.Series(SOk.rolling(window=nD, center=False).mean(), name = 'SO%d'+str(nD))
            SOk = SOk.rolling(window=nS, center=False).mean()
            SOd = SOd.rolling(window=nS, center=False).mean()
            df = df.join(SOk)
            df = df.join(SOd)
            return df
        
        
        # Mass Index
        def MassI(df):
            Range = df['high'] - df['low']
            EX1 = pd.ewma(Range, span = 9, min_periods = 8)
            EX2 = pd.ewma(EX1, span = 9, min_periods = 8)
            Mass = EX1 / EX2
            MassI = pd.Series(pd.rolling_sum(Mass, 25), name = 'Mass Index')
            df = df.join(MassI)
            return df
        
        
        # Vortex Indicator: http://www.vortexindicator.com/VFX_VORTEX.PDF
        def Vortex(df, n):
            i = 0
            TR = [0]
            while i < df.index[-1]:
                Range = max(df.get_value(i + 1, 'high'), df.get_value(i, 'close')) - min(df.get_value(i + 1, 'low'), df.get_value(i, 'close'))
                TR.append(Range)
                i = i + 1
            i = 0
            VM = [0]
            while i < df.index[-1]:
                Range = abs(df.get_value(i + 1, 'high') - df.get_value(i, 'low')) - abs(df.get_value(i + 1, 'low') - df.get_value(i, 'high'))
                VM.append(Range)
                i = i + 1
            VI = pd.Series(pd.rolling_sum(pd.Series(VM), n) / pd.rolling_sum(pd.Series(TR), n), name = 'Vortex_' + str(n))
            df = df.join(VI)
            return df
        
        
        # KST Oscillator
        def KST(df, r1, r2, r3, r4, n1, n2, n3, n4):
            M = df['close'].diff(r1 - 1)
            N = df['close'].shift(r1 - 1)
            ROC1 = M / N
            M = df['close'].diff(r2 - 1)
            N = df['close'].shift(r2 - 1)
            ROC2 = M / N
            M = df['close'].diff(r3 - 1)
            N = df['close'].shift(r3 - 1)
            ROC3 = M / N
            M = df['close'].diff(r4 - 1)
            N = df['close'].shift(r4 - 1)
            ROC4 = M / N
            KST = pd.Series(pd.rolling_sum(ROC1, n1) + pd.rolling_sum(ROC2, n2) * 2 + pd.rolling_sum(ROC3, n3) * 3 + pd.rolling_sum(ROC4, n4) * 4, name = 'KST_' + str(r1) + '_' + str(r2) + '_' + str(r3) + '_' + str(r4) + '_' + str(n1) + '_' + str(n2) + '_' + str(n3) + '_' + str(n4))
            df = df.join(KST)
            return df
        
        
        # True Strength Index
        def TSI(df, r, s):
            M = pd.Series(df['close'].diff(1))
            aM = abs(M)
            EMA1 = pd.Series(pd.ewma(M, span = r, min_periods = r - 1))
            aEMA1 = pd.Series(pd.ewma(aM, span = r, min_periods = r - 1))
            EMA2 = pd.Series(pd.ewma(EMA1, span = s, min_periods = s - 1))
            aEMA2 = pd.Series(pd.ewma(aEMA1, span = s, min_periods = s - 1))
            TSI = pd.Series(EMA2 / aEMA2, name = 'TSI_' + str(r) + '_' + str(s))
            df = df.join(TSI)
            return df
        
        
        # Accumulation/Distribution
        def ACCDIST(df, n):
            ad = (2 * df['close'] - df['high'] - df['low']) / (df['high'] - df['low']) * df['Volume']
            M = ad.diff(n - 1)
            N = ad.shift(n - 1)
            ROC = M / N
            AD = pd.Series(ROC, name = 'Acc/Dist_ROC_' + str(n))
            df = df.join(AD)
            return df
        
        
        # Force Index
        def FORCE(df, n):
            F = pd.Series(df['close'].diff(n) * df['Volume'].diff(n), name = 'Force_' + str(n))
            df = df.join(F)
            return df
        
        
        # Ease of Movement
        def EOM(df, p):
            if len(p)==2:
                p[1]=10
            EoM = (df['high'].diff(1) + df['low'].diff(1)) * (df['high'] - df['low']) / (2 * df['volume'])
            Eom_ma = pd.Series(EoM.rolling(p[1]).mean(), name = 'EoM_' + str(p[1]))
            
     
            
            #df = df.join(Eom_ma)
            a=2
            
            df['EMO']=Eom_ma
            
 
            
            return df
        
        
        # Coppock Curve
        def COPP(df, n):
            M = df['close'].diff(int(n * 11 / 10) - 1)
            N = df['close'].shift(int(n * 11 / 10) - 1)
            ROC1 = M / N
            M = df['close'].diff(int(n * 14 / 10) - 1)
            N = df['close'].shift(int(n * 14 / 10) - 1)
            ROC2 = M / N
            Copp = pd.Series(pd.ewma(ROC1 + ROC2, span = n, min_periods = n), name = 'Copp_' + str(n))
            df = df.join(Copp)
            return df
        
        
        # Keltner Channel
        def KELCH(df, n):
            KelChM = pd.Series(pd.rolling_mean((df['high'] + df['low'] + df['close']) / 3, n), name = 'KelChM_' + str(n))
            KelChU = pd.Series(pd.rolling_mean((4 * df['high'] - 2 * df['low'] + df['close']) / 3, n), name = 'KelChU_' + str(n))
            KelChD = pd.Series(pd.rolling_mean((-2 * df['high'] + 4 * df['low'] + df['close']) / 3, n), name = 'KelChD_' + str(n))
            df = df.join(KelChM)
            df = df.join(KelChU)
            df = df.join(KelChD)
            return df
        
        
        # Donchian Channel
        def DONCH(low, high, timeperiod: int = 20):
            if len(high) != len(low):
                return [], []
            dc_low = []
            dc_high = []
            for i in range(0, len(high)):
                if i < timeperiod - 1:
                    dc_low.append(np.nan)
                    dc_high.append(np.nan)
                else:
                    min_list = low.ix[i - (timeperiod - 1): i]
                    max_list = high.ix[i - (timeperiod - 1): i]
                    if len(min_list) == 0 or len(max_list) == 0:
                        dc_low.append(np.nan)
                        dc_high.append(np.nan)
                    else:
                        dc_min = min(min_list)
                        dc_max = max(max_list)
                        dc_low.append(dc_min)
                        dc_high.append(dc_max)
            return dc_low, dc_high
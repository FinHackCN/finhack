import talib as ta
import numpy as np
from finhack.market.astock.astock import AStock

class ta_lib:
    def BBANDS(df,p):
        if len(p)<=2:
            p=[p,90]
            
        #print(p)
        df['UPPER'], df['MIDDLE'], df['LOWER'] = ta.BBANDS(df.close, timeperiod=p[1], nbdevup=2, nbdevdn=2, matype=0)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def DEMA(df,p):
        if len(p)<=2:
            p=[p,90]
        df['DEMA']=ta.DEMA(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def EMA(df,p):
        if len(p)<=2:
            p=[p,90]
        df['EMA'] =ta.EMA(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def HT_TRENDLINE(df,p):
        df['HTTRENDLINE'] = ta.HT_TRENDLINE(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MA(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MA']=ta.MA(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MAMA(df,p):
        df['MAMA'], df['FAMA'] = ta.MAMA(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MIDPOINT(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MIDPOINT']  = ta.MIDPOINT(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MIDPRICE(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MIDPRICE']  = ta.MIDPRICE(df.high, df.low, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def SAR(df,p):
        if len(p)<=2:
            p=[p,0,0]
        df['SAR']=ta.SAR(df.high, df.low, acceleration=p[1], maximum=p[2])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def SAREXT(df,p):
        if len(p)<=2:
            p=[p,0,0,0]
        df['SAREXT']=ta.SAREXT(df.high, df.low, p[1], p[2], p[3])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def SMA(df,p):
        if len(p)<=2:
            p=[p,90]
        df['SMA']=ta.SMA(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def T3(df,p):
        if len(p)<=2:
            p=[p,90]
        df['T3'] =ta.T3(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def TEMA(df,p):
        if len(p)<=2:
            p=[p,90]
        df['TEMA']=ta.TEMA(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def TRIMA(df,p):
        if len(p)<=2:
            p=[p,90]
        df['TRIMA'] =ta.TRIMA(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def WMA(df,p):
        if len(p)<=2:
            p=[p,90]
        df['WMA']=ta.WMA(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ADX(df,p):
        if len(p)<=2:
            p=[p,90]
        df['ADX']=ta.ADX(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ADXR(df,p):
        if len(p)<=2:
            p=[p,90]
        df['ADXR']=ta.ADXR(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def APO(df,p):
        if len(p)<=2:
            p=[p,30,90]
        df['APO']=ta.APO(df.close, p[1], p[2])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def AROON(df,p):
        if len(p)<=2:
            p=[p,90]
        df['AROONDOWN'], df['ARRONUP'] = ta.AROON(df.high, df.low, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def AROONOSC(df,p):
        if len(p)<=2:
            p=[p,90]
        df['AROONOSC']     = ta.AROONOSC(df.high, df.low, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def BOP(df,p):
        if len(p)<=2:
            p=[p,90]
        df['BOP']=ta.BOP(df.open, df.high, df.low, df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def CCI(df,p):
        if len(p)<=2:
            p=[p,90]
        df['CCI']=ta.CCI(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def CMO(df,p):
        if len(p)<=2:
            p=[p,90]
        df['CMO']=ta.CMO(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def DX(df,p):
        if len(p)<=2:
            p=[p,90]
        df['DX'] =ta.DX(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MACD(df,p):
        if len(p)<=2:
            p=[p,12,26,9]
        df['MACD'], df['MACDSIGNAL'], df['MACDHIST'] = ta.MACD(df.close, fastperiod=p[1], slowperiod=p[2], signalperiod=p[3])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MACDEXT(df,p):
        if len(p)<=2:
            p=[p,12,26,9]
        df['MACDX'], df['MACDSIGNALX'], df['MACDHISTX'] = ta.MACDEXT(df.close, fastperiod=p[1], slowperiod=p[2], signalperiod=p[3])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MACDFIX(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MACDFIX'], df['MACDSIGNALFIX'], df['MACDHISTFIX'] = ta.MACDFIX(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MFI(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MFI']=ta.MFI(df.high, df.low, df.close, df.volume, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MINUS_DI(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MINUSDI']     = ta.MINUS_DI(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MINUS_DM(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MINUSDM']     = ta.MINUS_DM(df.high, df.low, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MOM(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MOM']=ta.MOM(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def PLUS_DI(df,p):
        if len(p)<=2:
            p=[p,90]
        df['PLUSDI']      = ta.PLUS_DI(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def PLUS_DM(df,p):
        if len(p)<=2:
            p=[p,90]
        df['PLUSDM']      = ta.PLUS_DM(df.high, df.low, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def PPO(df,p):
        if len(p)<=2:
            p=[p,30,90]
        df['PPO']=ta.PPO(df.close, p[1], p[2])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ROC(df,p):
        if len(p)<=2:
            p=[p,90]
        df['ROC']=ta.ROC(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ROCR(df,p):
        if len(p)<=2:
            p=[p,90]
        df['ROCR']=ta.ROCR(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ROCR100(df,p):
        if len(p)<=2:
            p=[p,90]
        df['ROCR100']      = ta.ROCR100(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def RSI(df,p):
        if len(p)<=2:
            p=[p,90]
        df['RSI']=ta.RSI(df.close,p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def STOCH(df,p):
        if len(p)<=2:
            p=[p,90]
        df['SLOWK'], df['SLOWD'] = ta.STOCH(df.high, df.low, df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def STOCHF(df,p):
        if len(p)<=2:
            p=[p,90]
        df['FASTK'], df['FASTD'] = ta.STOCHF(df.high, df.low, df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def STOCHRSI(df,p):
        if len(p)<=2:
            p=[p,90]
        df['FASTKRSI'], df['FASTDRSI'] = ta.STOCHRSI(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def TRIX(df,p):
        if len(p)<=2:
            p=[p,90]
        df['TRIX']=ta.TRIX(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ULTOSC(df,p):
        if len(p)<=2:
            p=[p,90]
        df['ULTOSC']=ta.ULTOSC(df.high, df.low, df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def WILLR(df,p):
        if len(p)<=2:
            p=[p,90]
        df['WILLR'] =ta.WILLR(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def AD(df,p):
        if len(p)<=2:
            p=[p,90]
        df['AD'] =ta.AD(df.high, df.low, df.close, df.volume)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ADOSC(df,p):
        if len(p)<=2:
            p=[p,90]
        df['ADOSC'] =ta.ADOSC(df.high, df.low, df.close, df.volume)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def OBV(df,p):
        if len(p)<=2:
            p=[p,90]
        df['OBV']=ta.OBV(df.close, df.volume)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ATR(df,p):
        if len(p)<=2:
            p=[p,90]
        df['ATR']=ta.ATR(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def NATR(df,p):
        if len(p)<=2:
            p=[p,90]
        df['NATR']=ta.NATR(df.high, df.low, df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def TRANGE(df,p):
        df['TRANGE']=ta.TRANGE(df.high, df.low, df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def AVGPRICE(df,p):
        df['AVGPRICE']     = ta.AVGPRICE(df.open, df.high, df.low, df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MEDPRICE(df,p):
        df['MEDPRICE']     = ta.MEDPRICE(df.high, df.low)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def TYPPRICE(df,p):
        df['TYPPRICE']     = ta.TYPPRICE(df.high, df.low, df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def WCLPRICE(df,p):
        df['WCLPRICE']     = ta.WCLPRICE(df.high, df.low, df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def HT_DCPERIOD(df,p):
        df['HTDCPERIOD']  = ta.HT_DCPERIOD(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def HT_DCPHASE(df,p):
        df['HTDCPHASE']   = ta.HT_DCPHASE(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def HT_PHASOR(df,p):
        df['INPHASE'], df['QUADRATURE'] = ta.HT_PHASOR(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def HT_SINE(df,p):
        df['SINE'] , df['LEADSINE'] = ta.HT_SINE(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def HT_TRENDMODE(df,p):
        df['HTTRENDMODE'] = ta.HT_TRENDMODE(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def BETA(df,p):
        if len(p)<=2:
            p=[p,90]
        df['BETA']=ta.BETA(df.high, df.low, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def CORREL(df,p):
        if len(p)<=2:
            p=[p,90]
        df['CORREL']=ta.CORREL(df.high, df.low, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def LINEARREG(df,p):
        if len(p)<=2:
            p=[p,90]
        df['LINEARREG']    = ta.LINEARREG(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def LINEARREG_ANGLE(df,p):
        if len(p)<=2:
            p=[p,90]
        df['LINEARREGANGLE'] = ta.LINEARREG_ANGLE(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def LINEARREG_INTERCEPT(df,p):
        if len(p)<=2:
            p=[p,90]
        df['LINEARREGINTERCEPT'] = ta.LINEARREG_INTERCEPT(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def LINEARREG_SLOPE(df,p):
        if len(p)<=2:
            p=[p,90]
        df['LINEARREGSLOPE'] = ta.LINEARREG_SLOPE(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def STDDEV(df,p):
        #print(p)
        if len(p)<=2:
            p=[p,90]
        df['STDDEV']=ta.STDDEV(df.close, p[1], 1)
        
        #print(df)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def TSF(df,p):
        if len(p)<=2:
            p=[p,90]
        df['TSF']=ta.TSF(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def VAR(df,p):
        if len(p)<=2:
            p=[p,90]
        df['VAR']=ta.VAR(df.close, p[1], 1)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ACOS(df,p):
        df['ACOS']=ta.ACOS(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ASIN(df,p):
        df['ASIN']=ta.ASIN(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ATAN(df,p):
        df['ATAN']=ta.ATAN(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def CEIL(df,p):
        df['CEIL']=ta.CEIL(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def COS(df,p):
        df['COS']=ta.COS(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def COSH(df,p):
        df['COSH']=ta.COSH(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def EXP(df,p):
        df['EXP']=ta.EXP(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def FLOOR(df,p):
        df['FLOOR'] =ta.FLOOR(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def LN(df,p):
        df['LN'] =ta.LN(df.close)  # Log
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def LOG10(df,p):
        df['LOG10'] =ta.LOG10(df.close)  # Log
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def SIN(df,p):
        df['SIN']=ta.SIN(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def SINH(df,p):
        df['SINH']=ta.SINH(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def SQRT(df,p):
        df['SQRT']=ta.SQRT(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def TAN(df,p):
        df['TAN']=ta.TAN(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def TANH(df,p):
        df['TANH']=ta.TANH(df.close)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def ADD(df,p):
        df['ADD']=ta.ADD(df.high, df.low)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def DIV(df,p):
        df['DIV']=ta.DIV(df.high, df.low)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MAX(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MAX']=ta.MAX(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MAXINDEX(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MAXINDEX']     = ta.MAXINDEX(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MIN(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MIN']=ta.MIN(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MININDEX(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MININDEX']     = ta.MININDEX(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MINMAXINDEX(df,p):
        if len(p)<=2:
            p=[p,90]
        df['MINIDX'], df['MAXIDX'] = ta.MINMAXINDEX(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def MULT(df,p):
        df['MULT']=ta.MULT(df.high, df.low)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def SUB(df,p):
        df['SUB']=ta.SUB(df.high, df.low)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df

    def SUM(df,p):
        if len(p)<=2:
            p=[p,90]
        df['SUM']=ta.SUM(df.close, p[1])
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df






    def kline(df,p):
        df['2CROWS'] = ta.CDL2CROWS(df.open, df.high, df.low, df.close)
        df['3BLACKCROWS'] = ta.CDL3BLACKCROWS(df.open, df.high, df.low, df.close)
        df['3INSIDE'] = ta.CDL3INSIDE(df.open, df.high, df.low, df.close)
        df['3LINESTRIKE'] = ta.CDL3LINESTRIKE(df.open, df.high, df.low, df.close)
        df['3OUTSIDE'] = ta.CDL3OUTSIDE(df.open, df.high, df.low, df.close)
        df['3STARSINSOUTH'] = ta.CDL3STARSINSOUTH(df.open, df.high, df.low, df.close)
        df['3WHITESOLDIERS'] = ta.CDL3WHITESOLDIERS(df.open, df.high, df.low, df.close)
        df['ABANDONEDBABY'] = ta.CDLABANDONEDBABY(df.open, df.high, df.low, df.close)
        df['ADVANCEBLOCK'] = ta.CDLADVANCEBLOCK(df.open, df.high, df.low, df.close)
        df['BELTHOLD'] = ta.CDLBELTHOLD(df.open, df.high, df.low, df.close)
        df['BREAKAWAY'] = ta.CDLBREAKAWAY(df.open, df.high, df.low, df.close)
        df['CLOSINGMARUBOZU'] = ta.CDLCLOSINGMARUBOZU(df.open, df.high, df.low, df.close)
        df['CONCEALBABYSWALL'] = ta.CDLCONCEALBABYSWALL(df.open, df.high, df.low, df.close)
        df['COUNTERATTACK'] = ta.CDLCOUNTERATTACK(df.open, df.high, df.low, df.close)
        df['DARKCLOUDCOVER'] = ta.CDLDARKCLOUDCOVER(df.open, df.high, df.low, df.close)
        df['DOJI'] = ta.CDLDOJI(df.open, df.high, df.low, df.close)
        df['DOJISTAR'] = ta.CDLDOJISTAR(df.open, df.high, df.low, df.close)
        df['DRAGONFLYDOJI'] = ta.CDLDRAGONFLYDOJI(df.open, df.high, df.low, df.close)
        df['ENGULFING'] = ta.CDLENGULFING(df.open, df.high, df.low, df.close)
        df['DOJISTAR'] = ta.CDLDOJISTAR(df.open, df.high, df.low, df.close)
        df['EVENINGSTAR'] = ta.CDLEVENINGSTAR(df.open, df.high, df.low, df.close)
        df['GAPSIDESIDEWHITE'] = ta.CDLGAPSIDESIDEWHITE(df.open, df.high, df.low, df.close)
        df['GRAVESTONEDOJI'] = ta.CDLGRAVESTONEDOJI(df.open, df.high, df.low, df.close)
        df['HAMMER'] = ta.CDLHAMMER(df.open, df.high, df.low, df.close)
        df['HANGINGMAN'] = ta.CDLHANGINGMAN(df.open, df.high, df.low, df.close)
        df['HARAMI'] = ta.CDLHARAMI(df.open, df.high, df.low, df.close)
        df['HARAMICROSS'] = ta.CDLHARAMICROSS(df.open, df.high, df.low, df.close)
        df['HIGHWAVE'] = ta.CDLHIGHWAVE(df.open, df.high, df.low, df.close)
        df['HIKKAKE'] = ta.CDLHIKKAKE(df.open, df.high, df.low, df.close)
        df['HIKKAKEMOD'] = ta.CDLHIKKAKEMOD(df.open, df.high, df.low, df.close)
        df['HOMINGPIGEON'] = ta.CDLHOMINGPIGEON(df.open, df.high, df.low, df.close)
        df['IDENTICAL3CROWS'] = ta.CDLIDENTICAL3CROWS(df.open, df.high, df.low, df.close)
        df['INNECK'] = ta.CDLINNECK(df.open, df.high, df.low, df.close)
        df['INVERTEDHAMMER'] = ta.CDLINVERTEDHAMMER(df.open, df.high, df.low, df.close)
        df['KICKING'] = ta.CDLKICKING(df.open, df.high, df.low, df.close)
        df['KICKINGBYLENGTH'] = ta.CDLKICKINGBYLENGTH(df.open, df.high, df.low, df.close)
        df['LADDERBOTTOM'] = ta.CDLLADDERBOTTOM(df.open, df.high, df.low, df.close)
        df['LONGLEGGEDDOJI'] = ta.CDLLONGLEGGEDDOJI(df.open, df.high, df.low, df.close)
        df['LONGLINE'] = ta.CDLLONGLINE(df.open, df.high, df.low, df.close)
        df['MARUBOZU'] = ta.CDLMARUBOZU(df.open, df.high, df.low, df.close)
        df['MATCHINGLOW'] = ta.CDLMATCHINGLOW(df.open, df.high, df.low, df.close)
        df['MATHOLD'] = ta.CDLMATHOLD(df.open, df.high, df.low, df.close)
        df['MORNINGDOJISTAR'] = ta.CDLMORNINGDOJISTAR(df.open, df.high, df.low, df.close)
        df['MORNINGSTAR'] = ta.CDLMORNINGSTAR(df.open, df.high, df.low, df.close)
        df['ONNECK'] = ta.CDLONNECK(df.open, df.high, df.low, df.close)
        df['PIERCING'] = ta.CDLPIERCING(df.open, df.high, df.low, df.close)
        df['RICKSHAWMAN'] = ta.CDLRICKSHAWMAN(df.open, df.high, df.low, df.close)
        df['RISEFALL3METHODS'] = ta.CDLRISEFALL3METHODS(df.open, df.high, df.low, df.close)
        df['SEPARATINGLINES'] = ta.CDLSEPARATINGLINES(df.open, df.high, df.low, df.close)
        df['SHOOTINGSTAR'] = ta.CDLSHOOTINGSTAR(df.open, df.high, df.low, df.close)
        df['SHORTLINE'] = ta.CDLSHORTLINE(df.open, df.high, df.low, df.close)
        df['SPINNINGTOP'] = ta.CDLSPINNINGTOP(df.open, df.high, df.low, df.close)
        df['STALLEDPATTERN'] = ta.CDLSTALLEDPATTERN(df.open, df.high, df.low, df.close)
        df['STICKSANDWICH'] = ta.CDLSTICKSANDWICH(df.open, df.high, df.low, df.close)
        df['TAKURI'] = ta.CDLTAKURI(df.open, df.high, df.low, df.close)
        df['TASUKIGAP'] = ta.CDLTASUKIGAP(df.open, df.high, df.low, df.close)
        df['THRUSTING'] = ta.CDLTHRUSTING(df.open, df.high, df.low, df.close)
        df['TRISTAR'] = ta.CDLTRISTAR(df.open, df.high, df.low, df.close)
        df['UNIQUE3RIVER'] = ta.CDLUNIQUE3RIVER(df.open, df.high, df.low, df.close)
        df['UPSIDEGAP2CROWS'] = ta.CDLUPSIDEGAP2CROWS(df.open, df.high, df.low, df.close)
        df['XSIDEGAP3METHODS'] = ta.CDLXSIDEGAP3METHODS(df.open, df.high, df.low, df.close) 
        df['EVENINGDOJISTAR']=ta.CDLEVENINGDOJISTAR(df.open, df.high, df.low, df.close) 
        return df




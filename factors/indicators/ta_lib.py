import talib as ta
import numpy as np
from library.astock import AStock

class ta_lib:

    # def macd(df,p):
    #     if len(p)==1:
    #         p=[p,12,26,9]
    #     df['dif'], df['dea'], df['macd'] = ta.MACD(df.close, fastperiod=p[1], slowperiod=p[2], signalperiod=p[3])
    #     return df

    # def adx(df,p):
    #     if len(p)==1:
    #         p=[p,90]
    #     df['adx'] = ta.ADXR(df.high, df.low, df.close, p[1])
    #     return df

    # def adxr(df,p):
    #     if len(p)==1:
    #         p=[p,90]
    #     df['adxr'] = ta.ADXR(df.high, df.low, df.close, p[1])
    #     return df

    # def apo(df,p):
    #     if len(p)==1:
    #         p=[p,12,26]
    #     df['apo'] = ta.APO(df.close, p[1], p[2])
    #     return df

    # def aroon(df,p):
    #     if len(p)==1:
    #         p=[p,90]
    #     df['aroondown'], df['aroonup'] = ta.AROON(df.high, df.low, p[1])
    #     return df

    # def boll(df,p):
    #     df['bollup'], df['bollmid'], df['bolldown'] = ta.BBANDS(df.close, timeperiod=90, nbdevup=2, nbdevdn=2, matype=0) 
    #     return df

    def isOnBollDown(df,P):
        # df['isOnBollDown']=np.sign(df['close_0']df['bolldown_90_2_2_0_0'])  
        return df

    def rim(df,p):
        df_rim=AStock.alignStockFactors(df,'stock_finhack_rim','date',filed='name,industry,value,value_end,value_max,vp,vep,vmp,rcount',conv=1,db='finhack')
        df['rimn']=df_rim['name'].astype("string")
        df['rimi']=df_rim['industry'].astype("string")
        df['rimv']=df_rim['value']
        df['rimve']=df_rim['value_end']
        df['rimvm']=df_rim['value_max']
        df['rimvp']=df_rim['vp']
        df['rimvep']=df_rim['vep']
        df['rimvmp']=df_rim['vmp']
        df['rimrc']=df_rim['rcount']
        return df

    def basic(df,p): 
        df_basic=AStock.alignStockFactors(df,'astock_price_daily_basic','trade_date',filed='turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv',conv=0,db='tushare')
        df['turnoverRate']=df_basic['turnover_rate']
        df['turnoverRatef']=df_basic['turnover_rate_f']
        df['volumeRatio']=df_basic['volume_ratio']
        df['pe']=df_basic['pe']
        df['peTtm']=df_basic['pe_ttm']
        df['pb']=df_basic['pb']
        df['ps']=df_basic['ps']
        df['psTtm']=df_basic['ps_ttm']
        df['dvRatio']=df_basic['dv_ratio']
        df['dvTtm']=df_basic['dv_ttm']
        df['totalShare']=df_basic['total_share']
        df['floatShare']=df_basic['float_share']
        df['freeShare']=df_basic['free_share']
        df['totalMv']=df_basic['total_mv']
        df['circMv']=df_basic['circ_mv']
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

    def gettalib(df,p):
            ## TAlib.Generate Indicators
    
        ## Overlap Studies Functions
        df['UPPER'], df['MIDDLE'], df['LOWER'] = ta.BBANDS(df.close, timeperiod=90, nbdevup=2, nbdevdn=2, matype=0)
        df['DEMA']         = ta.DEMA(df.close, 90)
        df['EMA90']        = ta.EMA(df.close, 90)
        df['HTTRENDLINE'] = ta.HT_TRENDLINE(df.close)
        df['MA90']         = ta.MA(df.close, 90)
        df['MAMA'], df['FAMA'] = ta.MAMA(df.close)
        try:
            df['MAVP']         = ta.MAVP(df.close, df.EMA90)
        except Exception as e:
            df['MAVP'] =np.nan
            
        df['MIDPOINT']     = ta.MIDPOINT(df.close, 90)
        df['MIDPRICE']     = ta.MIDPRICE(df.high, df.low, 90)
        df['SAR']          = ta.SAR(df.high, df.low, acceleration=0, maximum=0)
        df['SAREXT']       = ta.SAREXT(df.high, df.low, 0, 0, 0)
        df['SMA']          = ta.SMA(df.close, 90)
        df['T3']           = ta.T3(df.close, 90)
        df['TEMA']         = ta.TEMA(df.close, 90)
        df['TRIMA']        = ta.TRIMA(df.close, 90)
        df['WMA']          = ta.WMA(df.close, 90)
    
        ## Momentum Indicator Funnctions
        df['ADX']          = ta.ADX(df.high, df.low, df.close, 90)
        df['ADXR']         = ta.ADXR(df.high, df.low, df.close, 90)
        df['APO']          = ta.APO(df.close, 30, 90)
        df['AROONDOWN'], df['ARRONUP'] = ta.AROON(df.high, df.low, 90)
        df['AROONOSC']     = ta.AROONOSC(df.high, df.low, 90)
        df['BOP']          = ta.BOP(df.open, df.high, df.low, df.close)
        df['CCI']          = ta.CCI(df.high, df.low, df.close, 90)
        df['CMO']          = ta.CMO(df.close, 90)
        df['DX']           = ta.DX(df.high, df.low, df.close, 90)
        df['MACD'], df['MACDSIGNAL'], df['MACDHIST'] = ta.MACD(df.close, fastperiod=12, slowperiod=26, signalperiod=9)
        df['MACDX'], df['MACDSIGNALX'], df['MACDHISTX'] = ta.MACDEXT(df.close, fastperiod=12, slowperiod=26, signalperiod=9)
        df['MACDFIX'], df['MACDSIGNALFIX'], df['MACDHISTFIX'] = ta.MACDFIX(df.close, 90)
        df['MFI']          = ta.MFI(df.high, df.low, df.close, df.volume, 90)
        df['MINUSDI']     = ta.MINUS_DI(df.high, df.low, df.close, 90)
        df['MINUSDM']     = ta.MINUS_DM(df.high, df.low, 90)
        df['MOM']          = ta.MOM(df.close, 90)
        df['PLUSDI']      = ta.PLUS_DI(df.high, df.low, df.close, 90)
        df['PLUSDM']      = ta.PLUS_DM(df.high, df.low, 90)
        df['PPO']          = ta.PPO(df.close, 30, 90)
        df['ROC']          = ta.ROC(df.close, 90)
        df['ROCR']         = ta.ROCR(df.close, 90)
        df['ROCR100']      = ta.ROCR100(df.close, 90)
        df['RSI']          = ta.RSI(df.close,90)
        df['SLOWK'], df['SLOWD'] = ta.STOCH(df.high, df.low, df.close)
        df['FASTK'], df['FASTD'] = ta.STOCHF(df.high, df.low, df.close)
        df['FASTKRSI'], df['FASTDRSI'] = ta.STOCHRSI(df.close, 90)
        df['TRIX']         = ta.TRIX(df.close, 90)
        df['ULTOSC']       = ta.ULTOSC(df.high, df.low, df.close)
        df['WILLR']        = ta.WILLR(df.high, df.low, df.close, 90)
    
        ## Volume Indicator Functions
        df['AD']           = ta.AD(df.high, df.low, df.close, df.volume)
        df['ADOSC']        = ta.ADOSC(df.high, df.low, df.close, df.volume)
        df['OBV']          = ta.OBV(df.close, df.volume)
    
        ## Volatility Indicator Functions
        df['ATR']          = ta.ATR(df.high, df.low, df.close, 90)
        df['NATR']         = ta.NATR(df.high, df.low, df.close, 90)
        df['TRANGE']       = ta.TRANGE(df.high, df.low, df.close)
    
        ## Price Transform Functions
        df['AVGPRICE']     = ta.AVGPRICE(df.open, df.high, df.low, df.close)
        df['MEDPRICE']     = ta.MEDPRICE(df.high, df.low)
        df['TYPPRICE']     = ta.TYPPRICE(df.high, df.low, df.close)
        df['WCLPRICE']     = ta.WCLPRICE(df.high, df.low, df.close)
    
        ## Cycle Indicator Functions
        df['HTDCPERIOD']  = ta.HT_DCPERIOD(df.close)
        df['HTDCPHASE']   = ta.HT_DCPHASE(df.close)
        df['INPHASE'], df['QUADRATURE'] = ta.HT_PHASOR(df.close)
        df['SINE'] , df['LEADSINE'] = ta.HT_SINE(df.close)
        df['HTTRENDMODE'] = ta.HT_TRENDMODE(df.close)
    
        ## Beta
        df['BETA']         = ta.BETA(df.high, df.low, 90)
        df['CORREL']       = ta.CORREL(df.high, df.low, 90)
        df['LINEARREG']    = ta.LINEARREG(df.close, 90)
        df['LINEARREGANGLE'] = ta.LINEARREG_ANGLE(df.close, 90)
        df['LINEARREGINTERCEPT'] = ta.LINEARREG_INTERCEPT(df.close, 90)
        df['LINEARREGSLOPE'] = ta.LINEARREG_SLOPE(df.close, 90)
        df['STDDEV']       = ta.STDDEV(df.close, 90, 1)
        df['TSF']          = ta.TSF(df.close, 90)
        df['VAR']          = ta.VAR(df.close, 90, 1)
    
        ## Math Transform Functions
        df['ACOS']         = ta.ACOS(df.close)
        df['ASIN']         = ta.ASIN(df.close)
        df['ATAN']         = ta.ATAN(df.close)
        df['CEIL']         = ta.CEIL(df.close)
        df['COS']          = ta.COS(df.close)
        df['COSH']         = ta.COSH(df.close)
        df['EXP']          = ta.EXP(df.close)
        df['FLOOR']        = ta.FLOOR(df.close)
        df['LN']           = ta.LN(df.close)  # Log
        df['LOG10']        = ta.LOG10(df.close)  # Log
        df['SIN']          = ta.SIN(df.close)
        df['SINH']         = ta.SINH(df.close)
        df['SQRT']         = ta.SQRT(df.close)
        df['TAN']          = ta.TAN(df.close)
        df['TANH']         = ta.TANH(df.close)
    
        ## Math Operator Functions
        df['ADD']          = ta.ADD(df.high, df.low)
        df['DIV']          = ta.DIV(df.high, df.low)
        df['MAX']          = ta.MAX(df.close, 90)
        df['MAXINDEX']     = ta.MAXINDEX(df.close, 90)
        df['MIN']          = ta.MIN(df.close, 90)
        df['MININDEX']     = ta.MININDEX(df.close, 90)
        df['MINIDX'], df['MAXIDX'] = ta.MINMAXINDEX(df.close, 90)
        df['MULT']         = ta.MULT(df.high, df.low)
        df['SUB']          = ta.SUB(df.high, df.low)
        df['SUM']          = ta.SUM(df.close, 90)
        df = df.replace([np.inf, -np.inf], np.nan) 
        return df


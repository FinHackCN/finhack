# import numpy as np

# class QIML365:
# #QIML365 2022-01-16
# #累计震动升降指标
# #当ASI为正，说明趋势会继续，当ASI为负，说明趋势会终结
#     def ASI(df,p):
#         def SI(N):
#             df_c=df.copy()
#             df_c=df_c.shift(N)
#             close_1=df_c['close'].shift(1)
#             open_1=df_c['open'].shift(1)
#             low_1=df_c['low'].shift(1)
#             A=abs(df_c.high-close_1)
#             B=abs(df_c.low-close_1)
#             C=abs(df_c.high-low_1)
#             D=abs(close_1-open_1)
#             E=df_c.close-close_1
#             F=df_c.close-df_c.open
#             G=close_1-open_1
#             X=E+0.5+G
#             K=max(A,B)
#             R=np.where(A>B and A>C,A+0.5*B+0.25*D,np.where(B>A and B>C,B+0.5*A+0.25*D,C+0.25*D))
#             SI=16*X/R+K
#             return SI
        
#         if len(p)==1:
#             p=[p,20]
#             N=p[1]
            
#         ASI=0
#         for i in range(0,n):
#             ASI=ASI+SI
        
#         df['ASI']=ASI
#         return df
        
#     def is_ASI(df,p):
#         is_ASI=np.where(df['ASI']>0,True,False)
#         df['isASI']=ASI
#         return df
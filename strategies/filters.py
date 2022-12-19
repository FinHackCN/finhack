class filters:
    #只保留主版本
    def only_main(df):
        df=df[df["ts_code"].str.startswith("00") | df["ts_code"].str.startswith("60")]
        return df
        
    def only_main_not_st(df):
        df=df[df["ts_code"].str.startswith("00") | df["ts_code"].str.startswith("60")]
        print(df)
        exit()
        return df
class filters:
    #只保留主版本
    def only_main(df):
        df=df[df["ts_code"].str.startswith("00")]
        return df
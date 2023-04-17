rm -f /data/code/finhack//data/cache/factors/*
rm -f /data/code/finhack/data/cache/price/*
rm -f /data/code/finhack/data/factors/single_factors/*
rm -f /data/code/finhack/data/factors/code_factors/*
rm -f /data/code/finhack/data/cache/single_factors_tmp1/*
rm -f /data/code/finhack/data/cache/single_factors_tmp2/*
rm -rf /data/code/finhack/data/factors/date_factors/*
rm -f /data/code/finhack/data/logs/*.log
rm -f /data/code/finhack/data/models/*
find  /data/code/finhack/data/logs/backtest/  -type f -name "*.*" | xargs rm -f


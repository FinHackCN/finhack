rm -f /data/cache/factors/*
rm -f data/cache/price/*
rm -f data/factors/single_factors/*
rm -f data/factors/code_factors/*
rm -f data/cache/single_factors_tmp1/*
rm -f data/cache/single_factors_tmp2/*
rm -rf data/factors/date_factors/*
rm -f data/logs/*.log
rm -f data/models/*
find  data/logs/backtest/  -type f -name "*.*" | xargs rm -f


#source /root/anaconda3/etc/profile.d/conda.sh
source /root/.bashrc
conda activate finhack
/root/anaconda3/envs/finhack/bin/python -u /root/anaconda3/envs/finhack/bin/finhack backtest run --project_path=/data/code/demo_project --background
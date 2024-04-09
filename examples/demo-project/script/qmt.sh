#source /root/anaconda3/etc/profile.d/conda.sh
# source /root/.bashrc
# conda activate finhack
cd /data/code/demo_project
/root/anaconda3/envs/finhack/bin/python -u /root/anaconda3/envs/finhack/bin/finhack trader run \
 --project_path=/data/code/demo_project/ \
  --vendor=qmt --id=r_ad6709f1d13f642e16d74a40190d06c5 \
 --strategy=QMTStrategy --model_id=929526e7c5f8807a04205c17cc95b742  \
 --params='{"stocknum": "5", "refresh_rate": "5", "stop_loss_threshold": "0.95", "stop_gain_threshold": "1.2"}'  >>/tmp/finhack_qmt.log
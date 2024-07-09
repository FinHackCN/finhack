#source /root/anaconda3/etc/profile.d/conda.sh
# source /root/.bashrc
# conda activate finhack
cd /data/code/demo_project
/root/anaconda3/envs/finhack/bin/python -u /root/anaconda3/envs/finhack/bin/finhack trader run \
 --project_path=/data/code/demo_project/ \
 --vendor=qmt --id=r_golden \
 --strategy=QMTStrategy --model_id=97b190dd39d86b464dd76597cef2dc48 \
 --params='{"stocknum": "30", "refresh_rate": "3", "stop_loss_threshold": "0.9", "stop_gain_threshold": "1.3"}'  >>/tmp/finhack_qmt.log
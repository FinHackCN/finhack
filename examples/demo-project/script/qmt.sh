#source /root/anaconda3/etc/profile.d/conda.sh
# source /root/.bashrc
# conda activate finhack
cd /data/code/demo_project
/root/anaconda3/envs/finhack/bin/python -u /root/anaconda3/envs/finhack/bin/finhack trader run \
 --project_path=/data/code/demo_project/ \
  --vendor=qmt --id=qmt001 \
 --strategy=QMTStrategy --model_id=b6ae6db48944caf7f0138452701bcdd1  \
 --params='{"stocknum": "5", "refresh_rate": "3"}'  >>/tmp/finhack_qmt.log
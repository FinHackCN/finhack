<template>
  <d2-container>
    <template slot="header">回测详情</template>
    <template>
      
    <div>
    <el-row :gutter="20">
      <el-col :span="6">
        <div>
          <el-statistic title="夏普比率">
            <template slot="formatter"> {{ sharpe }} </template>
          </el-statistic>
        </div>
      </el-col>
      <el-col :span="6">
      </el-col>
    </el-row>
    
    <el-row :gutter="20">
								<div class="card-body">
									<div class="chart-container" style="min-height: 375px"><div class="chartjs-size-monitor" style="position: absolute; inset: 0px; overflow: hidden; pointer-events: none; visibility: hidden; z-index: -1;"><div class="chartjs-size-monitor-expand" style="position:absolute;left:0;top:0;right:0;bottom:0;overflow:hidden;pointer-events:none;visibility:hidden;z-index:-1;"><div style="position:absolute;width:1000000px;height:1000000px;left:0;top:0"></div></div><div class="chartjs-size-monitor-shrink" style="position:absolute;left:0;top:0;right:0;bottom:0;overflow:hidden;pointer-events:none;visibility:hidden;z-index:-1;"><div style="position:absolute;width:200%;height:200%;left:0; top:0"></div></div></div>
										<canvas id="bt-chart" width="408" height="375" style="display: block; width: 408px; height: 375px;" class="chartjs-render-monitor"></canvas>
									</div>
                </div>
    </el-row>
    
  </div>   
      
    </template>
  </d2-container>
</template>

<script>
import Chart from 'chart.js'
import * as api from './api'
import {drawChart} from './btchart'

export default {
  name: 'page1',
  data () {
    return {
      sharpe:0
    }
  },
  methods: {

  },
  mounted() {
    api.GetDetail(this.$route.query.id).then((res)=>{
      const bt=res.data
      this.sharpe=bt.sharpe
      

      const rObject = JSON.parse(bt.returns)
      const date_t = Object.keys(rObject);  // 获取键列表
      const returns = Object.values(rObject);  // 获取值列表
      
      const bObject = JSON.parse(bt.benchreturns)
      const benchreturns = Object.values(bObject); 
      
      var date_list=[]
      for(var i=0;i<date_t.length;i++){
        const date = new Date(parseInt(date_t[i]));
        const year = date.getFullYear();
        const month = ("0" + (date.getMonth() + 1)).slice(-2);
        const day = ("0" + date.getDate()).slice(-2);
        const dt=year + "-" + month + "-" + day;
        date_list.push(dt)
      }
      
      
      var sr=[]
      var br=[]
      var er=[]
      
      var srv=1
      for (var i=0;i<returns.length;i++){
        if (returns[i]==null || returns[i]==0 ||isNaN(parseFloat(returns[i]))){
         returns[i]=1
        }
        srv=srv*(1+returns[i])
        sr.push(srv-1)
      }
      
      
      var brv=1
      for (var i=0;i<benchreturns.length;i++){
        if (benchreturns[i]==null || benchreturns[i]==0 ||isNaN(parseFloat(benchreturns[i]))){
          benchreturns[i]=1
        }
        brv=brv*(1+benchreturns[i])
        br.push(brv-1)
      }      
      
      for (var i=0;i<sr.length;i++){
        er.push(sr[i]-br[i])
      }
      

      
      drawChart(date_list,sr,br,er)
    })
    

  }
}
</script>

<template>
  <d2-container>
    
    
    
    <template slot="header">回测详情</template>
    <template>
      
    <div>



  <el-row :gutter="20">
    <el-col :span="10"><div class="grid-content bg-purple">
        
      <el-row>
      
        <el-button type="primary">保存策略</el-button>
        <el-button type="primary">运行策略</el-button>
      
      </el-row>
      
      <el-row>
          <CommonEditor
            :value="code"
            language="python"
            @input="changeTextarea"
            style="height: 80vh"
          ></CommonEditor>
      </el-row>
 
      
    </div></el-col>
    
    
    
    <el-col :span="10"><div class="grid-content bg-purple">
      
          <el-row>  
          
         
            <div class="demo-input-size">
                  <el-form :inline="true"  class="demo-form-inline">
                     <el-form-item label="活动区域">
               <el-date-picker
            type="date"
            placeholder="选择日期">
          </el-date-picker>
          </el-form-item>
     <el-form-item label="活动区域">
              <el-input  placeholder="审批人"  size="small"></el-input>
    </el-form-item>
      <el-form-item label="活动区域">
              <el-button type="primary" size="small">查询</el-button>
     </el-form-item>
          </el-form>
    </div>
     </el-row>  
      
      
      
    </div></el-col>
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


import CommonEditor from './CommonEditor.vue'
 
export default {
  components: { CommonEditor },
  data() {
    return {
        code: 'select * from Data'
    }
  },
  methods: {
    changeTextarea(val) {
      this.code = val
    }
  },
}


</script>


<style type="text/css">
  .el-row {
    margin-bottom: 20px;
  }
</style>
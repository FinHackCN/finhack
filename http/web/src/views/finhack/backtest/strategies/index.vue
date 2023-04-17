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
  </div>   
      
    </template>
  </d2-container>
</template>

<script>
import * as api from './api'
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
    api.test().then((res)=>{
 

    })
    

  }
}
</script>

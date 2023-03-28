<template>
  <div>
    <img
      src="./image/d2-help-button@2x.png"
      style="width: 138px;"
      @click="dialogVisible = true">
    <el-dialog
      title="帮助"
      width="800px"
      :visible.sync="dialogVisible"
      :append-to-body="true">
      <div style="margin-top: -25px; margin-bottom: -25px;">
        <el-button-group class="d2-mb">
          <el-button @click="$open('https://github.com/greper/d2-crud-plus')">
            <d2-icon name="github" class="d2-mr-5 "/>
            主页
          </el-button>
          <el-button @click="$open('http://qiniu.veryreader.com/D2CrudPlusExample/index.html')">
            <d2-icon name="book" class="d2-mr-5"/>
            示例
          </el-button>
          <el-button @click="$open('https://greper.github.io/d2-crud-plus/')">
            <d2-icon name="book" class="d2-mr-5"/>
            文档(Git)
          </el-button>
          <el-button @click="$open('http://greper.gitee.io/d2-crud-plus/')">
            <d2-icon name="book" class="d2-mr-5"/>
            文档(码云)
          </el-button>
          <el-button @click="$open('https://fairyever.com/d2-admin/doc/zh/')">
            <d2-icon name="book" class="d2-mr-5"/>
            d2-admin 文档
          </el-button>
          <el-button @click="$open('https://fairyever.com/d2-admin/doc/zh/ecosystem-d2-crud/')">
            <d2-icon name="book" class="d2-mr-5"/>
            d2-crud 文档
          </el-button>
        </el-button-group>
        <el-row :gutter="20">
<!--          <el-col :span="12">-->
<!--            <el-alert :closable="false" type="info" title="QQ群 [ 1500+ / 2000 人 ]" class="d2-mb"/>-->
<!--            <img src="./image/qq.jpg" style="width: 100%;">-->
<!--          </el-col>-->
          <el-col :span="8">
            <el-alert :closable="false" type="info" title="微信加作者好友寻求帮助" class="d2-mb"/>
            <img src="./image/we.png" style="width: 100%;">
          </el-col>
        </el-row>
      </div>
    </el-dialog>
  </div>
</template>

<script>
export default {
  data () {
    return {
      dialogVisible: false
    }
  }
}
</script>

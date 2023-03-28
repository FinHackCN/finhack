<template>
  <div>
    <el-form ref="form" :model="form" :rules="rules" label-width="80px">
      <el-form-item label="标题" prop="title">
        <el-input v-model="form.title"></el-input>
      </el-form-item>
      <el-form-item label="key值" prop="key">
        <el-input v-model="form.key"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="onSubmit">立即创建</el-button>
        <el-button>取消</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import * as api from '../api'

export default {
  name: 'addTabs',
  inject: ['refreshView'],
  data () {
    return {
      form: {
        title: null,
        key: null
      },
      rules: {
        title: [
          {
            required: true,
            message: '请输入'
          }
        ],
        key: [
          {
            required: true,
            message: '请输入'
          },
          {
            pattern: /^[A-Za-z0-9]+$/,
            message: '只能是英文和数字'
          }
        ]
      }
    }
  },
  methods: {
    onSubmit () {
      const that = this
      that.$refs.form.validate((valid) => {
        if (valid) {
          api.createObj(that.form).then(res => {
            this.$message.success('新增成功')
            this.refreshView()
          })
        } else {
          console.log('error submit!!')
          return false
        }
      })
    }
  }
}
</script>

<style scoped>

</style>

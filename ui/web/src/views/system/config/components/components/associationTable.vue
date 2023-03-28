<template>
  <div>
    <el-form :model="formObj" ref="association">
      <el-form-item label="关联表" prop="table" :rules="[
      { required: true, message: '必填项', trigger: 'blur' }
    ]">
        <el-select v-model="formObj.table" filterable clearable placeholder="请选择" @change="handleChange">
          <el-option
            v-for="item in tableOptions"
            :key="item.table"
            :label="item.tableName"
            :value="item.table">
            <span style="float: left">{{ item.tableName }}</span>
            <span style="float: right; color: #8492a6; font-size: 13px">{{ item.table }}</span>
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="显示字段" prop="field" :rules="[
      { required: true, message: '必填项', trigger: 'blur' }
    ]">
        <el-select v-model="formObj.field" filterable clearable placeholder="请选择">
          <el-option
            v-for="item in labelOptions"
            :key="item.table"
            :label="item.title"
            :value="item.field">
            <span style="float: left">{{ item.field }}</span>
            <span style="float: right; color: #8492a6; font-size: 13px">{{ item.title }}</span>
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="储存字段" prop="primarykey" :rules="[
      { required: true, message: '必填项', trigger: 'blur' }
    ]">
        <el-select v-model="formObj.primarykey" filterable clearable placeholder="请选择">
          <el-option
            v-for="(item,index) in labelOptions"
            :key="index"
            :label="item.title"
            :value="item.field">
            <span style="float: left">{{ item.field }}</span>
            <span style="float: right; color: #8492a6; font-size: 13px">{{ item.title }}</span>
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="过滤条件" prop="oldSearchField" :rules="[
      { required: true, message: '必填项', trigger: 'blur' }
    ]">
        <el-select v-model="formObj.oldSearchField" multiple filterable clearable placeholder="请选择"
                   @change="handleSearch">
          <el-option
            v-for="(item,index) in labelOptions"
            :key="index"
            :label="item.title"
            :value="item.field">
            <span style="float: left">{{ item.field }}</span>
            <span style="float: right; color: #8492a6; font-size: 13px">{{ item.title }}</span>
          </el-option>
        </el-select>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import * as api from '../../api'

export default {
  name: 'associationTable',
  props: {
    value: {
      type: Object
    }
  },
  model: {
    prop: 'value',
    event: 'updateVal'
  },
  data () {
    return {
      formObj: {
        table: null,
        primarykey: null,
        field: null,
        searchField: null,
        oldSearchField: null
      },
      searchField: null,
      tableOptions: [],
      labelOptions: []
    }
  },
  methods: {
    // 初始化数据
    init () {
      api.GetAssociationTable().then(res => {
        const { data } = res
        this.tableOptions = data
        // 设置默认选中
        this.formObj.table = data[0].table
        this.labelOptions = data[0].tableFields
        this.formObj.primarykey = 'id'
        this.formObj.field = 'id'
      })
    },
    // 选中事件
    handleChange (val) {
      const that = this
      const { tableFields } = that.tableOptions.find(item => {
        return item.table === val
      })
      that.labelOptions = tableFields
    },
    // 过滤条件选中
    handleSearch (val) {
      const that = this
      const fields = that.labelOptions.filter(item => {
        return val.indexOf(item.field) > -1
      })
      that.formObj.searchField = fields
    },
    // 更新数据
    handleUpdate () {
      this.$emit('updateVal', this.formObj)
    },
    // 数据验证
    onSubmit () {
      let res = false
      this.$refs.association.validate((valid) => {
        if (valid) {
          res = true
        } else {
          return false
        }
      })
      return res
    }
  },
  created () {
    this.init()
  }
}
</script>

<style scoped>

</style>

<template>
  <d2-container :class="{ 'page-compact': crud.pageOptions.compact }">
    <d2-crud-x
      ref="d2Crud"
      v-bind="_crudProps"
      v-on="_crudListeners"
      @resetPassword="resetPassword"
    >
      <div slot="header">
        <crud-search
          ref="search"
          :options="crud.searchOptions"
          @submit="handleSearch"
        />
        <el-button-group>
          <el-button
            size="small"
            v-permission="'Create'"
            type="primary"
            @click="addRow"
          >
            <i class="el-icon-plus" /> 新增
          </el-button>
          <el-button size="small" type="danger" @click="batchDelete">
            <i class="el-icon-delete"></i> 批量删除
          </el-button>
          <el-button
            size="small"
            type="warning"
            @click="onExport"
            v-permission="'Export'"
            ><i class="el-icon-download" /> 导出
          </el-button>
          <importExcel
            api="api/system/user/"
            v-permission="'Import'"
            >导入
          </importExcel>
        </el-button-group>
        <crud-toolbar
          :search.sync="crud.searchOptions.show"
          :compact.sync="crud.pageOptions.compact"
          :columns="crud.columns"
          @refresh="doRefresh()"
          @columns-filter-changed="handleColumnsFilterChanged"
        />
      </div>
      <span slot="PaginationPrefixSlot" class="prefix">
        <el-button
          class="square"
          size="mini"
          title="批量删除"
          @click="batchDelete"
          icon="el-icon-delete"
          :disabled="!multipleSelection || multipleSelection.length == 0"
        />
      </span>
    </d2-crud-x>
    <el-dialog
      title="密码重置"
      :visible.sync="dialogFormVisible"
      :close-on-click-modal="false"
      width="30%"
    >
      <el-form :model="resetPwdForm" ref="resetPwdForm" :rules="passwordRules">
        <el-form-item label="密码" prop="pwd">
          <el-input
            v-model="resetPwdForm.pwd"
            type="password"
            show-password
            clearable
            autocomplete="off"
          ></el-input>
        </el-form-item>
        <el-form-item label="再次输入密码" prop="pwd2">
          <el-input
            v-model="resetPwdForm.pwd2"
            type="password"
            show-password
            clearable
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogFormVisible = false">取 消</el-button>
        <el-button type="primary" @click="resetPwdSubmit">重 置</el-button>
      </div>
    </el-dialog>
  </d2-container>
</template>

<script>
import * as api from './api'
import { crudOptions } from './crud'
import { d2CrudPlus } from 'd2-crud-plus'
export default {
  name: 'user',
  mixins: [d2CrudPlus.crud],
  data () {
    var validatePass = (rule, value, callback) => {
      const pwdRegex = new RegExp('(?=.*[0-9])(?=.*[a-zA-Z]).{8,30}')
      if (value === '') {
        callback(new Error('请输入密码'))
      } else if (!pwdRegex.test(value)) {
        callback(new Error('您的密码复杂度太低(密码中必须包含字母、数字)'))
      } else {
        if (this.resetPwdForm.pwd2 !== '') {
          this.$refs.resetPwdForm.validateField('pwd2')
        }
        callback()
      }
    }
    var validatePass2 = (rule, value, callback) => {
      if (value === '') {
        callback(new Error('请再次输入密码'))
      } else if (value !== this.resetPwdForm.pwd) {
        callback(new Error('两次输入密码不一致!'))
      } else {
        callback()
      }
    }
    return {
      dialogFormVisible: false,
      resetPwdForm: {
        id: null,
        pwd: null,
        pwd2: null
      },
      passwordRules: {
        pwd: [
          { required: true, message: '必填项' },
          { validator: validatePass, trigger: 'blur' }
        ],
        pwd2: [
          { required: true, message: '必填项' },
          { validator: validatePass2, trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    getCrudOptions () {
      this.crud.searchOptions.form.user_type = 0
      return crudOptions(this)
    },
    pageRequest (query) {
      return api.GetList(query)
    },
    addRequest (row) {
      return api.AddObj(row)
    },
    updateRequest (row) {
      return api.UpdateObj(row)
    },
    delRequest (row) {
      return api.DelObj(row.id)
    },
    batchDelRequest (ids) {
      return api.BatchDel(ids)
    },
    onExport () {
      const that = this
      this.$confirm('是否确认导出所有数据项?', '警告', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(function () {
        const query = that.getSearch().getForm()
        return api.exportData({ ...query })
      })
    },
    // 重置密码弹框
    resetPassword ({ row }) {
      this.dialogFormVisible = true
      this.resetPwdForm.id = row.id
    },
    // 重置密码确认
    resetPwdSubmit () {
      const that = this
      that.$refs.resetPwdForm.validate((valid) => {
        if (valid) {
          const params = {
            id: that.resetPwdForm.id,
            newPassword: that.$md5(that.resetPwdForm.pwd),
            newPassword2: that.$md5(that.resetPwdForm.pwd2)
          }
          api.ResetPwd(params).then((res) => {
            that.dialogFormVisible = false
            that.resetPwdForm = {
              id: null,
              pwd: null,
              pwd2: null
            }
            that.$message.success('修改成功')
          })
        } else {
          that.$message.error('表单校验失败，请检查')
        }
      })
    },
    // 部门懒加载
    loadChildrenMethod ({ row }) {
      return new Promise(resolve => {
        setTimeout(() => {
          const childs = [
            { id: row.id + 100000, parent: row.id, name: row.name + 'Test45', type: 'mp4', size: null, date: '2021-10-03', hasChild: true },
            { id: row.id + 150000, parent: row.id, name: row.name + 'Test56', type: 'mp3', size: null, date: '2021-07-09', hasChild: false }
          ]
          resolve(childs)
        }, 500)
      })
    }
  }
}
</script>

<style lang="scss">
.yxtInput {
  .el-form-item__label {
    color: #49a1ff;
  }
}
</style>

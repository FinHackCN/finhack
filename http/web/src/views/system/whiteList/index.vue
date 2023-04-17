<template>
  <d2-container :class="{ 'page-compact': crud.pageOptions.compact }">

    <d2-crud-x ref="d2Crud" v-bind="_crudProps" v-on="_crudListeners">
      <div slot="header">
        <crud-search
          ref="search"
          :options="crud.searchOptions"
          @submit="handleSearch"
        />
        <el-button-group>
          <el-button size="small" type="primary" @click="addRow"
            ><i class="el-icon-plus" /> 新增</el-button
          >
        </el-button-group>
        <crud-toolbar
          :search.sync="crud.searchOptions.show"
          :compact.sync="crud.pageOptions.compact"
          :columns="crud.columns"
          @refresh="doRefresh()"
          @columns-filter-changed="handleColumnsFilterChanged"
        />
      </div>
      <template slot="createBtnFormSlot" slot-scope="scope">
        <el-button
          :disabled="scope.mode === 'view'"
          icon="el-icon-plus"
          type="primary"
          circle
          @click="onLinkBtn"
        ></el-button>
      </template>
    </d2-crud-x>
  </d2-container>
</template>

<script>
import * as api from './api'
import { crudOptions } from './crud'
import { d2CrudPlus } from 'd2-crud-plus'
export default {
  name: 'whiteList',
  mixins: [d2CrudPlus.crud],
  data () {
    return {}
  },
  methods: {
    getCrudOptions () {
      return crudOptions(this)
    },
    pageRequest (query) {
      const menuId = this.$route.params.id
      return api.GetList({ ...query, menu: menuId })
    },
    addRequest (row) {
      const menuId = this.$route.params.id
      return api.createObj(row, menuId)
    },
    updateRequest (row) {
      return api.UpdateObj(row)
    },
    delRequest (row) {
      return api.DelObj(row.id)
    },
    // 跳转到添加按钮界面
    onLinkBtn () {
      this.$router.push({ path: '/button' })
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

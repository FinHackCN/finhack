<!--
 * @创建文件时间: 2021-06-01 22:41:21
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-08-12 00:06:38
 * 联系Qq:1638245306
 * @文件介绍:
-->
<template>
  <d2-container :class="{ 'page-compact': crud.pageOptions.compact }">
    <!--    <template slot="header">测试页面1</template>-->
    <d2-crud-x
      ref="d2Crud"
      v-bind="_crudProps"
      v-on="_crudListeners"
      @createPermission="createPermission"
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
    </d2-crud-x>
  </d2-container>
</template>

<script>
import * as api from './api'
import { crudOptions } from './crud'
import { d2CrudPlus } from 'd2-crud-plus'
export default {
  name: 'menus',
  mixins: [d2CrudPlus.crud],
  data () {
    return {}
  },
  methods: {
    getCrudOptions () {
      return crudOptions(this)
    },
    pageRequest (query) {
      return api.GetList(query)
    },
    addRequest (row) {
      return api.createObj(row)
    },
    updateRequest (row) {
      return api.UpdateObj(row)
    },
    delRequest (row) {
      return api.DelObj(row.id)
    },
    // 授权
    createPermission (scope) {
      this.$router.push({
        name: 'menuButton',
        query: { id: scope.row.id, name: scope.row.name }
      })
    },
    // 返回views目录下所有vue文件路径
    searchFiles () {
      const files = require.context('@/views', true, /\.vue$/)
      const result = []
      files.keys().forEach((key) => {
        result.push({
          label: key.replace(/(\.\/|\.vue)/g, ''),
          value: key.replace(/(\.\/|\.vue)/g, '')
        })
      })
      return result
    },
    /**
     * 懒加载
     * @param row
     * @returns {Promise<unknown>}
     */
    loadContentMethod ({ row }) {
      return new Promise(resolve => {
        api.GetList({ parent: row.id }).then(res => {
          resolve(res.data.data)
        })
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

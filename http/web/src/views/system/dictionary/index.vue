<template>
  <d2-container :class="{ 'page-compact': crud.pageOptions.compact }">
    <!--    <template slot="header">测试页面1</template>-->
    <d2-crud-x
      ref="d2Crud"
      v-bind="_crudProps"
      v-on="_crudListeners"
      @dictionaryConfigure="dictionaryConfigure"
    >
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
    </d2-crud-x>
    <el-drawer :visible.sync="drawer" :size="700">
      <div slot="title">
        <span>字典列表</span>
        <el-tag size="small" style="margin-left: 10px">{{
          dictionaryRow.label
        }}</el-tag>
      </div>
      <sub-dictionary
        style="margin-top: 80px; margin-left: 10px"
        :dictionaryRow="dictionaryRow"
      >
      </sub-dictionary>
    </el-drawer>
  </d2-container>
</template>

<script>
import * as api from './api'
import { crudOptions } from './crud'
import { d2CrudPlus } from 'd2-crud-plus'
import SubDictionary from '@/views/system/dictionary/subDictionary/index'
export default {
  name: 'dictionary',
  components: { SubDictionary },
  mixins: [d2CrudPlus.crud],
  data () {
    return {
      drawer: false,
      dictionaryRow: {}
    }
  },
  methods: {
    getCrudOptions () {
      return crudOptions(this)
    },
    pageRequest (query) {
      query.is_value = false
      return api.GetList(query)
    },
    addRequest (row) {
      d2CrudPlus.util.dict.clear()
      return api.createObj(row)
    },
    updateRequest (row) {
      d2CrudPlus.util.dict.clear()
      return api.UpdateObj(row)
    },
    delRequest (row) {
      return api.DelObj(row.id)
    },
    // 字典配置
    dictionaryConfigure (scope) {
      this.drawer = true
      this.dictionaryRow = scope.row
    },
    doAfterRowChange (row) {
      this.doRefresh({ from: 'afterRowChange' })
      this.$store.dispatch('d2admin/dictionary/load')
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

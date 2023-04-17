<template>
  <d2-container>
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
    </d2-crud-x>
  </d2-container>
</template>

<script>
import * as api from './api'
import { crudOptions } from './crud'
import { d2CrudPlus } from 'd2-crud-plus'
export default {
  name: 'subDictionary',
  mixins: [d2CrudPlus.crud],
  props: {
    // 容器样式
    dictionaryRow: {
      type: Object,
      required: true
    }
  },
  watch: {
    dictionaryRow () {
      this.doRefresh({ from: 'load' })
    }
  },
  data () {
    return {}
  },
  methods: {
    getCrudOptions () {
      return crudOptions(this)
    },
    pageRequest (query) {
      query.is_value = true
      query.parent = this.dictionaryRow.id
      return api.GetList(query)
    },
    addRequest (row) {
      d2CrudPlus.util.dict.clear()
      row.is_value = true
      row.parent = this.dictionaryRow.id
      return api.createObj(row)
    },
    updateRequest (row) {
      d2CrudPlus.util.dict.clear()
      row.is_value = true
      row.parent = this.dictionaryRow.id
      return api.UpdateObj(row)
    },
    delRequest (row) {
      return api.DelObj(row.id)
    },
    doAfterRowChange (row) {
      this.doRefresh({ from: 'afterRowChange' })
      this.$store.dispatch('d2admin/dictionary/load')
    }
  }
}
</script>

<style lang="scss" scoped>
.yxtInput {
  .el-form-item__label {
    color: #49a1ff;
  }
}
</style>

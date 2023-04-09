<template>
  <d2-container :class="{ 'page-compact': crud.pageOptions.compact }">
    <template slot="header">测试页面1</template>
    
  </d2-container>
</template>

<script>
import * as api from './api'
import { crudOptions } from './crud'
import { d2CrudPlus } from 'd2-crud-plus'
export default {
  name: 'page1',
  mixins: [d2CrudPlus.crud],
  data () {
    return {}
  },
  methods: {
    pageRequest (query) {
      return api.GetList(query)
    }
  }
}
</script>

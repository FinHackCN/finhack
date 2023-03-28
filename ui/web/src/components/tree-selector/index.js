import { d2CrudPlus } from 'd2-crud-plus'
import types from './types'

function install (Vue) {
  Vue.component('d2p-tree-selector', () => import('./lib/tree-selector'))
  if (d2CrudPlus != null) {
    d2CrudPlus.util.columnResolve.addTypes(types)
  }
}

export default {
  install
}

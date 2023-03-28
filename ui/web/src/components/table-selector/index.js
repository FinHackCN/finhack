/*
 * @创建文件时间: 2021-08-02 23:56:15
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-08-09 22:15:56
 * 联系Qq:1638245306
 * @文件介绍:
 */
import { d2CrudPlus } from 'd2-crud-plus'
import group from './group'

function install (Vue, options) {
  Vue.component('table-selector-input', () => import('./table-selector'))
  // Vue.component('d2p-row-format', () => import('./row'))
  if (d2CrudPlus != null) {
    // 注册字段类型`demo-extend`
    d2CrudPlus.util.columnResolve.addTypes(group)
  }
}

// 导出install， 通过`vue.use(D2pDemoExtend)`安装后 ，`demo-extend` 就可以在`crud.js`中使用了
export default {
  install
}

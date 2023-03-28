import Vue from 'vue'

import d2Container from './d2-container'

// 注意 有些组件使用异步加载会有影响
Vue.component('d2-container', d2Container)
Vue.component('d2-icon', () => import('./d2-icon'))
Vue.component('d2-icon-svg', () => import('./d2-icon-svg/index.vue'))
Vue.component('importExcel', () => import('./importExcel/index.vue'))
Vue.component('foreignKey', () => import('./foreign-key/index.vue'))
Vue.component('manyToMany', () => import('./many-to-many/index.vue'))
Vue.component('d2p-tree-selector', () => import('./tree-selector/lib/tree-selector.vue'))
Vue.component('dept-format', () => import('./dept-format/lib/dept-format.vue'))

function install (Vue) {
  Vue.component('dept-format', () => import('./lib/dept-format'))
}

export default {
  install
}

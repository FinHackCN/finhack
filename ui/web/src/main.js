/*
 * @创建文件时间: 2021-06-01 22:41:19
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-08-12 00:57:05
 * 联系Qq:1638245306
 * @文件介绍:
 */
// Vue
import Vue from 'vue'
import i18n from './i18n'
import App from './App'
// 核心插件
import d2Admin from '@/plugin/d2admin'
// store
import store from '@/store/index'

// 菜单和路由设置
import router from './router'
import { menuHeader } from '@/menu'

// 按钮权限
import '@/plugin/permission' // 加载permission

// d2-crud-plus 安装与初始化
import './install'
// 配置vxe-table
import 'xe-utils'
import VXETable from 'vxe-table'
import 'vxe-table/lib/style.css'

// md5加密
import md5 from 'js-md5'

// websocket
import websocket from '@/api/websocket'

// 核心插件
Vue.use(d2Admin)
Vue.use(VXETable)
Vue.prototype.$md5 = md5
Vue.prototype.$websocket = websocket

new Vue({
  router,
  store,
  i18n,
  render: h => h(App),
  beforeCreate () {
    // 初始化配置
    this.$store.dispatch('d2admin/settings/load')
    this.$store.dispatch('d2admin/dictionary/load')
  },
  created () {

    // 处理路由 得到每一级的路由设置
    // this.$store.commit('d2admin/page/init', frameInRoutes)
    // 设置顶栏菜单
    // this.$store.commit('d2admin/menu/headerSet', menuHeader)
    // 设置侧边栏菜单
    // this.$store.commit('d2admin/menu/asideSet', menuAside)
    // 初始化菜单搜索功能
    // this.$store.commit('d2admin/search/init', menuAside)
  },
  mounted () {
    // 展示系统信息
    this.$store.commit('d2admin/releases/versionShow')
    // 用户登录后从数据库加载一系列的设置
    this.$store.dispatch('d2admin/account/load')
    // 获取并记录用户 UA
    this.$store.commit('d2admin/ua/get')
    // 初始化全屏监听
    this.$store.dispatch('d2admin/fullscreen/listen')
  },
  watch: {
    // 检测路由变化切换侧边栏内容
    '$route.matched': {
      handler (matched) {
        if (matched.length > 0) {
          const _side = menuHeader.filter(menu => menu.path === matched[0].path)
          if (_side.length > 0) {
            this.$store.commit('d2admin/menu/asideSet', _side[0].children)
          }
        }
      },
      immediate: true
    }
  }
}).$mount('#app')

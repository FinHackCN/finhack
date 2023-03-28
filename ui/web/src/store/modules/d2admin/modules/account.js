/*
 * @创建文件时间: 2021-06-01 22:41:21
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-08-13 00:06:07
 * 联系Qq:1638245306
 * @文件介绍: 登录和登出
 */
import { Message, MessageBox } from 'element-ui'
import util from '@/libs/util.js'
import router from '@/router'
import store from '@/store/index'
import { SYS_USER_LOGIN, SYS_USER_LOGOUT } from '@/views/system/login/api'
import { request } from '@/api/service'

export default {
  namespaced: true,
  actions: {
    /**
     * @description 登录
     * @param {Object} context
     * @param {Object} payload username {String} 用户账号
     * @param {Object} payload password {String} 密码
     * @param {Object} payload route {Object} 登录成功后定向的路由对象 任何 vue-router 支持的格式
     */
    async login ({ dispatch }, {
      username = '',
      password = '',
      captcha = '',
      captchaKey = ''
    } = {}) {
      let res = await SYS_USER_LOGIN({
        username,
        password,
        captcha,
        captchaKey
      })
      // 设置 cookie 一定要存 uuid 和 token 两个 cookie
      // 整个系统依赖这两个数据进行校验和存储
      // uuid 是用户身份唯一标识 用户注册的时候确定 并且不可改变 不可重复
      // token 代表用户当前登录状态 建议在网络请求中携带 token
      // 如有必要 token 需要定时更新，默认保存一天
      res = res.data
      util.cookies.set('uuid', res.userId)
      util.cookies.set('token', res.access)
      util.cookies.set('refresh', res.refresh)
      // 设置 vuex 用户信息
      // await dispatch('d2admin/user/set', {
      //   name: res.name,
      //   user_id: res.userId,
      //   avatar: res.avatar,
      //   role_info: res.role_info,
      //   dept_info: res.dept_info
      // }, { root: true })
      var userInfoRes = await request({
        url: '/api/system/user/user_info/',
        method: 'get',
        params: {}
      })
      await store.dispatch('d2admin/user/set', userInfoRes.data, { root: true })
      // 用户登录后从持久化数据加载一系列的设置
      await dispatch('load')
    },
    /**
     * @description 注销用户并返回登录页面
     * @param {Object} context
     * @param {Object} payload confirm {Boolean} 是否需要确认
     */
    logout ({ commit, dispatch }, { confirm = false } = {}) {
      /**
       * @description 注销
       */
      async function logout () {
        await SYS_USER_LOGOUT({ refresh: util.cookies.get('refresh') }).then(() => {
          // 删除cookie
          util.cookies.remove('token')
          util.cookies.remove('uuid')
          util.cookies.remove('refresh')
        })
        // 清空 vuex 用户信息
        await dispatch('d2admin/user/set', {}, { root: true })
        store.commit('d2admin/menu/asideSet', []) // 设置侧边栏菜单
        store.commit('d2admin/search/init', []) // 设置搜索
        sessionStorage.removeItem('menuData')

        store.dispatch('d2admin/db/databaseClear')

        // 跳转路由
        router.push({ name: 'login' })
        router.go(0)
      }
      // 判断是否需要确认
      if (confirm) {
        commit('d2admin/gray/set', true, { root: true })
        MessageBox.confirm('确定要注销当前用户吗', '注销用户', { type: 'warning' })
          .then(() => {
            commit('d2admin/gray/set', false, { root: true })
            logout()
          })
          .catch(() => {
            commit('d2admin/gray/set', false, { root: true })
            Message({ message: '取消注销操作' })
          })
      } else {
        logout()
      }
    },
    /**
         * @description 用户登录后从持久化数据加载一系列的设置
         * @param {Object} context
         */
    async load ({ dispatch }) {
      // 加载用户名
      await dispatch('d2admin/user/load', null, { root: true })
      // 加载主题
      await dispatch('d2admin/theme/load', null, { root: true })
      // 加载页面过渡效果设置
      await dispatch('d2admin/transition/load', null, { root: true })
      // 持久化数据加载上次退出时的多页列表
      await dispatch('d2admin/page/openedLoad', null, { root: true })
      // 持久化数据加载侧边栏配置
      await dispatch('d2admin/menu/asideLoad', null, { root: true })
      // 持久化数据加载全局尺寸
      await dispatch('d2admin/size/load', null, { root: true })
      // 持久化数据加载颜色设置
      await dispatch('d2admin/color/load', null, { root: true })
    }
  }
}

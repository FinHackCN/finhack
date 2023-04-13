import { request } from '@/api/service'

export const urlPrefix = '/api/init/settings/'

// 系统配置
export default {
  namespaced: true,
  state: {
    data: {}
  },
  actions: {
    /**
     * @description 请求最新配置
     * @param {Object} context
     */
    async init ({ state, dispatch, commit }) {
      // 请求配置
      request({
        url: urlPrefix,
        method: 'get'
      }).then(async res => {
        // 赋值
        await dispatch('d2admin/db/set', {
          dbName: 'sys',
          path: 'settings.init',
          value: res.data,
          user: true
        }, { root: true })
        dispatch('load')
      })
    },
    /**
     * @description 本地加载配置
     * @param {Object} context
     */
    async load ({ state, dispatch, commit }) {
      // store 赋值
      const data = await dispatch('d2admin/db/get', {
        dbName: 'sys',
        path: 'settings.init',
        defaultValue: {},
        user: true
      }, { root: true })
      commit('set', data)
    }
  },
  mutations: {
    /**
     * @description 获取配置
     * @param {Object} state state
     * @param {String} key active
     * @param {Object} value active
     */
    async get (state, key, value) {
      return state[key]
    },
    /**
     * @description 赋值系统配置
     * @param {Object} state state
     * @param {Object} value active
     */
    async set (state, value) {
      state.data = value
      state.keepRecord = value['login.keep_record']
      return state.data
    }
  }
}

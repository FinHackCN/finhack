import { request } from '@/api/service'
import util from '@/libs/util'

export default {
  namespaced: true,
  state: {
    // 用户信息
    data: undefined
  },
  actions: {
    /**
     * @description 初始化部门数据
     * @param {Object} context
     * @param {*} info info
     */
    async getDeptName ({ state, dispatch }, { data }) {
      const nameDict = {}
      for (const items of data) {
        if (items.children) {
          const filterData = await dispatch('getDeptName', { data: items.children })
          for (var key in filterData) {
            nameDict[key] = filterData[key]
          }
        }
        nameDict[items.id] = items.name
      }
      return nameDict
    },
    async load ({ state, dispatch }, info) {
      // 持久化
      const ret = await request({
        url: '/api/system/dept/all_dept/'
      })
      const data = util.ArrayToTree(ret.data.data || ret.data, null, null, [])
      state.data = await dispatch('getDeptName', { data: data })
    }
  }
}

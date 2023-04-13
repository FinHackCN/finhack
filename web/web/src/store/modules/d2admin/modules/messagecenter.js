
export default {
  namespaced: true,
  state: {
    // 未读消息
    unread: 0
  },
  getters: {
    unread (state) {
      return state.unread
    }
  },
  actions: {
    /**
     * @description 添加一个日志
     * @param {Object} context
     * @param {String} param message {String} 信息
     * @param {String} param type {String} 类型
     * @param {Object} payload meta {Object} 附带的信息
     */
    async setUnread ({ state, commit }, number) {
      commit('set', number)
    }
  },
  mutations: {
    /**
     * 设置未读消息
     * @param state
     * @param number
     */
    async set (state, number) {
      state.unread = number
    }
  }
}

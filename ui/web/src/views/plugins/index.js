import Vue from 'vue'

function importAll (r) {
  const __modules = []
  r.keys().forEach(key => {
    if (!key.match(new RegExp('^test_.*$')) && key.split('/').length >= 3) {
      __modules.push(key.split('/')[1])
    }
  })
  return __modules
}

export const checkPlugins = function install (pluginName) {
  let pluginsList
  pluginsList = importAll(require.context('./', true, /index\.js$/))
  if (pluginsList && pluginsList.indexOf(pluginName) !== -1) {
    try {
      const Module = import('@/views/plugins/' + pluginName + '/src/index')
      // 注册组件
      if (Module.default) {
        Vue.use(Module.default)
      }
      // 本地插件
      return 'local'
    } catch (exception) {}
  }
  pluginsList = importAll(require.context('@great-dream/', true, /index\.js$/))
  if (pluginsList && pluginsList.indexOf(pluginName) !== -1) {
    // node_modules 封装插件
    try {
      const Module = import('@great-dream/' + pluginName + '/src/index')
      // 注册组件
      if (Module.default) {
        Vue.use(Module.default)
      }
      // 本地插件
      return 'plugins'
    } catch (exception) {}
  }
  // 未找到插件
  return undefined
}

export const plugins = async function install (Vue, options) {
  // 查找 src/views/plugins 目录所有插件，插件目录下需有 index.js 文件
  // 再查找 node_modules/@great-dream/ 目录下所有插件
  // 进行去重并vue注册导入
  if (window.pluginsAll) return
  let components = []
  components = components.concat(importAll(require.context('./', true, /index\.js$/)))
  components = components.concat(importAll(require.context('@great-dream/', true, /index\.js$/)))
  components = Array.from(new Set(components))
  components.filter(async (key, index) => {
    try {
      const Module = await import('@/views/plugins/' + key + '/src/index')
      // 注册组件
      if (Module.default) {
        Vue.use(Module.default)
        return true
      }
      return false
    } catch (exception) {
      try {
        const Module = await import('@great-dream/' + key + '/src/index')
        // 注册组件
        if (Module.default) {
          Vue.use(Module.default)
          return true
        }
        return false
      } catch (exception) {
        console.log(`[${key}]插件注册失败:`, exception)
        return false
      }
    }
  })
  console.log('注册成功插件：', components)
  window.pluginsAll = components
  return components
}

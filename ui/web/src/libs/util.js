import cookies from './util.cookies'
import db from './util.db'
import log from './util.log'
import dayjs from 'dayjs'
import filterParams from './util.params'

const util = {
  cookies,
  db,
  log,
  filterParams
}

/**
 * @description 更新标题
 * @param {String} titleText 标题
 */
util.title = function (titleText) {
  const processTitle = process.env.VUE_APP_TITLE || 'D2Admin'
  window.document.title = `${processTitle}${titleText ? ` | ${titleText}` : ''}`
}

/**
 * @description 打开新页面
 * @param {String} url 地址
 */
util.open = function (url) {
  var a = document.createElement('a')
  a.setAttribute('href', url)
  a.setAttribute('target', '_blank')
  a.setAttribute('id', 'd2admin-link-temp')
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(document.getElementById('d2admin-link-temp'))
}
/**
 * @description 校验是否为租户模式。租户模式把域名替换成 域名 加端口
 */
util.baseURL = function () {
  var baseURL = process.env.VUE_APP_API
  var param = baseURL.split('/')[3] || ''
  if (window.pluginsAll && window.pluginsAll.indexOf('dvadmin-tenants-web') !== -1 && (!param || baseURL.startsWith('/'))) {
    // 1.把127.0.0.1 替换成和前端一样域名
    // 2.把 ip 地址替换成和前端一样域名
    // 3.把 /api 或其他类似的替换成和前端一样域名
    // document.domain
    var host = baseURL.split('/')[2]
    if (host) {
      var prot = baseURL.split(':')[2] || 80
      if (prot === 80 || prot === 443) {
        host = document.domain
      } else {
        host = document.domain + ':' + prot
      }
      baseURL = baseURL.split('/')[0] + '//' + baseURL.split('/')[1] + host + '/' + param
    } else {
      baseURL = location.protocol + '//' + location.hostname + (location.port ? ':' : '') + location.port + baseURL
    }
  }
  if (!baseURL.endsWith('/')) {
    baseURL += '/'
  }
  return baseURL
}

util.wsBaseURL = function () {
  var baseURL = process.env.VUE_APP_API
  var param = baseURL.split('/')[3] || ''
  if (window.pluginsAll && window.pluginsAll.indexOf('dvadmin-tenants-web') !== -1 && (!param || baseURL.startsWith('/'))) {
    // 1.把127.0.0.1 替换成和前端一样域名
    // 2.把 ip 地址替换成和前端一样域名
    // 3.把 /api 或其他类似的替换成和前端一样域名
    // document.domain
    var host = baseURL.split('/')[2]
    if (host) {
      var prot = baseURL.split(':')[2] || 80
      if (prot === 80 || prot === 443) {
        host = document.domain
      } else {
        host = document.domain + ':' + prot
      }
      baseURL = baseURL.split('/')[0] + '//' + baseURL.split('/')[1] + host + '/' + param
    } else {
      baseURL = location.protocol + '//' + location.hostname + (location.port ? ':' : '') + location.port + baseURL
    }
  } else if (param !== '' || baseURL.startsWith('/')) {
    baseURL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.hostname + (location.port ? ':' : '') + location.port + baseURL
  }
  if (!baseURL.endsWith('/')) {
    baseURL += '/'
  }
  if (baseURL.startsWith('http')) { // https 也默认会被替换成 wss
    baseURL = baseURL.replace('http', 'ws')
  }
  return baseURL
}
/**
 * 自动生成ID
 */
util.autoCreateCode = function () {
  return dayjs().format('YYYYMMDDHHmmssms') + Math.round(Math.random() * 80 + 20)
}
/**
 * 自动生成短 ID
 */
util.autoShortCreateCode = function () {
  var Num = ''
  for (var i = 0; i < 4; i++) {
    Num += Math.floor(Math.random() * 10)
  }
  return dayjs().format('YYMMDD') + Num
}

/**
 * 生产随机字符串
 */
util.randomString = function (e) {
  e = e || 32
  var t = 'ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678'
  var a = t.length
  var n = ''
  for (let i = 0; i < e; i++) n += t.charAt(Math.floor(Math.random() * a))
  return n
}

util.ArrayToTree = function (rootList, parentValue, parentName, list) {
  for (const item of rootList) {
    if (item.parent === parentValue) {
      if (parentName) {
        item.name = parentName + '/' + item.name
      }
      list.push(item)
    }
  }

  for (const i of list) {
    // 如果子元素里面存在children就直接递归，不存在就生成一个children
    if (i.children) {
      util.ArrayToTree(rootList, i.id, i.name, i.children)
    } else {
      i.children = []
      util.ArrayToTree(rootList, i.id, i.name, i.children)
    }

    if (i.children.length === 0) {
      delete i.children
    }
  }
  return list
}

export default util

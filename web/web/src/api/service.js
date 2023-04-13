import axios from 'axios'
import Adapter from 'axios-mock-adapter'
import { get } from 'lodash'
import util from '@/libs/util'
import { dataNotFound, errorCreate, errorLog } from './tools'
import router from '@/router'
import qs from 'qs'

/**
 * @description 创建请求实例
 */
axios.defaults.retry = 1
axios.defaults.retryDelay = 1000

export function getErrorMessage (msg) {
  if (typeof msg === 'string') {
    return msg
  }
  if (typeof msg === 'object') {
    if (msg.code === 'token_not_valid') {
      util.cookies.remove('token')
      util.cookies.remove('uuid')
      router.push({ path: '/login' })
      router.go(0)
      return '登录超时，请重新登录！'
    }
    if (msg.code === 'user_not_found') {
      util.cookies.remove('token')
      util.cookies.remove('uuid')
      router.push({ path: '/login' })
      router.go(0)
      return '用户无效，请重新登录！'
    }
    return Object.values(msg)
  }
  if (Object.prototype.toString.call(msg).slice(8, -1) === 'Array') {
    return msg
  }
  return msg
}

function createService () {
  // 创建一个 axios 实例
  const service = axios.create({
    baseURL: util.baseURL(),
    timeout: 20000,
    paramsSerializer: (params) => qs.stringify(params, { indices: false })
  })
  // 请求拦截
  service.interceptors.request.use(
    config => config,
    error => {
      // 发送失败
      console.log(error)
      return Promise.reject(error)
    }
  )
  // 响应拦截
  service.interceptors.response.use(
    response => {
      // dataAxios 是 axios 返回数据中的 data
      let dataAxios = response.data
      if (response.headers['content-disposition']) {
        dataAxios = response
      }
      // 这个状态码是和后端约定的
      const { code } = dataAxios
      // 根据 code 进行判断
      if (code === undefined) {
        // 如果没有 code 代表这不是项目后端开发的接口 比如可能是 D2Admin 请求最新版本
        return dataAxios
      } else {
        // 有 code 代表这是一个后端接口 可以进行进一步的判断
        switch (code) {
          case 2000:
            // [ 示例 ] code === 2000 代表没有错误
            // TODO 可能结果还需要code和msg进行后续处理，所以去掉.data返回全部结果
            // return dataAxios.data
            return dataAxios
          case 401:
            // TODO 置换token 未完善
            util.cookies.remove('token')
            util.cookies.remove('uuid')
            util.cookies.remove('refresh')
            router.push({ path: '/login' })
            errorCreate(`${getErrorMessage(dataAxios.msg)}`)
            break
          case 404:
            dataNotFound(`${dataAxios.msg}`)
            break
          case 4000:
            // 删除cookie
            errorCreate(`${getErrorMessage(dataAxios.msg)}`)
            break
          case 400:
            errorCreate(`${dataAxios.msg}`)
            break
          default:
            // 不是正确的 code
            errorCreate(`${dataAxios.msg}: ${response.config.url}`)
            break
        }
      }
    },
    error => {
      const status = get(error, 'response.status')
      switch (status) {
        case 400:
          error.message = '请求错误'
          break
        case 401:
          refreshTken().then(res => {
            util.cookies.set('token', res.access)
          }).catch(e => {
            router.push({ name: 'login' })
            router.go(0)
            error.message = '未认证，请登录'
          })
          break
        case 403:
          error.message = '拒绝访问'
          break
        case 404:
          error.message = `请求地址出错: ${error.response.config.url}`
          break
        case 408:
          error.message = '请求超时'
          break
        case 500:
          error.message = '服务器内部错误'
          break
        case 501:
          error.message = '服务未实现'
          break
        case 502:
          error.message = '网关错误'
          break
        case 503:
          error.message = '服务不可用'
          break
        case 504:
          error.message = '网关超时'
          break
        case 505:
          error.message = 'HTTP版本不受支持'
          break
        default:
          break
      }
      errorLog(error)
      return Promise.reject(error)
    }
  )
  return service
}

/**
 * @description 创建请求方法
 * @param {Object} service axios 实例
 */
function createRequestFunction (service) {
  // 校验是否为租户模式。租户模式把域名替换成 域名 加端口
  return function (config) {
    const token = util.cookies.get('token')
    // 进行布尔值兼容
    var params = get(config, 'params', {})
    for (const key of Object.keys(params)) {
      if (String(params[key]) === 'true') {
        params[key] = 1
      }
      if (String(params[key]) === 'false') {
        params[key] = 0
      }
    }
    const configDefault = {
      headers: {
        Authorization: 'JWT ' + token,
        'Content-Type': get(config, 'headers.Content-Type', 'application/json')
      },
      timeout: 60000,
      baseURL: util.baseURL(),
      data: {},
      params: params
    }
    return service(Object.assign(configDefault, config))
  }
}

// 用于真实网络请求的实例和请求方法
export const service = createService()
export const request = createRequestFunction(service)

// 用于模拟网络请求的实例和请求方法
export const serviceForMock = createService()
export const requestForMock = createRequestFunction(serviceForMock)

// 网络请求数据模拟工具
export const mock = new Adapter(serviceForMock)

// 刷新token
const refreshTken = function () {
  const refresh = util.cookies.get('refresh')
  return request({
    url: 'token/refresh/',
    method: 'post',
    data: {
      refresh: refresh
    }
  })
}

/**
 * 下载文件
 * @param url
 * @param params
 * @param method
 * @param filename
 */
export const downloadFile = function ({ url, params, method, filename = '文件导出' }) {
  request({
    url: url,
    method: method,
    params: params,
    responseType: 'blob'
    // headers: {Accept: 'application/vnd.openxmlformats-officedocument'}
  }).then(res => {
    const xlsxName = window.decodeURI(res.headers['content-disposition'].split('=')[1])
    const fileName = xlsxName || `${filename}.xlsx`
    if (res) {
      const blob = new Blob([res.data], { type: 'charset=utf-8' })
      const elink = document.createElement('a')
      elink.download = fileName
      elink.style.display = 'none'
      elink.href = URL.createObjectURL(blob)
      document.body.appendChild(elink)
      elink.click()
      URL.revokeObjectURL(elink.href) // 释放URL 对象0
      document.body.removeChild(elink)
    }
  })
}

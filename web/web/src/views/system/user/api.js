import { request, downloadFile } from '@/api/service'
export const urlPrefix = '/api/system/user/'

export function GetList (query) {
  return request({
    url: urlPrefix,
    method: 'get',
    params: { ...query }
  })
}

export function AddObj (obj) {
  return request({
    url: urlPrefix,
    method: 'post',
    data: obj
  })
}

export function UpdateObj (obj) {
  return request({
    url: urlPrefix + obj.id + '/',
    method: 'put',
    data: obj
  })
}

export function DelObj (id) {
  return request({
    url: urlPrefix + id + '/',
    method: 'delete',
    data: { soft_delete: true }
  })
}

export function BatchDel (keys) {
  return request({
    url: urlPrefix + 'multiple_delete/',
    method: 'delete',
    data: { keys }
  })
}

/**
 * 重置密码
 * @param obj
 * @returns {*}
 * @constructor
 */
export function ResetPwd (obj) {
  return request({
    url: urlPrefix + obj.id + '/reset_password/',
    method: 'put',
    data: obj
  })
}

/**
 * 导出
 * @param params
 */
export function exportData (params) {
  return downloadFile({
    url: urlPrefix + 'export/',
    params: params,
    method: 'post'
  })
}

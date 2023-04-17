import { request } from '@/api/service'
export const urlPrefix = '/api/system/message_center/'
export function GetList (query) {
  return request({
    url: urlPrefix,
    method: 'get',
    params: query
  })
}

/**
 * 获取自己接收的消息
 * @param query
 * @returns {*}
 * @constructor
 */
export function GetSelfReceive (query) {
  return request({
    url: urlPrefix + 'get_self_receive/',
    method: 'get',
    params: query
  })
}

export function GetObj (obj) {
  return request({
    url: urlPrefix + obj.id + '/',
    method: 'get',
    params: {}
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
    data: { id }
  })
}


import { request } from '@/api/service'

export const urlPrefix = '/api/system/api_white_list/'

export function GetList (query) {
  return request({
    url: urlPrefix,
    method: 'get',
    params: query
  })
}

export function createObj (obj, id) {
  const data = { ...obj, menu: id }
  return request({
    url: urlPrefix,
    method: 'post',
    data: data
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

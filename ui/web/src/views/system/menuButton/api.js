/*
 * @创建文件时间: 2021-06-01 22:41:21
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-06-06 12:25:38
 * 联系Qq:1638245306
 * @文件介绍: 菜单权限接口
 */
import { request } from '@/api/service'

export const urlPrefix = '/api/system/menu_button/'

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

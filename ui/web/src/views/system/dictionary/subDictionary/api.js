import { request } from '@/api/service'
import XEUtils from 'xe-utils'
export const urlPrefix = '/api/system/dictionary/'

/**
 * 列表查询
 */
export function GetList (query) {
  return request({
    url: urlPrefix,
    method: 'get',
    params: query
  }).then(res => {
    // 将列表数据转换为树形数据
    res.data.data = XEUtils.toArrayTree(res.data.data, { parentKey: 'parent' })
    return res
  })
}
/**
 * 新增
 */
export function createObj (obj) {
  return request({
    url: urlPrefix,
    method: 'post',
    data: obj
  })
}

/**
 * 修改
 */
export function UpdateObj (obj) {
  return request({
    url: urlPrefix + obj.id + '/',
    method: 'put',
    data: obj
  })
}
/**
 * 删除
 */
export function DelObj (id) {
  return request({
    url: urlPrefix + id + '/',
    method: 'delete',
    data: { id }
  })
}

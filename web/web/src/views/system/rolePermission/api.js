/*
 * @创建文件时间: 2021-06-01 22:41:21
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-08-12 16:29:27
 * 联系Qq:1638245306
 * @文件介绍: 角色管理接口
 */
import { request } from '@/api/service'

export const urlPrefix = '/api/system/role/'

export function GetList (query) {
  return request({
    url: urlPrefix,
    method: 'get',
    params: query
  }).then(res => {
    return res.data.data
  })
}

export function createObj (obj) {
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

// 通过角色id,获取菜单数据
export function GetMenuData (obj) {
  return request({
    url: '/api/system/role/role_get_menu/',
    method: 'get',
    params: {}
  }).then(res => {
    // 将列表数据转换为树形数据
    return res.data
  })
}

/**
 * 获取数据权限
 * @param obj
 * @returns {*}
 * @constructor
 */
export function GetDataScope () {
  return request({
    url: '/api/system/role/data_scope/',
    method: 'get',
    params: {}
  })
}

/**
 * 获取角色部门
 * @returns {*}
 * @constructor
 */
export function GetDataScopeDept () {
  return request({
    url: '/api/system/role/data_scope_dept/',
    method: 'get',
    params: {}
  })
}

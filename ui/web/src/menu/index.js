/*
 * @创建文件时间: 2021-06-01 22:41:21
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-11-19 21:35:56
 * 联系Qq:1638245306
 * @文件介绍: 菜单获取
 */
import { uniqueId } from 'lodash'
import { request } from '@/api/service'
import XEUtils from 'xe-utils'
import { frameInRoutes } from '@/router/routes'
const _import = require('@/libs/util.import.' + process.env.NODE_ENV)
const pluginImport = require('@/libs/util.import.plugin')
/**
 * @description 给菜单数据补充上 path 字段
 * @description https://github.com/d2-projects/d2-admin/issues/209
 * @param {Array} menu 原始的菜单数据
 */
function supplementPath (menu) {
  return menu.map(e => ({
    ...e,
    path: e.path || uniqueId('d2-menu-empty-'),
    ...e.children ? {
      children: supplementPath(e.children)
    } : {}
  }))
}

export const menuHeader = supplementPath([])

export const menuAside = supplementPath([])

// 请求菜单数据,用于解析路由和侧边栏菜单
export const getMenu = function () {
  return request({
    url: '/api/system/menu/web_router/',
    method: 'get',
    params: {}
  }).then((res) => {
    // 设置动态路由
    const menuData = res.data.data
    sessionStorage.setItem('menuData', JSON.stringify(menuData))
    return menuData
  })
}

/**
 * 校验路由是否有效
 */
export const checkRouter = function (menuData) {
  const result = []
  for (const item of menuData) {
    try {
      if (item.path !== '' && item.component) {
        (item.component && item.component.substr(0, 8) === 'plugins/') ? pluginImport(item.component.replace('plugins/', '')) : _import(item.component)
      }
      result.push(item)
    } catch (err) {
      console.log(`导入菜单错误，会导致页面无法访问，请检查文件是否存在：${item.component}`)
    }
  }
  return result
}
/**
 * 将获取到的后端菜单数据,解析为前端路由
 */
export const handleRouter = function (menuData) {
  const result = []
  for (const item of menuData) {
    if (item.path !== '' && item.component) {
      const obj = {
        path: item.path,
        name: item.component_name,
        component: (item.component && item.component.substr(0, 8) === 'plugins/') ? pluginImport(item.component.replace('plugins/', '')) : _import(item.component),
        meta: {
          title: item.name,
          auth: true,
          cache: item.cache
        }
      }
      result.push(obj)
    } else {
      if (item.is_link === 0) {
        delete item.path
      }
    }
  }
  frameInRoutes[0].children = [...result]
  return frameInRoutes
}

/**
 * 将前端的侧边菜单进行处理
 */
export const handleAsideMenu = function (menuData) {
  // 将列表数据转换为树形数据
  const data = XEUtils.toArrayTree(menuData, {
    parentKey: 'parent',
    strict: true
  })
  const menu = [
    { path: '/index', title: '控制台', icon: 'home' },
    ...data
  ]
  return supplementPath(menu)
}

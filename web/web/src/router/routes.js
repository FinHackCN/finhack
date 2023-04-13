import layoutHeaderAside from '@/layout/header-aside'
import { checkPlugins } from '@/views/plugins/index.js'
// 由于懒加载页面太多的话会造成webpack热更新太慢，所以开发环境不使用懒加载，只有生产环境使用懒加载
const _import = require('@/libs/util.import.' + process.env.NODE_ENV)
const pluginImport = require('@/libs/util.import.plugin')
/**
 * 在主框架内显示
 */
const frameIn = [{
  path: '/',
  redirect: { name: 'index' },
  component: layoutHeaderAside,
  children: [
    // 控制台
    {
      path: 'index',
      name: 'index',
      meta: {
        auth: true
      },
      component: _import('dashboard/workbench/index')
    },
    {
      path: 'userInfo',
      name: 'userInfo',
      meta: {
        title: '个人信息',
        auth: true
      },
      component: () => import('@/layout/header-aside/components/header-user/userinfo')
    },
    // dashboard 工作台
    {
      path: 'workbench',
      name: 'workbench',
      meta: {
        title: '工作台',
        auth: true
      },
      component: _import('dashboard/workbench')
    },
    // 刷新页面 必须保留
    {
      path: 'refresh',
      name: 'refresh',
      hidden: true,
      component: _import('system/function/refresh')
    },
    // 页面重定向 必须保留
    {
      path: 'redirect/:route*',
      name: 'redirect',
      hidden: true,
      component: _import('system/function/redirect')
    }
  ]
}]

/**
 * 在主框架之外显示
 */
const frameOut = [
  // 登录
  {
    path: '/login',
    name: 'login',
    component: _import('system/login')
  }
]
/**
 * 第三方登录
 */
const pluginsType = checkPlugins('dvadmin-oauth2-web')
if (pluginsType) {
  frameOut.push({
    path: '/oauth2',
    name: 'login',
    component: pluginsType === 'local' ? _import('plugins/dvadmin-oauth2-web/src/login/index') : pluginImport('dvadmin-oauth2-web/src/login/index')
  })
}
/**
 * 错误页面
 */
const errorPage = [{
  path: '/404',
  name: '404',
  component: _import('system/error/404')
}]

// 导出需要显示菜单的
export const frameInRoutes = frameIn

// 重新组织后导出
export default [
  ...frameIn,
  ...frameOut,
  ...errorPage
]

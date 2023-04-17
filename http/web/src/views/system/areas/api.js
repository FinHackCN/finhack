import { request } from '@/api/service'

export const urlPrefix = '/api/system/area/'

export function GetList (query) {
  if ((!query.pcode || query.pcode.length === 0) && !query.name && !query.code) {
    query.level = 1
    delete query.pcode
  }
  return request({
    url: urlPrefix,
    method: 'get',
    params: { ...query, limit: 100 }
  }).then(res => {
    // 将列表数据转换为树形数据
    res.data.data.map(value => {
      value.hasChildren = value.pcode_count !== 0
    })
    return res
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

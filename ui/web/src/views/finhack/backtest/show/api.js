import { request } from '@/api/service'
export const urlPrefix = '/api/finhack/backtest/'

export function GetList(query) {
  query={
    'instance_id':'5735ef0cdc1495766c30ddcddb1d9cbd'
  }
  return request({
    url: urlPrefix,
    method: 'post',
    params: query
  })
}


import { request } from '@/api/service'
export const urlPrefix = '/api/finhack/backtest/'

export function GetDetail(id) {
  return request({
    url: urlPrefix+id,
    method: 'GET',
    params: {}
  })
}


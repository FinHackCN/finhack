import { request } from '@/api/service'
export const urlPrefix = '/api/finhack/test/'

export function test() {
  return request({
    url: urlPrefix,
    method: 'GET',
    params: {}
  })
}


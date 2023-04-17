export const BUTTON_VALUE_TO_COLOR_MAPPING = {
  1: 'success',
  true: 'success',
  0: 'danger',
  false: 'danger',
  Search: 'warning', // 查询
  Update: 'primary', // 编辑
  Create: 'success', // 新增
  Retrieve: 'info', // 单例
  Delete: 'danger' // 删除
}

export function getButtonSettings (objectSettings) {
  return objectSettings.map(item => {
    return {
      label: item.label,
      value: item.value,
      color: BUTTON_VALUE_TO_COLOR_MAPPING[item.value]
    }
  })
}
// v2.0.2 中已弃用，改为 vm.dictionary('button_status_bool')
// 启用 true/ 禁用 false
export const BUTTON_STATUS_BOOL = getButtonSettings([{ label: '启用', value: true }, { label: '禁用', value: false }])

// 启用 1/ 禁用 0
export const BUTTON_STATUS_NUMBER = getButtonSettings([{ label: '启用', value: 1 }, { label: '禁用', value: 0 }])

// 是 1/ 否 0
export const BUTTON_WHETHER_NUMBER = getButtonSettings([{ label: '是', value: 1 }, { label: '否', value: 0 }])
// 是 true/ 否 false
export const BUTTON_WHETHER_BOOL = getButtonSettings([{ label: '是', value: true }, { label: '否', value: false }])
// 用户类型
export const USER_TYPE = getButtonSettings([{ label: '后台用户', value: 0 }, { label: '前台用户', value: 1 }])

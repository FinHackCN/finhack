/*
 * @创建文件时间: 2021-08-02 23:55:30
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-08-08 12:27:45
 * 联系Qq:1638245306
 * @文件介绍:
 */
export default {
  // 字段类型配置，注册之后即可在crud.js中使用了
  'table-selector': {
    // 表单组件配置
    form: { component: { name: 'table-selector-input', props: { color: 'danger' } } },
    // 行组件配置
    component: { name: 'values-format', props: {} },
    // 行展示时居中
    align: 'center'
    // 您还可以写更多默认配置
  }
}

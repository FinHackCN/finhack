<!--
 * @创建文件时间: 2021-11-09 15:41:29
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-12-08 14:41:20
 * 联系Qq:1638245306
 * @文件介绍: 
-->
# 表格选择框配置说明

## crud.js
```
 {
        title: '单选本地',
        key: 'select1',
        sortable: true,
        search: {
          disabled: true
        },
        type: 'table-selector',
        dict: {
          url: '/api/system/user/',
          value: 'id', // 数据字典中value字段的属性名
          label: 'name', // 数据字典中label字段的属性名
          getData: (url, dict, { form, component }) => {
            return request({ url: url, params: { page: 1, limit: 1 } }).then(ret => {
              component._elProps.page = ret.data.page
              component._elProps.limit = ret.data.limit
              component._elProps.total = ret.data.total
              return ret.data.data
            })
          }
        },
        form: {
          component: {
            span: 12,
            props: { multiple: true },
            elProps: {
              pagination: true,
              columns: [
                {
                  field: "name",
                  title: "名称",
                },
                {
                  field: "username",
                  title: "账号",
                },
                {
                  field: "role",
                  title: "角色Id",
                },
                {
                  field: "dept",
                  title: "部门Id",
                },

              ]
            }
          }
        }
      }
```

## 配置说明
```
详细文档:
1.http://d2-crud-plus.docmirror.cn/d2-crud-plus/guide/dict.html
2.https://xuliangzhan_admin.gitee.io/vxe-table/#/grid/api
```
 
| Name       | Description      | Type    | Required | Default        |
| ---------- | ---------------- | ------- | -------- | -------------- |
| type       | 字段所使用的组件 | String  | true     | table-selector |
| dict       | 字典的配置       | Object  | true     | {}             |
| multiple   | 是否多选         | Boolean | false    | false          |
| pagination | 是否分页         | Boolean | false    | false          |
| columns    | 表格的列配置     | Array   | true     | []             |
| field      | 字段             | String  | true     | ''             |
| title      | 字段名称         | String  | true     | ''             |
___

## events
| Name | Description | Params |
|------|-------------|--------|
| radioChange| 表格行单选的点击事件| row,rowIndex|


```
form:{
  component:{
    on: {      //单选事件监听
              radioChange({ event, scope }) {
                scope.form.channel_number = event.row.channel_number
              }  
      },
  }
}
```


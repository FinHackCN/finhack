# 多对多表格显示配置说明
本组件用于多对多返回数据使用,例如:角色信息
```angular2html
role_info = [
    {"id":1,"name":"普通用户"},
    {"id":2,"name":"管理员"}
]

#crud的配置
component: {
name: 'manyToMany',
valueBinding: 'role_info',
children: 'name'
}
```
## crud.js
```
 {
        component: {
          name: 'manyToMany',
          valueBinding: 'role_name',
          children: 'name'
        }
 }
```

## 配置说明


| Name       | Description      | Type    | Required | Default        |
| ---------- | ---------------- | ------- | -------- | -------------- |
| name       | 字段所使用的组件 | String  | true     | manyToMany |
| valueBinding       | row中的key       | String  | true     | -             |
| children   | 数组中的key         | String | true    | name          |

# 一对多表格显示配置说明
本组件用于多对多返回数据使用,例如:角色信息
```angular2html
dept_name = "dvadmin团队"

#crud的配置
component: {
name: 'foreignKey',
valueBinding: 'dept_name'
}
```
## crud.js
```
 {
        component: {
          name: 'foreignKey',
          valueBinding: 'dept_name',
        }
 }
```

## 配置说明


| Name       | Description      | Type    | Required | Default        |
| ---------- | ---------------- | ------- | -------- | -------------- |
| name       | 字段所使用的组件 | String  | true     | foreignKey |
| valueBinding       | row中的key       | String  | true     | -             |

export const crudOptions = (vm) => {
  return {
    pageOptions: {
      compact: true
    },
    options: {
      height: '100%'
    },
    viewOptions: {
      componentType: 'row'
    },
    formOptions: {
      defaultSpan: 12 // 默认的表单 span
    },
    columns: [
      {
        title: 'ID',
        key: 'id',
        width: 90,
        form: {
          disabled: true
        }
      },
      {
        title: '单选本地',
        key: 'select1',
        sortable: true,
        search: {
          disabled: true
        },
        type: 'select',
        dict: {
          data: [{ value: '1', label: '开启', color: 'success' }, { value: '0', label: '关闭', color: 'danger' }, { value: '2', label: '停止', color: 'info' }]
        }
      },
      {
        title: '多选,本地,自动染色',
        key: 'select2',
        sortable: true,
        width: 180,
        search: {
          disabled: false,
          title: '多选'
        },
        type: 'select',
        form: {
          title: '多选本地',
          component: {
            props: {
              filterable: true,
              multiple: true,
              clearable: true
            }
          }
        },
        dict: {
          data: [{ value: 'sz', label: '深圳' }, { value: 'gz', label: '广州' }, { value: 'wh', label: '武汉' }, { value: 'sh', label: '上海' }]
        }
        // component: { props: { color: 'auto' } } // install.js 有配置自动染色
      }
    ]
  }
}

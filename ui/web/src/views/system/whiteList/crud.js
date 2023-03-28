import { request } from '@/api/service'

export const crudOptions = (vm) => {
  return {
    pageOptions: {
      compact: true
    },
    options: {
      tableType: 'vxe-table',
      rowKey: false,
      width: '100%',
      height: '100%' // 表格高度100%, 使用toolbar必须设置
    },
    rowHandle: {
      width: 180,
      edit: {
        thin: true,
        text: '编辑'
      },
      remove: {
        thin: true,
        text: '删除'
      }
    },
    indexRow: { // 或者直接传true,不显示title，不居中
      title: '序号',
      align: 'center',
      width: 100
    },
    viewOptions: {
      disabled: true,
      componentType: 'form'
    },
    formOptions: {
      defaultSpan: 24 // 默认的表单 span
    },
    columns: [
      {
        title: '关键词',
        key: 'search',
        show: false,
        disabled: true,
        search: {
          disabled: false
        },
        form: {
          disabled: true,
          component: {
            placeholder: '请输入关键词'
          }
        },
        view: { // 查看对话框组件的单独配置
          disabled: true
        }
      },
      {
        title: 'ID',
        key: 'id',
        show: false,
        width: 90,
        form: {
          disabled: true
        }
      },
      {
        title: '请求方式',
        key: 'method',
        sortable: true,
        search: {
          disabled: false
        },
        type: 'select',
        dict: {
          data: [
            {
              label: 'GET',
              value: 0
            },
            {
              label: 'POST',
              value: 1
            },
            {
              label: 'PUT',
              value: 2
            },
            {
              label: 'DELETE',
              value: 3
            }
          ]
        },
        form: {
          rules: [ // 表单校验规则
            {
              required: true,
              message: '必填项'
            }
          ],
          component: {
            span: 12
          },
          itemProps: {
            class: { yxtInput: true }
          }
        }
      },
      {
        title: '接口地址',
        key: 'url',
        sortable: true,
        search: {
          disabled: true
        },
        type: 'select',
        dict: {
          url: '/swagger.json',
          label: 'label',
          value: 'value',
          getData: (url, dict) => {
            return request({ url: url }).then(ret => {
              const res = Object.keys(ret.paths)
              const data = []
              for (const item of res) {
                const obj = {}
                obj.label = item
                obj.value = item
                data.push(obj)
              }
              return data
            })
          }
        },
        form: {
          rules: [ // 表单校验规则
            {
              required: true,
              message: '必填项'
            }
          ],
          component: {
            span: 24,
            props: {
              elProps: {
                allowCreate: true,
                filterable: true,
                clearable: true
              }

            }
          },
          itemProps: {
            class: { yxtInput: true }
          },
          helper: {
            render (h) {
              return (< el-alert title="请正确填写，以免请求时被拦截。匹配单例使用正则,例如:/api/xx/.*?/" type="warning"/>
              )
            }
          }
        }
      },
      {
        title: '数据权限认证',
        key: 'enable_datasource',
        search: {
          disabled: false
        },
        width: 150,
        type: 'radio',
        dict: {
          data: vm.dictionary('button_status_bool')
        },
        form: {
          value: true,
          component: {
            span: 12
          }
        }
      },
      {
        title: '备注',
        key: 'description',
        search: {
          disabled: true
        },
        type: 'textarea',
        form: {
          component: {
            placeholder: '请输入内容',
            showWordLimit: true,
            maxlength: '200',
            props: {
              type: 'textarea'
            }
          }
        }
      }
    ]
  }
}

/*
 * @创建文件时间: 2021-06-03 00:50:28
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-07-29 22:49:02
 * 联系Qq:1638245306
 * @文件介绍: 菜单的按钮和接口配置
 */
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
    columns: [{
      title: '关键词',
      key: 'search',
      show: false,
      disabled: true,
      search: {
        disabled: false,
        component: {
          props: {
            clearable: true
          },
          placeholder: '请输入关键词'
        }
      },
      form: {
        disabled: true
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
      title: '权限名称',
      key: 'name',
      sortable: true,
      width: 150,
      search: {
        disabled: false
      },
      type: 'select',
      dict: {
        data: vm.dictionary('system_button')
      },
      form: {
        rules: [ // 表单校验规则
          { required: true, message: '必填项' }
        ],
        component: {
          span: 12,
          props: {
            clearable: true,
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
        valueChange (key, value, form, { getColumn, mode, component, immediate, getComponent }) {
          if (value != null) {
            // console.log('component.dictOptions', component.dictOptions)
            const obj = component.dictOptions.find(item => {
              // console.log(item.label, value)
              return item.value === value
            })
            if (obj && obj.value) {
              form.name = obj.label
              form.value = obj.value
            }
          }
        },
        helper: {
          render (h) {
            return (< el-alert title="可手动输入不在列表中的新值" type="warning" description="比较常用的建议放在字典管理中"/>
            )
          }
        }
      }
    },
    {
      title: '权限值',
      key: 'value',
      sortable: true,
      width: 200,
      search: {
        disabled: false
      },
      type: 'input',
      form: {
        rules: [ // 表单校验规则
          { required: true, message: '必填项' }
        ],
        component: {
          span: 12,
          placeholder: '请输入权限值',
          props: {
            elProps: {
              clearable: true
            }
          }
        },
        itemProps: {
          class: { yxtInput: true }
        },
        helper: {
          render (h) {
            return (< el-alert title="用于前端按钮权限的判断展示" type="warning" description="使用方法：vm.hasPermissions(权限值)"/>
            )
          }
        }
      }
    },
    {
      title: '请求方式',
      key: 'method',
      sortable: true,
      width: 150,
      search: {
        disabled: false
      },
      type: 'select',
      dict: {
        data: [
          { label: 'GET', value: 0 },
          { label: 'POST', value: 1 },
          { label: 'PUT', value: 2 },
          { label: 'DELETE', value: 3 }
        ]
      },
      form: {
        rules: [ // 表单校验规则
          { required: true, message: '必填项' }
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
      key: 'api',
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
          { required: true, message: '必填项' }
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
            return (< el-alert title="请正确填写，以免请求时被拦截。匹配单例使用正则,例如:/api/xx/.*?/" type="warning" />
            )
          }
        }
      }
    }
    ]
  }
}

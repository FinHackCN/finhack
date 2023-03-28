import { request } from '@/api/service'

export const crudOptions = (vm) => {
  return {
    indexRow: { // 或者直接传true,不显示title，不居中
      width: 60,
      title: '序号',
      align: 'center'
    },
    options: {
      tableType: 'vxe-table',
      rowKey: true, // 必须设置，true or false
      height: '100%' // 表格高度100%, 使用toolbar必须设置
    },
    rowHandle: {
      width: 160,
      fixed: 'right',
      view: false,
      edit: {
        thin: true,
        text: '',
        show () {
          return vm.tabActivted !== 'receive'
        },
        disabled () {
          return !vm.hasPermissions('Update')
        }
      },
      remove: {
        thin: true,
        text: '',
        show () {
          return vm.tabActivted !== 'receive'
        },
        disabled () {
          return !vm.hasPermissions('Delete')
        }
      },
      custom: [
        {
          thin: true,
          text: null,
          icon: 'el-icon-view',
          size: 'small',
          disabled () {
            return !vm.hasPermissions('Retrieve')
          },
          order: 1,
          emit: 'onView'
        }
      ]
    },
    columns: [
      {
        title: 'id',
        key: 'id',
        width: 100,
        form: { disabled: true }
      },
      {
        title: '标题',
        key: 'title',
        search: {
          disabled: false
        },
        width: 200,
        form: {
          rules: [ // 表单校验规则
            {
              required: true,
              message: '必填项'
            }
          ],
          component: { span: 24, placeholder: '请输入标题' }
        }
      },
      {
        title: '是否已读',
        key: 'is_read',
        type: 'select',
        width: 100,
        show () {
          return vm.tabActivted === 'receive'
        },
        dict: {
          data: [
            { label: '已读', value: true, color: 'success' },
            { label: '未读', value: false, color: 'danger' }
          ]
        },
        form: {
          disabled: true
        }
      },
      {
        title: '目标类型',
        key: 'target_type',
        type: 'radio',
        width: 120,
        show () {
          return vm.tabActivted === 'send'
        },
        dict: { data: [{ value: 0, label: '按用户' }, { value: 1, label: '按角色' }, { value: 2, label: '按部门' }, { value: 3, label: '通知公告' }] },
        form: {
          component: {
            span: 24,
            props: {
              type: 'el-radio-button'
            }
          },
          rules: [
            {
              required: true,
              message: '必选项',
              trigger: ['blur', 'change']
            }
          ]
        }
      },
      {
        title: '目标用户',
        key: 'target_user',
        search: {
          disabled: true
        },
        width: 130,
        type: 'table-selector',
        disabled: true,
        dict: {
          cache: false,
          url: '/api/system/user/',
          value: 'id', // 数据字典中value字段的属性名
          label: 'name', // 数据字典中label字段的属性名
          getData: (url, dict, {
            form,
            component
          }) => {
            return request({
              url: url,
              params: {
                page: 1,
                limit: 10
              }
            }).then(ret => {
              component._elProps.page = ret.data.page
              component._elProps.limit = ret.data.limit
              component._elProps.total = ret.data.total
              return ret.data.data
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
          itemProps: {
            class: { yxtInput: true }
          },
          component: {
            span: 24,
            show (context) {
              return context.form.target_type === 0
            },
            pagination: true,
            props: { multiple: true },
            elProps: {
              columns: [
                {
                  field: 'name',
                  title: '用户名称'
                },
                {
                  field: 'phone',
                  title: '用户电话'
                }
              ]
            }
          }
        },
        component: {
          name: 'manyToMany',
          valueBinding: 'user_info',
          children: 'name'
        }
      },
      {
        title: '目标角色',
        key: 'target_role',
        search: {
          disabled: true
        },
        disabled: true,
        width: 130,
        type: 'table-selector',
        dict: {
          cache: false,
          url: '/api/system/role/',
          value: 'id', // 数据字典中value字段的属性名
          label: 'name', // 数据字典中label字段的属性名
          getData: (url, dict, {
            form,
            component
          }) => {
            return request({
              url: url,
              params: {
                page: 1,
                limit: 10
              }
            }).then(ret => {
              component._elProps.page = ret.data.page
              component._elProps.limit = ret.data.limit
              component._elProps.total = ret.data.total
              return ret.data.data
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
          itemProps: {
            class: { yxtInput: true }
          },
          component: {
            span: 24,
            show (context) {
              return context.form.target_type === 1
            },
            pagination: true,
            props: { multiple: true },
            elProps: {
              columns: [
                {
                  field: 'name',
                  title: '角色名称'
                },
                {
                  field: 'key',
                  title: '权限标识'
                }
              ]
            }
          }
        },
        component: {
          name: 'manyToMany',
          valueBinding: 'role_info',
          children: 'name'
        }
      },
      {
        title: '目标部门',
        key: 'target_dept',
        search: {
          disabled: true
        },
        width: 130,
        type: 'table-selector',
        dict: {
          cache: false,
          url: '/api/system/dept/all_dept/',
          isTree: true,
          value: 'id', // 数据字典中value字段的属性名
          label: 'name', // 数据字典中label字段的属性名
          children: 'children', // 数据字典中children字段的属性名
          getData: (url, dict, {
            form,
            component
          }) => {
            return request({
              url: url
            }).then(ret => {
              return ret.data.data
            })
          }
        },
        disabled: true,
        form: {
          rules: [ // 表单校验规则
            {
              required: true,
              message: '必填项'
            }
          ],
          itemProps: {
            class: { yxtInput: true }
          },
          component: {
            span: 24,
            show (context) {
              return context.form.target_type === 2
            },
            props: {
              multiple: true,
              elProps: {
                treeConfig: {
                  transform: true,
                  rowField: 'id',
                  parentField: 'parent',
                  expandAll: true
                },
                columns: [
                  {
                    field: 'name',
                    title: '部门名称',
                    treeNode: true
                  },
                  {
                    field: 'status_label',
                    title: '状态'
                  },
                  {
                    field: 'parent_name',
                    title: '父级部门'
                  }
                ]
              }
            }
          }
        },
        component: {
          name: 'manyToMany',
          valueBinding: 'dept_info',
          children: 'name'
        }
      },
      {
        title: '内容',
        key: 'content',
        minWidth: 300,
        type: 'editor-quill', // 富文本图片上传依赖file-uploader，请先配置好file-uploader
        form: {
          rules: [ // 表单校验规则
            {
              required: true,
              message: '必填项'
            }
          ],
          component: {
            disabled: () => {
              return vm.getEditForm().disable
            },
            props: {
              uploader: {
                type: 'form' // 上传后端类型【cos,aliyun,oss,form】
              }
            },
            events: {
              'text-change': (event) => {
                console.log('text-change:', event)
              }
            }
          }
        }
      }
    ].concat(vm.commonEndColumns({
      create_datetime: { showTable: true },
      update_datetime: { showTable: false }
    }))
  }
}

export const crudOptions = (vm) => {
  return {
    pageOptions: {
      compact: true
    },
    options: {
      tableType: 'vxe-table',
      rowKey: true, // 必须设置，true or false
      rowId: 'id',
      height: '100%', // 表格高度100%, 使用toolbar必须设置
      highlightCurrentRow: false

    },
    rowHandle: {
      view: {
        thin: true,
        text: '',
        disabled () {
          return !vm.hasPermissions('Retrieve')
        }
      },
      width: 230,
      edit: {
        thin: true,
        text: '',
        disabled () {
          return !vm.hasPermissions('Update')
        }
      },
      remove: {
        thin: true,
        text: '',
        disabled () {
          return !vm.hasPermissions('Delete')
        }
      },
      custom: [{
        show (index, row) {
          return true
        },
        disabled () {
          return !vm.hasPermissions('Update')
        },
        text: '权限管理',
        type: 'warning',
        size: 'small',
        emit: 'createPermission'
      }]

    },
    indexRow: { // 或者直接传true,不显示title，不居中
      title: '序号',
      align: 'center',
      width: 100
    },

    viewOptions: {
      componentType: 'form'
    },
    formOptions: {
      defaultSpan: 24, // 默认的表单 span
      width: '35%'
    },
    columns: [{
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
          props: {
            clearable: true
          },
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
      title: '角色名称',
      key: 'name',
      sortable: true,
      search: {
        disabled: false,
        component: {
          props: {
            clearable: true
          }
        }
      },

      type: 'input',
      form: {
        rules: [ // 表单校验规则
          { required: true, message: '角色名称必填项' }
        ],
        component: {
          props: {
            clearable: true
          },
          placeholder: '请输入角色名称'
        },
        itemProps: {
          class: { yxtInput: true }
        }
      }
    },
    {
      title: '权限标识',
      key: 'key',
      sortable: true,
      form: {
        rules: [ // 表单校验规则
          { required: true, message: '权限标识必填项' }
        ],
        component: {
          props: {
            clearable: true
          },
          placeholder: '请输入标识字符'
        },
        itemProps: {
          class: { yxtInput: true }
        }
      }
    }, {
      title: '排序',
      key: 'sort',
      sortable: true,
      width: 80,
      type: 'number',
      form: {
        value: 1,
        component: {
          placeholder: '请输入排序'
        }
      }
    },
    {
      title: '是否管理员',
      key: 'admin',
      sortable: true,
      type: 'radio',
      dict: {
        data: vm.dictionary('button_whether_bool')
      },
      form: {
        value: false,
        component: {
          placeholder: '请选择是否管理员',
          show (context) {
            return vm.info.is_superuser
          }
        }
      }
    },
    {
      title: '状态',
      key: 'status',
      sortable: true,
      search: {
        disabled: false
      },
      type: 'radio',
      dict: {
        data: vm.dictionary('button_status_bool')
      },
      form: {
        value: true,
        component: {
          placeholder: '请选择状态'
        }
      },
      component: { props: { color: 'auto' } }
    }
    ].concat(vm.commonEndColumns())
  }
}

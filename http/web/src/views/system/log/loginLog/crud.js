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
      fixed: 'right',
      view: {
        thin: true,
        text: '',
        disabled () {
          return !vm.hasPermissions('Retrieve')
        }
      },
      width: 70,
      edit: {
        thin: true,
        text: '',
        show: false,
        disabled () {
          return !vm.hasPermissions('Update')
        }
      },
      remove: {
        thin: true,
        text: '删除',
        show: false,
        disabled () {
          return !vm.hasPermissions('Delete')
        }
      }
    },
    viewOptions: {
      componentType: 'form'
    },
    formOptions: {
      disabled: true,
      defaultSpan: 12 // 默认的表单 span
    },
    indexRow: { // 或者直接传true,不显示title，不居中
      title: '序号',
      align: 'center',
      width: 70
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
          show: false,
          component: {
            placeholder: '请输入关键词'
          }
        }
      },
      {
        title: 'ID',
        key: 'id',
        width: 90,
        disabled: true,
        form: {
          disabled: true
        }
      },
      {
        title: '登录用户名',
        key: 'username',
        search: {
          disabled: false
        },
        width: 140,
        type: 'input',
        form: {
          disabled: true,
          component: {
            placeholder: '请输入登录用户名'
          }
        }
      },
      {
        title: '登录ip',
        key: 'ip',
        search: {
          disabled: false
        },
        width: 130,
        type: 'input',
        form: {
          disabled: true,
          component: {
            placeholder: '请输入登录ip'
          }
        }
      }, {
        title: '运营商',
        key: 'isp',
        search: {
          disabled: true
        },
        disabled: true,
        width: 180,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入运营商'
          }
        }
      }, {
        title: '大州',
        key: 'continent',
        width: 80,
        type: 'input',
        form: {
          disabled: true,
          component: {
            placeholder: '请输入大州'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '国家',
        key: 'country',
        width: 80,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入国家'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '省份',
        key: 'province',
        width: 80,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入省份'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '城市',
        key: 'city',
        width: 80,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入城市'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '县区',
        key: 'district',
        width: 80,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入县区'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '区域代码',
        key: 'area_code',
        width: 100,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入区域代码'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '英文全称',
        key: 'country_english',
        width: 120,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入英文全称'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '简称',
        key: 'country_code',
        width: 100,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入简称'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '经度',
        key: 'longitude',
        width: 80,
        type: 'input',
        disabled: true,
        form: {
          component: {
            placeholder: '请输入经度'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '纬度',
        key: 'latitude',
        width: 80,
        type: 'input',
        disabled: true,
        form: {
          component: {
            placeholder: '请输入纬度'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '登录类型',
        key: 'login_type',
        width: 100,
        type: 'select',
        search: {
          disabled: false
        },
        dict: {
          data: [{ label: '普通登录', value: 1 }, { label: '微信扫码登录', value: 2 }]
        },
        form: {
          component: {
            placeholder: '请选择登录类型'
          }
        },
        component: { props: { color: 'auto' } } // 自动染色
      }, {
        title: '操作系统',
        key: 'os',
        width: 180,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入操作系统'
          }
        }
      }, {
        title: '浏览器名',
        key: 'browser',
        width: 180,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入浏览器名'
          }
        }
      }, {
        title: 'agent信息',
        key: 'agent',
        disabled: true,
        width: 180,
        type: 'input',
        form: {
          component: {
            placeholder: '请输入agent信息'
          }
        }
      }, {
        fixed: 'right',
        title: '登录时间',
        key: 'create_datetime',
        width: 160,
        type: 'datetime'
      }
    ]
  }
}

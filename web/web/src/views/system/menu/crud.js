import { request } from '@/api/service'
import { urlPrefix as menuPrefix } from './api'
import XEUtils from 'xe-utils'
export const crudOptions = (vm) => {
  // 验证路由地址
  const validateWebPath = (rule, value, callback) => {
    const isLink = vm.getEditForm().is_link
    let pattern = /^\/.*?/
    if (isLink) {
      pattern = /^((https|http|ftp|rtsp|mms)?:\/\/)[^\s]+/g
    } else {
      pattern = /^\/.*?/
    }
    if (!pattern.test(value)) {
      callback(new Error('请正确的地址'))
    } else {
      callback()
    }
  }
  return {
    pagination: false,
    pageOptions: {
      compact: true
    },
    options: {
      tableType: 'vxe-table',
      rowKey: true,
      rowId: 'id',
      height: '100%', // 表格高度100%, 使用toolbar必须设置
      highlightCurrentRow: false,
      // defaultExpandAll: true,
      // expandAll: true,
      treeConfig: {
        transform: true,
        rowField: 'id',
        parentField: 'parent',
        expandAll: true,
        hasChild: 'hasChild',
        lazy: true,
        loadMethod: vm.loadContentMethod
      }
    },
    rowHandle: {
      view: {
        thin: true,
        text: '',
        disabled () {
          return !vm.hasPermissions('Retrieve')
        }
      },
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
      width: 230,
      fixed: 'right',
      custom: [{
        show (index, row) {
          if (row.web_path && !row.is_link) {
            return true
          }
          return false
        },
        disabled () {
          return !vm.hasPermissions('Update')
        },
        text: ' 菜单按钮',
        type: 'warning',
        size: 'small',
        emit: 'createPermission'
      }]

    },
    indexRow: { // 或者直接传true,不显示title，不居中
      title: '序号',
      align: 'center',
      width: 80
    },
    viewOptions: {
      componentType: 'form'
    },
    formOptions: {
      defaultSpan: 12 // 默认的表单 span
    },
    columns: [
      {
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
          disabled: true,
          component: {
            props: {
              clearable: true
            }
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
        width: 60,
        form: {
          component: {
            show: false
          }
        }
      },
      {
        title: '父级菜单',
        key: 'parent',
        show: false,
        search: {
          disabled: true
        },
        type: 'cascader',
        dict: {
          url: menuPrefix,
          cache: false,
          isTree: true,
          value: 'id', // 数据字典中value字段的属性名
          label: 'name', // 数据字典中label字段的属性名
          children: 'children', // 数据字典中children字段的属性名
          getData: (url, dict, { form, component }) => { // 配置此参数会覆盖全局的getRemoteDictFunc
            return request({ url: url, params: { limit: 999, status: 1, is_catalog: 1 } }).then(ret => {
              const responseData = ret.data.data
              const result = XEUtils.toArrayTree(responseData, { parentKey: 'parent', strict: true })
              return [{ id: null, name: '根节点', children: result }]
            })
          }
        },
        form: {
          component: {
            props: {
              elProps: {
                clearable: true,
                showAllLevels: false, // 仅显示最后一级
                props: {
                  checkStrictly: true, // 可以不需要选到最后一级
                  emitPath: false,
                  clearable: true
                }
              }
            }
          }
        }
      },
      {
        title: '菜单名称',
        key: 'name',
        sortable: true,
        treeNode: true, // 设置为树形列
        search: {
          disabled: false,
          component: {
            props: {
              clearable: true
            }
          }
        },
        minWidth: 180,
        type: 'input',
        form: {
          rules: [ // 表单校验规则
            { required: true, message: '菜单名称必填项' }
          ],
          component: {
            props: {
              clearable: true
            },
            placeholder: '请输入菜单名称'
          },
          itemProps: {
            class: { yxtInput: true }
          }

        }
      },
      {
        title: '图标',
        key: 'icon',
        width: 60,
        type: 'icon-selector',
        form: {
          component: {
            placeholder: '请输入图标'
          }
        }
      },
      {
        title: '排序',
        key: 'sort',
        width: 60,
        type: 'number',
        form: {
          value: 1,
          component: {
            placeholder: '请输入排序'
          }
        }
      },
      {
        title: '是否目录',
        key: 'is_catalog',
        width: 100,
        type: 'dict-switch',
        search: {
          disabled: true
        },
        dict: {
          data: vm.dictionary('button_whether_bool')
        },
        form: {
          value: false,
          component: {
            placeholder: '请选择是否目录'
          },
          valueChange (key, value, form, { getColumn, mode, component, immediate, getComponent }) {
            if (!value) {
              form.web_path = undefined
              form.component = undefined
              form.component_name = undefined
              form.cache = false
              form.is_link = false
            }
          }
        }
      },
      {
        title: '外链接',
        key: 'is_link',
        width: 70,
        type: 'radio',
        dict: {
          data: vm.dictionary('button_whether_bool')
        },
        form: {
          value: false,
          component: {
            show (context) {
              const { form } = context
              return !form.is_catalog
            },
            placeholder: '请选择是否外链接'
          },
          valueChange (key, value, form, { getColumn, mode, component, immediate, getComponent }) {
            form.web_path = undefined
            form.component = undefined
            form.component_name = undefined
            if (value) {
              getColumn('web_path').title = '外链接地址'
              getColumn('web_path').component.placeholder = '请输入外链接地址'
              getColumn('web_path').helper = {
                render (h) {
                  return (< el-alert title="外链接地址,请以https|http|ftp|rtsp|mms开头" type="warning" />
                  )
                }
              }
            } else {
              getColumn('web_path').title = '路由地址'
              getColumn('web_path').component.placeholder = '请输入路由地址'
              getColumn('web_path').helper = {
                render (h) {
                  return (< el-alert title="浏览器中url的地址,请以/开头" type="warning" />
                  )
                }
              }
            }
          }
        }
      },
      {
        title: '路由地址',
        key: 'web_path',
        width: 150,
        show: false,
        form: {
          rules: [
            { required: true, message: '请输入正确的路由地址' },
            { validator: validateWebPath, trigger: 'change' }
          ],
          component: {
            show (context) {
              const { form } = context
              return !form.is_catalog
            },
            props: {
              clearable: true
            },
            placeholder: '请输入路由地址'
          },
          helper: {
            render (h) {
              return (< el-alert title="浏览器中url的地址,请以/开头" type="warning" />
              )
            }
          }
        }
      },
      {
        title: '组件地址',
        key: 'component',
        type: 'select',
        show: false,
        dict: {
          cache: false,
          data: vm.searchFiles()
        },
        form: {
          rules: [
            { required: true, message: '请选择组件地址' }
          ],
          component: {
            show (context) {
              const { form } = context
              return !form.is_catalog && !form.is_link
            },
            props: {
              clearable: true,
              filterable: true // 可过滤选择项
            },
            placeholder: '请输入组件地址'
          },
          helper: {
            render (h) {
              return (< el-alert title="src/views下的文件夹地址" type="warning" />
              )
            }
          }
        }
      },
      {
        title: '组件名称',
        key: 'component_name',
        width: 170,
        form: {
          rules: [
            { required: true, message: '请输入组件名称' }
          ],
          component: {
            show (context) {
              const { form } = context
              return !form.is_catalog && !form.is_link
            },
            props: {
              clearable: true
            },
            placeholder: '请输入组件名称'
          },
          helper: {
            render (h) {
              return (< el-alert title="xx.vue文件中的name" type="warning" />
              )
            }
          }
        }
      },
      {
        title: '拥有权限',
        key: 'menuPermission',
        type: 'select',
        width: 300,
        form: {
          disabled: true,
          component: {
            elProps: { // el-select的配置项，https://element.eleme.cn/#/zh-CN/component/select
              filterable: true,
              multiple: true,
              clearable: true
            }
          }
        }
      },
      {
        title: '缓存',
        key: 'cache',
        search: {
          disabled: false
        },
        width: 60,
        type: 'radio',
        dict: {
          data: vm.dictionary('button_whether_bool')
        },
        form: {
          value: false,
          component: {
            show (context) {
              const { form } = context
              return !form.is_catalog
            },
            placeholder: '请选择是否缓存'
          },
          helper: {
            render (h) {
              return (< el-alert title="是否开启页面缓存,需要组件名称和xx.vue页面的name一致" type="warning" />
              )
            }
          }
        }
      },
      {
        title: '侧边可见',
        key: 'visible',
        search: {
          disabled: false
        },
        width: 75,
        type: 'radio',
        dict: {
          data: vm.dictionary('button_whether_bool')
        },
        form: {
          value: true,
          component: {
            placeholder: '请选择侧边可见'
          },
          rules: [ // 表单校验规则
            { required: true, message: '侧边可见必填项' }
          ],
          helper: {
            render (h) {
              return (< el-alert title="是否显示在侧边菜单中" type="warning" />
              )
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
        width: 70,
        type: 'radio',
        dict: {
          data: vm.dictionary('button_status_bool')
        },
        form: {
          value: true,
          component: {
            placeholder: '请选择状态'
          },
          rules: [ // 表单校验规则
            { required: true, message: '状态必填项' }
          ]
        }
      }
    ].concat(vm.commonEndColumns({
      update_datetime: { showTable: false }
    }))
  }
}

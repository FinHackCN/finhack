import { request } from '@/api/service'

export const crudOptions = (vm) => {
  return {
    pageOptions: {
      compact: true
    },
    options: {
      tableType: 'vxe-table',
      rowKey: false, // 必须设置，true or false
      height: '100%',
      rowId: 'code',
      highlightCurrentRow: true,
      treeConfig: { // 树形数据配置
        lazy: true,
        children: 'children',
        hasChild: 'hasChildren',
        loadMethod: ({ row }) => {
          return request({
            url: '/api/system/area/',
            method: 'get',
            params: { pcode: row.code, limit: 999 }
          }).then(ret => {
            ret.data.data.map(value => { value.hasChildren = value.pcode_count !== 0 })
            row.hasChildren = false
            return ret.data.data
          })
        },
        iconLoaded: 'el-icon-loading' // 美化loading图标
      }
    },
    rowHandle: {
      show: false,
      width: 140,
      view: {
        thin: true,
        text: '',
        show: false,
        disabled () {
          return !vm.hasPermissions('Retrieve')
        }
      },
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
        text: '',
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
      defaultSpan: 24, // 默认的表单 span
      width: '30%'
    },
    indexRow: { // 或者直接传true,不显示title，不居中
      title: '序号',
      align: 'center',
      width: 100
    },
    columns: [
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
        title: '父级地区',
        key: 'pcode',
        show: false,
        search: {
          disabled: false
        },
        type: 'area-selector',
        // dict: {
        //   url: areaJointPrefix,
        //   lazy: true,
        //   isTree: true,
        //   cache: false,
        //   value: 'code', // 数据字典中value字段的属性名
        //   label: 'name', // 数据字典中label字段的属性名
        //   children: 'children' // 数据字典中children字段的属性名
        // },
        form: {
          component: {
            showAllLevels: false, // 仅显示最后一级
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
        title: '名称',
        key: 'name',
        search: {
          disabled: false
        },
        treeNode: true,
        width: 160,
        type: 'input',
        form: {
          rules: [ // 表单校验规则
            { required: true, message: '名称必填项' }
          ],
          component: {
            placeholder: '请输入名称'
          },
          itemProps: {
            class: { yxtInput: true }
          }
        }
      },
      {
        title: '地区编码',
        key: 'code',
        search: {
          disabled: false
        },
        type: 'input',
        form: {
          rules: [ // 表单校验规则
            { required: true, message: '地区编码必填项' }
          ],
          component: {
            placeholder: '请输入地区编码'
          },
          itemProps: {
            class: { yxtInput: true }
          }
        }
      },
      {
        title: '拼音',
        key: 'pinyin',
        search: {
          disabled: true
        },
        type: 'input',
        form: {
          rules: [ // 表单校验规则
            { required: true, message: '拼音必填项' }
          ],
          itemProps: {
            class: { yxtInput: true }
          },
          component: {
            placeholder: '请输入拼音'
          }
        }
      }, {
        title: '地区层级',
        key: 'level',
        search: {
          disabled: true
        },
        type: 'input',
        form: {
          disabled: false,
          rules: [ // 表单校验规则
            { required: true, message: '拼音必填项' }
          ],
          itemProps: {
            class: { yxtInput: true }
          },
          component: {
            placeholder: '请输入拼音'
          }
        }
      }, {
        title: '首字母',
        key: 'initials',
        form: {
          rules: [ // 表单校验规则
            { required: true, message: '首字母必填项' }
          ],
          itemProps: {
            class: { yxtInput: true }
          },
          component: {
            placeholder: '请输入首字母'
          }
        }
      },
      {
        title: '是否启用',
        key: 'enable',
        search: {
          disabled: false
        },
        width: 90,
        type: 'radio',
        dict: {
          data: vm.dictionary('button_status_bool')
        },
        form: {
          value: true,
          itemProps: {
            class: { yxtInput: true }
          }
        }
      }
    ].concat(vm.commonEndColumns())
  }
}

export const crudOptions = (vm) => {
  return {

    pageOptions: {
      compact: true
    },
    options: {
      rowId: 'id',
      height: '100%', // 表格高度100%, 使用toolbar必须设置
      border: false
    },
    rowHandle: {
      width: 140,
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
      }
    },
    viewOptions: {
      componentType: 'form'
    },
    formOptions: {
      appendToBody: true, // 子表格必须 否则弹出对话框无法显示最顶层
      defaultSpan: 24, // 默认的表单 span
      width: '35%'
    },
    columns: [
      {
        title: '名称',
        key: 'label',

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
            { required: true, message: '名称必填项' }
          ],
          component: {
            props: {
              clearable: true
            },
            placeholder: '请输入名称'
          },
          itemProps: {
            class: { yxtInput: true }
          }
        }
      },
      {
        title: '数据值类型',
        type: 'select',
        key: 'type',
        search: {
          disabled: true,
          component: {
            props: {
              clearable: true
            }
          }
        },
        show: false,
        dict: {
          data: [
            { label: 'text', value: 0 },
            { label: 'number', value: 1 },
            { label: 'date', value: 2 },
            { label: 'datetime', value: 3 },
            { label: 'time', value: 4 },
            { label: 'file', value: 5 },
            { label: 'boolean', value: 6 },
            { label: 'images', value: 7 }
          ]
        },
        form: {
          rules: [ // 表单校验规则
            { required: true, message: '数据值类型必填项' }
          ],
          value: 0,
          component: {
            props: {
              clearable: true
            },
            placeholder: '请选择数据值类型'
          },
          itemProps: {
            class: { yxtInput: true }
          },
          valueChange (key, value, form, { getColumn, mode, component, immediate, getComponent }) {
            const template = vm.getEditFormTemplate('value')
            // 选择框重新选择后，情况value值
            if (!immediate) {
              form.value = undefined
            }
            if (value === 0) {
              template.component.name = 'el-input'
            } else if (value === 1) {
              template.component.name = 'el-input-number'
            } else if (value === 2) {
              template.component.name = 'el-date-picker'
              template.component.props = {
                type: 'date',
                valueFormat: 'yyyy-MM-dd'
              }
            } else if (value === 3) {
              template.component.name = 'el-date-picker'
              template.component.props = {
                type: 'datetime',
                valueFormat: 'yyyy-MM-dd HH:mm:ss'
              }
            } else if (value === 4) {
              template.component.name = 'el-time-picker'
              template.component.props = {
                pickerOptions: {
                  arrowControl: true
                },
                valueFormat: 'HH:mm:ss'
              }
            } else if (value === 5) {
              template.component.name = 'd2p-file-uploader'
              template.component.props = { elProps: { listType: 'text' } }
            } else if (value === 6) {
              template.component.name = 'dict-switch'
              template.component.value = true
              template.component.props = {
                dict: {
                  data: [
                    { label: '是', value: 'true' },
                    { label: '否', value: 'false' }
                  ]
                }
              }
            } else if (value === 7) {
              template.component.name = 'd2p-cropper-uploader'
              template.component.props = { accept: '.png,.jpeg,.jpg,.ico,.bmp,.gif', cropper: { viewMode: 1 } }
            }
          },
          valueChangeImmediate: true
        }
      },
      {
        title: '数据值',
        key: 'value',
        search: {
          disabled: true,
          component: {
            props: {
              clearable: true
            }
          }
        },
        view: {
          component: { props: { height: 100, width: 100 } }
        },
        // 提交时,处理数据
        valueResolve (row, col) {
          const value = row[col.key]
          const type = row.type
          if (type === 5 || type === 7) {
            if (value != null) {
              if (value.length >= 0) {
                if (value instanceof Array) {
                  row[col.key] = value.toString()
                } else {
                  row[col.key] = value
                }
              } else {
                row[col.key] = null
              }
            }
          } else {
            row[col.key] = value
          }
        },
        // 接收时,处理数据
        valueBuilder (row, col) {
          const value = row[col.key]
          const type = row.type
          if (type === 5 || type === 7) {
            if (value != null && value) {
              row[col.key] = value.split(',')
            }
          } else {
            row[col.key] = value
          }
        },
        type: 'input',
        form: {
          rules: [ // 表单校验规则
            { required: true, message: '数据值必填项' }
          ],
          component: {
            props: {
              clearable: true
            },
            placeholder: '请输入数据值'
          },
          itemProps: {
            class: { yxtInput: true }
          }
        }
      }, {
        title: '状态',
        key: 'status',
        width: 80,
        search: {
          disabled: false
        },
        type: 'radio',
        dict: {
          data: vm.dictionary('button_status_bool')
        },
        form: {
          value: true,
          rules: [ // 表单校验规则
            { required: true, message: '状态必填项' }
          ],
          component: {
          },
          itemProps: {
            class: { yxtInput: true }
          }
        }
      },
      {
        title: '排序',
        key: 'sort',
        width: 70,
        type: 'number',
        form: {
          value: 1,
          component: {
          },
          rules: [ // 表单校验规则
            { required: true, message: '排序必填项' }
          ],
          itemProps: {
            class: { yxtInput: true }
          }
        }
      }, {
        title: '标签颜色',
        key: 'color',
        width: 90,
        search: {
          disabled: true
        },
        type: 'select',
        dict: {
          data: [
            { label: 'success', value: 'success', color: 'success' },
            { label: 'primary', value: 'primary', color: 'primary' },
            { label: 'info', value: 'info', color: 'info' },
            { label: 'danger', value: 'danger', color: 'danger' },
            { label: 'warning', value: 'warning', color: 'warning' }
          ]
        },
        form: {
          component: {
            props: {
              clearable: true
            }
          },
          itemProps: {
            class: { yxtInput: true }
          }
        }
      }
    ].concat(vm.commonEndColumns({
      update_datetime: {
        showForm: false,
        showTable: false
      },
      create_datetime: {
        showForm: false,
        showTable: false
      }
    }))
  }
}

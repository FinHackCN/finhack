import { request } from '@/api/service'
import { urlPrefix  } from "./api";
import router from'../../../../router'
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
    rowHandle: {
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
      },
 
      // fixed: 'right',
      custom: [{
        text: '查看回测',
        size: 'small',
        emit: 'show-emit'
      }]

    },
    columns: [
      {
        title: 'id',
        key: 'id',
        width: 90,
        form: {
          disabled: true
        },
        component:{ //添加和修改时form表单的组件
          name: 'values-format',  
          style:{cursor:'pointer'},
          events:{  
               'click': function (e) {
                 console.log(router)
                 const v=e.props.value
                 //alert(v)

                  router.push({path:'/backtest/show',query: {id:v}})
              },
          }
      }
 

      },
      {
        title: '初始资金',
        key: 'init_cash',
        width: 90,
        form: {
          disabled: true
        }
      },
      {
        title: '结束资金',
        key: 'total_value',
        width: 90,
        form: {
          disabled: true
        }
      },
      {
        title: '夏普比率',
        key: 'sharpe',
        sortable: 'custom',
        width: 90,
        form: {
          disabled: true
        }
      },
      {
        title: '最大回撤',
        key: 'max_down',
        width: 90,
        form: {
          disabled: true
        }
      },
    ]
  }
}

import { request } from '@/api/service'
import { urlPrefix  } from "./api";

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
        title: 'id',
        key: 'id',
        width: 90,
        form: {
          disabled: true
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

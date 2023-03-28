import Vue from 'vue'
// import d2Crud from '@d2-project/d2-crud'
import d2CrudX from 'd2-crud-x'
import { d2CrudPlus } from 'd2-crud-plus'
import {
  D2pAreaSelector,
  D2pDemoExtend,
  D2pFileUploader,
  D2pFullEditor,
  D2pIconSelector,
  D2pUploader
} from 'd2p-extends' // 源码方式引入，上传组件支持懒加载
// http请求
import { request } from '@/api/service'
import util from '@/libs/util'
import XEUtils from 'xe-utils'
import store from '@/store/index'
import types from '@/config/d2p-extends/types'
import { checkPlugins, plugins } from '@/views/plugins'

/**
 // vxe0
 import 'xe-utils'
 import VXETable from 'vxe-table'
 import 'vxe-table/lib/index.css'
 Vue.use(VXETable)
 **/

// 按如下重命名引入可与官方版共存，index.vue中标签用<d2-crud-x />使用加强版
// 不传name，则d2CrudX的标签仍为<d2-crud>,不可与官方版共存
Vue.use(d2CrudX, { name: 'd2-crud-x' })
// 注册dvadmin插件
Vue.use(plugins)
// // 官方版【此处为演示与官方版共存而引入，全新项目中可以用d2-crud-x完全替代官方版】
// Vue.use(d2Crud)
/**
 * @description 校验插件是否安装
 * @param {String} pluginName 插件名称
 */
Vue.prototype.checkPlugins = checkPlugins
// 引入d2CrudPlus
Vue.use(d2CrudPlus, {
  starTip: false,
  getRemoteDictFunc (url, dict) {
    // 此处配置你的字典http请求方法
    // 实际使用请改成request
    return request({
      url: url,
      params: dict.body,
      method: 'get'
    }).then(ret => {
      if (dict.isTree) {
        return XEUtils.toArrayTree(ret.data.data || ret.data, { parentKey: 'parent', strict: false })
      } else {
        return ret.data.data || ret.data
      }
    })
  },
  commonOption () { // 公共配置
    return {
      format: {
        page: { // page接口返回的数据结构配置，
          request: {
            current: 'page',
            size: 'limit'
          },
          response: {
            current: 'page', // 当前页码 ret.data.current
            size: 'limit', // 当前页码 ret.data.current
            // size: (data) => { return data.size }, // 每页条数，ret.data.size, 你也可以配置一个方法，自定义返回
            total: 'total', // 总记录数 ret.data.total
            records: 'data' // 列表数组 ret.data.records
          }
        }
      },
      pageOptions: {
        compact: true
      },
      options: {
        size: 'small'
      },
      formOptions: {
        nullToBlankStr: true, // 提交修改表单时，将undefinded的数据修改为空字符串''，可以解决无法清空字段的问题
        defaultSpan: 12, // 默认的表单 span
        saveRemind: true,
        labelWidth: '110px'
      },
      viewOptions: {
        disabled: false,
        componentType: 'form' // 【form,row】 表单组件 或 行组件展示
      },
      rowHandle: {
        width: 260,
        edit: {
          type: 'primary'
        }
      }
    }
  }
})

// 安装扩展插件
// Vue.use(D2pTreeSelector)
Vue.use(D2pAreaSelector)
Vue.use(D2pIconSelector)
Vue.use(D2pFullEditor, {
  ueditor: {
    serverUrl: '/api/ueditor/'
  }
})
Vue.use(D2pDemoExtend)
Vue.use(D2pFileUploader)
Vue.use(D2pUploader, {
  defaultType: 'form',
  cos: {
    domain: 'https://d2p-demo-1251260344.cos.ap-guangzhou.myqcloud.com',
    bucket: 'd2p-demo-1251260344',
    region: 'ap-guangzhou',
    secretId: '', //
    secretKey: '', // 传了secretKey 和secretId 代表使用本地签名模式（不安全，生产环境不推荐）
    getAuthorization (custom) { // 不传secretKey代表使用临时签名模式,此时此参数必传（安全，生产环境推荐）
      return request({
        url: '/upload/cos/getAuthorization',
        method: 'get'
      }).then(ret => {
        // 返回结构如下
        // ret.data:{
        //   TmpSecretId,
        //   TmpSecretKey,
        //   XCosSecurityToken,
        //   ExpiredTime, // SDK 在 ExpiredTime 时间前，不会再次调用 getAuthorization
        // }
        return ret.data
      })
    }
  },
  alioss: {
    domain: 'https://d2p-demo.oss-cn-shenzhen.aliyuncs.com',
    bucket: 'd2p-demo',
    region: 'oss-cn-shenzhen',
    accessKeyId: '',
    accessKeySecret: '',
    getAuthorization (custom, context) { // 不传accessKeySecret代表使用临时签名模式,此时此参数必传（安全，生产环境推荐）
      return request({
        url: '/upload/alioss/getAuthorization',
        method: 'get'
      }).then(ret => {
        return ret.data
      })
    },
    sdkOpts: { // sdk配置
      secure: true // 默认为非https上传,为了安全，设置为true
    }
  },
  qiniu: {
    bucket: 'd2p-demo',
    getToken (custom) {
      return request({
        url: '/upload/qiniu/getToken',
        method: 'get'
      }).then(ret => {
        return ret.data // {token:xxx,expires:xxx}
      })
    },
    domain: 'http://d2p.file.veryreader.com'
  },
  form: {
    action: util.baseURL() + 'api/system/file/',
    name: 'file',
    data: {}, // 上传附加参数
    headers () {
      return {
        Authorization: 'JWT ' + util.cookies.get('token')
      }
    },
    type: 'form',
    successHandle (ret, option) {
      if (ret.data === null || ret.data === '') {
        throw new Error('上传失败')
      }
      return { url: util.baseURL() + ret.data.url, key: option.data.key, id: ret.data.id }
    },
    withCredentials: false // 是否带cookie
  }
})
d2CrudPlus.util.columnResolve.addTypes(types)
// 修改官方字段类型
const selectType = d2CrudPlus.util.columnResolve.getType('select')
selectType.component.props.color = 'auto' // 修改官方的字段类型，设置为支持自动染色
// 获取字典配置
Vue.prototype.dictionary = function (name) {
  return store.state.d2admin.dictionary.data[name]
}
// 获取字典label值
Vue.prototype.getDictionaryLabel = function (name, value) {
  const data = store.state.d2admin.dictionary.data[name]
  if (data && data instanceof Array) {
    for (var i = 0, len = data.length; i < len; i++) {
      if (data[i].value === value) {
        return data[i].label
      }
    }
    return ''
  }
  return store.state.d2admin.dictionary.data[name]
}
// 获取系统配置
Vue.prototype.systemConfig = function (name) {
  return store.state.d2admin.settings.data[name]
}
// 默认Columns 结尾 showForm：显示在form中，showTable：显示在table中
Vue.prototype.commonEndColumns = function (param = {}) {
  /**
   * @param {Object} {
    description: {
      showForm: true,
      showTable: false
    },
    dept_belong_id: {
      showForm: false,
      showTable: false
    },
    modifier_name: {
      showForm: false,
      showTable: false
    },
    update_datetime: {
      showForm: false,
      showTable: true
    },
    create_datetime: {
      showForm: false,
      showTable: true
    }
  }
   */
  const showData = {
    description: {
      showForm: (param.description && param.description.showForm) !== undefined ? param.description.showForm : true,
      showTable: (param.description && param.description.showTable) !== undefined ? param.description.showTable : false
    },
    dept_belong_id: {
      showForm: (param.dept_belong_id && param.dept_belong_id.showForm) !== undefined ? param.dept_belong_id.showForm : false,
      showTable: (param.dept_belong_id && param.dept_belong_id.showTable) !== undefined ? param.dept_belong_id.showTable : false,
      showSearch: (param.dept_belong_id && param.dept_belong_id.showSearch) !== undefined ? param.dept_belong_id.showSearch : false
    },
    modifier_name: {
      showForm: (param.modifier_name && param.modifier_name.showForm) !== undefined ? param.modifier_name.showForm : false,
      showTable: (param.modifier_name && param.modifier_name.showTable) !== undefined ? param.modifier_name.showTable : false
    },
    update_datetime: {
      showForm: (param.update_datetime && param.update_datetime.showForm) !== undefined ? param.update_datetime.showForm : false,
      showTable: (param.update_datetime && param.update_datetime.showTable) !== undefined ? param.update_datetime.showTable : true
    },
    creator_name: {
      showForm: (param.creator_name && param.creator_name.showForm) !== undefined ? param.creator_name.showForm : false,
      showTable: (param.creator_name && param.creator_name.showTable) !== undefined ? param.creator_name.showTable : false
    },
    create_datetime: {
      showForm: (param.create_datetime && param.create_datetime.showForm) !== undefined ? param.create_datetime.showForm : false,
      showTable: (param.create_datetime && param.create_datetime.showTable) !== undefined ? param.create_datetime.showTable : true
    }
  }
  return [
    {
      title: '备注',
      key: 'description',
      show: showData.description.showTable,
      search: {
        disabled: true
      },
      type: 'textarea',
      form: {
        disabled: !showData.description.showForm,
        component: {
          placeholder: '请输入内容',
          showWordLimit: true,
          maxlength: '200',
          props: {
            type: 'textarea'
          }
        }
      }
    },
    {
      title: '修改人',
      show: showData.modifier_name.showTable,
      width: 100,
      key: 'modifier_name',
      form: {
        disabled: !showData.modifier_name.showForm
      }
    },
    {
      title: '所属部门',
      key: 'dept_belong_id',
      show: showData.dept_belong_id.showTable,
      width: 150,
      search: {
        disabled: !showData.dept_belong_id.showSearch
      },
      type: 'tree-selector',
      dict: {
        cache: false,
        url: '/api/system/dept/all_dept/',
        // isTree: true,
        // dept: true,
        value: 'id', // 数据字典中value字段的属性名
        label: 'name', // 数据字典中label字段的属性名
        children: 'children' // 数据字典中children字段的属性名
        // getData: (url, dict, {
        //   _,
        //   component
        // }) => {
        //   return request({
        //     url: url
        //   }).then(ret => {
        //     return XEUtils.toArrayTree(ret.data, { parentKey: 'parent', strict: false })
        //   })
        // }
      },
      component: {
        name: 'dept-format',
        props: { multiple: false, clearable: true }
      },
      form: {
        disabled: !showData.dept_belong_id.showForm,
        component: {
          props: { multiple: false, clearable: true }
        },
        helper: {
          render (h) {
            return (< el-alert title="默认不填则为当前创建用户的部门ID" type="info" />
            )
          }
        }
      },
      // 接收时,处理数据
      valueBuilder (row, col) {
        if (row[col.key]) {
          row[col.key] = Number(row[col.key])
        }
      }
    },
    {
      title: '更新时间',
      key: 'update_datetime',
      width: 160,
      show: showData.update_datetime.showTable,
      type: 'datetime',
      sortable: true,
      form: {
        disabled: !showData.update_datetime.showForm
      }
    },
    {
      title: '创建时间',
      key: 'create_datetime',
      width: 160,
      search: {
        disabled: !showData.create_datetime.showForm,
        width: 240,
        component: { // 查询框组件配置，默认根据form配置生成
          name: 'el-date-picker',
          props: {
            type: 'daterange',
            'range-separator': '至',
            'start-placeholder': '开始',
            'end-placeholder': '结束',
            valueFormat: 'yyyy-MM-dd'
          }
        }
      },
      show: showData.create_datetime.showTable,
      type: 'datetime',
      sortable: true,
      form: {
        disabled: !showData.create_datetime.showForm
      }
    }
  ]
}

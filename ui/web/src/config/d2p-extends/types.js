import util from '@/libs/util.js'
export default {
  'image-uploader': {
    form: { component: { name: 'd2p-file-uploader', props: { elProps: { listType: 'picture-card', accept: '.png,.jpeg,.jpg,.ico,.bmp,.gif' } } } },
    component: { name: 'd2p-images-format' },
    view: {
      component: { props: { height: 100, width: 100 } }
    },
    align: 'center',
    // 提交时,处理数据
    valueResolve (row, col) {
      const value = row[col.key]
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
    },
    // 接收时,处理数据
    valueBuilder (row, col) {
      const value = row[col.key]
      if (value != null && value) {
        row[col.key] = value.split(',')
        // 进行组装地址，纠正地址
        row[col.key].map((val, index) => {
          if (val.startsWith('/api')) {
            row[col.key][index] = val
          } else if (val.startsWith('/')) {
            row[col.key][index] = util.baseURL() + val.slice(1)
          } else {
            row[col.key][index] = !val.startsWith('http') ? util.baseURL() + val : val
          }
        })
      }
    }
  },
  'avatar-uploader': {
    form: { component: { name: 'd2p-file-uploader', props: { elProps: { limit: 1, listType: 'avatar', accept: '.png,.jpeg,.jpg,.ico,.bmp,.gif', showFileList: false } } } },
    component: { name: 'd2p-images-format' },
    view: {
      component: { props: { height: 100, width: 100 } }
    },
    align: 'center',
    // 提交时,处理数据
    valueResolve (row, col) {
      const value = row[col.key]
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
    },
    // 接收时,处理数据
    valueBuilder (row, col) {
      const value = row[col.key]
      if (value != null && value) {
        row[col.key] = value.split(',')
        // 进行组装地址，纠正地址
        row[col.key].map((val, index) => {
          if (val.startsWith('/api')) {
            row[col.key][index] = val
          } else if (val.startsWith('/')) {
            row[col.key][index] = util.baseURL() + val.slice(1)
          } else {
            row[col.key][index] = !val.startsWith('http') ? util.baseURL() + val : val
          }
        })
      }
    }
  },
  'file-uploader': {
    form: { component: { name: 'd2p-file-uploader', props: { elProps: { listType: 'text' } } } },
    component: { name: 'd2p-files-format' },
    // 提交时,处理数据
    valueResolve (row, col) {
      const value = row[col.key]
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
    },
    // 接收时,处理数据
    valueBuilder (row, col) {
      const value = row[col.key]
      if (value != null && value) {
        row[col.key] = value.split(',')
        // 进行组装地址，纠正地址
        row[col.key].map((val, index) => {
          if (val.startsWith('/api')) {
            row[col.key][index] = val
          } else if (val.startsWith('/')) {
            row[col.key][index] = util.baseURL() + val.slice(1)
          } else {
            row[col.key][index] = !val.startsWith('http') ? util.baseURL() + val : val
          }
        })
      }
    }
  },
  'avatar-cropper': {
    form: { component: { name: 'd2p-cropper-uploader', props: { accept: '.png,.jpeg,.jpg,.ico,.bmp,.gif', cropper: { viewMode: 1 } } } },
    component: { name: 'd2p-images-format' },
    align: 'center',
    view: {
      component: { props: { height: 100, width: 100 } }
    },
    // 提交时,处理数据
    valueResolve (row, col) {
      const value = row[col.key]
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
    },
    // 接收时,处理数据
    valueBuilder (row, col) {
      const value = row[col.key]
      if (value != null && value) {
        row[col.key] = value.split(',')
        // 进行组装地址，纠正地址
        row[col.key].map((val, index) => {
          if (val.startsWith('/api')) {
            row[col.key][index] = val
          } else if (val.startsWith('/')) {
            row[col.key][index] = util.baseURL() + val.slice(1)
          } else {
            row[col.key][index] = !val.startsWith('http') ? util.baseURL() + val : val
          }
        })
      }
    }
  },
  'tree-selector': {
    form: { component: { name: 'd2p-tree-selector', props: { } } },
    component: { name: 'values-format', props: {} }
  }
}

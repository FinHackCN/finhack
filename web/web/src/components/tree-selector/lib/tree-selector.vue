<template>
  <div class="d2p-tree-selector">
    <div  class="el-cascader el-cascader--default" :class="{'is-disabled':disabled}" @click="openDialog">
      <div class="el-input el-input--default el-input--suffix" :class="{'is-disabled':disabled}" >
        <el-input ref="reference" :disabled="disabled" :placeholder="value ? '' : placeholder"/>
        <span class="el-input__suffix">
          <span class="el-input__suffix-inner">
            <i class="el-input__icon el-icon-arrow-down" @click="openDialog"/>
          </span>
        </span>
      </div>
      <div class="el-cascader__tags" ref="tags">
        <transition-group @after-leave="resetInputHeight" >
          <el-tag
            v-for="item in selected"
            :key="getValueKey(item)"
            :closable="clearable"
            :size="collapseTagSize"
            :hit="false"
            type="info"
            @close="itemClosed(item)"
            disable-transitions>
            <span class="el-select__tags-text">{{ getValueLabel(item) }}</span>
          </el-tag>
        </transition-group>
      </div>
    </div>
    <el-dialog custom-class="d2p-tree-selector-dialog"
      :title="dialogTitle"
      :visible.sync="dialogVisible"
      width="30%" append-to-body>
      <div class="tree-wrapper">
        <div v-if="treeFilter" class="filter-bar" style="padding-bottom: 20px">
          <el-input
            prefix-icon="el-icon-search"
            :placeholder="filterPlaceholder"
            v-model="filterText" size="small" >
          </el-input>
        </div>

        <div class="tree-body">
          <el-tree
            :data="_options"
            @check-change="handleCheckChange"
            @current-change="handleCurrentChange"
            :filterNodeMethod="filterNode"
            v-bind="_elProps"
            ref="elTree">
          </el-tree>
        </div>

      </div>

      <span slot="footer" class="dialog-footer">
          <el-button @click="dialogVisible = false">{{cancelText}}</el-button>
          <el-button type="primary" @click="selectSubmit">{{confirmText}}</el-button>
        </span>
    </el-dialog>
  </div>
</template>

<script>
import lodash from 'lodash'
import { d2CrudPlus } from 'd2-crud-plus'
import log from 'd2p-extends/src/utils/util.log'
// 树形选择组件，需要import {D2pTreeSelector} from 'd2p-extends'
export default {
  name: 'd2p-tree-selector',
  mixins: [d2CrudPlus.input, d2CrudPlus.inputDict],
  props: {
    // 值
    value: {
      type: [Number, String, Boolean, Array, Object]
    },
    // 过滤，value中的nodes过滤方法 参数为nodes
    filter: {
      type: Function,
      require: false
    },
    // 占位符
    placeholder: {
      type: String,
      required: false,
      default: '请选择'
    },
    // 过滤的placeholder
    filterPlaceholder: {
      type: String,
      default: '输入关键字进行过滤'
    },
    dialogTitle: {
      type: String,
      default: '选择'
    },
    cancelText: {
      type: String,
      default: '取消'
    },
    confirmText: {
      type: String,
      default: '确定'
    },
    // 树形组件节点过滤，可以配置elProps.filterNodeMethod ，覆盖默认的过滤方法
    treeFilter: {
      type: Boolean,
      require: false,
      default: true
    },
    // 是否多选，传入false为单选
    multiple: {
      type: Boolean,
      default: true
    },
    // 是否忽略选中节点的子节点
    ignoreFullCheckedChildren: { type: Boolean, default: true },
    // 是否只返回叶子节点
    leafOnly: { type: Boolean, default: false },
    // 是否包含半选节点
    includeHalfChecked: { type: Boolean, default: false },
    // el-tree的属性配置
    elProps: {
      type: Object
    },
    /**
     * 是否可以清除
     */
    clearable: {
      type: Boolean,
      default: true
    },
    // 数据字典配置
    dict: {
      type: Object,
      require: false
    }
  },
  data () {
    return {
      currentValue: undefined,
      collapseTags: false,
      selected: [],
      dialogVisible: false,
      filterText: undefined
    }
  },
  created () {
    // if (this.dict) {
    //   this.dict = d2CrudPlus.util.dict.mergeDefault(this.dict, true)
    // }
    // this.initData()
  },
  computed: {
    _elProps () {
      const defaultElProps = {
        showCheckbox: this.multiple,
        highlightCurrent: !this.multiple,
        props: {}
      }
      if (this.dict != null) {
        if (this.dict.label != null) { defaultElProps.props.label = this.dict.label }
        if (this.dict.value != null) { defaultElProps.props.value = this.dict.value }
        if (this.dict.children != null) { defaultElProps.props.children = this.dict.children }
      }
      defaultElProps.nodeKey = defaultElProps.props.value
      lodash.merge(defaultElProps, this.elProps)
      return defaultElProps
    },
    collapseTagSize () {
      return ['small', 'mini'].indexOf(this.selectSize) > -1
        ? 'mini'
        : 'small'
    }
  },
  watch: {
    filterText (val) {
      this.$refs.elTree.filter(val)
    }
  },
  methods: {
    // initData () {
    //   d2CrudPlus.util.dict.get(this.dict).then(ret => {
    //     this.$set(this, 'data', ret)
    //     this.setValue(this.value)
    //   })
    // },
    onDictLoaded () {
      log.debug('onDictLoaded', this.dict, this.value)
      this.setValue(this.value)
    },
    setValue (value) {
      log.debug('setValue:', this.currentValue, this.value, this._options)
      if (this.currentValue === this.value) {
        return
      }
      let arrValue = value
      if (value == null) {
        this.selected = []
      }

      if (!(arrValue instanceof Array)) {
        arrValue = [arrValue]
      }
      if (this.dict && this.dict.getNodes) {
        log.debug('getNodes:', arrValue)
        this.dict.getNodes(arrValue).then(nodes => {
          this.selectedNodes(nodes, value)
        })
      } else {
        const nodes = []
        if (this._options == null || this._options.length === 0) {
          return
        }
        for (const item of arrValue) {
          const data = this._options
          const node = d2CrudPlus.util.dict.getByValue(item, data, this.dict)
          if (node != null) {
            nodes.push(node)
          }
        }
        this.selectedNodes(nodes, value)
      }
    },
    selectedNodes (nodes, value) {
      const selected = []
      for (const node of nodes) {
        node.id = node[this.dict.value]
        selected.push(node)
      }
      log.debug('selected:', selected)
      this.$set(this, 'selected', selected)
      this.resetInputHeight()
    },
    handleCheckChange (event) {
      this.$emit('check-change', event)
    },
    handleCurrentChange (event) {
      this.$emit('current-change', event)
    },
    openDialog () {
      if (this.disabled) {
        return
      }
      this.dialogVisible = true
      setTimeout(() => {
        if (this.selected != null) {
          const ids = this.selected.map(item => item[this._elProps.props.value])
          ids.forEach(id => {
            const current = this.$refs.elTree.store.nodesMap[id]
            if (current != null) {
              this.doExpandParent(current)
            }
          })
          this.$nextTick(() => {
            if (this.multiple) {
              this.$refs.elTree.setCheckedKeys(ids, this.leafOnly)
            } else if (ids.length > 0) {
              this.$refs.elTree.setCurrentKey(ids[0])
            }
          })
        }
      }, 1)
    },
    doExpandParent (node) {
      if (node.parent != null) {
        this.doExpandParent(node.parent)
      }
      node.expanded = true
    },
    getValueKey (item) {
      if (this._elProps.props.value != null) {
        return item[this._elProps.props.value]
      } else {
        return item.value
      }
    },
    getValueLabel (item) {
      if (this._elProps.props.label != null) {
        return item[this._elProps.props.label]
      } else {
        return item.label
      }
    },
    getValueChildren (item) {
      let children = 'children'
      if (this._elProps.props.children != null) {
        children = this._elProps.props.children
      }
      return item[children]
    },
    selectSubmit () {
      const nodes = this.refreshSelected()
      this.dialogVisible = false
      this.doValueInputChanged(nodes)
    },
    doValueInputChanged (nodes) {
      let values = this.formatValue(nodes)
      this.resetInputHeight()
      if (!this.multiple) {
        values = values && values.length > 0 ? values[0] : undefined
      }
      this.currentValue = values
      if (this.dispatch) {
        this.dispatch('ElFormItem', 'el.form.blur')
      }
      this.$emit('input', values)
    },
    itemClosed (item) {
      const newNodes = lodash.without(this.selected, item)
      console.log('new value', item, newNodes)
      this.$set(this, 'selected', newNodes)
      this.doValueInputChanged(newNodes)
    },
    refreshSelected () {
      let nodes = null
      if (this.multiple) {
        nodes = this.$refs.elTree.getCheckedNodes(this.leafOnly, this.includeHalfChecked)
      } else {
        const node = this.$refs.elTree.getCurrentNode()
        if (node == null) {
          nodes = []
        } else {
          nodes = [node]
        }
      }
      if (this.ignoreFullCheckedChildren) {
        nodes = this.filterFullCheckedChildren(nodes)
      }
      if (this.filter != null) {
        nodes = this.filter(nodes)
      }
      log.debug('selected', this.selected)
      this.$set(this, 'selected', nodes)
      return nodes
    },
    resetInputHeight () {
      if (this.collapseTags && !this.filterable) return
      this.$nextTick(() => {
        if (!this.$refs.reference) return
        const inputChildNodes = this.$refs.reference.$el.childNodes
        const input = [].filter.call(inputChildNodes, item => item.tagName === 'INPUT')[0]
        const tags = this.$refs.tags
        const sizeInMap = this.initialInputHeight || 40
        const height = this.selected.length === 0
          ? sizeInMap + 'px'
          : Math.max(
            tags ? (tags.clientHeight + (tags.clientHeight > sizeInMap ? 6 : 0)) : 0,
            sizeInMap
          ) + 'px'
        input.style.height = height
        if (this.visible && this.emptyText !== false) {
          this.broadcast('ElSelectDropdown', 'updatePopper')
        }
      })
    },
    filterFullCheckedChildren (nodes) {
      const ignored = new Set()
      for (const item of nodes) {
        const children = this.getValueChildren(item)
        if (children != null) {
          for (const child of children) {
            ignored.add(this.getValueKey(child))
          }
        }
      }
      const values = []
      for (const item of nodes) {
        const key = this.getValueKey(item)
        if (!ignored.has(key)) {
          values.push(item)
        }
      }
      return values
    },
    formatValue (nodes) {
      const values = []
      for (const item of nodes) {
        values.push(this.getValueKey(item))
      }
      return values
    },
    filterNode (value, data) {
      if (!value) return true
      return this.getValueLabel(data).indexOf(value) !== -1
    },
    onChange (value) {
      this.$emit('change', value)

      if (this.dispatch) {
        this.dispatch('ElFormItem', 'el.form.blur')
      }
    }
  }
}
</script>
<style lang="scss">
.d2p-tree-selector{
  width: 100%;
  .el-cascader{
    width: 100%;
  }
  .is-disabled .el-tag__close.el-icon-close {
    display: none;
  }
}
.d2p-tree-selector-dialog{
  &.el-dialog{
    max-height:70vh;
    display: flex;
    flex-direction: column;
    .el-dialog__body{
      flex:1;
      overflow-y: auto;
    }
    .el-dialog__header{
      padding: 20px 20px 20px;
      border-bottom: 1px solid #eee;
    }
    .el-dialog__footer {
      padding: 10px 20px 10px;
      border-top: 1px solid #eee;
    }
  }
}
</style>

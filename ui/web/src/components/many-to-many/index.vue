<template>
  <div>
    <el-tag  style="margin-right: 10px" :type="color" v-for="(item,index) in currentValue" :key="index">{{
        item[key]
      }}
    </el-tag>
  </div>
</template>
<script>
// 行展示组件进阶版
// 本示例演示要对传入的值做一些改变，然后再展示
export default {
  name: 'many-to-many',
  props: {
    color: {
      require: false
    },
    value: {
      type: Array,
      required: false
    }
  },
  data () {
    return {
      currentValue: [],
      key: 'name'
    }
  },
  watch: {
    value (nv, ov) {
      const { row } = this.$parent.scope
      const { children } = this.$parent
      if (children) {
        const valueBinding = this.$parent.valueBinding
        this.setValue(row[valueBinding])
        this.key = children
      } else {
        this.setValue([])
      }
    }
  },
  created () {
    const { row } = this.$parent.scope
    const { children } = this.$parent
    if (children) {
      const valueBinding = this.$parent.valueBinding
      this.setValue(row[valueBinding])
      this.key = children
    } else {
      this.setValue([])
    }
  },
  methods: {
    setValue (value) {
      // 在这里对 传入的value值做处理
      this.currentValue = value
    }
  }
}
</script>

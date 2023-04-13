<template>
  <div>
    <el-tag v-if="currentValue" :type="color">{{ currentValue }}</el-tag>
    <span v-else></span>
  </div>
</template>
<script>
// 行展示组件进阶版
// 本示例演示要对传入的值做一些改变，然后再展示
export default {
  name: 'foreign-key',
  props: {
    color: {
      require: false
    },
    value: {
      type: [String, Number],
      required: false
    }
  },
  data () {
    return {
      currentValue: ''
    }
  },
  watch: {
    value (nv, ov) {
      const { row } = this.$parent.scope
      const valueBinding = this.$parent.valueBinding
      this.setValue(row[valueBinding])
    }
  },
  created () {
    const { row } = this.$parent.scope
    const valueBinding = this.$parent.valueBinding
    this.setValue(row[valueBinding])
  },
  methods: {
    setValue (value) {
      // 在这里对 传入的value值做处理
      this.currentValue = value
    }
  }
}
</script>

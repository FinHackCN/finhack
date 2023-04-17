<template>
  <div>
    {{$store.state.d2admin.dept.data[currentValue] || ''}}
  </div>
</template>
<script>
// 行展示组件进阶版
// 本示例演示要对传入的值做一些改变，然后再展示
export default {
  name: 'dept-format',
  props: {
    // 接收row.xxx的值
    value: {
      type: Number || String,
      required: false
    },
    color: {
      require: false
    }
  },
  data () {
    return {
      currentValue: ''
    }
  },
  watch: {
    value (value) {
      // this.$emit('change', value)
      if (this.currentValue === value) {
        return
      }
      this.setValue(value)
    }
  },
  created () {
    this.setValue(this.value)
  },
  methods: {
    setValue (value) {
      // 在这里对 传入的value值做处理
      this.currentValue = String(this.value)
      // 根据值的key 递归获取对应的名称
    }
  }
}
</script>
Footer

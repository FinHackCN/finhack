<template>
  <el-tooltip effect="dark" :content="'颜色'" placement="bottom">
    <el-color-picker
      class="can-hover"
      :value="value"
      :predefine="predefine"
      size="mini"
      @change="set"/>
  </el-tooltip>
</template>

<script>
import { mapState, mapActions } from 'vuex'

export default {
  name: 'd2-header-color',
  data () {
    return {
      predefine: [
        '#ff4500',
        '#ff8c00',
        '#ffd700',
        '#90ee90',
        '#00ced1',
        '#1e90ff',
        '#c71585'
      ]
    }
  },
  computed: {
    ...mapState('d2admin/color', [
      'value'
    ])
  },
  watch: {
    value (value) {
      this.set(value)
    }
  },
  methods: {
    ...mapActions('d2admin/color', [
      'set'
    ])
  }
}
</script>

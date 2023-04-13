<template>
  <div id="app">
    <router-view />
<!--  用来兼容乾坤前端微服务  -->
    <div id="qiankun"></div>
  </div>
</template>

<script>
import util from '@/libs/util'
export default {
  name: 'app',
  watch: {
    '$i18n.locale': 'i18nHandle'
  },
  created () {
    this.i18nHandle(this.$i18n.locale)
  },
  methods: {
    i18nHandle (val, oldVal) {
      util.cookies.set('lang', val)
      document.querySelector('html').setAttribute('lang', val)
    }
  }
}
</script>

<style lang="scss">
@import "~@/assets/style/public-class.scss";
@import "~@/assets/style/yxt-public.scss";
</style>

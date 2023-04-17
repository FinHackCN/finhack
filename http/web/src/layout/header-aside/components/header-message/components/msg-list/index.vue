<template>
<div>
  <el-divider content-position="left">消息中心</el-divider>
  <div v-if="msgObj">
     <h3>{{msgObj.title}}</h3>
    <div class="content-style" v-html="msgObj.content"></div>
  </div>
  <div v-else>
    <el-empty :image-size="100"></el-empty>
  </div>
  <el-divider></el-divider>
  <div style="text-align: center">
    <el-button type="text" @click="toPage">前往通知中心</el-button>
  </div>
</div>
</template>

<script>
import { mapActions } from 'vuex'
export default {
  name: 'msgList',
  props: {
    msgObj: {
      type: Object,
      default: null
    }
  },
  methods: {
    ...mapActions('d2admin/page', [
      'open'
    ]),
    toPage () {
      // this.open({name:'messageCenter'})
      this.$router.push({
        name: 'messageCenter'
      })
    }
  }
}
</script>

<style scoped>
  .msg-list{
    padding: 5px 0px;
    border-bottom: 1px solid #eeeeee;
  }
  .content-style{
    width: 370px;
    overflow: hidden;
    text-overflow: ellipsis;
    display: -webkit-box;
    -webkit-line-clamp: 3; /*3表示只显示3行*/
    -webkit-box-orient: vertical;
  }
</style>

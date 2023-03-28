<template>
  <d2-container>
    <div class="page-header">

      <el-avatar src="https://q1.qlogo.cn/g?b=qq&amp;nk=190848757&amp;s=640" class="user-avatar">

      </el-avatar>
      <div class="title">
        <h1>早安, DVAdmin, 开始您一天的工作吧！</h1>
        <span> 今日晴，20℃ - 32℃！ </span>
      </div>
    </div>

    <el-row :gutter="20">
      <el-col :span="12">
        <el-card class="box-card">
          <div slot="header" class="clearfix">
            <span>友情链接</span>

            <el-button style="float: right; padding: 3px 0" type="text">
              <el-link href="https://bbs.django-vue-admin.com" target="_blank" type="primary">更多</el-link>
            </el-button>
          </div>
          <el-row>
              <el-col :span="8" v-for="({name,imageUrl,slogan,link},index) in projects" :key="index">
              <el-card shadow="hover" style="padding: 0">
                <div class="project-detail">
                  <div>
                    <a :href="link" target="_blank">
                      <img :src="imageUrl" alt="">
                      <span v-text="name" class="name"></span>
                    </a>
                  </div>
                  <div v-text="slogan" class="slogan" :title="slogan"></div>
                </div>

              </el-card>
              </el-col>
          </el-row>
        </el-card>

      </el-col>

      <el-col :span="12">
        <div class="grid-content bg-purple">

          <el-card class="box-card" >
            <div slot="header" class="clearfix">
              <span>快捷导航</span>
            </div>
            <el-row>
              <el-col :span="8" v-for="({name,icon,route,color},index) of navigators" :key="index" style="padding: 0">
                <el-card shadow="hover">
                  <div  style="display: flex;align-items: center;flex-direction: column;cursor: pointer" @click="()=>{gotoRoute(route)}">
                    <d2-icon-svg :name="icon" style="width: 25px;height: 25px;" :style="{fill:color}"/>
                    <div style="text-align: center;font-size: 12px;margin-top: 20px" v-text="name"></div>
                  </div>
                </el-card>
              </el-col>
            </el-row>
          </el-card>

          <el-card class="box-card"  style="margin-top: 25px">
            <div class="work">
              <d2-icon-svg name="work" style="margin-left: 50%;transform: translateX(-50%);height: 216px"/>
            </div>
          </el-card>
        </div>
      </el-col>
    </el-row>

  </d2-container>
</template>

<script>
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { PieChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent
} from 'echarts/components'

use([
  CanvasRenderer,
  PieChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent
])
export default {
  name: 'workbench',
  data () {
    return {
      projects: [
        {
          name: '官方文档',
          imageUrl: '/image/django-vue-admin.png',
          slogan: 'Django-Vue-Admin 是一套全部开源的快速开发平台，毫无保留给个人及企业免费使用。',
          link: 'http://django-vue-admin.com'
        },
        {
          name: '官方论坛',
          imageUrl: '/image/django-vue-admin.png',
          slogan: 'Django-Vue-Admin 官方论坛',
          link: 'http://bbs.django-vue-admin.com'
        },
        {
          name: 'D2admin',
          imageUrl: '/image/d2-pub.png',
          slogan: 'D2Admin (opens new window)是一个完全 开源免费 的企业中后台产品前端集成方案，使用最新的前端技术栈，' +
            '小于 60kb 的本地首屏 js 加载，已经做好大部分项目前期准备工作，并且带有大量示例代码，助力管理系统快速开发。',
          link: 'https://d2.pub/zh'
        },
        {
          name: 'SimpleUi',
          imageUrl: '/image/simple-ui.png',
          slogan: '一个基于Django Admin的现代化主题。',
          link: 'https://simpleui.72wo.com/'
        },
        {
          name: '若依',
          imageUrl: '/image/ruoyi.png',
          slogan: '基于SpringBoot、Shiro、Mybatis的权限后台管理系统。',
          link: 'http://ruoyi.vip/'
        },
        {
          name: 'Gin-Vue-Admin',
          imageUrl: '/image/gin-vue-admin.png',
          slogan: '使用gin+vue进行极速开发的全栈后台管理系统。',
          link: 'https://www.gin-vue-admin.com/'
        },
        {
          name: 'DCM',
          imageUrl: '/image/django-comment-migrate.png',
          slogan: '这是一个Django model注释迁移的app',
          link: 'https://github.com/starryrbs/django-comment-migrate'
        },
        {
          name: 'Jetbrains',
          imageUrl: '/image/jetbrains.jpeg',
          slogan: '我们构建我们的软件，让您可以享受构建自己的软件的乐趣',
          link: 'https://www.jetbrains.com/'
        },
        {
          name: 'Django',
          imageUrl: '/image/django.png',
          slogan: '有期限的完美主义者的网络框架。',
          link: 'https://github.com/django/django'
        }
      ],
      navigators: [
        {
          name: '控制台',
          icon: 'home',
          route: {
            name: 'index'
          },
          color: 'rgb(31, 218, 202);'
        },
        {
          name: '部门管理',
          icon: 'department',
          route: {
            name: 'dept'
          },
          color: 'rgb(225, 133, 37);'
        },
        {
          name: '角色管理',
          icon: 'role',
          route: {
            name: 'role'
          },
          color: 'rgb(191, 12, 44);'
        },
        {
          name: '菜单管理',
          icon: 'menu',
          route: {
            name: 'menu'
          },
          color: 'rgb(63, 178, 127);'
        },
        {
          name: '用户管理',
          icon: 'user',
          route: {
            name: 'user'
          },
          color: 'rgb(191, 12, 44);'
        },
        {
          name: '日志管理',
          icon: 'log',
          route: {
            name: 'operationLog'
          },
          color: 'rgb(0, 216, 255);'
        }
      ],
      chartData: {
        columns: ['日期', '销售额'],
        rows: [
          { 日期: '1月1日', 销售额: 123 },
          { 日期: '1月2日', 销售额: 1223 },
          { 日期: '1月3日', 销售额: 2123 },
          { 日期: '1月4日', 销售额: 4123 },
          { 日期: '1月5日', 销售额: 3123 },
          { 日期: '1月6日', 销售额: 7123 }
        ]
      }
    }
  },
  methods: {
    gotoRoute (route) {
      this.$router.push(route)
    }
  }
}
</script>

<style scoped lang="scss">

  $userAvatarLength: 72px;

  .page-header{
    box-sizing: border-box;
    padding: 16px;
    .user-avatar{
        width: $userAvatarLength;
        height: $userAvatarLength;
        line-height: $userAvatarLength;
      display: inline-block;
    }

    .title{
      display: inline-block;
      padding: 0 0 0 15px;
      position: relative;
      top: -5px;

      h1{
        font-size: 1.125rem;
        font-weight: 500;
        line-height: 1.75rem;
      }
      span{
        font-size: 14px;
        color: rgba(0,0,0,.45);
      }
    }

  }

  .project-detail{
    color: rgba(0,0,0,.45);
    height: 65px;
     img {
       width: 25px;
       height: 25px;
     }
    .name{
      margin-left: 1rem;
      font-size: 1rem;
      line-height: 2rem;
      height: 2rem;
      display: inline-block;
      color: rgba(0,0,0,.85);
      position: relative;
      top: -5px;
    }
    .slogan{
      font-size: 12px;
      padding: 5px 0;
      overflow:hidden;
      text-overflow:ellipsis;
      white-space:nowrap;
    }
    .team{
      font-size: 14px;
    }
  }

  .activity{
    padding: 0;
    .activity-avatar{
      width: 40px;
      height: 40px;
      line-height: 40px;
    }
    .activity-detail{
      padding: 10px;
      line-height: 15px;
      font-size: 14px;
      color: rgba(0,0,0,.85);
    }
  }
  .chart {
    height: 408px;
  }

  .el-divider--horizontal{
    margin: 4px 0;
    background: 0 0;
    border-top: 1px solid #e8eaec;

  }
  .el-card, .el-message {
    border-radius: 0;
  }
</style>

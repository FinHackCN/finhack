<template>
  <div class="page-login"></div>
</template>

<script>
import { mapActions, mapState } from 'vuex'
import localeMixin from '@/locales/mixin.js'
import * as api from '@/views/system/login/api'

export default {
  mixins: [localeMixin],
  beforeCreate () {
    // 初始化配置
    this.$store.dispatch('d2admin/settings/init')
  },
  data () {
    return {
      processTitle: process.env.VUE_APP_TITLE || 'D2Admin',
      backgroundImage: 'url(' + this.loginBackground + ')',
      // 表单
      formLogin: {
        username: '',
        password: '',
        captcha: ''
      },
      // 表单校验
      rules: {
        username: [
          {
            required: true,
            message: '请输入用户名',
            trigger: 'blur'
          }
        ],
        password: [
          {
            required: true,
            message: '请输入密码',
            trigger: 'blur'
          }
        ]
      },
      captchaKey: null,
      image_base: null,
      // 快速登录，用于dev开发环境
      selectUsersDialogVisible: false,
      users: [
        {
          name: '超管',
          username: 'superadmin',
          password: 'admin123456'
        },
        {
          name: '管理员',
          username: 'admin',
          password: 'admin123456'
        }
      ]
    }
  },
  computed: {
    ...mapState('d2admin', {
      siteLogo: state => state.settings.data['login.site_logo'] || require('./image/dvadmin.png'), // 网站logo地址
      keepRecord: state => state.settings.data['login.keep_record'],
      siteName: state => state.settings.data['login.site_name'], // 网站名称
      copyright: state => state.settings.data['login.copyright'],
      loginBackground: state => state.settings.data['login.login_background'] || require('./image/bg.jpg'), // 登录页背景图
      helpUrl: state => state.settings.data['login.help_url'], // 帮助
      privacyUrl: state => state.settings.data['login.privacy_url'], // 隐私
      clauseUrl: state => state.settings.data['login.clause_url'], // 条款
      captchaState: state => state.settings.data['base.captcha_state'] !== undefined ? state.settings.data['base.captcha_state'] : true // 验证码
    })
  },
  mounted () {
  },
  beforeDestroy () {
  },
  methods: {
    ...mapActions('d2admin/account', ['login']),
    /**
       * 获取验证码
       */
    getCaptcha () {
      if (this.captchaState !== undefined && !this.captchaState) return
      api.getCaptcha().then((ret) => {
        this.formLogin.captcha = null
        this.captchaKey = ret.data.key
        this.image_base = ret.data.image_base
      })
    },
    /**
       * @description 提交表单
       */
    // 提交登录信息
    submit () {
      const that = this
      this.$refs.loginForm.validate((valid) => {
        if (valid) {
          // 登录
          // 注意 这里的演示没有传验证码
          // 具体需要传递的数据请自行修改代码
          this.login({
            username: that.formLogin.username,
            password: that.$md5(that.formLogin.password),
            captcha: that.formLogin.captcha,
            captchaKey: that.captchaKey
          })
            .then(() => {
              // 重定向对象不存在则返回顶层路径
              this.$router.replace(this.$route.query.redirect || '/')
            })
            .catch(() => {
              this.getCaptcha()
            })
        } else {
          // 登录表单校验失败
          this.$message.error('表单校验失败，请检查')
        }
      })
    },
    // 快速登录
    handleUserBtnClick (user) {
      this.formLogin.username = user.username
      this.formLogin.password = user.password
      // this.submit()
      this.selectUsersDialogVisible = false
      if (!this.captchaState) {
        this.submit()
      }
    }
  },
  created () {
    this.$store.dispatch('d2admin/db/databaseClear')
    this.getCaptcha()
  }
}
</script>

<style lang="scss" scoped>
  // ----
  .page-login {
    position: absolute;
    top: 0;
    left: 0;
    height: 100%;
    width: 100%;
    background-image: url(./image/bg.jpg);
    background-position: center 0;
    background-repeat: no-repeat;
    background-attachment: fixed;
    background-size: cover;
    -webkit-background-size: cover; /* 兼容Webkit内核浏览器如Chrome和Safari */
    -o-background-size: cover; /* 兼容Opera */
    zoom: 1;
  }

  ::v-deep .el-card__body {
    height: 100%;
    padding: 0;
  }

  .card {
    height: 100%;
    width: 100%;
    border-radius: 30px;
    padding: 0;
    margin-top: 12%;
  }

  .right-card {
    float: right;
    text-align: center;
    width: 50%;
    height: 100%;
  }

  .right-card h1 {
    color: #098dee;
    margin-bottom: 40px;
    margin-top: 40px;
  }

  .button-login {
    width: 100%;
    margin-top: 30px;
  }

  ::v-deep .el-input-group__append {
    padding: 0;
  }

  // footer
  .page-login--content-footer {
    margin-top: 10%;
    padding: 1em 0;

    .page-login--content-footer-locales {
      padding: 0px;
      margin: 0px;
      margin-bottom: 15px;
      font-size: 12px;
      line-height: 12px;
      text-align: center;
      color: $color-text-normal;

      a {
        color: $color-text-normal;
        margin: 0 0.5em;

        &:hover {
          color: $color-text-main;
        }
      }
    }

    .page-login--content-footer-copyright {
      padding: 0px;
      margin: 0px;
      margin-bottom: 10px;
      font-size: 12px;
      line-height: 12px;
      text-align: center;
      color: $color-text-normal;

      a {
        color: $color-text-normal;
      }
    }

    .page-login--content-footer-options {
      padding: 0px;
      margin: 0px;
      font-size: 12px;
      line-height: 12px;
      text-align: center;

      a {
        color: $color-text-normal;
        margin: 0 1em;
      }
    }
  }
</style>

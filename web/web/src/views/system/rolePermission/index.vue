<!--
 * @创建文件时间: 2021-06-01 22:41:21
 * @Auther: 猿小天
 * @最后修改人: 猿小天
 * @最后修改时间: 2021-09-26 21:18:29
 * 联系Qq:1638245306
 * @文件介绍:授权管理
-->
<template>
  <div>
    <div style="margin: 10px">
      <el-button
        type="primary"
        size="mini"
        @click="submitPermisson"
        v-permission="'Save'"
      >保存
      </el-button>
    </div>
    <el-container style="height: 80vh; border: 1px solid #eee">
      <el-aside width="300px" style="border:1px solid #eee;padding: 20px;">
        <div style="margin: 10px;">
          <div style="margin-bottom: 20px">
            <div class="yxt-flex-align-center">
              <div class="yxt-divider"></div>
              <span>数据授权</span>
              <el-tooltip
                class="item"
                effect="dark"
                :content="dataAuthorizationTips"
                placement="right"
              >
                <el-icon class="el-icon-question"></el-icon>
              </el-tooltip>
            </div>
          </div>

          <div>
            <el-select
              v-show="roleObj.name"
              v-model="roleObj.data_range"
              @change="dataScopeSelectChange"
            >
              <el-option
                v-for="item in dataScopeOptions"
                :key="item.value"
                :label="item.label"
                :value="item.value"
              ></el-option>
            </el-select>
          </div>

          <div v-show="roleObj.data_range === 4" class="dept-tree">
            <el-tree
              :data="deptOptions"
              show-checkbox
              default-expand-all
              :default-checked-keys="deptCheckedKeys"
              ref="dept"
              node-key="id"
              :check-strictly="true"
              :props="{ label: 'name' }"
            ></el-tree>
          </div>

        </div>
      </el-aside>
      <el-main>
        <div style="margin: 10px;">
          <div>
            <div style="margin-bottom: 20px">
              <div class="yxt-flex-align-center">
                <div class="yxt-divider"></div>
                <span>菜单授权</span>
                <el-tooltip
                  class="item"
                  effect="dark"
                  :content="menuAuthorizationTips"
                  placement="right"
                >
                  <el-icon class="el-icon-question"></el-icon>
                </el-tooltip>
              </div>
            </div>
            <el-tree
              class="flow-tree"
              ref="menuTree"
              :data="menuOptions"
              node-key="id"
              default-expand-all
              show-checkbox
              :expand-on-click-node="false"
              :default-checked-keys="menuCheckedKeys"
              :check-on-click-node="false"
              empty-text="请先选择角色"
              :check-strictly="menuCheckStrictly"
              @check-change="handleCheckClick"
            >
              <span class="custom-tree-node" slot-scope="{ node, data }">
                <div class="yxt-flex-between">
                  <div :style="{width:((4-node.level)*18+100)+'px'}">{{ data.name }}</div>
                  <div>
                    <el-checkbox
                      v-for="(item, index) in data.menuPermission"
                      :key="index"
                      v-model="item.checked"
                    >{{ item.name }}</el-checkbox>
                  </div>
                </div>
              </span>
            </el-tree>
          </div>
        </div>
        <el-backtop target=".el-main"></el-backtop>
      </el-main>
    </el-container>
  </div>
</template>

<script>
import * as api from './api'
import XEUtils from 'xe-utils'

export default {
  name: 'rolePermission',
  props: {
    roleObj: {
      type: Object,
      default () {
        return {
          name: null,
          data_range: null
        }
      }
    }
  },
  data () {
    return {
      filterText: '',
      data: [],
      menuOptions: [],
      permissionData: [],
      menuCheckedKeys: [], // 菜单默认选中的节点
      menuCheckStrictly: false,
      deptOptions: [],
      deptCheckedKeys: [],
      dataScopeOptions: [
        {
          value: 0,
          label: '仅本人数据权限'
        },
        {
          value: 1,
          label: '本部门及以下数据权限'
        },
        {
          value: 2,
          label: '本部门数据权限'
        },
        {
          value: 3,
          label: '全部数据权限'
        },
        {
          value: 4,
          label: '自定义数据权限'
        }
      ],
      dataAuthorizationTips: '授权用户可操作的数据范围',
      menuAuthorizationTips: '授权用户在菜单中可操作的范围'
    }
  },
  watch: {
    filterText (val) {
      this.$refs.tree.filter(val)
    }
  },
  methods: {
    filterNode (value, data) {
      if (!value) return true
      return data.label.indexOf(value) !== -1
    },
    getCrudOptions () {
      // eslint-disable-next-line no-undef
      return crudOptions(this)
    },
    pageRequest (query) {
      return api.GetList(query).then(res => {
        res.map((value, index) => {
          value.node_id = index
        })
        this.data = res
        this.$nextTick().then(() => {
          this.initNode()
        })
      })
    },
    initNode () {
      this.getDeptData()
      this.getMenuData(this.roleObj)
      this.menuCheckedKeys = this.roleObj.menu // 加载已勾选的菜单
      this.menuCheckStrictly = true // 父子不相互关联
      this.deptCheckedKeys = this.roleObj.dept
      this.GetDataScope()
    },
    addRequest (row) {
      return api.createObj(row)
    },
    updateRequest (row) {
      return api.UpdateObj(row)
    },
    delRequest (row) {
      return api.DelObj(row.id)
    },
    // 获取部门数据
    getDeptData () {
      api.GetDataScopeDept().then(ret => {
        this.deptOptions = XEUtils.toArrayTree(ret.data, { parentKey: 'parent', strict: false })
      })
    },
    // 获取菜单数据
    getMenuData (data) {
      api.GetMenuData(data).then(res => {
        res.forEach(x => {
          // 根据当前角色的permission,对menuPermisson进行勾选处理
          x.menuPermission.forEach(a => {
            if (data.permission.indexOf(a.id) > -1) {
              this.$set(a, 'checked', true)
            } else {
              this.$set(a, 'checked', false)
            }
          })
        })
        // 将菜单列表转换为树形列表
        this.menuOptions = XEUtils.toArrayTree(res, {
          parentKey: 'parent',
          strict: true
        })
      })
    },
    // 获取权限范围
    GetDataScope () {
      api.GetDataScope().then(res => {
        this.dataScopeOptions = res.data
      })
    },
    // 所有勾选菜单节点数据
    getMenuAllCheckedKeys () {
      // 目前被选中的菜单节点
      const checkedKeys = this.$refs.menuTree.getCheckedKeys()
      // 半选中的菜单节点
      const halfCheckedKeys = this.$refs.menuTree.getHalfCheckedKeys()
      checkedKeys.unshift.apply(checkedKeys, halfCheckedKeys)
      return checkedKeys
    },
    // 所有自定义权限时,勾选的部门节点数据
    getDeptAllCheckedKeys () {
      // 目前被选中的部门节点
      const checkedKeys = this.$refs.dept.getCheckedKeys()
      // 半选中的部门节点
      const halfCheckedKeys = this.$refs.dept.getHalfCheckedKeys()
      checkedKeys.unshift.apply(checkedKeys, halfCheckedKeys)
      return checkedKeys
    },
    // 提交修改
    submitPermisson () {
      this.roleObj.menu = this.getMenuAllCheckedKeys() // 获取选中的菜单
      this.roleObj.dept = this.getDeptAllCheckedKeys() // 获取选中的部门
      const menuData = XEUtils.toTreeArray(this.menuOptions)
      const permissionData = []
      menuData.forEach(x => {
        const checkedPermission = x.menuPermission.filter(f => {
          return f.checked
        })

        if (checkedPermission.length > 0) {
          for (const item of checkedPermission) {
            permissionData.push(item.id)
          }
        }
      })
      this.roleObj.permission = permissionData
      return this.updateRequest(this.roleObj).then(res => {
        this.$message.success('更新成功')
      })
    },
    /** 选择角色权限范围触发 */
    dataScopeSelectChange (value) {
      if (value !== 4) {
        // this.$refs.dept.setCheckedKeys([]);
      }
    },
    /**
     * 菜单树点击,全选权限部分数据
     * @param data
     */
    handleCheckClick (data, checked) {
      const {
        menuPermission,
        children,
        parent
      } = data
      this.menuCheckStrictly = false
      for (const item of menuPermission) {
        this.$set(item, 'checked', checked)
      }
      if (children && parent) {
        for (const item of children) {
          this.$refs.menuTree.setChecked(item.id, checked)
        }
      }
    }
  },
  created () {
    this.pageRequest()
  }
}
</script>

<style lang="scss">
.yxtInput {
  .el-form-item__label {
    color: #49a1ff;
  }
}

.dept-tree::-webkit-scrollbar {
  display: none; /* Chrome Safari */
}

.dept-tree {
  height: 160px;
  overflow-y: scroll;
  scrollbar-width: none; /* firefox */
  -ms-overflow-style: none; /* IE 10+ */
}

.flow-tree {
  overflow: auto;
  flex: 1;

  margin: 10px;

  .el-tree-node {
    .el-tree-node__children {
      overflow: visible !important
    }
  }
}

</style>

/**
 * 对请求参数进行过滤
 *@param that=>this
 *@param array:其他字段数组
 */
const filterParams = function (that, array) {
  that.$nextTick(() => {
    const arr = that.crud.columns
    const columnKeys = arr.map(item => {
      return item.key
    })
    let newArray = [...columnKeys, array, 'id']
    newArray = [...new Set(newArray)]
    that.crud.searchOptions.form.query = '{' + newArray.toString() + '}'
  })
}
export default filterParams

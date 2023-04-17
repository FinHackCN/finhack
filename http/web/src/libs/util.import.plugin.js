module.exports = file => {
  var result
  try {
    result = require('@great-dream/' + file).default
  } catch (error) {
    result = require('@/views/plugins/' + file).default
  }
  return result
}

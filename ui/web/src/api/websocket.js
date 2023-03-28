import ElementUI from 'element-ui'
import util from '@/libs/util'
import store from '@/store'
function initWebSocket (e) {
  const token = util.cookies.get('token')
  if (token) {
    const wsUri = util.wsBaseURL() + 'ws/' + token + '/'
    this.socket = new WebSocket(wsUri)// 这里面的this都指向vue
    this.socket.onerror = webSocketOnError
    this.socket.onmessage = webSocketOnMessage
    this.socket.onclose = closeWebsocket
  }
}

function webSocketOnError (e) {
  ElementUI.Notification({
    title: '',
    message: 'WebSocket连接发生错误' + JSON.stringify(e),
    type: 'error',
    position: 'bottom-right',
    duration: 3000
  })
}

/**
 * 接收消息
 * @param e
 * @returns {any}
 */
function webSocketOnMessage (e) {
  const data = JSON.parse(e.data)
  const { unread } = data
  store.dispatch('d2admin/messagecenter/setUnread', unread || 0)
  if (data.contentType === 'SYSTEM') {
    ElementUI.Notification({
      title: '系统消息',
      message: data.content,
      type: 'success',
      position: 'bottom-right',
      duration: 3000
    })
  } else if (data.contentType === 'ERROR') {
    ElementUI.Notification({
      title: '',
      message: data.content,
      type: 'error',
      position: 'bottom-right',
      duration: 0
    })
  } else if (data.contentType === 'INFO') {
    ElementUI.Notification({
      title: '温馨提示',
      message: data.content,
      type: 'success',
      position: 'bottom-right',
      duration: 0
    })
  } else {
    ElementUI.Notification({
      title: '温馨提示',
      message: data.content,
      type: 'info',
      position: 'bottom-right',
      duration: 3000
    })
  }
}
// 关闭websiocket
function closeWebsocket () {
  console.log('连接已关闭...')
  ElementUI.Notification({
    title: 'websocket',
    message: '连接已关闭...',
    type: 'danger',
    position: 'bottom-right',
    duration: 3000
  })
}

/**
 * 发送消息
 * @param message
 */
function webSocketSend (message) {
  this.socket.send(JSON.stringify(message))
}
export default {
  initWebSocket, closeWebsocket, webSocketSend
}

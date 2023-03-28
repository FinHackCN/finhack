export default {
  title: {
    title: '标题',
    key: 'title',
    component: {
      span: 24,
      placeholder: '请输入标题',
      disabled: true
    },
    rules: [
      {
        required: true,
        message: '必填项'
      }
    ],
    order: 10
  },
  content: {
    title: '内容',
    key: 'content',
    component: {
      name: 'd2p-quill',
      span: 24,
      disabled: true,
      props: {
        uploader: {
          type: 'form'
        }
      },
      events: {}
    },
    rules: [
      {
        required: true,
        message: '必填项'
      }
    ],
    order: 10
  }
}

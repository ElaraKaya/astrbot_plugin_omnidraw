# 异步图片任务

custom_async_endpoint 先提交任务，再按任务 ID 轮询。

内置默认主机按任务接口查询：

GET {origin}/v1/tasks/{task_id}

提交响应里的 data 是数组，任务 ID 取 data[0].task_id。完成后图片地址在 data.result.images[].url[]。

参考图不能直接传 base64。本地图或 data URL 会先 POST {origin}/v1/uploads/images（表单字段 file），再把返回的 url 放进 image_urls。已经是 http/https 的参考图原样传入。

其它异步节点仍把任务 ID 接在提交地址后面：{endpoint}/{task_id}。

这些都可以按节点改，留空则保持上面的默认行为：

- async_poll_template：轮询地址。占位符 {origin}、{endpoint}、{task_id}。
- async_success_status：成功状态，逗号或换行分隔。留空为 SUCCESS、SUCCEEDED、COMPLETED。
- async_image_path：取图点路径，例如 data.result.images.0.url.0。数字是数组下标，[] 表示第一项。
- async_upload_template：参考图上传地址。填了之后，本地图和 data URL 会先上传，响应要有顶层 url。
- async_image_field：参考图字段。image_urls、images、urls 或以 _urls 结尾时按数组提交。

轮询间隔 3 秒，上限是节点 timeout。明确失败立即报错，超时按节点失败处理。

直接返回图片的同步自定义接口不受影响。

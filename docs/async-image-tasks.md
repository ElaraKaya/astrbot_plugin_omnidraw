# 异步图片任务

custom_async_endpoint 先提交任务，再按任务 ID 轮询。

通用规则：

- 轮询地址留空时，把任务 ID 接在提交地址后面，即 `{endpoint}/{task_id}`。
- 参考图上传地址留空时，直接把图片 base64 放进提交体。
- 参考图字段名留空时，按 `image`、`image2`、`image3` 分别提交。
- 成功状态留空时，认 SUCCESS、SUCCEEDED、COMPLETED。

所以一个中转站只要符合上面四条，什么都不用填就能用。不符合，就按节点覆盖：

- async_poll_template：轮询地址。占位符 {origin}、{endpoint}、{task_id}。任务不在提交地址下的必须填，例如 `{origin}/v1/tasks/{task_id}`。
- async_success_status：成功状态，逗号或换行分隔，不区分大小写。
- async_image_path：取图点路径，例如 `data.result.images.0.url.0`。数字是数组下标，`[]` 表示第一项。
- async_upload_template：参考图上传地址，表单字段 `file`，响应要有顶层 `url`。留空表示不上传。填了这个就必须填 async_image_field。
- async_image_field：参考图字段。填 `image_urls`、`images`、`urls` 或以 `_urls` 结尾时按数组提交，最多 16 张；否则第一个填字段本身，其余填 `字段名2`、`字段名3`，最多 3 张。

任务 ID 会从 `id`、`task_id`、`taskId`、`job_id`、`request_id` 以及嵌套的 `data`、`task`、`result`、`response` 里递归找。提交响应里直接带图片地址时不再轮询。

轮询间隔 3 秒，上限是节点 timeout。明确失败立即报错，超时按节点失败处理。连续 4 次轮询报错直接失败。

需要特殊 Header 或请求签名的接口，以上配置项不够，另外接。

直接返回图片的同步自定义接口不受影响。

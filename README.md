<img width="1913" height="908" alt="ScreenShot_2026-04-30_185648_880" src="https://github.com/user-attachments/assets/b4e99b19-de83-40a9-8e97-f2d974556c76" /># FPBrowser2API：指纹浏览器驱动的半自动化 AI 生成框架

> 项目主题：利用指纹浏览器半自动化模拟真人环境 + 逆向注入/页面上下文代码执行，实现高并发自动化 AI 视频 & AI 图片生成。当前框架已围绕 Sora、veo3.1 / VEO/Google Flow、Grok、banana2/pro、Seedance2.0 国际站等方向设计；理论上可扩展到任意网站的自动化与接口逆向研究，并内置 Cloudflare/claudeflare 验证页检测、等待、自愈重启与自动点击等处理策略，目标是在授权研究环境中自动过 claudeflare/Cloudflare（实际通过率取决于账号、代理、环境与站点策略）。

## 重要声明

- 本项目**只供技术研究、学习验证、私有测试环境使用**，请勿用于商业化运营、批量滥用、违反目标站点条款或任何违法违规用途。
- 使用者应自行承担账号、额度、风控、合规和数据安全风险。
- 即梦国际站 / Dreamina / Seedance2.0 国际站相关能力也在研究中，代码中已有 `jimeng_task_executor.py` 等探索实现，后续会继续完善。
- 联系方式：微信 `aimh8com`。

## 为什么需要指纹浏览器

本框架不是传统的裸 HTTP 调用脚本，而是通过指纹浏览器维持一个更接近真人使用的浏览器环境：

1. 每个任务绑定一个独立的浏览器窗口、Cookie、LocalStorage、UA、代理和浏览器指纹。
2. 用户可以先在指纹浏览器中手动登录目标站点、完成必要验证或订阅准备。
3. 后端再通过指纹浏览器暴露的 CDP 地址连接到该窗口，在页面上下文中执行 `fetch`、上传文件、轮询状态、读取结果。
4. 当遇到 Cloudflare/Turnstile 等挑战页时，执行器会检测页面、等待自动放行、尝试点击验证控件，必要时关闭并重开指纹窗口进行自愈。

推荐使用 RoxyBrowser：请到 <https://roxybrowser.com?code=0416Z62A> 下载客户端，注册并登录后创建空间、项目和浏览器窗口。


<img width="1920" height="857" alt="ScreenShot_2026-04-30_185608_912" src="https://github.com/user-attachments/assets/c619aab0-1198-40e3-a490-69a378842086" />
<img width="1910" height="883" alt="ScreenShot_2026-04-30_185628_966" src="https://github.com/user-attachments/assets/467b9021-ad71-400c-94fe-b24ceed925bc" />
<img width="1919" height="914" alt="ScreenShot_2026-04-30_185638_945" src="https://github.com/user-attachments/assets/34238cc6-66c0-41eb-97b0-405014ea467c" />
<img width="1913" height="908" alt="ScreenShot_2026-04-30_185648_880" src="https://github.com/user-attachments/assets/1684701a-12dd-4dfd-a89f-90c8169e0ea7" />


## 快速启动

```bash
cd fpbrowser2api

# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 首次使用 Playwright 时安装 Chromium 驱动
python -m playwright install chromium

# 启动服务
python main.py
```

默认监听：`http://127.0.0.1:8002`

首次默认账号：

- 用户名：`admin`
- 密码：`admin`

基础配置位于 `config/setting.toml`（可参考 `config/setting_example.toml`）。服务启动后会把关键配置写入数据库，并支持在管理后台中修改，重启后仍生效。

## 启动 / 停止 / 重启（服务脚本）

Linux/Mac（或 Git Bash）：

```bash
cd fpbrowser2api
chmod +x ./fpbrowser2api_service.sh
./fpbrowser2api_service.sh start|stop|restart|status
```

Windows PowerShell：

```powershell
cd fpbrowser2api
powershell -ExecutionPolicy Bypass -File .\fpbrowser2api_service.ps1 start|stop|restart|status
```

## 管理后台

- 登录页：`/login`
- 系统配置：`/admin/system`
- 项目管理：`/admin/projects`
- 任务类型管理：`/admin/task-types`
- 任务列表：`/admin/tasks`
- 测试页面：`/admin/test`
- 请求日志：`/admin/logs`
- 用户管理：`/admin/users`

## 如何连接指纹浏览器

本项目当前以 RoxyBrowser 为主要适配对象，核心链路如下：

```text
管理后台配置浏览器服务器
  -> FPBrowserClient 调 RoxyBrowser 本地/局域网 API
  -> 同步空间(workspaceId)、项目(projectIds)、窗口(dirId)
  -> 任务类型绑定具体窗口
  -> TaskService 根据任务类型和额度选择窗口
  -> PlaywrightBrowserContext 打开/复用窗口并连接 CDP
  -> 站点执行器在页面上下文中 fetch / 上传 / 轮询 / 读取结果
```

### 1. RoxyBrowser 侧准备

1. 下载并登录 RoxyBrowser：<https://roxybrowser.com?code=0416Z62A>。
2. 在 RoxyBrowser 中创建空间（Workspace）、项目和浏览器窗口。
3. 为窗口配置代理、账号信息、目标站点登录态，并确保窗口可以正常访问目标站点。
4. 启用 RoxyBrowser 的本地 API / 局域网 API，并记录：
   - API 地址，例如 `http://127.0.0.1:xxxxx` 或局域网地址。
   - API token（如 RoxyBrowser 侧开启了 token 校验）。
   - `workspaceId`：本项目中对应 `space_id`，必须是纯数字。
   - `dirId`：本项目中对应 `window_key`，代表具体浏览器窗口。

### 2. FPBrowser2API 侧配置

在管理后台中按顺序配置：

1. **项目管理**：创建一个本地项目。
2. **浏览器服务器**：填写 RoxyBrowser API 地址：
   - `vendor`：`roxy`
   - `lan_addr`：RoxyBrowser API base URL
   - `access_key`：RoxyBrowser token，可为空
3. **空间**：填写 RoxyBrowser `workspaceId`，可选填写 Roxy 项目 ID 过滤条件。
4. **同步窗口**：后台会调用：
   - `GET /browser/list_v3` 获取窗口列表。
   - `GET /browser/detail` 获取窗口明细。
   - 提取 `dirId`、窗口名称、平台账号、代理 IP、国家、内核版本等信息写入本地数据库。
5. **任务类型绑定窗口**：创建任务类型并选择处理器，例如：
   - `sora_gen_video`
   - `veo_workflow`
   - `grok_workflow`
   - `dreamina_workflow`

### 3. 运行时连接过程

连接逻辑主要在以下文件中：

- `src/services/fp_browser_client.py`：RoxyBrowser API 适配层。
- `src/services/playwright_broswer_context.py`：通用 Playwright + 指纹浏览器上下文。
- `src/services/task_service.py`：任务调度、窗口池、额度和并发控制。

运行时会先查询窗口是否已经打开：

1. 调用 RoxyBrowser `GET /browser/connection_info` 获取已打开窗口的 `http/ws` 调试地址。
2. 若窗口未打开，则调用 `POST /browser/open`，传入 `workspaceId`、`dirId`、启动参数、headless/pure_mode 等。
3. RoxyBrowser 返回 `data.http` 或 `data.ws` 后，框架会规范化为 CDP endpoint。
4. 使用 `playwright.chromium.connect_over_cdp(endpoint)` 连接到真实指纹浏览器窗口。
5. 执行器选择可用页面或打开目标页面，并通过页面上下文执行 `fetch`、文件上传、Cookie/Token 读取等操作。
6. 任务完成后通常只断开本地 Playwright/CDP 连接，不立即关闭指纹窗口，以便保留登录态并降低频繁开关窗口带来的风控风险。

## 核心服务一：`src/services/sora_task_executor.py`

`sora_task_executor.py` 是 Sora 站点侧执行器，入口函数是 `sora_gen_video`，由 `task_service.py` 根据任务类型分发调用。

### 职责边界

- 不负责管理 RoxyBrowser API 细节，这部分由 `fp_browser_client.py` 和 `playwright_broswer_context.py` 处理。
- 专注 Sora 站点逻辑：鉴权抓取、任务创建、进度轮询、草稿发布、余额检查、邀请码读取、角色创建等。

### 关键能力

- **窗口级会话缓存**：`SoraSession` 按 `vendor + base_url + space_id + window_key` 缓存，复用同一个指纹窗口和 CDP 连接。
- **登录态/Token 获取**：在指纹窗口页面上下文中访问 `/api/auth/session`，读取 `accessToken` 和过期时间。
- **页面内接口调用**：使用页面上下文 `fetch` 调 Sora 后端接口，继承窗口 Cookie、UA、代理和指纹。
- **生成任务提交**：调用 `/backend/nf/create` 创建视频生成任务，支持 prompt、首帧图、比例/方向、时长帧数等参数。
- **进度轮询**：轮询 pending / drafts 等接口，持续回写任务进度。
- **草稿发布与结果提取**：任务完成后从 drafts 中找到对应生成结果，发布并返回 `share_url`、`watermark_free_url`、`thumb_url` 等。
- **角色创建**：支持通过 `video_url` 或 `generation_id + head_url` 创建 Sora 角色，包含上传、状态轮询、头像处理、finalize、公开设置等步骤。
- **额度/会员辅助**：支持 `nf_check`、订阅信息、邀请码、清空 drafts 等管理端操作。
- **Cloudflare 自愈**：检测 “Just a moment”、`/cdn-cgi/`、Turnstile 等页面特征，等待放行、尝试 checkbox 点击，必要时重启指纹窗口。
- **并发保护**：创建任务使用会话级锁串行化，避免同一窗口同时提交多个创建请求；不同窗口可并发工作。

### 常见 Sora payload

```json
{
  "prompt": "a cinematic robot walking in the rain",
  "first_image_url": "https://example.com/first-frame.png",
  "size_ratio": "9:16",
  "duration": 300,
  "sora_url": "https://sora.chatgpt.com/drafts",
  "sora_pending_max_wait_seconds": 900
}
```

创建角色示例：

```json
{
  "video_url": "https://example.com/character.mp4",
  "character_video_timestamps": "0,3",
  "sora_url": "https://sora.chatgpt.com/drafts"
}
```

或基于已有生成结果创建角色：

```json
{
  "generation_id": "gen_xxx",
  "head_url": "https://example.com/avatar.png"
}
```

## 核心服务二：`src/services/veo_workflow_executor.py`

`veo_workflow_executor.py` 是 VEO / Google Labs / Flow 工作流执行器，入口函数是 `veo_workflow`，同样由 `task_service.py` 分发调用。

### 职责边界

- 使用通用指纹浏览器上下文打开或复用 Google Labs / Flow 页面。
- 在页面上下文中执行 Flow / aisandbox / media API 请求。
- 管理项目 ID、access token、reCAPTCHA token、图片上传、视频轮询、图片放大等 VEO 相关逻辑。

### 关键能力

- **VeoSession 会话缓存**：按窗口缓存，减少重复打开浏览器和重复登录。
- **Google 登录辅助**：管理端提供打开/连接窗口能力，可在 Google 登录页中辅助选择账号、输入凭据、进入 Flow。
- **Access Token 获取与复用**：从指纹窗口 Cookie / NextAuth 会话中换取 Labs access token，并可写回 `task_type_windows` 缓存。
- **Flow 项目管理**：支持从 payload 指定 `veo_project_id/project_id/current_project_id`，也支持从 `veo_url` 解析，或从绑定窗口的 `veo_flow_projects` 随机选择项目。
- **VEO 视频生成**：
  - T2V：文生视频。
  - I2V：首帧/首尾帧图生视频。
  - R2V / Ingredients：多图参考生成视频，最多按执行器逻辑处理多张参考图。
  - 支持横版/竖版模型、会员档位模型修正、任务提交重试与状态轮询。
- **AI 图片生成**：当 `n_frames` 显式为 `1` 时进入图片模式，支持文生图、图生图、多图参考图生图；默认 NARWHAL，也支持 GEM_PIX_2 / Banana 类图片模型路径。
- **2K 放大与 OSS**：当 `resolution` / `veo_image_resolution` 指定 2K 时调用 `flow/upsampleImage`，可将大图上传到 OSS，避免 base64 结果撑爆数据库。
- **额度刷新**：支持读取 aisandbox credits，并解析 Google AI 活动页中的下一次额度更新时间。
- **Cloudflare 自愈**：与 Sora 类似，具备挑战页检测、等待、点击、重启窗口和恢复目标页能力。

### 常见 VEO payload

文生视频：

```json
{
  "prompt": "a futuristic city at sunset, cinematic camera movement",
  "veo_project_id": "project_xxx",
  "n_frames": 300,
  "aspect_ratio": "16:9",
  "veo_url": "https://labs.google/fx"
}
```

图生视频：

```json
{
  "prompt": "make the character walk forward naturally",
  "project_id": "project_xxx",
  "first_image_url": "https://example.com/start.png",
  "last_image_url": "https://example.com/end.png",
  "n_frames": 300,
  "aspect_ratio": "9:16"
}
```

多图 Ingredients / R2V：

```json
{
  "prompt": "combine these product references into a cinematic product video",
  "project_id": "project_xxx",
  "Ingredients_images": [
    "https://example.com/ref1.png",
    "https://example.com/ref2.png",
    "https://example.com/ref3.png"
  ],
  "n_frames": 300
}
```

图片 / Banana 类工作流：

```json
{
  "prompt": "turn this sketch into a polished commercial poster",
  "project_id": "project_xxx",
  "n_frames": 1,
  "image_url": "https://example.com/input.png",
  "image_model_name": "GEM_PIX_2",
  "veo_image_resolution": "2k"
}
```

## 对外 API

对外接口使用 `Authorization: Bearer <api_key>` 鉴权，默认 API Key 可在 `config/setting.toml` 或管理后台系统配置中修改。

### 获取任务类型

```bash
curl -H "Authorization: Bearer fpb123456" \
  http://127.0.0.1:8002/v1/task-types-public
```

### 创建任务

```bash
curl -X POST http://127.0.0.1:8002/v1/tasks \
  -H "Authorization: Bearer fpb123456" \
  -H "Content-Type: application/json" \
  -d '{
    "task_type_code": "veo_workflow",
    "payload": {
      "prompt": "a cinematic flying dragon",
      "project_id": "project_xxx",
      "n_frames": 300
    }
  }'
```

也可以传入 `mapping_id` 指定某个窗口绑定：

```json
{
  "task_type_code": "sora_gen_video",
  "mapping_id": 1,
  "payload": {
    "prompt": "a cute cat playing piano",
    "size_ratio": "16:9",
    "duration": 300
  }
}
```

### 查询任务

```bash
curl -H "Authorization: Bearer fpb123456" \
  http://127.0.0.1:8002/v1/tasks/<task_id>
```

## 任务类型与窗口池

- 任务类型配置决定一个任务由哪个执行器处理，例如 `sora_gen_video`、`veo_workflow`、`grok_workflow`、`dreamina_workflow`。
- 一个任务类型可以绑定多个指纹浏览器窗口，实现多账号、多代理、多窗口并发。
- `TaskService` 会根据窗口启用状态、剩余额度、冷却时间、错误次数和并发槽位选择合适窗口。
- 窗口池可以预先打开并维护目标页面，减少任务启动耗时；巡检逻辑会周期性检测 Cloudflare/登录态等异常。
- 管理端支持手动打开、保持打开、重启窗口、刷新 access token、刷新额度、清缓存、创建 VEO Flow 项目等操作。

## 目录结构简述

```text
fpbrowser2api/
├─ config/setting_example.toml          # 配置示例
├─ main.py                              # 服务入口
├─ src/api/routes.py                    # 对外任务 API
├─ src/api/admin.py                     # 管理后台 API
├─ src/services/fp_browser_client.py    # RoxyBrowser API 适配
├─ src/services/playwright_broswer_context.py # Playwright/CDP 通用上下文
├─ src/services/task_service.py         # 队列、窗口池、任务调度
├─ src/services/sora_task_executor.py   # Sora 生成/角色/额度执行器
├─ src/services/veo_workflow_executor.py# VEO/Flow 视频图片执行器
├─ src/services/grok_workflow_executor.py
└─ src/services/jimeng_task_executor.py # 即梦/Dreamina/Seedance 方向研究
```

## 研究路线

- VEO 3.1 / Google Flow：文生视频、图生视频、Ingredients 多图参考、图片生成、2K 放大。
- Sora：视频生成、首帧图、草稿发布、角色创建、额度/订阅/邀请码管理。
- Grok Imagine：指纹窗口登录态 + 自动化提交与轮询。
- Banana2 / Pro / GEM_PIX_2：图片生成、多图参考、放大与结果存储。
- 即梦国际站 / Dreamina / Seedance2.0：正在研究国际站登录态、额度、生成接口和任务轮询链路。
- 通用逆向自动化框架：将“指纹浏览器真人环境 + 页面上下文 fetch + 管理端窗口池 + 执行器插件化”复用到更多站点。

## 再次声明

本项目是技术研究框架，不保证任何站点的稳定可用性，也不承诺规避第三方风控的成功率。请勿商业化，请勿用于违规批量生成、账号滥用或绕过平台规则的生产用途。

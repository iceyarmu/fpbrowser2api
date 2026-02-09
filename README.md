# FPBrowser2API（指纹浏览器管理与调用）

## 启动

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

# 启动服务
python main.py

```

默认监听：`http://127.0.0.1:8002`

## 管理后台

- 登录页：`/login`
- 系统配置：`/admin/system`
- 项目管理：`/admin/projects`
- 任务类型管理：`/admin/task-types`
- 测试页面：`/admin/test`
- 请求日志：`/admin/logs`

首次默认账号：

- 用户名：`admin`
- 密码：`admin`

## 配置

基础配置位于 `config/setting.toml`（可参考 `config/setting_example.toml`）。
服务启动后会把关键配置写入数据库，并支持在管理后台中修改（重启后仍生效）。


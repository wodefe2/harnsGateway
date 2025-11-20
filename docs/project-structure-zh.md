## 工程代码结构概览

### 整体概览

- 本项目是一个 Go 工业数据采集网关（harnsGateway），既可以作为硬网关部署在嵌入式设备上，也可以作为软网关部署在边缘服务器上。
- 网关通过多种南向协议（Modbus、S7、OPC UA 等）采集设备数据，经内部处理后，再向北向系统（MQTT、时序数据库等）推送。
- 代码整体采用「命令行入口（`cmd`）+ 业务库（`pkg`）+ 构建脚本（`hack`）+ 接口定义（`apis`）」的组织方式。

### 顶层目录结构

- `cmd/`：各个可执行程序的入口。
  - `cmd/gateway/`：主网关服务。
    - `gateway.go`：`main` 函数，调用 `app.NewGatewayCmd()` 启动网关。
    - `app/server.go`：基于 Cobra 的命令实现，负责解析参数、加载配置、启动 HTTP 服务并优雅退出。
    - `options/`：网关配置与命令行参数定义（端口、Redis、证书、时序库地址等），`Options.Config()` 负责组装下游各个 Manager。
    - `config/`：将创建好的 `DeviceMgr`、`GatewayMgr` 等汇总成统一 `Config`。
  - `cmd/cim/`、`cmd/clickhouse/`、`cmd/consumer/`：其他相关服务（如 CIM 服务、写 ClickHouse 服务、数据消费服务），结构与 `gateway` 类似：`app + options + config`。

- `pkg/`：核心业务逻辑和基础设施。
  - `pkg/device/`：设备管理核心。
    - `manager.go`：`Manager` 负责设备生命周期管理，包括：
      - 启动时从本地存储加载设备；
      - 创建 / 更新 / 删除设备；
      - 按设备类型选择具体协议管理器；
      - 维护心跳设备列表、启动采集、写入时序库等。
    - `server.go`：REST 层，基于 Gin 的 `InstallHandler` 注册 `/api/v1/devices` 系列接口（增删改查、状态切换、动作控制、文件上传等），内部调用 `Manager`。
    - `interface.go`、`constant.go`：设备管理接口和常量（状态、错误等）。

  - `pkg/gateway/`：网关元信息与运行状态。
    - `gateway.go`：定义 `GatewayMeta` 结构（含 `Secret` 与 `runtime.ObjectMeta`），以及响应模型。
    - `manager.go`：管理网关元信息：
      - 首次启动生成网关 ID / Secret 并持久化；
      - 后续启动从存储中加载；
      - 调用 `pkg/host` 采集 CPU / 内存 / 磁盘等运行信息。
    - `server.go`：REST 路由，提供：
      - `/gatewayMeta`：获取网关元信息；
      - `/gatewayCpu`、`/gatewayMem`、`/gatewayDisk`：获取资源使用情况。

  - `pkg/generic/`：通用基础设施封装。
    - `httpserver.go`：创建 Gin `Engine`，设置运行模式和统一中间件（日志、`Recovery`）。
    - `server.go`：通用 `Server` 结构（`Router` + `Port` + 允许的 `Methods`）。
    - `store.go`：通用资源存储 `Store`，面向 `runtime.Device`：
      - 利用反射维护设备类型 → Go 类型的映射；
      - 底层通过 `pkg/storage` 的 `Storage` 接口读写 JSON 文件。
    - `constant.go`：通用常量（如采集状态、协议相关枚举映射）以及设备类型到构造函数的映射。
    - `options/`：通用 CLI 基础选项处理（如 `--config`、`--default-config`、`--help` 等），供各个服务共用。

  - `pkg/runtime/`：领域模型与通用类型。
    - `types.go`：定义：
      - 通用响应模型 `ResponseModel`；
      - 时序数据结构：`PublishData`、`TimeSeriesData`、`PointData`；
      - REST 选项：`CreateOptions`、`GetOptions`、`ListOptions` 等。
    - `meta.go`：`ObjectMeta` 及相关元数据结构（ID、Version、修改时间等）。
    - `constraint.go`、`validation.go`：通用校验逻辑。
    - `serializer.go`、`filter.go`、`deepcopy.go`：序列化、过滤、深拷贝等工具。
    - `constant/`：数据类型、采集状态、Modbus 枚举等常量（字符串 ↔ 内部表示的映射）。

  - `pkg/protocol/`：各工业协议实现层。
    - `modbus/`：
      - `manager.go`：`ModbusDeviceManager` 实现协议层的设备管理：
        - 将 API 层的 `v1.ModBusDevice` 转成内部 `runtime` 的 `ModBusDevice`；
        - 校验变量名称格式、维护变量列表和映射；
        - 负责 Update 时变量 diff 与 upsert。
      - `modbusbroker.go`：采集 Broker，与 Modbus 设备建立连接，按照变量配置生成读写报文并解析返回数据。
      - `model/`：Modbus 配置层模型（RTU、TCP、RtuOverTcp 等）。
      - `runtime/`：网关内部用的 Modbus 设备结构、客户端实现、常量等。
    - `opcua/`、`s7/`：结构与 Modbus 类似，分别实现 OPC UA 和 S7 协议的设备模型、管理器、Broker 和 runtime。

  - `pkg/apis/`：HTTP 层通用定义。
    - `constants.go`：HTTP 头常量（`ETag`、`If-Match` 等）以及通用错误（如 `ErrMismatch`）。
    - `response/`：
      - `responseError`：封装错误码与消息；
      - `MultiError`：支持聚合多个错误并序列化为 JSON；
      - 提供 `ErrResourceExists`、`ErrDeviceNotFound` 等常见错误构造函数。

  - `pkg/ts/`：时序库（InfluxDB）封装。
    - `influxdb.go`：`TsManager` 基于 InfluxDB v2 客户端：
      - 维护写入服务；
      - 批量写入 `write.Point`，异步处理写入错误日志。

  - `pkg/storage/`：存储抽象与实现。
    - `storage.go`：定义 `Storage` 接口和 `StoreGroup`（如设备、网关）。
    - `store_fs.go`：文件系统实现：
      - 按资源创建子目录；
      - 提供 `Create/Update/Delete/List` 操作，读写 JSON 文件。
    - `store_linux.go` / `store_darwin.go` / `store_windows.go`：
      - Linux / macOS：根目录固定为 `/var/lib/harnsgateway`；
      - Windows：位于当前用户主目录的 `harnsgateway` 文件夹。

  - `pkg/host/`：不同操作系统上的主机信息采集实现（CPU、内存、磁盘等），对上层 `pkg/gateway` 提供统一接口。

  - `pkg/cim/`、`pkg/ck/`、`pkg/data/`：为 `cmd/cim`、`cmd/clickhouse`、`cmd/consumer` 等服务提供对应的 Manager 与 HTTP Handler。
    - 例如 `pkg/data/manager.go`：
      - 使用 BoltDB 持久化 Tag 名称；
      - 支持从 Excel 导入 Tag 列表；
      - 使用 GORM 从外部数据库定时拉取数据并进行处理。

  - `pkg/v1/`：对外开放的 API 视图层模型。
    - `types.go`、`modbus.go`、`opcua.go`、`s7.go`：
      - 定义 REST 请求/响应中的设备结构；
      - 使用 Gin 的 `binding` 标签进行字段校验（必填、长度、字符限制等）。
    - `action.go`：设备控制动作相关数据结构。

  - `pkg/web/`：
    - `server.go`：
      - 将 Gin Router、`options.Options` 和 `config.Config` 组装成最终 HTTP 服务；
      - 在 `/api/v1` 路径下挂载 `device.InstallHandler` 和 `gateway.InstallHandler`；
      - 决定启用 HTTP 还是 HTTPS（根据是否提供证书与私钥）；
      - `Serve()` 负责启动 Web 服务并在退出时优雅关闭；
      - `Daemon()` 使用 `robfig/cron` 定时调用 `DeviceMgr.Daemon()` 等后台任务。

  - `pkg/version/`：版本信息与 `--version` 旗标对应逻辑。
  - `pkg/tsdb/`、`pkg/utils/`：内部使用的通用库（tsdb 工具、差集计算、随机数、UUID、文件工具等）。

- `apis/`：OpenAPI / YAML 接口定义，例如：
  - `create-modbus-device.yaml`：创建 Modbus 设备的接口文档；
  - `gateway.yaml`：网关相关接口文档；
  - 这些文件与 `pkg/device/server.go`、`pkg/gateway/server.go` 的实际 REST 路由对应。

- `hack/`：构建和校验脚本。
  - `make-rules/build.sh`、`verify-golang.sh` 等，用于本地或 CI 中构建和检查。

- `test/`：协议相关的简单测试/示例程序（如 Modbus、S7 的测试用例）。

- `_output/`：构建后的二进制文件输出目录（由 `Makefile` 和 `hack` 脚本生成）。

- 其他根目录文件：
  - `README.md`、`README_zh.md`：项目介绍与快速入门。
  - `Makefile`：统一构建入口。
  - `LICENSE`、`CHANGELOG/`：版权和版本变更说明。

### 主网关运行流程简述

1. 入口 `cmd/gateway/gateway.go` 调用 `app.NewGatewayCmd()`：
   - 使用 Cobra 解析命令行参数；
   - 支持通过基础选项加载配置文件（`--config`）和打印默认配置（`--default-config`）。
2. 在 `options.Options.Config()` 中：
   - 初始化 `GatewayMgr`：读取或生成网关元信息（ID、Secret 等）并持久化；
   - 创建用于设备持久化的 `generic.Store`（底层是 `pkg/storage` 的文件存储）；
   - 初始化 Redis 客户端与 InfluxDB `TsManager`；
   - 创建 `DeviceMgr` 并调用 `Init()`：
     - 从本地存储加载所有已注册设备；
     - 按设备类型创建对应协议的 Broker；
     - 为连接失败设备加入心跳检测列表。
3. `pkg/web.NewServer()`：
   - 使用 `generic.Default()` 得到 Gin Router；
   - 构建 `web.Server`，在 `/api/v1` 路由下安装设备与网关的 Handler。
4. 调用 `Server.Daemon()`：
   - 使用 `cron` 定时执行后台任务（如周期性采集或守护逻辑）。
5. 调用 `Server.Serve()`：
   - 启动 HTTP 或 HTTPS 服务；
   - 返回一个 `exit(ctx)` 函数，用于优雅关闭 HTTP 服务与 `DeviceMgr`。
6. 主函数中监听 OS 信号（`SIGINT`/`SIGTERM`）：
   - 收到退出信号后，创建带超时的 `context.Context`；
   - 依次调用 `exit(ctx)`、关闭 stop 通道，通知后台协程退出，从而实现优雅停机。

### 说明

- 上述结构主要针对当前仓库的 `gateway` 主服务；`cim`、`clickhouse`、`consumer` 等服务在入口组织方式上类似，但业务逻辑不同。
- 如需进一步细化某一模块（例如仅关注 `pkg/device + pkg/protocol` 的交互），可以在此文档基础上拆分出专门的小节。 


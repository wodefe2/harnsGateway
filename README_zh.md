**其他语言版本: [English](README.md), [中文](README_zh.md).**

## harnsGateway是什么

HarnsGateway 可以安装在嵌入式设备上作为硬网关采集工业数据, 也可以安装在边缘系统上面作为软网关采集数据.

## harnsGateway主要功能是什么

* **从南端采集数据**  
  支持的协议列表:

1. ModbusTcp ModbusRtu ModbusRtuOverTcp
2. S71500
3. OpcUA

* **获取北端输入反控设备**
  支持的协议列表:

1. ModbusTcp ModbusRtu ModbusRtuOverTcp
2. S71500

* **边缘计算**

## 如何构建

1. git clone https://github.com/harnsFactory/harnsgateway.git
2. cd harnsgateway
3. make
4. cd harnsgateway/_output/bin/

## 如何启动

1. QuickStart</br> ./gateway --mqtt-broker-urls=127.0.0.1:1883 &
2. Systemd

## 如何使用

例如 **连接Modbus设备**

1. 打开Modbus模拟软件(ModSim32) 并且更新如下参数: deviceId = 1,functionCode =
   3,并且设置第一个寄存器的值等于188.</br>[第一步](https://postimg.cc/sBFyrN2M) </br>然后通过502端口启动服务.
2. 在harnsGateway上创建一个Modbus设备([接口文档](apis/create-modbus-device.yaml)).设备的id属性会在MQTT topic中使用.</br> [第二步](https://postimg.cc/svYFZdpy)
3. 获取harnsGateway元信息( [接口文档](apis/gateway.yaml) ).网关的id属性会在MQTT topic中使用.</br> [第三步](https://postimg.cc/GHYxf9zP)
4. 订阅MQTT topic.</br> [第四步](https://postimg.cc/ppTGRwqq) </br>Topic为'data/{gatewayId}/v1/{deviceId}'.
5. 删除设备.

## 如何启动测试用例







## 如何使用

例如 **连接Modbus设备**

1. 打开Modbus模拟软件(ModSim32) 并且更新如下参数: deviceId = 1,functionCode =
   3,并且设置第一个寄存器的值等于188.</br>[第一步](https://postimg.cc/sBFyrN2M) </br>然后通过502端口启动服务.
2. 在harnsGateway上创建一个Modbus设备([接口文档](apis/create-modbus-device.yaml)).设备的id属性会在MQTT topic中使用.</br> [第二步](https://postimg.cc/svYFZdpy)
3. 获取harnsGateway元信息( [接口文档](apis/gateway.yaml) ).网关的id属性会在MQTT topic中使用.</br> [第三步](https://postimg.cc/GHYxf9zP)
4. 订阅MQTT topic.</br> [第四步](https://postimg.cc/ppTGRwqq) </br>Topic为'data/{gatewayId}/v1/{deviceId}'.
5. 删除设备.

## 如何启动测试用例
• 技术框架

- Go 1.20 项目，统一由 Go Modules 管理依赖（go.mod）。核心组件是 Cobra CLI、Gin HTTP 框架、Paho MQTT 客户端、klog 日志库以及 gopsutil 系统信息采集等（go.mod）。
- CLI 层通过 Cobra 构建命令行入口和参数体系，封装在 cmd/gateway/app/server.go:24。
- HTTP 服务基于 Gin，统一在 pkg/web/server.go:22 构造路由分组并挂载设备与网关相关 REST 接口。
- 设备存储与加载通过自定义的泛型 Store 封装，底层是本地文件系统 JSON 存储，路径定在 /var/lib/harnsgateway（pkg/generic/store.go:22，pkg/storage/store_linux.go:3）。
- 设备协议抽象在 pkg/generic/constant.go:14，将逻辑设备与 Modbus/S7/Opc UA 等采集实现进行映射，并以 runtime.Broker 接口提供统一采集行为。

运行原理

- 进程启动后创建命令行实例并解析参数/配置，支持 --config、日志级别、MQTT Broker 等选项（cmd/gateway/app/server.go:24，cmd/gateway/options/options.go:40）。
- Options.Config 初始化网关元数据管理器、文件存储和 MQTT 客户端，随后实例化设备管理器（cmd/gateway/options/options.go:65）。网关信息首次启动会自动生成并落盘（pkg/gateway/manager.go:44）。
- 设备管理器启动时加载本地已注册设备、为每台设备创建协议对应的 Broker，并维护心跳探测与状态机；采集数据后按约定主题发布到 MQTT（pkg/device/manager.go:60、pkg/protocol/modbus/modbusbroker.go:44）。
- REST API 直接操作设备管理器：创建/更新时按 deviceType 选择具体协议模型并做 JSON 校验，支持 PATCH/PUT/DELETE、状态切换与动作下发（pkg/device/server.go:23）。
- 网关侧 API 提供基础运行指标（CPU/Mem/Disk）查询，数据来自 gopsutil 定期采集（pkg/gateway/server.go:10，pkg/gateway/manager.go:75）。
- HTTP 服务启动时根据是否提供证书选择 TLS 或明文端口，并在退出时协调关闭采集协程与 MQTT 连接，实现优雅停机（pkg/web/server.go:48）。

程序入口

- 主服务入口：cmd/gateway/gateway.go:10，执行 app.NewGatewayCmd() 并运行 REST + MQTT 采集服务。
- 另有 IoT 模拟器的独立入口：cmd/iotsimulator/iotsimulator.go:10，结构与网关相同，用于模拟设备侧数据。




• 网关命令行参数

- --port/-P 指定 HTTP/TLS 监听端口，默认 32200（cmd/gateway/options/options.go:29,56）。
- --graceful-timeout 设定优雅停机等待时间（默认 15s），用于退出前等待已有连接完成（cmd/gateway/options/options.go:31,57）。
- --mqtt-broker-urls 可重复的 MQTT Broker 列表，默认 tcp://127.0.0.1:1883，支持 tcp/ssl/ws 方案（cmd/gateway/options/options.go:37,58）。
- --mqtt-username/-u 与 --mqtt-password/-p 配置 MQTT 认证信息，默认为空字符串（cmd/gateway/options/options.go:32-33,59-60）。
- --cert-file 与 --key-file 指向 TLS 证书/私钥文件；两者同时提供才会启用 HTTPS，默认留空以启用 HTTP（cmd/gateway/options/options.go:23-24,61-62）。

通用基础参数

- --config/-c 从 YAML/JSON 文件加载配置，命令行仍可覆盖其中的字段（pkg/generic/options/flag.go:33-45,115-148）。
- --default-config 打印当前默认配置模板并退出，方便写配置文件（pkg/generic/options/flag.go:64-84）。
- --help/-h 输出使用说明并退出；启用了自定义 Usage/Help 逻辑（pkg/generic/options/flag.go:87-99）。
- 日志相关：-v（数值型日志级别，默认 2）、--vmodule（按文件粒度设定级别）、--logging-format（可选 text/json 等，默认 text），其余 Kubernetes 日志旗标被隐藏（pkg/generic/options/log.go:60-84）。
- --version[=raw] 输出版本信息并退出；--version 打印人类可读内容，--version=raw 返回结构化信息（pkg/version/verflag/verflag.go:75-111）。

IoT 模拟器附加参数

- cmd/iotsimulator 共享以上通用旗标，并额外支持 --device-number/-d、--key-number/-k 控制模拟设备与点位数量，以及与网关同名的 MQTT 参数（cmd/iotsimulator/options/options.go:44-50）。


• 项目通过 pkg/storage 的文件系统存储把网关和设备数据写成 JSON 文件保存在本地磁盘。

- 目录结构来自 StoreGroup 的映射（pkg/storage/storage.go:10-29），初始化时会创建 device、gateway 子目录（pkg/storage/store_fs.go:31-78）。
- Linux/macOS 基础目录固定为 /var/lib/harnsgateway（pkg/storage/store_linux.go:4、pkg/storage/store_darwin.go:4）；Windows 则落在当前用户主目录下的 harnsgateway 文件夹
  （pkg/storage/store_windows.go:15-23）。
- 网关元信息写到 gateway/meta 文件，读取/初始化逻辑见 pkg/gateway/manager.go:44-66，文件名常量在 pkg/gateway/gateway.go:28。
- 设备对象保存在 device/devices/<设备类型>.<ID> 这种路径下，每个文件一台设备（pkg/generic/store.go:39-99），内容同样是 JSON。


# 总结
* DataBits、stopBits只有在串口的modbus协议中需要，是链路层的参数。
* collectorCycle是推送到mqtt topic的频率和参数采集频率，单位是秒


amount是什么含义

  里要申请的寄存器数量：

  - 当功能码是 03/04（保持/输入寄存器）时，amount 决定 GenerateReadMessage 里 maxDataSize 的累加（pkg/protocol/modbus/
    modbusbroker.go:100-113）；驱动实际读取的寄存器数 = amount。
  - 浮点或 32/64 位整型内部会根据数据类型自动扩展到 2 或 4 个寄存器（pkg/runtime/constant/datatype.go:46-53），所以多数情况下可以不显
    式设置 amount，保持默认 0，让驱动按数据类型计算。如果手动填值必须确保与数据类型占用一致，否则会造成长度与解析不符。

  因此通常建议：布尔或 16 位整型→amount=1（或留空），32 位/浮点→amount=2，64 位→amount=4；只在需要显式覆盖默认长度时才改动。



modbus address配置
› 你按数字举个例子，我要读400001-400002的两个寄存器为float32时，该如何配置


• I'll put together a clear, simple JSON example for the float32 range 400001-400002 starting at zero-based address 0 with amount 2,
  including parameter explanations in Chinese. I'll also mention relevant file and line references in code like pkg/protocol/modbus/
  model/modbustcp.go:69 and format the final message with bullets and inline code for clarity.

─ Worked for 6s ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

• 要把传统寄存器号 400001–400002 读成一个 float32：

  - Modbus 网关里的地址从 0 开始计数，而 4xxxx 表示法从 400001 起。所以 400001 对应内部地址 0，400002 对应地址 1（apis/create-modbus-
    device.yaml:221-224）。
  - float32 占 2 个寄存器（pkg/runtime/constant/datatype.go:46-53），驱动会自动按数据类型拉取 2 个 word。但为了直观，也可把 amount 设
    成 2。
  - 读取保持寄存器用功能码 3（pkg/protocol/modbus/runtime/type.go:144-214），因此 functionCode 填 3。

  单变量示例（一行 JSON）：

  {"name":"示例float32","id":"demo","eTag":"1","modTime":"2025-11-
  11T17:41:42.877387204+08:00","topic":"demoTopic","deviceCode":"demo","deviceType":"modbus","deviceModel":"modbusTcp","collectStatus"
  :"stopped","collectorCycle":5,"variableInterval":5,"address":{"location":"127.0.0.1","option":
  {"port":502}},"slave":1,"memoryLayout":"ABCD","positionAddress":0,"variables":
  [{"dataType":"float32","name":"A@@B@@C","address":0,"bits":0,"functionCode":3,"rate":1,"amount":2,"defaultValue":0,"accessMode":"r"}
  ]}

  关键点：address=0 对应 400001，amount=2 覆盖 400001–400002，共同组成一个 float32。如果你的从站使用不同的字节序，就把 memoryLayout 改
  成相应的顺序（pkg/protocol/modbus/runtime/type.go:180-205）。


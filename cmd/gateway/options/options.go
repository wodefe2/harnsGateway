package options

import (
	"harnsgateway/cmd/gateway/config"
	"harnsgateway/pkg/device"
	"harnsgateway/pkg/gateway"
	"harnsgateway/pkg/generic"
	baseoptions "harnsgateway/pkg/generic/options"
	"harnsgateway/pkg/storage"
	"harnsgateway/pkg/ts"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
)

type Options struct {
	Port string        `json:"port"`
	Wait time.Duration `json:"graceful-timeout"`
	// MqttBrokerUrls []string      `json:"mqtt-broker-urls"`
	// MqttUsername   string        `json:"mqtt-username"`
	// MqttPassword   string        `json:"mqtt-password"`
	CertFile      string `json:"cert-file"`
	KeyFile       string `json:"key-file"`
	RedisUrl      string `json:"redis-url"`
	RedisUsername string `json:"redis-username"`
	Password      string `json:"redis-password"`
	DB            int    `json:"redis-db"`
	Placeholder   string `json:"placeholder"`
	TsUrl         string `json:"ts-url"`
	TsToken       string `json:"ts-token"`
	baseoptions.BaseOptions
	// logs.BaseOptions
}

const (
	_defaultPort = "32200"
	_defaultWait = 15 * time.Second
	// _defaultRedisUrl      = "10.56.223.27:8596"
	_defaultRedisUrl = "127.0.0.1:6379"
	// _defaultRedisPassword = "Di@redis#1TScsG"
	_defaultRedisPassword = ""
	_defaultRedisDB       = 0
	_defaultRedisUsername = ""
	_defaultPlaceholder   = "@"
	_defaultTsUrl         = "http://localhost:8086"
	_defaultTsToken       = "Token p8_C7aRxDOMA3Q8lP_pbmtmRw30u7IhS9SjH6sDJuzuIMWNitOCyNBlM9lJWHQIrIZjrR51xb33_jQEftZmK_A=="
)

// var (
// 	_defaultMqttBrokerUrls = []string{"tcp://127.0.0.1:1883"}
// )

func NewDefaultOptions() *Options {
	return &Options{
		Port:          _defaultPort,
		Wait:          _defaultWait,
		RedisUrl:      _defaultRedisUrl,
		RedisUsername: _defaultRedisUsername,
		Password:      _defaultRedisPassword,
		DB:            _defaultRedisDB,
		Placeholder:   _defaultPlaceholder,
		TsUrl:         _defaultTsUrl,
		TsToken:       _defaultTsToken,
		BaseOptions:   baseoptions.NewDefaultBaseOptions(),
		// CertFile:       "",
		// KeyFile:        "",
		// BaseOptions: logs.NewOptions(),
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	// refer to node port assignment https://rancher.com/docs/rancher/v2.x/en/installation/requirements/ports/#commonly-used-ports
	fs.StringVarP(&o.Port, "port", "P", o.Port, "Port exposed")
	fs.DurationVar(&o.Wait, "graceful-timeout", o.Wait, "The duration for which the server gracefully wait for existing connections to finish - e.g. 15s or 1m")
	fs.StringVarP(&o.RedisUrl, "redis-url", "", o.RedisUrl, "The redis server url")
	fs.StringVar(&o.RedisUsername, "redis-username", o.RedisUsername, "The redis username")
	fs.StringVarP(&o.Password, "redis-password", "p", o.Password, "The redis password")
	fs.IntVarP(&o.DB, "redis-db", "d", o.DB, "The redis db")
	fs.StringVarP(&o.Placeholder, "placeholder", "", o.Placeholder, "The placeholder")
	fs.StringVarP(&o.TsUrl, "ts-url", "", o.TsUrl, "The ts url")
	fs.StringVarP(&o.TsToken, "ts-token", "", o.TsToken, "The ts token")
	fs.StringVarP(&o.CertFile, "cert-file", "", o.CertFile, "The Cert file")
	fs.StringVarP(&o.KeyFile, "key-file", "", o.KeyFile, "The Key file")
}

func (o *Options) Config(stopCh <-chan struct{}) (*config.Config, error) {

	o.TsToken = ensureTsTokenPrefix(o.TsToken)
	c := &config.Config{}
	gatewayMgr := gateway.NewGatewayManager(stopCh)
	gatewayMgr.Init()
	c.GatewayMgr = gatewayMgr

	gatewayMeta, _ := gatewayMgr.GetGatewayMeta()
	store, _ := generic.NewStore(storage.StoreGroupToString[storage.StoreGroupDevice], storage.Devices, generic.DeviceTypeObjectMap)
	o.logConnectionParams()
	// redis
	client := redis.NewClient(&redis.Options{
		Addr:     o.RedisUrl,
		Username: o.RedisUsername,
		Password: o.Password,
		DB:       o.DB,
	})
	// ts
	tsManager := ts.NewTsManager(o.TsUrl, o.TsToken)

	deviceMgr := device.NewManager(store, tsManager, client, gatewayMeta, o.Placeholder, stopCh)
	deviceMgr.Init()

	c.DeviceMgr = deviceMgr
	c.KeyFile = o.KeyFile
	c.CertFile = o.CertFile
	return c, nil
}

func (o *Options) logConnectionParams() {
	redisHost, redisPort := parseHostPort(o.RedisUrl)
	klog.InfoS("Redis connection parameters", "host", redisHost, "port", redisPort, "username", o.RedisUsername, "password", o.Password, "db", o.DB)

	influxHost, influxPort := parseHostPort(o.TsUrl)
	klog.InfoS("InfluxDB connection parameters", "host", influxHost, "port", influxPort, "username", "", "password", o.TsToken)
}

func parseHostPort(raw string) (string, string) {
	if raw == "" {
		return "", ""
	}

	// Try URL parsing first to support schemes like tcp:// or http://
	if u, err := url.Parse(raw); err == nil {
		if u.Hostname() != "" || u.Port() != "" {
			return u.Hostname(), u.Port()
		}
	}

	// Fallback to host:port without scheme
	if host, port, err := net.SplitHostPort(raw); err == nil {
		return host, port
	}

	return raw, ""
}

func ensureTsTokenPrefix(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}

	if strings.HasPrefix(strings.ToLower(token), "token ") {
		return token
	}

	return "Token " + token
}

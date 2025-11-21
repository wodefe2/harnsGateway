package model

import (
	"container/list"
	"context"
	"fmt"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	opc "harnsgateway/pkg/protocol/opcua/runtime"
	"k8s.io/klog/v2"
	"sync"
	"time"
)

type OpcUa struct {
}

func (o *OpcUa) NewClients(address *opc.Address, dataFrameCount int) (*opc.Clients, error) {
	tcpChannel := dataFrameCount/5 + 1
	usernamePasswordAuth := len(address.Option.Username) > 0 && len(address.Option.Password) > 0

	var endpoint string
	if address.Option.Port <= 0 {
		endpoint = address.Location
	} else {
		endpoint = fmt.Sprintf("%s:%d", address.Location, address.Option.Port)
	}
	// var endpoints []*ua.EndpointDescription
	endpointsCtx, endpointsCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer endpointsCancel()
	endpoints, err := opcua.GetEndpoints(endpointsCtx, endpoint)
	if err != nil {
		klog.V(2).InfoS("Failed to connect opc ua server")
		return nil, err
	}
	var ep *ua.EndpointDescription
	for _, endpointDescription := range endpoints {
		if endpointDescription.SecurityMode == ua.MessageSecurityModeNone {
			ep = endpointDescription
		}
	}

	opts := []opcua.Option{}
	if usernamePasswordAuth {
		opts = append(opts, opcua.AuthUsername(address.Option.Username, address.Option.Password))
		opts = append(opts, opcua.SecurityFromEndpoint(ep, ua.UserTokenTypeUserName))
	} else {
		opts = append(opts, opcua.SecurityFromEndpoint(ep, ua.UserTokenTypeAnonymous))
	}
	// opcua.Se

	// opts = append(opts, opcua.Inse)

	ms := list.New()
	for i := 0; i < tcpChannel; i++ {
		var c *opcua.Client
		var err error
		c, err = opcua.NewClient(endpoint, opts...)
		if err != nil {
			klog.V(2).InfoS("Failed to get opc ua client")
			return nil, err
		}
		connectCtx, connectCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err = c.Connect(connectCtx); err != nil {
			connectCancel()
			klog.V(2).InfoS("Failed to connect opc ua server")
			return nil, err
		}
		connectCancel()
		m := &opc.UaClient{
			Timeout: 1,
			Client:  c,
		}
		ms.PushBack(m)
	}

	clients := &opc.Clients{
		Messengers:   ms,
		Max:          tcpChannel,
		Idle:         tcpChannel,
		Mux:          &sync.Mutex{},
		NextRequest:  1,
		ConnRequests: make(map[uint64]chan opc.Messenger, 0),
		NewMessenger: func() (opc.Messenger, error) {
			var c *opcua.Client
			var err error
			c, err = opcua.NewClient(endpoint, opcua.SecurityMode(ua.MessageSecurityModeNone))
			if err != nil {
				klog.V(2).InfoS("Failed to get opc ua client")
				return nil, err
			}
			connectCtx, connectCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer connectCancel()
			if err = c.Connect(connectCtx); err != nil {
				klog.V(2).InfoS("Failed to connect opc ua server")
				return nil, err
			}

			return &opc.UaClient{
				Timeout: 1,
				Client:  c,
			}, nil
		},
	}
	return clients, nil
}

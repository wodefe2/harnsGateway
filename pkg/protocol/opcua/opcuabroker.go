package opcua

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	genericruntime "harnsgateway/pkg/generic/runtime"
	"harnsgateway/pkg/protocol/opcua/model"
	opcuaruntime "harnsgateway/pkg/protocol/opcua/runtime"
	"harnsgateway/pkg/runtime"
	"harnsgateway/pkg/runtime/constant"

	"github.com/gopcua/opcua/ua"
	"k8s.io/klog/v2"
)

var _ runtime.Broker = (*OpcUaBroker)(nil)

// 一次最多1000个
type OpuUaDataFrame struct {
	Variables        []*opcuaruntime.Variable
	RequestVariables *ua.ReadRequest
}

type OpcUaBroker struct {
	ExitCh                     chan struct{}
	Device                     *opcuaruntime.OpcUaDevice
	Clients                    *opcuaruntime.Clients
	NamespaceVariableDataFrame []*OpuUaDataFrame
	VariableCount              int
	VariableCh                 chan *runtime.ParseVariableResult
}

func NewBroker(d runtime.Device) (runtime.Broker, chan *runtime.ParseVariableResult, error) {
	device, ok := d.(*opcuaruntime.OpcUaDevice)
	if !ok {
		klog.V(2).InfoS("Unsupported device,type not OpcUa")
		return nil, nil, constant.ErrDeviceType
	}

	groupOf := genericruntime.VariablesInGroupOf[*opcuaruntime.Variable](device.Variables, 1000)
	namespaceVariableDataFrame := make([]*OpuUaDataFrame, 0, 0)
	variableCount := 0

	for _, variables := range groupOf {
		requestVariables := make([]*ua.ReadValueID, 0, 0)
		validVariables := make([]*opcuaruntime.Variable, 0, len(variables))
		for _, variable := range variables {
			readValueID, err := buildReadValueID(variable)
			if err != nil {
				klog.V(2).InfoS("Skip OPC UA variable with invalid address", "deviceId", device.ID, "variable", variable.Name, "err", err)
				continue
			}
			validVariables = append(validVariables, variable)
			requestVariables = append(requestVariables, readValueID)
		}
		if len(validVariables) == 0 {
			continue
		}
		variableCount += len(validVariables)
		namespaceVariableDataFrame = append(namespaceVariableDataFrame, &OpuUaDataFrame{
			Variables: validVariables,
			RequestVariables: &ua.ReadRequest{
				MaxAge:             2000,
				TimestampsToReturn: ua.TimestampsToReturnBoth,
				NodesToRead:        requestVariables}})
	}
	if len(namespaceVariableDataFrame) == 0 {
		klog.V(2).InfoS("Unnecessary to collect from OPC device.Because of the variables is empty", "deviceId", device.ID)
		return nil, nil, constant.ErrDeviceEmptyVariable
	}

	clients, err := model.OpcUaModelers[device.DeviceModel].NewClients(device.Address, len(namespaceVariableDataFrame))
	if err != nil {
		klog.V(2).InfoS("Failed to connect OPC device", "error", err, "deviceId", device.ID)
		return nil, nil, constant.ErrConnectDevice
	}

	// 设置 OPC UA 设备连接读超时时间为 3 秒
	for e := clients.Messengers.Front(); e != nil; e = e.Next() {
		if messenger, ok := e.Value.(opcuaruntime.Messenger); ok {
			if c, ok := messenger.(*opcuaruntime.UaClient); ok {
				c.Timeout = 3
			}
		}
	}
	originalNewMessenger := clients.NewMessenger
	clients.NewMessenger = func() (opcuaruntime.Messenger, error) {
		m, err := originalNewMessenger()
		if err != nil {
			return nil, err
		}
		if c, ok := m.(*opcuaruntime.UaClient); ok {
			c.Timeout = 3
		}
		return m, nil
	}

	mtc := &OpcUaBroker{
		Device:                     device,
		ExitCh:                     make(chan struct{}, 0),
		NamespaceVariableDataFrame: namespaceVariableDataFrame,
		VariableCh:                 make(chan *runtime.ParseVariableResult, 1),
		VariableCount:              variableCount,
		Clients:                    clients,
	}
	return mtc, mtc.VariableCh, nil
}

func (broker *OpcUaBroker) Destroy(ctx context.Context) {
	broker.ExitCh <- struct{}{}
	broker.Clients.Destroy(ctx)
	close(broker.VariableCh)
}

func buildReadValueID(variable *opcuaruntime.Variable) (*ua.ReadValueID, error) {
	switch variable.DataType {
	case constant.NUMBER, constant.STRING:
		// allowed
	default:
		return nil, fmt.Errorf("unsupported dataType %v", variable.DataType)
	}

	nodeID, err := buildNodeID(variable.Namespace, variable.Address)
	if err != nil {
		return nil, err
	}
	return &ua.ReadValueID{NodeID: nodeID}, nil
}

func parseNumericAddress(address interface{}) (uint32, error) {
	switch v := address.(type) {
	case uint32:
		return v, nil
	case uint64:
		return uint32(v), nil
	case uint:
		return uint32(v), nil
	case int:
		return uint32(v), nil
	case int32:
		return uint32(v), nil
	case int64:
		return uint32(v), nil
	case float32:
		return uint32(v), nil
	case float64:
		return uint32(v), nil
	case string:
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(n), nil
	default:
		return 0, fmt.Errorf("unexpected address type %T", address)
	}
}

// buildNodeID chooses numeric or string NodeID based on the address type/content.
// - Numeric types build a numeric NodeID.
// - String addresses that are numeric are treated as numeric NodeIDs.
// - Other strings are treated as string NodeIDs.
func buildNodeID(namespace uint16, address interface{}) (*ua.NodeID, error) {
	switch v := address.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if len(trimmed) == 0 {
			return nil, fmt.Errorf("address is empty string")
		}
		if n, err := strconv.ParseUint(trimmed, 10, 32); err == nil {
			return ua.NewNumericNodeID(namespace, uint32(n)), nil
		}
		return ua.NewStringNodeID(namespace, trimmed), nil
	default:
		addr, err := parseNumericAddress(address)
		if err != nil {
			return nil, err
		}
		return ua.NewNumericNodeID(namespace, addr), nil
	}
}

func (broker *OpcUaBroker) Collect(ctx context.Context) {
	go func() {
		for {
			cycleStart := time.Now()
			if !broker.poll(ctx) {
				return
			}
			select {
			case <-broker.ExitCh:
				return
			default:
				target := time.Duration(broker.Device.CollectorCycle) * time.Second
				cycleDuration := time.Since(cycleStart)
				if target <= 0 {
					klog.V(4).InfoS("OPC UA poll cycle completed", "deviceId", broker.Device.ID, "durationSeconds", cycleDuration.Seconds())
					continue
				}
				if cycleDuration > target {
					klog.V(3).InfoS("OPC UA poll exceeded cycle budget", "deviceId", broker.Device.ID, "durationSeconds", cycleDuration.Seconds(), "cycleSeconds", broker.Device.CollectorCycle)
					continue
				}
				klog.V(4).InfoS("OPC UA poll cycle completed", "deviceId", broker.Device.ID, "durationSeconds", cycleDuration.Seconds())
				time.Sleep(target - cycleDuration)
			}
		}
	}()
}

func (broker *OpcUaBroker) DeliverAction(ctx context.Context, obj map[string]interface{}) error {
	// TODO implement me
	panic("implement me")
}

func (broker *OpcUaBroker) poll(ctx context.Context) bool {
	select {
	case <-broker.ExitCh:
		return false
	default:
		sw := &sync.WaitGroup{}
		dfvCh := make(chan *opcuaruntime.ParseVariableResult, 0)
		frameCount := len(broker.NamespaceVariableDataFrame)
		klog.V(4).InfoS("OPC UA poll started", "deviceId", broker.Device.ID, "frames", frameCount)
		for _, dataFrames := range broker.NamespaceVariableDataFrame {
			sw.Add(1)
			go broker.message(ctx, dataFrames, dfvCh, sw)
		}
		go broker.rollVariable(ctx, dfvCh)
		sw.Wait()
		close(dfvCh)
		klog.V(4).InfoS("OPC UA poll finished", "deviceId", broker.Device.ID, "frames", frameCount)
		return true
	}

}

func (broker *OpcUaBroker) message(ctx context.Context, dataFrame *OpuUaDataFrame, pvrCh chan *opcuaruntime.ParseVariableResult, sw *sync.WaitGroup) {
	defer sw.Done()
	messenger, err := broker.Clients.GetMessenger(ctx)
	if err != nil {
		klog.V(2).InfoS("Failed to get OPC UA messenger", "deviceId", broker.Device.ID, "error", err)
		pvrCh <- &opcuaruntime.ParseVariableResult{Err: []error{err}}
	}
	defer broker.Clients.ReleaseMessenger(messenger)

	var response *ua.ReadResponse
	klog.V(4).InfoS("OPC UA frame request", "deviceId", broker.Device.ID, "variables", len(dataFrame.Variables))
	if err := broker.retry(func(messenger opcuaruntime.Messenger, dataFrame *OpuUaDataFrame) error {
		response, err = messenger.Read(ctx, dataFrame.RequestVariables)
		return err
	}, messenger, dataFrame); err != nil {
		klog.V(2).InfoS("Failed to connect opc ua server by retry", "deviceId", broker.Device.ID, "error", err)
		pvrCh <- &opcuaruntime.ParseVariableResult{Err: []error{err}}
		return
	}

	if response == nil {
		klog.V(2).InfoS("Failed to get opc ua server response", "deviceId", broker.Device.ID)
		return
	}

	variables := make([]*opcuaruntime.Variable, 0, len(dataFrame.Variables))
	for i, variable := range dataFrame.Variables {
		if response.Results[i].Status == ua.StatusOK {
			variable.SetValue(response.Results[i].Value.Value())
			variables = append(variables, &opcuaruntime.Variable{
				DataType:     variable.DataType,
				Name:         variable.Name,
				Address:      variable.Address,
				Namespace:    variable.Namespace,
				DefaultValue: variable.DefaultValue,
				Value:        variable.Value,
			})
		} else {
			klog.V(3).InfoS("OPC UA node returned bad status", "deviceId", broker.Device.ID, "namespace", variable.Namespace, "node", variable.Address, "status", response.Results[i].Status.Error())
		}
	}

	pvrCh <- &opcuaruntime.ParseVariableResult{Err: nil, VariableSlice: variables}
}

func (broker *OpcUaBroker) retry(fun func(m opcuaruntime.Messenger, dataFrame *OpuUaDataFrame) error, m opcuaruntime.Messenger, dataFrame *OpuUaDataFrame) error {
	for i := 0; i < 3; i++ {
		err := fun(m, dataFrame)
		attempt := i + 1
		if err == nil {
			if attempt > 1 {
				klog.V(4).InfoS("OPC UA frame recovered after retry", "deviceId", broker.Device.ID, "attempt", attempt)
			}
			return nil
		}
		switch {
		case err == io.EOF && m.Available():
			newMessenger, err := broker.Clients.NewMessenger()
			if err != nil {
				return err
			}
			m.Reset(newMessenger)
			klog.V(3).InfoS("OPC UA messenger recreated", "deviceId", broker.Device.ID, "attempt", attempt, "reason", "eof")
			i = i - 1
			continue
		case errors.Is(err, ua.StatusBadSessionIDInvalid):
			newMessenger, err := broker.Clients.NewMessenger()
			if err != nil {
				return err
			}
			m.Reset(newMessenger)
			klog.V(3).InfoS("OPC UA messenger recreated", "deviceId", broker.Device.ID, "attempt", attempt, "reason", "sessionInvalid")
			i = i - 1
			continue
		case errors.Is(err, ua.StatusBadSessionNotActivated):
			newMessenger, err := broker.Clients.NewMessenger()
			if err != nil {
				return err
			}
			m.Reset(newMessenger)
			klog.V(3).InfoS("OPC UA messenger recreated", "deviceId", broker.Device.ID, "attempt", attempt, "reason", "sessionNotActivated")
			i = i - 1
			continue
		case errors.Is(err, ua.StatusBadServerNotConnected):
			newMessenger, err := broker.Clients.NewMessenger()
			if err != nil {
				return err
			}
			m.Reset(newMessenger)
			klog.V(3).InfoS("OPC UA messenger recreated", "deviceId", broker.Device.ID, "attempt", attempt, "reason", "serverNotConnected")
			i = i - 1
			continue
		case errors.Is(err, ua.StatusBadSecureChannelIDInvalid):
			klog.V(3).InfoS("OPC UA secure channel invalid", "deviceId", broker.Device.ID, "attempt", attempt)
			continue
		default:
			klog.V(2).InfoS("Failed to read opc ua server data", "deviceId", broker.Device.ID, "attempt", attempt, "err", err)
		}
	}
	klog.V(2).InfoS("OPC UA frame exhausted retries", "deviceId", broker.Device.ID)
	return opcuaruntime.ErrManyRetry
}

func (broker *OpcUaBroker) rollVariable(ctx context.Context, ch chan *opcuaruntime.ParseVariableResult) {
	rvs := make([]runtime.VariableValue, 0, broker.VariableCount)
	errs := make([]error, 0)
	for {
		select {
		case pvr, ok := <-ch:
			if !ok {
				if len(errs) > 0 {
					klog.V(2).InfoS("OPC UA poll completed with errors", "deviceId", broker.Device.ID, "errorCount", len(errs), "sample", summarizeErrors(errs, 3))
				}
				broker.VariableCh <- &runtime.ParseVariableResult{Err: errs, VariableSlice: rvs}
				return
			} else if pvr.Err != nil {
				errs = append(errs, pvr.Err...)
				klog.V(3).InfoS("OPC UA frame returned errors", "deviceId", broker.Device.ID, "errorCount", len(pvr.Err), "sample", summarizeErrors(pvr.Err, 2))
			} else {
				for _, variable := range pvr.VariableSlice {
					rvs = append(rvs, variable)
				}
			}
		}
	}
}

func summarizeErrors(errs []error, limit int) string {
	if len(errs) == 0 || limit <= 0 {
		return ""
	}
	msgs := make([]string, 0, limit)
	for i, err := range errs {
		if i >= limit {
			break
		}
		msgs = append(msgs, err.Error())
	}
	if len(errs) > limit {
		return fmt.Sprintf("%s (and %d more)", strings.Join(msgs, "; "), len(errs)-limit)
	}
	return strings.Join(msgs, "; ")
}

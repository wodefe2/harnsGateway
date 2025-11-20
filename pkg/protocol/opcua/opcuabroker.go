package opcua

import (
	"context"
	"errors"
	"fmt"
	"github.com/gopcua/opcua/ua"
	genericruntime "harnsgateway/pkg/generic/runtime"
	"harnsgateway/pkg/protocol/opcua/model"
	opcuaruntime "harnsgateway/pkg/protocol/opcua/runtime"
	"harnsgateway/pkg/runtime"
	"harnsgateway/pkg/runtime/constant"
	"io"
	"k8s.io/klog/v2"
	"strconv"
	"strings"
	"sync"
	"time"
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

	for _, variables := range groupOf {
		requestVariables := make([]*ua.ReadValueID, 0, 0)
		for _, variable := range variables {
			switch variable.DataType {
			case constant.NUMBER:
				var id *ua.NodeID
				if address, isNumber := variable.Address.(float64); isNumber {
					id = ua.NewNumericNodeID(variable.Namespace, uint32(address))
				} else if address, isString := variable.Address.(string); isString {
					iAddress, err := strconv.Atoi(address)
					if err != nil {
						klog.V(2).InfoS("Failed to parse string address to uint32")
					}
					id = ua.NewNumericNodeID(variable.Namespace, uint32(iAddress))
				}
				requestVariables = append(requestVariables, &ua.ReadValueID{NodeID: id})
			case constant.STRING:
				address := variable.Address.(string)
				id := ua.NewStringNodeID(variable.Namespace, address)
				requestVariables = append(requestVariables, &ua.ReadValueID{NodeID: id})
			}
		}
		namespaceVariableDataFrame = append(namespaceVariableDataFrame, &OpuUaDataFrame{
			Variables: variables,
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
	mtc := &OpcUaBroker{
		Device:                     device,
		ExitCh:                     make(chan struct{}, 0),
		NamespaceVariableDataFrame: namespaceVariableDataFrame,
		VariableCh:                 make(chan *runtime.ParseVariableResult, 1),
		VariableCount:              len(device.Variables),
		Clients:                    clients,
	}
	return mtc, mtc.VariableCh, nil
}

func (broker *OpcUaBroker) Destroy(ctx context.Context) {
	broker.ExitCh <- struct{}{}
	broker.Clients.Destroy(ctx)
	close(broker.VariableCh)
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

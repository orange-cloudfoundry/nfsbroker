package nfsbroker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"sync"

	"path"

	"context"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
	"strings"
)

const (
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	DefaultContainerPath  = "/var/vcap/data"
)

const (
	Username string = "kerberosPrincipal"
	Secret   string = "kerberosKeytab"
)

var (
	ErrNoMountTargets         = errors.New("no mount targets found")
	ErrMountTargetUnavailable = errors.New("mount target not in available state")
)

type staticState struct {
	ServiceName string `json:"ServiceName"`
	ServiceId   string `json:"ServiceId"`
}

type ServiceInstance struct {
	ServiceID        string `json:"service_id"`
	PlanID           string `json:"plan_id"`
	OrganizationGUID string `json:"organization_guid"`
	SpaceGUID        string `json:"space_guid"`
	Share            string
}

type dynamicState struct {
	InstanceMap map[string]ServiceInstance
	BindingMap  map[string]brokerapi.BindDetails
}

type lock interface {
	Lock()
	Unlock()
}

type Broker struct {
	logger  lager.Logger
	dataDir string
	os      osshim.Os
	ioutil  ioutilshim.Ioutil
	mutex   lock
	clock   clock.Clock
	static  staticState
	dynamic dynamicState
}

func New(
	logger lager.Logger,
	serviceName, serviceId, dataDir string,
	os osshim.Os,
	ioutil ioutilshim.Ioutil,
	clock clock.Clock,
	a0 interface{}, a1 interface{}, a2 interface{},
	a3 interface{},
	a4 interface{},
	a5 interface{},
) *Broker {

	theBroker := Broker{
		logger:  logger,
		dataDir: dataDir,
		os:      os,
		ioutil:  ioutil,
		mutex:   &sync.Mutex{},
		clock:   clock,
		static: staticState{
			ServiceName: serviceName,
			ServiceId:   serviceId,
		},
		dynamic: dynamicState{
			InstanceMap: map[string]ServiceInstance{},
			BindingMap:  map[string]brokerapi.BindDetails{},
		},
	}

	theBroker.restoreDynamicState()

	return &theBroker
}

func (b *Broker) Services(_ context.Context) []brokerapi.Service {
	logger := b.logger.Session("services")
	logger.Info("start")
	defer logger.Info("end")

	return []brokerapi.Service{{
		ID:            b.static.ServiceId,
		Name:          b.static.ServiceName,
		Description:   "NFS volumes secured with Kerberos (see: https://example.com/knfs-volume-release/)",
		Bindable:      true,
		PlanUpdatable: false,
		Tags:          []string{"knfs"},
		Requires:      []brokerapi.RequiredPermission{PermissionVolumeMount},

		Plans: []brokerapi.ServicePlan{
			{
				Name:        "Existing",
				ID:          "Existing",
				Description: "a filesystem you have already provisioned by contacting <URL>",
			},
		},
	}}
}

func (b *Broker) Provision(context context.Context, instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, error) {
	logger := b.logger.Session("provision").WithData(lager.Data{"instanceID": instanceID})
	logger.Info("start")
	defer logger.Info("end")

	if b.instanceConflicts(details, instanceID) {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}

	type Configuration struct {
		Share string `json:"share"`
	}
	var configuration Configuration

	var decoder *json.Decoder = json.NewDecoder(bytes.NewBuffer(details.RawParameters))
	err := decoder.Decode(&configuration)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrRawParamsInvalid
	}

	if configuration.Share == "" {
		return brokerapi.ProvisionedServiceSpec{}, errors.New("config requires a \"share\" key")
	}

	b.dynamic.InstanceMap[instanceID] = ServiceInstance{
		details.ServiceID,
		details.PlanID,
		details.OrganizationGUID,
		details.SpaceGUID,
		configuration.Share}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.persist(b.dynamic)

	return brokerapi.ProvisionedServiceSpec{IsAsync: false}, nil
}

func (b *Broker) Deprovision(context context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.DeprovisionServiceSpec, error) {
	logger := b.logger.Session("deprovision")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	_, instanceExists := b.dynamic.InstanceMap[instanceID]
	if !instanceExists {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
	}

	return brokerapi.DeprovisionServiceSpec{IsAsync: false, OperationData: "deprovision"}, nil
}

func (b *Broker) Bind(context context.Context, instanceID string, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
	logger := b.logger.Session("bind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	logger.Info("Starting nfsbroker bind")
	instanceDetails, ok := b.dynamic.InstanceMap[instanceID]
	if !ok {
		return brokerapi.Binding{}, brokerapi.ErrInstanceDoesNotExist
	}

	if details.AppGUID == "" {
		return brokerapi.Binding{}, brokerapi.ErrAppGuidNotProvided
	}

	mode, err := evaluateMode(details.Parameters)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	if b.bindingConflicts(bindingID, details) {
		return brokerapi.Binding{}, brokerapi.ErrBindingAlreadyExists
	}

	b.dynamic.BindingMap[bindingID] = details

	var exist bool

	var nfsprmres interface{}
	var uid interface{}
	var gid interface{}

	if uid, exist = details.Parameters["uid"]; !exist {
		return brokerapi.Binding{}, errors.New("config requires a \"uid\"")
	}

	if gid, exist = details.Parameters["gid"]; !exist {
		return brokerapi.Binding{}, errors.New("config requires a \"gid\"")
	}

	mountConfig := map[string]interface{}{"source": fmt.Sprintf("nfs://%s?uid=%s&gid=%s", instanceDetails.Share, uid.(string), gid.(string))}
	logger.Info("NFS Source " + EscapedToString(mountConfig["source"].(string)))

	//

	lstopt := map[string]int{
		// Fuse_NFS Options
		"fusenfs_allow_other_own_ids":1,
		"fusenfs_uid":2,
		"fusenfs_gid":2,

		// libNFS options

		// Fuse Option (see man mount.fuse)
		"default_permissions":1,
		"multithread":1,
		"allow_other":1,
		"allow_root":1,
		"umask":2,
		"direct_io":1,
		"kernel_cache":1,
		"auto_cache":1,
		"entry_timeout":2,
		"negative_timeout":2,
		"attr_timeout":2,
		"ac_attr_timeout":2,
		"large_read":1,
		"hard_remove":1,
		"fsname":2,
		"subtype":2,
		"blkdev":1,
		"intr":1,
		"mount_max":2,
		"max_read":2,
		"max_readahead":2,
		"async_read":1,
		"sync_read":1,
		"nonempty":1,
		"intr_signal":2,
		"use_ino":1,
		"readdir_ino":1,
		"debug":1,
	}

	for k, v := range lstopt {

		if nfsprmres, exist = details.Parameters[k]; !exist {
			continue
		}

		if v == 1 {
			// Mode flag

			valb, err := nfsprmres.(bool)

			if err == nil && valb == true {
				mountConfig = append(mountConfig, k, true)
				continue
			}

			vali, err := nfsprmres.(int)

			if err == nil && vali == 1 {
				mountConfig = append(mountConfig, k, true)
				continue
			}
		}

		if v == 2 {
			// Mode key = value

			mountConfig = append(mountConfig, k, nfsprmres.(string))
		}
	}

	logger.Info("Nfs Share + Options URL : " + EscapedToString(mountConfig[v].(string)))

	return brokerapi.Binding{
		Credentials: struct{}{}, // if nil, cloud controller chokes on response
		VolumeMounts: []brokerapi.VolumeMount{{
			ContainerDir: evaluateContainerPath(details.Parameters, instanceID),
			Mode:         mode,
			Driver:       "nfsv3driver",
			DeviceType:   "shared",
			Device: brokerapi.SharedDevice{
				VolumeId:    instanceID,
				MountConfig: mountConfig,
			},
		}},
	}, nil
}

func (b *Broker) Unbind(context context.Context, instanceID string, bindingID string, details brokerapi.UnbindDetails) error {
	logger := b.logger.Session("unbind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	if _, ok := b.dynamic.InstanceMap[instanceID]; !ok {
		return brokerapi.ErrInstanceDoesNotExist
	}

	if _, ok := b.dynamic.BindingMap[bindingID]; !ok {
		return brokerapi.ErrBindingDoesNotExist
	}

	delete(b.dynamic.BindingMap, bindingID)

	return nil
}

func (b *Broker) Update(context context.Context, instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	panic("not implemented")
}

func (b *Broker) LastOperation(_ context.Context, instanceID string, operationData string) (brokerapi.LastOperation, error) {
	logger := b.logger.Session("last-operation").WithData(lager.Data{"instanceID": instanceID})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	switch operationData {
	default:
		return brokerapi.LastOperation{}, errors.New("unrecognized operationData")
	}
}

func (b *Broker) instanceConflicts(details brokerapi.ProvisionDetails, instanceID string) bool {
	if existing, ok := b.dynamic.InstanceMap[instanceID]; ok {
		if !reflect.DeepEqual(details, existing) {
			return true
		}
	}
	return false
}

func (b *Broker) bindingConflicts(bindingID string, details brokerapi.BindDetails) bool {
	if existing, ok := b.dynamic.BindingMap[bindingID]; ok {
		if !reflect.DeepEqual(details, existing) {
			return true
		}
	}
	return false
}

func (b *Broker) persist(state interface{}) {
	logger := b.logger.Session("serialize-state")
	logger.Info("start")
	defer logger.Info("end")

	stateFile := filepath.Join(b.dataDir, fmt.Sprintf("%s-services.json", b.static.ServiceName))

	stateData, err := json.Marshal(state)
	if err != nil {
		b.logger.Error("failed-to-marshall-state", err)
		return
	}

	err = b.ioutil.WriteFile(stateFile, stateData, os.ModePerm)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-write-state-file: %s", stateFile), err)
		return
	}

	logger.Info("state-saved", lager.Data{"state-file": stateFile})
}

func evaluateContainerPath(parameters map[string]interface{}, volId string) string {
	if containerPath, ok := parameters["mount"]; ok && containerPath != "" {
		return containerPath.(string)
	}

	return path.Join(DefaultContainerPath, volId)
}

func evaluateMode(parameters map[string]interface{}) (string, error) {
	if ro, ok := parameters["readonly"]; ok {
		switch ro := ro.(type) {
		case bool:
			return readOnlyToMode(ro), nil
		default:
			return "", brokerapi.ErrRawParamsInvalid
		}
	}
	return "rw", nil
}

func readOnlyToMode(ro bool) string {
	if ro {
		return "r"
	}
	return "rw"
}

func (b *Broker) restoreDynamicState() {
	logger := b.logger.Session("restore-services")
	logger.Info("start")
	defer logger.Info("end")

	stateFile := filepath.Join(b.dataDir, fmt.Sprintf("%s-services.json", b.static.ServiceName))

	serviceData, err := b.ioutil.ReadFile(stateFile)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-read-state-file: %s", stateFile), err)
		return
	}

	dynamicState := dynamicState{}
	err = json.Unmarshal(serviceData, &dynamicState)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-unmarshall-state from state-file: %s", stateFile), err)
		return
	}
	logger.Info("state-restored", lager.Data{"state-file": stateFile})
	b.dynamic = dynamicState
}

func EscapedToString(source string) string {
	if strings.Contains(source, `\\u0026`) {
		return "Double Escaped"
	} else if strings.Contains(source, `\u0026`) {
		return "Single Escaped"
	} else if strings.Contains(source, `&`) {
		return "UnEscaped"
	} else {
		return "Not Found"
	}
}
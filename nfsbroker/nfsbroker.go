package nfsbroker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sync"

	"crypto/md5"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
	"strings"
	"strconv"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"os"
)

const (
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	DefaultContainerPath  = "/var/vcap/data"
)

const (
	Username string = "kerberosPrincipal"
	Secret   string = "kerberosKeytab"
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

type DynamicState struct {
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
	mutex   lock
	clock   clock.Clock
	static  staticState
	dynamic DynamicState
	store   Store
        configPath string
}

type Config struct {
        sourceOptions map[string]string
        mountOptions map[string]string

        sloppyMount bool
}

func New(
	logger lager.Logger,
	serviceName, serviceId, dataDir string,
	os osshim.Os,
	clock clock.Clock,
	store Store,
        configPath string,
) *Broker {

	theBroker := Broker{
		logger:  logger,
		dataDir: dataDir,
		os:      os,
		mutex:   &sync.Mutex{},
		clock:   clock,
		store:   store,
		static: staticState{
			ServiceName: serviceName,
			ServiceId:   serviceId,
		},
		dynamic: DynamicState{
			InstanceMap: map[string]ServiceInstance{},
			BindingMap:  map[string]brokerapi.BindDetails{},
		},
		configPath: configPath,
	}

	theBroker.store.Restore(logger, &theBroker.dynamic)

	return &theBroker
}

func (b *Broker) Services(_ context.Context) []brokerapi.Service {
	logger := b.logger.Session("services")
	logger.Info("start")
	defer logger.Info("end")

	return []brokerapi.Service{{
		ID:            b.static.ServiceId,
		Name:          b.static.ServiceName,
		Description:   "Existing NFSv3 volumes (see: https://code.cloudfoundry.org/nfs-volume-release/)",
		Bindable:      true,
		PlanUpdatable: false,
		Tags:          []string{"nfs"},
		Requires:      []brokerapi.RequiredPermission{PermissionVolumeMount},

		Plans: []brokerapi.ServicePlan{
			{
				Name:        "Existing",
				ID:          "Existing",
				Description: "A preexisting filesystem",
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

	defer b.store.Save(logger, &b.dynamic, instanceID, "")

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
	} else {
		delete(b.dynamic.InstanceMap, instanceID)
		b.store.Save(logger, &b.dynamic, instanceID, "")
	}

	return brokerapi.DeprovisionServiceSpec{IsAsync: false, OperationData: "deprovision"}, nil
}

func (b *Broker) Bind(context context.Context, instanceID string, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
	logger := b.logger.Session("bind")
	logger.Info("start", lager.Data{"bindingID": bindingID, "details": details})
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.store.Save(logger, &b.dynamic, "", bindingID)

	logger.Info("Starting nfsbroker bind")
	instanceDetails, ok := b.dynamic.InstanceMap[instanceID]
	if !ok {
		return brokerapi.Binding{}, brokerapi.ErrInstanceDoesNotExist
	}

	if details.AppGUID == "" {
		return brokerapi.Binding{}, brokerapi.ErrAppGuidNotProvided
	}

	var params interface{}

	if err := json.Unmarshal(details.RawParameters, &params); err != nil {
		return brokerapi.Binding{}, err
	}

	parameters := params.(map[string]interface{})

	mode, err := evaluateMode(parameters)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	if b.bindingConflicts(bindingID, details) {
		return brokerapi.Binding{}, brokerapi.ErrBindingAlreadyExists
	}

	b.dynamic.BindingMap[bindingID] = details

	myCnf := new(Config)

	if err := myCnf.getConf(b.configPath, logger); err != nil {
		return brokerapi.Binding{}, err;
	}

	if err := myCnf.filterArgs(parameters, logger); err != nil {
		return brokerapi.Binding{}, err;
	}

	source := fmt.Sprintf("nfs://%s", instanceDetails.Share)
	mountConfig := make(map[string]interface{})

	if mountConfig, err = myCnf.getMountConfig(source, logger); err != nil {
		return brokerapi.Binding{}, err;
	}
	
	logger.Info("Volume Service Binding", lager.Data{"Driver": "nfsv3driver", "MountConfig": mountConfig})
	
	s, err := b.hash(mountConfig)
	if err != nil {
		logger.Error("error-calculating-volume-id", err, lager.Data{"config": mountConfig, "bindingID": bindingID, "instanceID": instanceID})
		return brokerapi.Binding{}, err
	}
	volumeId := fmt.Sprintf("%s-%s", instanceID, s)

	return brokerapi.Binding{
		Credentials: struct{}{}, // if nil, cloud controller chokes on response
		VolumeMounts: []brokerapi.VolumeMount{{
			ContainerDir: evaluateContainerPath(parameters, instanceID),
			Mode:         mode,
			Driver:       "nfsv3driver",
			DeviceType:   "shared",
			Device: brokerapi.SharedDevice{
				VolumeId:    volumeId,
				MountConfig: mountConfig,
			},
		}},
	}, nil
}

func (b *Broker) hash(mountConfig map[string]interface{}) (string, error) {
	var (
		bytes []byte
		err   error
	)
	if bytes, err = json.Marshal(mountConfig); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", md5.Sum(bytes)), nil
}

func (b *Broker) Unbind(context context.Context, instanceID string, bindingID string, details brokerapi.UnbindDetails) error {
	logger := b.logger.Session("unbind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.store.Save(logger, &b.dynamic, "", bindingID)

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

func ignoreBindOpt(k string) bool {

	switch k {
	case "mount":
		return true
	case "readonly":
		return true
	case Username:
		return true
	case Secret:
		return true
	}

	return false
}


func (m *Config) getConf(configPath string, logger lager.Logger) error {

	type ConfigYaml  struct {
		SrcString string `yaml:"source_params"`
		MntString string `yaml:"mount_params"`
	}

	file, err := os.Open(configPath)
	if err != nil {
		logger.Fatal("bind-config", err, lager.Data{"file": configPath})
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		logger.Fatal("bind-config", err, lager.Data{"file": configPath})
	}

	var configYaml ConfigYaml

	err = yaml.Unmarshal(data, &configYaml)
	if err != nil {
		logger.Fatal("bind-config", err, lager.Data{"file": configPath})
	}

	m.mountOptions = m.parseConfig(strings.Split(configYaml.MntString, ","))
	m.sourceOptions = m.parseConfig(strings.Split(configYaml.SrcString, ","))
	m.sloppyMount = m.initSloppyMount(logger)

	logger.Debug("bind-config-loaded", lager.Data{"sloppyMount": m.sloppyMount, "sourceOptions": m.sourceOptions, "mountOptions": m.mountOptions})

	return nil
}

func (m *Config) parseConfig(listEntry []string) map[string]string {

	result := map[string]string{}

	for _,opt := range listEntry {

		key := strings.SplitN(opt, ":", 2)

		if len(key[0]) < 1 {
			continue
		}

		if len(key[1]) < 1 {
			result[key[0]] = ""
		} else {
			result[key[0]] = key[1]
		}
	}

	return result
}

func (m *Config) initSloppyMount(logger lager.Logger) bool {

	if _, ok := m.mountOptions["sloppy_mount"]; ok {

		if val,err := strconv.ParseBool(m.mountOptions["sloppy_mount"]); err == nil {
			return val
		}
	}

	return false
}

func (m *Config) filterArgs (entryList map[string]interface{}, logger lager.Logger) error {

	var errorList []string

	cleanEntry := m.uniformEntry(entryList, logger)

	for k, v := range cleanEntry {

		if v == "" || ignoreBindOpt(k) {
			continue
		}

		_,okm := m.mountOptions[k];
		_,oks := m.sourceOptions[k];

		if !okm && !oks {
			errorList = append(errorList, k);
			continue
		}

		if val, err := strconv.ParseBool(v); err == nil {
			if val == true && k == "sloppy_mount" {
				m.sloppyMount = true
				continue
			}
		}

		if okm {
			m.mountOptions[k] = v
		} else {
			m.sourceOptions[k] = v
		}
	}

	logger.Debug("bind-opts-parsed", lager.Data{"configMount": m.mountOptions, "configSource": m.sourceOptions, "sloppyMount": m.sloppyMount, "error": errorList})

	if len(errorList) > 0 && !m.sloppyMount {
		logger.Error("bind-opts", errors.New("Incompatibles bind options !"), lager.Data{"errors": errorList})
		return errors.New("Incompatibles bind options :" + strings.Join(errorList, ", "))
	}

	if len(errorList) > 0 {
		logger.Info("bind-opts", lager.Data{"incompatibles-opts": errorList})
	}

	return nil
}

func (m *Config) filterSource (entryList []string, logger lager.Logger) error {

	var errorList []string

	for _, p := range entryList {

		opt := strings.SplitN(p, ":", 2)

		if p == "" || ignoreBindOpt(opt[0]) {
			continue
		}

		_,okm := m.mountOptions[opt[0]];
		_,oks := m.sourceOptions[opt[0]];

		if !okm && !oks {
			errorList = append(errorList, opt[0]);
			continue
		}

		if len(opt) != 2 {
			opt = append(opt, "")
		}

		if val, err := strconv.ParseBool(opt[1]); err == nil {
			if val == true && opt[0] == "sloppy_mount" {
				m.sloppyMount = true
				continue
			}
		}

		if okm {
			m.mountOptions[opt[0]] = opt[1]
		} else {
			m.sourceOptions[opt[0]] = opt[1]
		}
	}

	logger.Debug("bind-opts-parsed", lager.Data{"configMount": m.mountOptions, "configSource": m.sourceOptions, "sloppyMount": m.sloppyMount, "error": errorList})

	if len(errorList) > 0 && !m.sloppyMount {
		logger.Error("bind-opts", errors.New("Incompatibles bind options !"), lager.Data{"errors": errorList})
		return errors.New("Incompatibles bind options :" + strings.Join(errorList, ", "))
	}

	if len(errorList) > 0 {
		logger.Info("bind-opts", lager.Data{"incompatibles-opts": errorList})
	}

	return nil
}

func (m *Config) makeShare(url *string, logger lager.Logger) error {

	srcPart := strings.SplitN(*url, "?", 2)

	if len(srcPart) == 1 {
		srcPart = append(srcPart, "")
	}

	if err := m.filterSource(strings.Split(srcPart[1], "&"), logger); err != nil {
		return err;
	}

	if uid, ok := m.sourceOptions["uid"]; !ok || len(uid) < 1 || uid == "0" {
		return errors.New("config requires a \"uid\"")
	}

	if gid, ok := m.sourceOptions["gid"]; !ok || len(gid) < 1 || gid == "0" {
		return errors.New("config requires a \"gid\"")
	}

	paramsList := []string{}

	for k,v := range m.sourceOptions  {
		if v == "" {
			continue
		}

		if val, err := strconv.ParseBool(v); err == nil {
			if val == true {
				paramsList = append(paramsList, fmt.Sprintf("%s=1", k))
			} else {
				paramsList = append(paramsList, fmt.Sprintf("%s=0", k))
			}
			continue
		}

		if val, err := strconv.ParseInt(v, 10, 16); err == nil {
			paramsList = append(paramsList, fmt.Sprintf("%s=%d", k, val))
			continue
		}

		paramsList = append(paramsList, fmt.Sprintf("%s=%s", k, v))
	}

	srcPart[1] = strings.Join(paramsList, "&")

	if len(srcPart[1]) < 1 {
		*url = srcPart[0]
	} else {
		*url = strings.Join(srcPart, "?")
	}

	return nil
}

func (m *Config) getMountConfig(source string, logger lager.Logger) (map[string]interface{}, error) {

	if err := m.makeShare(&source, logger); err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"source": source,
	}

	for k,v := range m.mountOptions  {

		if val, err := strconv.ParseBool(v); err == nil {
			result[k] = val
			continue
		}

		if val, err := strconv.ParseInt(v, 10, 16); err == nil {
			result[k] = val
			continue
		}

		result[k] = v
	}

	return result, nil
}

func (m *Config) uniformEntry (entryList map[string]interface{}, logger lager.Logger) map[string]string {

	result := map[string]string{}

	for k, v := range entryList {

		var value interface{}

		switch v.(type) {
		case int:
			value = strconv.FormatInt(int64(v.(int)), 10)
		case string:
			value = v.(string)
		case bool:
			value = strconv.FormatBool(v.(bool))
		default:
			value = ""
		}

		result[k] = value.(string)
	}

	return result
}


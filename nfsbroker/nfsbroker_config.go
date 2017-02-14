package nfsbroker

import (
	"strconv"
	"fmt"
	"strings"
	"errors"
)

type ConfigDetails struct {
	allowed  []string
	forced   map[string]string
	options  map[string]string

	mandatory   []string
}

type Config struct {
	source      ConfigDetails
	mount       ConfigDetails

	sloppyMount bool
}

func inArray(list []string, key string) bool {
	for _,k := range list {
		if k == key {
			return true
		}
	}

	return false
}

func NewNfsBrokerConfig(sourceFlag []string, mountFlag []string) **Config {
	myConf := new(Config)
	myConf.readConfAllowed(sourceFlag[0], mountFlag[0])
	myConf.readConfDefault(sourceFlag[1], mountFlag[1])
	myConf.source.mandatory = []string{
		"uid","gid",
	}

	return &myConf
}

func (m *Config) readConfAllowed(sourceFlag string, mountFlag string) error {
	if err := m.source.readConfAllowed(sourceFlag); err != nil {
		return  err
	}

	if err := m.mount.readConfAllowed(mountFlag); err != nil {
		return  err
	}

	return nil
}

func (m *Config) readConfDefault(sourceFlag string, mountFlag string) error {
	if err := m.source.readConfDefault(sourceFlag); err != nil {
		return  err
	}

	if err := m.mount.readConfDefault(mountFlag); err != nil {
		return  err
	}

	m.sloppyMount = m.mount.isSloppyMount()

	return nil
}

func (m * Config) setEntries(share string, opts map[string]interface{}, ignoreList []string) error {

	m.source.parseMap(opts, ignoreList)
	m.mount.parseMap(opts, ignoreList)

	allowed 	:= append(ignoreList, m.source.allowed...)
	allowed 	 = append(allowed, m.mount.allowed...)
	errorList 	:= m.source.parseUrl(share, ignoreList)
	m.sloppyMount 	 = m.mount.isSloppyMount()

	for k,_ := range opts {
		if !inArray(allowed, k) {
			errorList = append(errorList, k)
		}
	}

	if len(errorList) > 0 && m.sloppyMount != true {
		err := errors.New("Not allowed options : " + strings.Join(errorList, ", "))
		return err
	}

	if mdtErr := append(m.source.getMissMandatory(), m.mount.getMissMandatory()...); len(mdtErr) > 0 {
		err := errors.New("Missing mandatory options : " + strings.Join(mdtErr, ", "))
		return err
	}

	return nil
}

func (m *Config) getShare(share string) string {

	srcPart := strings.SplitN(share, "?", 2)

	if len(srcPart) < 2 {
		srcPart = append(srcPart, "")
	}

	srcPart[1] = strings.Join(m.source.makeParams(""), "&")

	if len(srcPart[1]) < 1 {
		srcPart = srcPart[:len(srcPart)-1]
	}

	return strings.Join(srcPart, "?")
}

func (m *Config) getMount() []string {

	return m.mount.makeParams("--")
}

func (m *Config) getMountConfig() map[string]interface{} {

	return m.mount.makeConfig()
}

func (m *ConfigDetails) readConfAllowed(flagString string) error {
	m.allowed = strings.Split(flagString, ",")
	return nil
}

func (m *ConfigDetails) readConfDefault(flagString string) error {
	m.options = m.parseConfig(strings.Split(flagString, ","))
	m.forced  = make(map[string]string)

	for k,v := range m.options {
		if !inArray(m.allowed, k) {
			m.forced[k] = v
			delete(m.options, k)
		}
	}

	return nil
}

func (m *ConfigDetails) readConf(flagString []string) error {
	if err := m.readConfAllowed(flagString[0]); err != nil {
		return err
	}

	if err := m.readConfDefault(flagString[1]); err != nil {
		return err
	}

	return nil
}

func (m *ConfigDetails) getMissMandatory() []string {
	result := []string{}

	for _,k := range m.mandatory {

		_,oko := m.options[k];
		_,okf := m.forced[k];

		if !okf && !oko {
			result = append(result, k)
		}
	}

	return result
}

func (m *ConfigDetails) parseConfig(listEntry []string) map[string]string {

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

func (m *ConfigDetails) isSloppyMount() bool {

	spm  := "";
	ok := false;

	if _, ok = m.options["sloppy_mount"]; ok {
		spm = m.options["sloppy_mount"];
		delete(m.options, "sloppy_mount")
	}

	if _, ok = m.forced["sloppy_mount"]; ok {
		spm = m.forced["sloppy_mount"];
		delete(m.forced, "sloppy_mount")
	}

	if len(spm) > 0 {
		if val,err := strconv.ParseBool(spm); err == nil {
			return val
		}
	}

	return false
}

func (m *ConfigDetails) uniformKeyData (key string, data interface{}) string {
	switch key {
	case "auto-traverse-mounts":
		return m.uniformData(data, true)

	case "dircache":
		return m.uniformData(data, true)

	}

	return m.uniformData(data, false)
}

func (m *ConfigDetails) uniformData (data interface{}, boolAsInt bool) string {

	switch data.(type) {
	case int:
		return strconv.FormatInt(int64(data.(int)), 10)

	case string:
		return data.(string)

	case bool:
		if boolAsInt {
			if data.(bool) {
				return "1"
			} else {
				return "0"
			}
		} else {
			return strconv.FormatBool(data.(bool))
		}
	}

	return ""
}

func (m *ConfigDetails) parseUrl (url string, ignoreList []string) []string {

	var errorList []string

	part := strings.SplitN(url, "?", 2)

	if len(part) < 2 {
		part = append(part, "")
	}

	for _, p := range strings.Split(part[1], "&") {
		if key := m.parseUrlParams(p, ignoreList); len(key) > 0 {
			errorList = append(errorList, key)
		}

	}

	return errorList
}

func (m *ConfigDetails) parseUrlParams (urlParams string, ignoreList []string) string {

	op := strings.SplitN(urlParams, "=", 2)

	if len (op) < 2 || len(op[1]) < 1 || op[1] == "" || inArray(ignoreList, op[0]) {
		return ""
	}

	if inArray(m.allowed, op[0]) {
		m.options[op[0]] = m.uniformKeyData(op[0], op[1])
		return ""
	}

	return op[0]
}

func (m *ConfigDetails) parseMap (entryList map[string]interface{}, ignoreList []string) []string {

	var errorList []string

	for k, v := range entryList {

		value := m.uniformData(v, false)

		if value == ""  || inArray(ignoreList, k) {
			continue
		}

		if inArray(m.allowed, k) {
			m.options[k] = value
		} else {
			errorList = append(errorList, k)
		}
	}

	return errorList
}

func (m *ConfigDetails) makeParams (prefix string) []string {
	params := []string{}

	for k,v := range m.makeConfig() {

		if k == "sloppy_mount" {
			continue
		}

		if val, err := strconv.ParseBool(v.(string)); err == nil {
			if val {
				params = append(params, fmt.Sprintf("%s%s", prefix, k))
			}
			continue
		}

		if val, err := strconv.ParseInt(v.(string), 10, 16); err == nil {
			params = append(params, fmt.Sprintf("%s%s=%d", prefix, k, val))
			continue
		}

		params = append(params, fmt.Sprintf("%s%s=%s", prefix, k, v.(string)))
	}

	return params
}

func (m *ConfigDetails) makeConfig () map[string]interface{} {

	params := map[string]interface{}{}

	for k,v := range m.options {
		params[k] = v
	}

	for k,v := range m.forced {
		params[k] = v
	}

	return params
}


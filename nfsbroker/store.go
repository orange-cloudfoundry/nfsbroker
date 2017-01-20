package nfsbroker

import (
	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/goshims/sqlshim"
	"code.cloudfoundry.org/lager"
)

//go:generate counterfeiter -o ../nfsbrokerfakes/fake_store.go . Store
type Store interface {
	Restore(logger lager.Logger, state *DynamicState) error
	Save(logger lager.Logger, state *DynamicState, instanceId, bindingId string) error
	Cleanup() error
}

func NewStore(logger lager.Logger, dbDriver, dbUsername, dbPassword, dbHostname, dbPort, dbName, fileName string) Store {
	if dbDriver != "" {
		store, err := NewSqlStore(logger, &sqlshim.SqlShim{}, dbDriver, dbUsername, dbPassword, dbHostname, dbPort, dbName)
		if err != nil {
			logger.Fatal("failed-creating-sql-store", err)
		}
		return store
	} else {
		return NewFileStore(fileName, &ioutilshim.IoutilShim{})
	}
}
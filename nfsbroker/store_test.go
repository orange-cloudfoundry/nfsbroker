package nfsbroker_test

import (
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/nfsbroker/nfsbroker"
	"github.com/pivotal-cf/brokerapi"

	"code.cloudfoundry.org/goshims/sqlshim/sql_fake"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SqlStore", func() {
	var (
		store     nfsbroker.Store
		logger    lager.Logger
		state     nfsbroker.DynamicState
		fakeSql   *sql_fake.FakeSql
		fakeSqlDb *sql_fake.FakeSqlDB
		err       error
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		fakeSql = &sql_fake.FakeSql{}
		fakeSqlDb = &sql_fake.FakeSqlDB{}
		fakeSql.OpenReturns(fakeSqlDb, nil)
		store, err = nfsbroker.NewSqlStore(logger, fakeSql, "postgres", "foo", "foo", "foo", "foo", "foo")
		Expect(err).ToNot(HaveOccurred())
		state = nfsbroker.DynamicState{
			InstanceMap: map[string]nfsbroker.ServiceInstance{
				"service-name": {
					Share: "server:/some-share",
				},
			},
			BindingMap: map[string]brokerapi.BindDetails{},
		}

	})

	It("should open a db connection", func() {
		Expect(fakeSql.OpenCallCount()).To(Equal(1))
	})

	It("should ping the connection to make sure it works", func() {
		Expect(fakeSqlDb.PingCallCount()).To(Equal(1))
	})

	It("should create tables if they don't exist", func() {
		Expect(fakeSqlDb.ExecCallCount()).To(Equal(2))
		Expect(fakeSqlDb.ExecArgsForCall(0)).To(ContainSubstring("CREATE TABLE IF NOT EXISTS service_instances"))
		Expect(fakeSqlDb.ExecArgsForCall(1)).To(ContainSubstring("CREATE TABLE IF NOT EXISTS service_bindings"))
	})

	Describe("Restore", func() {
		BeforeEach(func() {
			store.Restore(logger, &state)
		})

		Context("when it succeeds", func() {
			It("", func() {
				Expect(fakeSqlDb.QueryCallCount()).To(Equal(2))
			})
		})
	})

	Describe("Save", func() {
		Context("when the row is added", func() {
			BeforeEach(func() {
				store.Save(logger, &state, "service-name", "")
			})
			It("", func() {
				Expect(fakeSqlDb.ExecCallCount()).To(Equal(3))
				query, _ := fakeSqlDb.ExecArgsForCall(2)
				Expect(query).To(ContainSubstring("INSERT INTO service_instances (id, value) VALUES"))
			})
		})
		Context("when the row is removed", func() {
			BeforeEach(func() {
				store.Save(logger, &state, "non-existent-service-name", "")
			})
			It("", func() {
				Expect(fakeSqlDb.ExecCallCount()).To(Equal(3))
				query, _ := fakeSqlDb.ExecArgsForCall(2)
				Expect(query).To(ContainSubstring("DELETE FROM service_instances WHERE id="))
			})
		})
	})

	Describe("Cleanup", func() {
		var (
			err error
		)

		Context("when it succeeds", func() {
			BeforeEach(func() {
				err = store.Cleanup()
			})

			It("doesn't error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("closes the db connection", func() {
				Expect(fakeSqlDb.CloseCallCount()).To(Equal(1))
			})
		})
	})
})

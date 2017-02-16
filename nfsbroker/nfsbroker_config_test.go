package nfsbroker_test

import (
	. "code.cloudfoundry.org/nfsbroker/nfsbroker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
)

var _ = Describe("BrokerConfigDetails", func() {
	var (
		logger        lager.Logger
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker-config")
	})

	Context("ConfigDetails", func() {
		It("returns empty mount & source options, no mandatory", func() {
			ConfigMandatory := []string{}

			configDetails := NewNfsBrokerConfigDetails()
			configDetails.ReadConf("", "", ConfigMandatory)

			logger.Debug("configDetails Debug", lager.Data{"configDetails": *configDetails})

			Expect(len(configDetails.Allowed)).To(Equal(0))
			Expect(len(configDetails.Forced)).To(Equal(0))
			Expect(len(configDetails.Options)).To(Equal(0))
			Expect(configDetails.IsSloppyMount()).To(BeFalse())
			Expect(len(configDetails.CheckMandatory())).To(Equal(0))
		})

		It("returns empty mount & source options, with mandatory", func() {
			ConfigMandatory := []string{"uid", "gid"}
			configDetails := NewNfsBrokerConfigDetails()
			configDetails.ReadConf("", "", ConfigMandatory)

			logger.Debug("configDetails Debug", lager.Data{"configDetails": *configDetails})

			Expect(len(configDetails.Allowed)).To(Equal(0))
			Expect(len(configDetails.Forced)).To(Equal(0))
			Expect(len(configDetails.Options)).To(Equal(0))
			Expect(configDetails.IsSloppyMount()).To(BeFalse())
			Expect(len(configDetails.CheckMandatory())).To(Equal(2))
			Expect(configDetails.CheckMandatory()).To(Equal(ConfigMandatory))
		})

		It("returns not empty allowed & default, with mandatory", func() {
			ConfigAllowed := []string{"uid","gid"}
			ConfigOptions := map[string]string{
				"uid": "1004",
				"gid": "1002",
			}
			ConfigMandatory := []string{"uid", "gid"}
			configDetails := NewNfsBrokerConfigDetails()
			configDetails.ReadConf("uid,gid", "uid:1004,gid:1002", ConfigMandatory)

			logger.Debug("configDetails Debug", lager.Data{"configDetails": *configDetails})

			Expect(configDetails.Allowed).To(Equal(ConfigAllowed))
			Expect(len(configDetails.Forced)).To(Equal(0))
			Expect(configDetails.Options).To(Equal(ConfigOptions))
			Expect(configDetails.IsSloppyMount()).To(BeFalse())
			Expect(len(configDetails.CheckMandatory())).To(Equal(0))
		})

		It("returns not empty forced, with mandatory", func() {
			ConfigForced := map[string]string{
				"uid": "1004",
				"gid": "1002",
			}
			ConfigMandatory := []string{"uid", "gid"}
			configDetails := NewNfsBrokerConfigDetails()
			configDetails.ReadConf("", "uid:1004,gid:1002", ConfigMandatory)

			logger.Debug("configDetails Debug", lager.Data{"configDetails": *configDetails})

			Expect(len(configDetails.Allowed)).To(Equal(0))
			Expect(configDetails.Forced).To(Equal(ConfigForced))
			Expect(len(configDetails.Options)).To(Equal(0))
			Expect(configDetails.IsSloppyMount()).To(BeFalse())
			Expect(len(configDetails.CheckMandatory())).To(Equal(0))
		})
	})

	Context("Config", func() {
		It("returns empty mount & source, no mandatory", func() {
			source := NewNfsBrokerConfigDetails()
			source.ReadConf("", "", []string{})

			mounts := NewNfsBrokerConfigDetails()
			mounts.ReadConf("", "", []string{})

			config := NewNfsBrokerConfig(source, mounts)

			logger.Debug("config Initiated Debug", lager.Data{"config": *config})

			Expect(config.SetEntries("nfs://1.2.3.4",map[string]interface{}{},[]string{})).To(BeNil())

			Expect(len(config.GetMount())).To(Equal(0))
			Expect(len(config.GetMountConfig())).To(Equal(0))
			Expect(config.GetShare("nfs://1.2.3.4")).To(Equal("nfs://1.2.3.4"))
		})

		It("returns error, for empty source/mount and no empty source mandatory", func() {
			ConfigMandatory := []string{"uid","gid"}

			source := NewNfsBrokerConfigDetails()
			source.ReadConf("uid,gid", "", ConfigMandatory)

			mounts := NewNfsBrokerConfigDetails()
			mounts.ReadConf("sloppy_mount,nfs_uid,nfs_gid", "", []string{})

			config := NewNfsBrokerConfig(source, mounts)

			logger.Debug("config Initiated Debug", lager.Data{"config": *config})

			// Error : missing mandatory source
			Expect(config.SetEntries("nfs://1.2.3.4",map[string]interface{}{},[]string{})).To(HaveOccurred())

			Expect(len(config.GetMount())).To(Equal(0))
			Expect(len(config.GetMountConfig())).To(Equal(0))
			Expect(config.GetShare("nfs://1.2.3.4")).To(Equal("nfs://1.2.3.4"))
		})

		It("returns empty mount and not empty source by default value, for empty source/mount options, not empty allowed and source mandatory", func() {
			ConfigMandatory := []string{"uid","gid"}

			source := NewNfsBrokerConfigDetails()
			source.ReadConf("uid,gid", "uid:1004,gid:1002", ConfigMandatory)

			mounts := NewNfsBrokerConfigDetails()
			mounts.ReadConf("sloppy_mount,nfs_uid,nfs_gid", "", []string{})

			config := NewNfsBrokerConfig(source, mounts)

			logger.Debug("config Initiated Debug", lager.Data{"config": *config})

			Expect(config.SetEntries("nfs://1.2.3.4",map[string]interface{}{},[]string{})).To(BeNil())

			Expect(len(config.GetMount())).To(Equal(0))
			Expect(len(config.GetMountConfig())).To(Equal(0))

			share := config.GetShare("nfs://1.2.3.4")
			Expect(share).To(ContainSubstring("nfs://1.2.3.4?"))
			Expect(share).To(ContainSubstring("uid=1004"))
			Expect(share).To(ContainSubstring("gid=1002"))
		})

		It("returns empty mount and not empty source by forced value, for empty mount and empty source allowed but not empty source default and mandatory", func() {
			ConfigMandatory := []string{"uid","gid"}

			source := NewNfsBrokerConfigDetails()
			source.ReadConf("", "uid:1004,gid:1002", ConfigMandatory)

			mounts := NewNfsBrokerConfigDetails()
			mounts.ReadConf("", "", []string{})

			config := NewNfsBrokerConfig(source, mounts)

			logger.Debug("config Initiated Debug", lager.Data{"config": *config})

			Expect(config.SetEntries("nfs://1.2.3.4",map[string]interface{}{},[]string{})).To(BeNil())

			Expect(len(config.GetMount())).To(Equal(0))
			Expect(len(config.GetMountConfig())).To(Equal(0))

			share := config.GetShare("nfs://1.2.3.4")
			Expect(share).To(ContainSubstring("nfs://1.2.3.4?"))
			Expect(share).To(ContainSubstring("uid=1004"))
			Expect(share).To(ContainSubstring("gid=1002"))
		})

		It("returns empty mount and not empty source by forced value, no error return with sloppy_mount, for empty mount allowed and empty source allowed but not empty source default and mandatory and with sloppy_mount true in mount default", func() {
			ConfigMandatory := []string{"uid","gid"}

			source := NewNfsBrokerConfigDetails()
			source.ReadConf("", "uid:1004,gid:1002", ConfigMandatory)

			mounts := NewNfsBrokerConfigDetails()
			mounts.ReadConf("", "sloppy_mount:true", []string{})

			config := NewNfsBrokerConfig(source, mounts)

			logger.Debug("config Initiated Debug", lager.Data{"config": *config})

			Expect(config.SetEntries("nfs://1.2.3.4",map[string]interface{}{"test":"true"},[]string{})).To(BeNil())

			Expect(len(config.GetMount())).To(Equal(0))
			Expect(len(config.GetMountConfig())).To(Equal(0))

			share := config.GetShare("nfs://1.2.3.4")
			Expect(share).To(ContainSubstring("nfs://1.2.3.4?"))
			Expect(share).To(ContainSubstring("uid=1004"))
			Expect(share).To(ContainSubstring("gid=1002"))
		})

		It("returns mandatory in source and options in mount, for empty source allowed but not empty source default and mandatory, and with no empty mount (bool)", func() {
			ConfigMandatory := []string{"uid","gid"}

			source := NewNfsBrokerConfigDetails()
			source.ReadConf("", "uid:1004,gid:1002", ConfigMandatory)

			mounts := NewNfsBrokerConfigDetails()
			mounts.ReadConf("test", "sloppy_mount:true", []string{})

			config := NewNfsBrokerConfig(source, mounts)

			logger.Debug("config Initiated Debug", lager.Data{"config": *config})

			Expect(config.SetEntries("nfs://1.2.3.4",map[string]interface{}{"test":"true"},[]string{})).To(BeNil())

			Expect(config.GetMount()).To(Equal([]string{"--test"}))
			Expect(config.GetMountConfig()).To(Equal(map[string]interface{}{"test":"true"}))

			share := config.GetShare("nfs://1.2.3.4")
			Expect(share).To(ContainSubstring("nfs://1.2.3.4?"))
			Expect(share).To(ContainSubstring("uid=1004"))
			Expect(share).To(ContainSubstring("gid=1002"))
		})

		It("returns mandatory in source and options in mount, for empty source allowed but not empty source default and mandatory, and with no empty mount (int)", func() {
			ConfigMandatory := []string{"uid","gid"}

			source := NewNfsBrokerConfigDetails()
			source.ReadConf("", "uid:1004,gid:1002", ConfigMandatory)

			mounts := NewNfsBrokerConfigDetails()
			mounts.ReadConf("test", "sloppy_mount:true", []string{})

			config := NewNfsBrokerConfig(source, mounts)

			logger.Debug("config Initiated Debug", lager.Data{"config": *config})

			Expect(config.SetEntries("nfs://1.2.3.4",map[string]interface{}{"test":"1234"},[]string{})).To(BeNil())

			Expect(config.GetMount()).To(Equal([]string{"--test=1234"}))
			Expect(config.GetMountConfig()).To(Equal(map[string]interface{}{"test":"1234"}))

			share := config.GetShare("nfs://1.2.3.4")
			Expect(share).To(ContainSubstring("nfs://1.2.3.4?"))
			Expect(share).To(ContainSubstring("uid=1004"))
			Expect(share).To(ContainSubstring("gid=1002"))
		})

		It("returns mandatory in source and options in mount, for empty source allowed but not empty source default and mandatory, and with no empty mount (string)", func() {
			ConfigMandatory := []string{"uid","gid"}

			source := NewNfsBrokerConfigDetails()
			source.ReadConf("", "uid:1004,gid:1002", ConfigMandatory)

			mounts := NewNfsBrokerConfigDetails()
			mounts.ReadConf("test", "sloppy_mount:true", []string{})

			config := NewNfsBrokerConfig(source, mounts)

			logger.Debug("config Initiated Debug", lager.Data{"config": *config})

			Expect(config.SetEntries("nfs://1.2.3.4",map[string]interface{}{"test":"me"},[]string{})).To(BeNil())

			Expect(config.GetMount()).To(Equal([]string{"--test=me"}))
			Expect(config.GetMountConfig()).To(Equal(map[string]interface{}{"test":"me"}))

			share := config.GetShare("nfs://1.2.3.4")
			Expect(share).To(ContainSubstring("nfs://1.2.3.4?"))
			Expect(share).To(ContainSubstring("uid=1004"))
			Expect(share).To(ContainSubstring("gid=1002"))
		})
	})

})

/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package test

import (
	"time"

	"github.com/google/uuid"

	"github.com/vmware-tanzu/velero/pkg/cmd/cli/install"
	. "github.com/vmware-tanzu/velero/test/util/k8s"
)

const StorageClassName = "e2e-storage-class"
const StorageClassName2 = "e2e-storage-class-2"
const CSIStorageClassName = "e2e-csi-storage-class"
const FeatureCSI = "EnableCSI"
const VanillaZFS = "vanilla-zfs"
const Kind = "kind"
const Azure = "azure"
const AzureCSI = "azure-csi"
const AwsCSI = "aws-csi"
const AWS = "aws"
const Gcp = "gcp"
const Vsphere = "vsphere"

const UploaderTypeRestic = "restic"

var PublicCloudProviders = []string{AWS, Azure, Gcp, Vsphere}
var LocalCloudProviders = []string{Kind, VanillaZFS}
var CloudProviders = append(PublicCloudProviders, LocalCloudProviders...)

var InstallVelero bool
var UUIDgen uuid.UUID

var VeleroCfg VeleroConfig

type E2EReport struct {
	TestDescription string                 `yaml:"Test Description"`
	OtherFields     map[string]interface{} `yaml:",inline"`
}

var ReportData *E2EReport

type VeleroConfig struct {
	VeleroCfgInPerf
	install.Options
	VeleroCLI                         string
	VeleroImage                       string
	VeleroVersion                     string
	CloudCredentialsFile              string
	BSLConfig                         string
	BSLBucket                         string
	BSLPrefix                         string
	VSLConfig                         string
	CloudProvider                     string
	ObjectStoreProvider               string
	VeleroNamespace                   string
	AdditionalBSLProvider             string
	AdditionalBSLBucket               string
	AdditionalBSLPrefix               string
	AdditionalBSLConfig               string
	AdditionalBSLCredentials          string
	RegistryCredentialFile            string
	RestoreHelperImage                string
	UpgradeFromVeleroVersion          string
	UpgradeFromVeleroCLI              string
	MigrateFromVeleroVersion          string
	MigrateFromVeleroCLI              string
	Plugins                           string
	AddBSLPlugins                     string
	KibishiiDirectory                 string
	Debug                             bool
	GCFrequency                       string
	DefaultClusterContext             string
	StandbyClusterContext             string
	ClientToInstallVelero             *TestClient
	DefaultClient                     *TestClient
	StandbyClient                     *TestClient
	ClusterToInstallVelero            string
	DefaultClusterName                string
	StandbyClusterName                string
	ProvideSnapshotsVolumeParam       bool
	VeleroServerDebugMode             bool
	SnapshotMoveData                  bool
	DataMoverPlugin                   string
	StandbyClusterCloudProvider       string
	StandbyClusterPlugins             string
	StandbyClusterObjectStoreProvider string
	DebugVeleroPodRestart             bool
	IsUpgradeTest                     bool
	WithoutDisableInformerCacheParam  bool
	DisableInformerCache              bool
	CreateClusterRoleBinding          bool
	DefaultCLSServiceAccountName      string
	StandbyCLSServiceAccountName      string
	ServiceAccountNameToInstall       string
	EKSPolicyARN                      string
}

type VeleroCfgInPerf struct {
	NFSServerPath         string
	TestCaseDescribe      string
	BackupForRestore      string
	DeleteClusterResource bool
}

type SnapshotCheckPoint struct {
	NamespaceBackedUp string
	// SnapshotIDList is for Azure CSI Verification
	//  we can get SnapshotID from VolumeSnapshotContent from a certain backup
	SnapshotIDList []string
	ExpectCount    int
	PodName        []string
	EnableCSI      bool
}

type BackupConfig struct {
	BackupName                  string
	Namespace                   string
	BackupLocation              string
	UseVolumeSnapshots          bool
	ProvideSnapshotsVolumeParam bool
	Selector                    string
	TTL                         time.Duration
	IncludeResources            string
	ExcludeResources            string
	IncludeClusterResources     bool
	OrderedResources            string
	UseResticIfFSBackup         bool
	DefaultVolumesToFsBackup    bool
	SnapshotMoveData            bool
}

type VeleroCLI2Version struct {
	VeleroVersion string
	VeleroCLI     string
}

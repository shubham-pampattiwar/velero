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

package csi

import (
	"testing"

	volumegroupsnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func fakeDiscovery(servedVGSVersions ...string) *discoveryfake.FakeDiscovery {
	disco := &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{}}
	// An unrelated group, to make sure filtering by group works.
	disco.Resources = []*metav1.APIResourceList{
		{GroupVersion: "snapshot.storage.k8s.io/v1"},
	}
	for _, v := range servedVGSVersions {
		disco.Resources = append(disco.Resources, &metav1.APIResourceList{
			GroupVersion: VGSGroup + "/" + v,
		})
	}
	return disco
}

func TestResolveVGSGroupVersion(t *testing.T) {
	tests := []struct {
		name        string
		served      []string
		expectVer   string
		expectErr   bool
		expectNoAPI bool
	}{
		{name: "v1 only", served: []string{"v1"}, expectVer: "v1"},
		{name: "v1beta1 only", served: []string{"v1beta1"}, expectVer: "v1beta1"},
		{name: "v1beta2 only", served: []string{"v1beta2"}, expectVer: "v1beta2"},
		{name: "all served prefers v1", served: []string{"v1beta1", "v1beta2", "v1"}, expectVer: "v1"},
		{name: "betas served prefers v1beta2", served: []string{"v1beta1", "v1beta2"}, expectVer: "v1beta2"},
		{name: "none served", served: nil, expectNoAPI: true},
		{name: "only unknown version", served: []string{"v2"}, expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gv, err := ResolveVGSGroupVersion(fakeDiscovery(tt.served...))

			switch {
			case tt.expectNoAPI:
				require.ErrorIs(t, err, ErrVGSAPINotAvailable)
			case tt.expectErr:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				assert.Equal(t, VGSGroup, gv.Group)
				assert.Equal(t, tt.expectVer, gv.Version)
			}
		})
	}
}

// TestVGSClientVersionAgnostic proves the accessor performs I/O against whatever
// version the cluster serves (here v1beta1) while callers work with canonical v1
// typed structs.
func TestVGSClientVersionAgnostic(t *testing.T) {
	const version = "v1beta1"

	// Seed a VolumeGroupSnapshotClass served as v1beta1.
	vgsClass := &unstructured.Unstructured{}
	vgsClass.SetGroupVersionKind(schema.GroupVersionKind{Group: VGSGroup, Version: version, Kind: kindVGSClass})
	vgsClass.SetName("rbd-class")
	require.NoError(t, unstructured.SetNestedField(vgsClass.Object, "rbd.csi.ceph.com", "driver"))

	listKinds := map[schema.GroupVersionResource]string{
		{Group: VGSGroup, Version: version, Resource: resVGS}:        "VolumeGroupSnapshotList",
		{Group: VGSGroup, Version: version, Resource: resVGSClass}:   "VolumeGroupSnapshotClassList",
		{Group: VGSGroup, Version: version, Resource: resVGSContent}: "VolumeGroupSnapshotContentList",
	}
	dc := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, vgsClass)

	c := NewVGSClientWithGroupVersion(dc, schema.GroupVersion{Group: VGSGroup, Version: version}, logrus.New())

	gv, err := c.GroupVersion()
	require.NoError(t, err)
	assert.Equal(t, version, gv.Version)

	// List returns canonical v1 typed structs even though the wire version is v1beta1.
	classes, err := c.ListVGSClasses(t.Context())
	require.NoError(t, err)
	require.Len(t, classes.Items, 1)
	assert.Equal(t, "rbd.csi.ceph.com", classes.Items[0].Driver)

	// Create a VGS from a v1 typed struct; it is stored under v1beta1 and read back.
	className := "rbd-class"
	created, err := c.CreateVGS(t.Context(), &volumegroupsnapshotv1.VolumeGroupSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "vgs-1", Namespace: "ns-1"},
		Spec: volumegroupsnapshotv1.VolumeGroupSnapshotSpec{
			VolumeGroupSnapshotClassName: &className,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "vgs-1", created.Name)

	got, err := c.GetVGS(t.Context(), "ns-1", "vgs-1")
	require.NoError(t, err)
	require.NotNil(t, got.Spec.VolumeGroupSnapshotClassName)
	assert.Equal(t, className, *got.Spec.VolumeGroupSnapshotClassName)
}

func TestVGSClientUnavailable(t *testing.T) {
	c := NewVGSClient(nil, fakeDiscovery(), logrus.New())

	_, err := c.GroupVersion()
	require.ErrorIs(t, err, ErrVGSAPINotAvailable)

	_, err = c.ListVGSClasses(t.Context())
	require.ErrorIs(t, err, ErrVGSAPINotAvailable)
}

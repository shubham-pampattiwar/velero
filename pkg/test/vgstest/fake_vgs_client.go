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

// Package vgstest provides test helpers for the VolumeGroupSnapshot dynamic
// accessor. It lives in its own package so that pkg/util/csi tests (which import
// pkg/test) do not create an import cycle with these helpers.
package vgstest

import (
	"testing"

	volumegroupsnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	csiutil "github.com/vmware-tanzu/velero/pkg/util/csi"
)

// NewFakeVGSClient returns a csiutil.VGSClient backed by a fake dynamic client
// seeded with objs and pinned to the given VolumeGroupSnapshot API version (so no
// discovery call is made). Seed objects should be the v1 typed structs; they are
// converted to unstructured and stored under the pinned version's resources so the
// fake behaves like a version-agnostic dynamic client.
func NewFakeVGSClient(t *testing.T, version string, objs ...runtime.Object) *csiutil.VGSClient {
	t.Helper()

	// Scheme used only to derive the Kind of each typed seed object.
	typeScheme := runtime.NewScheme()
	require.NoError(t, volumegroupsnapshotv1.AddToScheme(typeScheme))

	unstructuredObjs := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		gvks, _, err := typeScheme.ObjectKinds(o)
		require.NoError(t, err)
		require.NotEmpty(t, gvks)

		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
		require.NoError(t, err)

		u := &unstructured.Unstructured{Object: m}
		u.SetGroupVersionKind(schema.GroupVersionKind{Group: csiutil.VGSGroup, Version: version, Kind: gvks[0].Kind})
		unstructuredObjs = append(unstructuredObjs, u)
	}

	listKinds := map[schema.GroupVersionResource]string{
		{Group: csiutil.VGSGroup, Version: version, Resource: "volumegroupsnapshots"}:        "VolumeGroupSnapshotList",
		{Group: csiutil.VGSGroup, Version: version, Resource: "volumegroupsnapshotclasses"}:  "VolumeGroupSnapshotClassList",
		{Group: csiutil.VGSGroup, Version: version, Resource: "volumegroupsnapshotcontents"}: "VolumeGroupSnapshotContentList",
	}

	// Use a clean scheme so the fake tracker keeps everything unstructured (no
	// typed<->unstructured conversion at List time).
	dc := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, unstructuredObjs...)

	gv := schema.GroupVersion{Group: csiutil.VGSGroup, Version: version}
	return csiutil.NewVGSClientWithGroupVersion(dc, gv, logrus.New())
}

// NewFakeVGSClientUnavailable returns a VGSClient whose cluster serves no
// VolumeGroupSnapshot API, so any operation fails with ErrVGSAPINotAvailable.
func NewFakeVGSClientUnavailable(t *testing.T) *csiutil.VGSClient {
	t.Helper()
	disco := &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{}}
	return csiutil.NewVGSClient(nil, disco, logrus.New())
}

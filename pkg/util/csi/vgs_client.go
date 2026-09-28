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
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	volumegroupsnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// VGSClient is a version-agnostic accessor for VolumeGroupSnapshot resources. It
// resolves the served API version once (lazily, on first use) and performs all
// I/O through the dynamic client against that version, converting to and from the
// canonical v1 typed structs. Callers work only with v1 types; the wire version is
// an implementation detail.
//
// This exists because the controller-runtime typed client cannot negotiate API
// versions: it sends the GVK of the Go type, so a v1-typed request fails with
// "no matches for kind" on a cluster that only serves v1beta1 (and vice versa).
// The VolumeGroupSnapshot field layout is identical across v1beta1/v1beta2/v1 for
// everything Velero reads or writes, so a single canonical typed view is safe.
type VGSClient struct {
	dyn   dynamic.Interface
	disco discovery.DiscoveryInterface
	log   logrus.FieldLogger

	mu sync.Mutex
	gv *schema.GroupVersion
}

// NewVGSClient constructs a VGSClient. No discovery call is made until the first
// VGS operation, so backups/restores that never touch VolumeGroupSnapshots pay
// nothing.
func NewVGSClient(dyn dynamic.Interface, disco discovery.DiscoveryInterface, log logrus.FieldLogger) *VGSClient {
	return &VGSClient{dyn: dyn, disco: disco, log: log}
}

// NewVGSClientWithGroupVersion constructs a VGSClient pinned to a specific served
// version, skipping discovery. Useful when the caller already knows the version
// (e.g. tests, or a caller that resolved it once elsewhere).
func NewVGSClientWithGroupVersion(dyn dynamic.Interface, gv schema.GroupVersion, log logrus.FieldLogger) *VGSClient {
	return &VGSClient{dyn: dyn, log: log, gv: &gv}
}

// GroupVersion resolves (and caches) the served VolumeGroupSnapshot GroupVersion.
// Returns ErrVGSAPINotAvailable if the cluster serves no VGS API.
func (c *VGSClient) GroupVersion() (schema.GroupVersion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.gv != nil {
		return *c.gv, nil
	}

	gv, err := ResolveVGSGroupVersion(c.disco)
	if err != nil {
		return schema.GroupVersion{}, err
	}

	c.gv = &gv
	c.log.Infof("Using VolumeGroupSnapshot API version %s", gv.String())
	return gv, nil
}

func gvrFor(gv schema.GroupVersion, resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: resource}
}

// toUnstructured converts a typed object to unstructured and stamps the served
// apiVersion/kind so the request targets whatever version the cluster serves.
func toUnstructured(obj any, gv schema.GroupVersion, kind string) (*unstructured.Unstructured, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert object to unstructured")
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(gv.String())
	u.SetKind(kind)
	return u, nil
}

func fromUnstructured(u *unstructured.Unstructured, out any) error {
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, out); err != nil {
		return errors.Wrap(err, "failed to convert unstructured to typed object")
	}
	return nil
}

// ---- VolumeGroupSnapshotClass (cluster-scoped) ----

// ListVGSClasses lists all VolumeGroupSnapshotClasses served by the cluster.
func (c *VGSClient) ListVGSClasses(ctx context.Context) (*volumegroupsnapshotv1.VolumeGroupSnapshotClassList, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	ul, err := c.dyn.Resource(gvrFor(gv, resVGSClass)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshotClassList{}
	for i := range ul.Items {
		item := volumegroupsnapshotv1.VolumeGroupSnapshotClass{}
		if err := fromUnstructured(&ul.Items[i], &item); err != nil {
			return nil, err
		}
		out.Items = append(out.Items, item)
	}
	return out, nil
}

// ---- VolumeGroupSnapshot (namespaced) ----

// CreateVGS creates a VolumeGroupSnapshot and returns the created object.
func (c *VGSClient) CreateVGS(ctx context.Context, vgs *volumegroupsnapshotv1.VolumeGroupSnapshot) (*volumegroupsnapshotv1.VolumeGroupSnapshot, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	u, err := toUnstructured(vgs, gv, kindVGS)
	if err != nil {
		return nil, err
	}

	created, err := c.dyn.Resource(gvrFor(gv, resVGS)).Namespace(vgs.Namespace).Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshot{}
	if err := fromUnstructured(created, out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetVGS fetches a VolumeGroupSnapshot by namespace/name.
func (c *VGSClient) GetVGS(ctx context.Context, namespace, name string) (*volumegroupsnapshotv1.VolumeGroupSnapshot, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	u, err := c.dyn.Resource(gvrFor(gv, resVGS)).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshot{}
	if err := fromUnstructured(u, out); err != nil {
		return nil, err
	}
	return out, nil
}

// ListVGS lists VolumeGroupSnapshots in a namespace matching the given labels.
func (c *VGSClient) ListVGS(ctx context.Context, namespace string, matchLabels map[string]string) (*volumegroupsnapshotv1.VolumeGroupSnapshotList, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	opts := metav1.ListOptions{}
	if len(matchLabels) > 0 {
		opts.LabelSelector = labels.SelectorFromSet(matchLabels).String()
	}

	ul, err := c.dyn.Resource(gvrFor(gv, resVGS)).Namespace(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshotList{}
	for i := range ul.Items {
		item := volumegroupsnapshotv1.VolumeGroupSnapshot{}
		if err := fromUnstructured(&ul.Items[i], &item); err != nil {
			return nil, err
		}
		out.Items = append(out.Items, item)
	}
	return out, nil
}

// DeleteVGS deletes a VolumeGroupSnapshot by namespace/name.
func (c *VGSClient) DeleteVGS(ctx context.Context, namespace, name string) error {
	gv, err := c.GroupVersion()
	if err != nil {
		return err
	}
	return c.dyn.Resource(gvrFor(gv, resVGS)).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

// ---- VolumeGroupSnapshotContent (cluster-scoped) ----

// CreateVGSC creates a VolumeGroupSnapshotContent and returns the created object.
func (c *VGSClient) CreateVGSC(ctx context.Context, vgsc *volumegroupsnapshotv1.VolumeGroupSnapshotContent) (*volumegroupsnapshotv1.VolumeGroupSnapshotContent, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	u, err := toUnstructured(vgsc, gv, kindVGSContent)
	if err != nil {
		return nil, err
	}

	created, err := c.dyn.Resource(gvrFor(gv, resVGSContent)).Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshotContent{}
	if err := fromUnstructured(created, out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetVGSC fetches a VolumeGroupSnapshotContent by name.
func (c *VGSClient) GetVGSC(ctx context.Context, name string) (*volumegroupsnapshotv1.VolumeGroupSnapshotContent, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	u, err := c.dyn.Resource(gvrFor(gv, resVGSContent)).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshotContent{}
	if err := fromUnstructured(u, out); err != nil {
		return nil, err
	}
	return out, nil
}

// ListVGSC lists VolumeGroupSnapshotContents matching the given labels.
func (c *VGSClient) ListVGSC(ctx context.Context, matchLabels map[string]string) (*volumegroupsnapshotv1.VolumeGroupSnapshotContentList, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	opts := metav1.ListOptions{}
	if len(matchLabels) > 0 {
		opts.LabelSelector = labels.SelectorFromSet(matchLabels).String()
	}

	ul, err := c.dyn.Resource(gvrFor(gv, resVGSContent)).List(ctx, opts)
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshotContentList{}
	for i := range ul.Items {
		item := volumegroupsnapshotv1.VolumeGroupSnapshotContent{}
		if err := fromUnstructured(&ul.Items[i], &item); err != nil {
			return nil, err
		}
		out.Items = append(out.Items, item)
	}
	return out, nil
}

// UpdateVGSC updates a VolumeGroupSnapshotContent (spec/metadata).
func (c *VGSClient) UpdateVGSC(ctx context.Context, vgsc *volumegroupsnapshotv1.VolumeGroupSnapshotContent) (*volumegroupsnapshotv1.VolumeGroupSnapshotContent, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	u, err := toUnstructured(vgsc, gv, kindVGSContent)
	if err != nil {
		return nil, err
	}

	updated, err := c.dyn.Resource(gvrFor(gv, resVGSContent)).Update(ctx, u, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshotContent{}
	if err := fromUnstructured(updated, out); err != nil {
		return nil, err
	}
	return out, nil
}

// UpdateVGSCStatus updates the status subresource of a VolumeGroupSnapshotContent.
func (c *VGSClient) UpdateVGSCStatus(ctx context.Context, vgsc *volumegroupsnapshotv1.VolumeGroupSnapshotContent) (*volumegroupsnapshotv1.VolumeGroupSnapshotContent, error) {
	gv, err := c.GroupVersion()
	if err != nil {
		return nil, err
	}

	u, err := toUnstructured(vgsc, gv, kindVGSContent)
	if err != nil {
		return nil, err
	}

	updated, err := c.dyn.Resource(gvrFor(gv, resVGSContent)).UpdateStatus(ctx, u, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}

	out := &volumegroupsnapshotv1.VolumeGroupSnapshotContent{}
	if err := fromUnstructured(updated, out); err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteVGSC deletes a VolumeGroupSnapshotContent by name.
func (c *VGSClient) DeleteVGSC(ctx context.Context, name string) error {
	gv, err := c.GroupVersion()
	if err != nil {
		return err
	}
	return c.dyn.Resource(gvrFor(gv, resVGSContent)).Delete(ctx, name, metav1.DeleteOptions{})
}

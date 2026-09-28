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
	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// VolumeGroupSnapshot API constants. The API group is stable across versions;
// only the version string changes (v1beta1 -> v1beta2 -> v1).
const (
	// VGSGroup is the VolumeGroupSnapshot API group.
	VGSGroup = "groupsnapshot.storage.k8s.io"

	// Kinds served by the group.
	kindVGS        = "VolumeGroupSnapshot"
	kindVGSClass   = "VolumeGroupSnapshotClass"
	kindVGSContent = "VolumeGroupSnapshotContent"

	// Resources (plural) served by the group.
	resVGS        = "volumegroupsnapshots"
	resVGSClass   = "volumegroupsnapshotclasses"
	resVGSContent = "volumegroupsnapshotcontents"
)

// vgsPreferredVersions lists the VolumeGroupSnapshot API versions Velero knows how
// to talk to, in order of preference. When a cluster serves more than one, the
// earliest entry wins (v1 is GA, the betas are legacy).
var vgsPreferredVersions = []string{"v1", "v1beta2", "v1beta1"}

// ErrVGSAPINotAvailable is returned when no VolumeGroupSnapshot API version is
// served by the cluster. Callers that only clean up VGS resources can treat this
// as a no-op rather than a hard failure.
var ErrVGSAPINotAvailable = errors.New("VolumeGroupSnapshot API is not available in the cluster")

// ResolveVGSGroupVersion discovers which VolumeGroupSnapshot API version the cluster
// serves and returns the highest-preference one (v1 > v1beta2 > v1beta1). Different
// clusters serve different versions (e.g. OCP 4.22 serves v1beta1, OCP 5.0 serves v1),
// so the version must be chosen at runtime rather than hardcoded.
func ResolveVGSGroupVersion(disco discovery.DiscoveryInterface) (schema.GroupVersion, error) {
	groups, err := disco.ServerGroups()
	if err != nil {
		return schema.GroupVersion{}, errors.Wrap(err, "failed to discover server API groups")
	}

	served := map[string]bool{}
	for _, g := range groups.Groups {
		if g.Name != VGSGroup {
			continue
		}
		for _, v := range g.Versions {
			served[v.Version] = true
		}
	}

	if len(served) == 0 {
		return schema.GroupVersion{}, ErrVGSAPINotAvailable
	}

	for _, v := range vgsPreferredVersions {
		if served[v] {
			return schema.GroupVersion{Group: VGSGroup, Version: v}, nil
		}
	}

	versions := make([]string, 0, len(served))
	for v := range served {
		versions = append(versions, v)
	}
	return schema.GroupVersion{}, errors.Errorf(
		"cluster serves VolumeGroupSnapshot but no version Velero supports (served: %v, supported: %v)",
		versions, vgsPreferredVersions)
}

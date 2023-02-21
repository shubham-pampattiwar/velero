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

package controller

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	pkgbackup "github.com/vmware-tanzu/velero/pkg/backup"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	"github.com/vmware-tanzu/velero/pkg/persistence"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	"github.com/vmware-tanzu/velero/pkg/plugin/framework"
	"github.com/vmware-tanzu/velero/pkg/util/encode"
)

// backupFinalizerReconciler reconciles a Backup object
type backupFinalizerReconciler struct {
	client            kbclient.Client
	clock             clock.Clock
	backupper         pkgbackup.Backupper
	newPluginManager  func(logrus.FieldLogger) clientmgmt.Manager
	metrics           *metrics.ServerMetrics
	backupStoreGetter persistence.ObjectBackupStoreGetter
	log               logrus.FieldLogger
}

// NewBackupFinalizerReconciler initializes and returns backupFinalizerReconciler struct.
func NewBackupFinalizerReconciler(
	client kbclient.Client,
	clock clock.Clock,
	backupper pkgbackup.Backupper,
	newPluginManager func(logrus.FieldLogger) clientmgmt.Manager,
	backupStoreGetter persistence.ObjectBackupStoreGetter,
	log logrus.FieldLogger,
	metrics *metrics.ServerMetrics,
) *backupFinalizerReconciler {
	return &backupFinalizerReconciler{
		client:            client,
		clock:             clock,
		backupper:         backupper,
		newPluginManager:  newPluginManager,
		backupStoreGetter: backupStoreGetter,
		log:               log,
		metrics:           metrics,
	}
}

// +kubebuilder:rbac:groups=velero.io,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=backups/status,verbs=get;update;patch
func (r *backupFinalizerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithFields(logrus.Fields{
		"controller": "backup-finalizer",
		"backup":     req.NamespacedName,
	})

	// Fetch the Backup instance.
	log.Debug("Getting Backup")
	backup := &velerov1api.Backup{}
	if err := r.client.Get(ctx, req.NamespacedName, backup); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Unable to find Backup")
			return ctrl.Result{}, nil
		}

		log.WithError(err).Error("Error getting Backup")
		return ctrl.Result{}, errors.WithStack(err)
	}

	switch backup.Status.Phase {
	case velerov1api.BackupPhaseFinalizingAfterPluginOperations, velerov1api.BackupPhaseFinalizingAfterPluginOperationsPartiallyFailed:
		// only process backups finalizing after  plugin operations are complete
	default:
		log.Debug("Backup is not awaiting finalizing, skipping")
		return ctrl.Result{}, nil
	}

	original := backup.DeepCopy()
	defer func() {
		// Always attempt to Patch the backup object and status after each reconciliation.
		if err := r.client.Patch(ctx, backup, kbclient.MergeFrom(original)); err != nil {
			log.WithError(err).Error("Error updating backup")
			return
		}
	}()

	location := &velerov1api.BackupStorageLocation{}
	if err := r.client.Get(ctx, kbclient.ObjectKey{
		Namespace: backup.Namespace,
		Name:      backup.Spec.StorageLocation,
	}, location); err != nil {
		return ctrl.Result{}, errors.WithStack(err)
	}
	pluginManager := r.newPluginManager(log)
	defer pluginManager.CleanupClients()

	backupStore, err := r.backupStoreGetter.Get(location, pluginManager, log)
	if err != nil {
		log.WithError(err).Error("Error getting a backup store")
		return ctrl.Result{}, errors.WithStack(err)
	}

	// Download item operations list and backup contents
	operations, err := backupStore.GetBackupItemOperations(backup.Name)
	if err != nil {
		log.WithError(err).Error("Error getting backup item operations")
		return ctrl.Result{}, errors.WithStack(err)
	}

	// Call itemBackupper.BackupItem for the list of items updated by async operations
	backupRequest := &pkgbackup.Request{
		Backup:          backup,
		StorageLocation: location,
	}
	log.Info("Setting up finalized backup temp file")
	backupFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.WithError(err).Error("error creating temp file for backup")
		return ctrl.Result{}, errors.WithStack(err)
	}
	defer closeAndRemoveFile(backupFile, log)

	log.Info("Getting backup item actions")
	actions, err := pluginManager.GetBackupItemActionsV2()
	if err != nil {
		log.WithError(err).Error("error getting Backup Item Actions")
		return ctrl.Result{}, errors.WithStack(err)
	}
	backupItemActionsResolver := framework.NewBackupItemActionResolverV2(actions)
	err = r.backupper.FinalizeBackup(log, backupRequest, backupFile, backupItemActionsResolver, operations)
	if err != nil {
		log.WithError(err).Error("error finalizing Backup")
		return ctrl.Result{}, errors.WithStack(err)
	}

	backupScheduleName := backupRequest.GetLabels()[velerov1api.ScheduleNameLabel]
	switch backup.Status.Phase {
	case velerov1api.BackupPhaseFinalizingAfterPluginOperations:
		backup.Status.Phase = velerov1api.BackupPhaseCompleted
		r.metrics.RegisterBackupSuccess(backupScheduleName)
		r.metrics.RegisterBackupLastStatus(backupScheduleName, metrics.BackupLastStatusSucc)
	case velerov1api.BackupPhaseFinalizingAfterPluginOperationsPartiallyFailed:
		backup.Status.Phase = velerov1api.BackupPhasePartiallyFailed
		r.metrics.RegisterBackupPartialFailure(backupScheduleName)
		r.metrics.RegisterBackupLastStatus(backupScheduleName, metrics.BackupLastStatusFailure)
	}
	backup.Status.CompletionTimestamp = &metav1.Time{Time: r.clock.Now()}
	recordBackupMetrics(log, backup, backupFile, r.metrics, true)

	// update backup metadata in object store
	backupJSON := new(bytes.Buffer)
	if err := encode.EncodeTo(backup, "json", backupJSON); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "error encoding backup json")
	}
	err = backupStore.PutBackupMetadata(backup.Name, backupJSON)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "error uploading backup json")
	}
	err = backupStore.PutBackupContentsFinalUpdates(backup.Name, backupFile)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "error uploading backup final content updates")
	}
	return ctrl.Result{}, nil
}

func (r *backupFinalizerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.Backup{}).
		Complete(r)
}

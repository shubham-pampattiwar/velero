package datamover

import (
	"context"
	"fmt"
	"time"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"

	snapmoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

const (
	ReconciledReasonError               = "Error"
	ConditionReconciled                 = "Reconciled"
	SnapMoverBackupPhaseFailed          = "Failed"
	SnapMoverBackupPhasePartiallyFailed = "PartiallyFailed"
)

func GetVolumeSnapMoverClient() (kbclient.Client, error) {
	client, err := kbclient.New(config.GetConfigOrDie(), kbclient.Options{})
	if err != nil {
		return nil, err
	}
	err = snapmoverv1alpha1.AddToScheme(client.Scheme())
	if err != nil {
		return nil, err
	}

	return client, err
}

func CleanupDatamoverArtifacts(backupName string, log logrus.FieldLogger, vscList snapshotv1api.VolumeSnapshotContentList, volumeSnapshotClient snapshotterClientSet.Interface) error {
	volumeSnapMoverClient, err := GetVolumeSnapMoverClient()
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	VSBList := snapmoverv1alpha1.VolumeSnapshotBackupList{}
	VSBListOptions := kbclient.MatchingLabels(map[string]string{
		velerov1api.BackupNameLabel: backupName,
	})

	err = volumeSnapMoverClient.List(context.TODO(), &VSBList, VSBListOptions)
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	// clean up VSBs as well as VSCs, VSB existence implies that the VSCs are from datamover workflow
	if len(VSBList.Items) > 0 {
		err = CheckIfVolumeSnapshotBackupsAreComplete(context.Background(), VSBList, log)
		if err != nil {
			log.Errorf("failed to wait for VolumeSnapshotBackups to be completed: %s", err.Error())
			return err
		}

		// Now Delete the VSBs
		for _, vsb := range VSBList.Items {
			log.Infof("Cleaning up completed VSB: %s/%s", vsb.Namespace, vsb.Name)
			err := volumeSnapMoverClient.Delete(context.Background(), &vsb)
			if err != nil {
				log.Warnf("failed to delete completed VolumeSnapshotBackup: %s", err.Error())
				return err
			}
		}

		// Delete the VSCs
		for _, vsc := range vscList.Items {
			log.Infof("Cleaning up datamover VSC: %s", vsc.Name)
			err := volumeSnapshotClient.SnapshotV1().VolumeSnapshotContents().Delete(context.TODO(), vsc.Name, metav1.DeleteOptions{})
			if err != nil {
				log.Errorf("fail to delete VolumeSnapshotContent %s: %s", vsc.Namespace, err.Error())
			}
		}
	}
	return nil
}

func CheckIfVolumeSnapshotBackupsAreComplete(ctx context.Context, volumesnapshotbackups snapmoverv1alpha1.VolumeSnapshotBackupList, log logrus.FieldLogger) error {
	eg, _ := errgroup.WithContext(ctx)
	// default timeout value is 10
	timeoutValue := "10m"
	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil {
		return errors.Wrapf(err, "error parsing the datamover timout")
	}
	interval := 5 * time.Second

	volumeSnapMoverClient, err := GetVolumeSnapMoverClient()
	if err != nil {
		return err
	}

	for _, vsb := range volumesnapshotbackups.Items {
		volumesnapshotbackup := vsb
		eg.Go(func() error {
			err := wait.PollImmediate(interval, timeout, func() (bool, error) {
				currentVSB := snapmoverv1alpha1.VolumeSnapshotBackup{}
				err := volumeSnapMoverClient.Get(ctx, kbclient.ObjectKey{Namespace: volumesnapshotbackup.Namespace, Name: volumesnapshotbackup.Name}, &currentVSB)
				if err != nil {
					return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotbackup %s/%s", volumesnapshotbackup.Namespace, volumesnapshotbackup.Name))
				}
				// check for a failed VSB
				if currentVSB.Status.Phase == SnapMoverBackupPhaseFailed ||
					currentVSB.Status.Phase == SnapMoverBackupPhasePartiallyFailed {
					return false, errors.Errorf("volumesnapshotbackup %s has failed status", currentVSB.Name)
				}

				if len(currentVSB.Status.Phase) == 0 || currentVSB.Status.Phase != snapmoverv1alpha1.SnapMoverBackupPhaseCompleted {
					log.Infof("Waiting for volumesnapshotbackup status.phase to change from %s to complete %s/%s. Retrying in %ds", currentVSB.Status.Phase, volumesnapshotbackup.Namespace, volumesnapshotbackup.Name, interval/time.Second)
					return false, nil
				}

				log.Infof("volumesnapshotbackup %s completed", volumesnapshotbackup.Name)
				return true, nil
			})
			if err == wait.ErrWaitTimeout {
				log.Errorf("Timed out awaiting reconciliation of volumesnapshotbackup %s/%s", volumesnapshotbackup.Namespace, volumesnapshotbackup.Name)
			}
			return err
		})
	}
	return eg.Wait()
}

func DeleteVSRsIfComplete(restoreName string, log logrus.FieldLogger, volumeSnapMoverClient kbclient.Client) error {
	if volumeSnapMoverClient == nil {
		var err error
		volumeSnapMoverClient, err = GetVolumeSnapMoverClient()
		if err != nil {
			log.Errorf(err.Error())
			return err
		}
	}

	VSRList := snapmoverv1alpha1.VolumeSnapshotRestoreList{}
	VSRListOptions := kbclient.MatchingLabels(map[string]string{
		velerov1api.RestoreNameLabel: restoreName,
	})

	err := volumeSnapMoverClient.List(context.TODO(), &VSRList, VSRListOptions)
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	if len(VSRList.Items) > 0 {
		err = CheckIfVolumeSnapshotRestoresAreComplete(context.Background(), VSRList, log)
		if err != nil {
			log.Errorf("failed to wait for VolumeSnapshotRestores to be completed: %s", err.Error())
			return err
		}

		// Now Delete the VSRs
		for _, vsr := range VSRList.Items {
			log.Infof("Cleaning up completed VSR: %s/%s", vsr.Namespace, vsr.Name)
			err := volumeSnapMoverClient.Delete(context.Background(), &vsr)
			if err != nil {
				log.Warnf("failed to delete completed VolumeSnapshotRestore: %s", err.Error())
				return err
			}
		}
	}
	return nil
}

func CheckIfVolumeSnapshotRestoresAreComplete(ctx context.Context, volumesnapshotrestores snapmoverv1alpha1.VolumeSnapshotRestoreList, log logrus.FieldLogger) error {
	eg, _ := errgroup.WithContext(ctx)
	// default timeout value is 10
	timeoutValue := "10m"
	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil {
		return errors.Wrapf(err, "error parsing the datamover timout")
	}
	interval := 5 * time.Second

	volumeSnapMoverClient, err := GetVolumeSnapMoverClient()
	if err != nil {
		return err
	}

	for _, vsr := range volumesnapshotrestores.Items {
		volumesnapshotrestore := vsr
		eg.Go(func() error {
			err := wait.PollImmediate(interval, timeout, func() (bool, error) {
				currentVSR := snapmoverv1alpha1.VolumeSnapshotRestore{}
				err := volumeSnapMoverClient.Get(ctx, kbclient.ObjectKey{Namespace: volumesnapshotrestore.Namespace, Name: volumesnapshotrestore.Name}, &currentVSR)
				if err != nil {
					return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotrestore %s/%s", volumesnapshotrestore.Namespace, volumesnapshotrestore.Name))
				}
				// check for a failed VSR
				for _, cond := range currentVSR.Status.Conditions {
					if cond.Status == metav1.ConditionFalse && cond.Reason == ReconciledReasonError && cond.Type == ConditionReconciled && (currentVSR.Status.Phase == SnapMoverBackupPhaseFailed || currentVSR.Status.Phase == SnapMoverBackupPhasePartiallyFailed) {
						return false, errors.Errorf("volumesnapshotrestore %s has failed status", currentVSR.Name)
					}
				}

				if len(currentVSR.Status.Phase) == 0 || currentVSR.Status.Phase != snapmoverv1alpha1.SnapMoverRestorePhaseCompleted {
					log.Infof("Waiting for volumesnapshotrestore status.phase to change from %s to complete %s/%s. Retrying in %ds", currentVSR.Status.Phase, volumesnapshotrestore.Namespace, volumesnapshotrestore.Name, interval/time.Second)
					return false, nil
				}

				log.Infof("volumesnapshotrestore %s completed", volumesnapshotrestore.Name)
				return true, nil
			})
			if err == wait.ErrWaitTimeout {
				log.Errorf("Timed out awaiting reconciliation of volumesnapshotrestore %s/%s", volumesnapshotrestore.Namespace, volumesnapshotrestore.Name)
			}
			return err
		})
	}
	return eg.Wait()
}

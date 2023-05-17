package datamover

import (
	"context"
	"fmt"
	"time"

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

func DeleteVSBsIfComplete(backupName string, log logrus.FieldLogger) error {
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
				for _, cond := range currentVSB.Status.Conditions {
					if cond.Status == metav1.ConditionFalse && cond.Reason == ReconciledReasonError && cond.Type == ConditionReconciled && (currentVSB.Status.Phase == SnapMoverBackupPhaseFailed || currentVSB.Status.Phase == SnapMoverBackupPhasePartiallyFailed) {
						return false, errors.Errorf("volumesnapshotbackup %s has failed status", currentVSB.Name)
					}
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

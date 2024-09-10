/*
Copyright 2020 the Velero contributors.

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

const (
	Backup                = "backup"
	BackupOperations      = "backup-operations"
	BackupDeletion        = "backup-deletion"
	BackupFinalizer       = "backup-finalizer"
	BackupRepo            = "backup-repo"
	BackupStorageLocation = "backup-storage-location"
	BackupSync            = "backup-sync"
	DownloadRequest       = "download-request"
	GarbageCollection     = "gc"
	PodVolumeBackup       = "pod-volume-backup"
	PodVolumeRestore      = "pod-volume-restore"
	Restore               = "restore"
	RestoreOperations     = "restore-operations"
	Schedule              = "schedule"
	ServerStatusRequest   = "server-status-request"
	RestoreFinalizer      = "restore-finalizer"
)

// DisableableControllers is a list of controllers that can be disabled
var DisableableControllers = []string{
	Backup,
	BackupOperations,
	BackupDeletion,
	BackupFinalizer,
	BackupSync,
	DownloadRequest,
	GarbageCollection,
	BackupRepo,
	Restore,
	RestoreOperations,
	Schedule,
	ServerStatusRequest,
	RestoreFinalizer,
}

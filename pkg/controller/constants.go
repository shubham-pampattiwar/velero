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
	BackupOperations      = "backup-operations"
	Backup                = "backup"
	BackupDeletion        = "backup-deletion"
	BackupFinalizer       = "backup-finalizer"
	BackupStorageLocation = "backup-storage-location"
	BackupSync            = "backup-sync"
	DownloadRequest       = "download-request"
	GarbageCollection     = "gc"
	PodVolumeBackup       = "pod-volume-backup"
	PodVolumeRestore      = "pod-volume-restore"
	BackupRepo            = "backup-repo"
	Restore               = "restore"
	Schedule              = "schedule"
	ServerStatusRequest   = "server-status-request"
)

// DisableableControllers is a list of controllers that can be disabled
var DisableableControllers = []string{
	BackupOperations,
	Backup,
	BackupDeletion,
	BackupFinalizer,
	BackupSync,
	DownloadRequest,
	GarbageCollection,
	BackupRepo,
	Restore,
	Schedule,
	ServerStatusRequest,
}

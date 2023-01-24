/*
Copyright 2019 the Velero contributors.

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

package archive

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"path/filepath"

	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
)

// Extractor unzips/extracts a backup tarball to a local
// temp directory.
type Extractor struct {
	log logrus.FieldLogger
	fs  filesystem.Interface
}

func NewExtractor(log logrus.FieldLogger, fs filesystem.Interface) *Extractor {
	return &Extractor{
		log: log,
		fs:  fs,
	}
}

// UnzipAndExtractBackup extracts a reader on a gzipped tarball to a local temp directory
func (e *Extractor) UnzipAndExtractBackup(sources []io.Reader) (string, error) {
	var gzrs []*gzip.Reader
	defer func() {
		for _, gzr := range gzrs {
			gzr.Close()
		}
	}()
	for _, src := range sources {
		gzr, err := gzip.NewReader(src)
		if err != nil {
			e.log.Infof("error creating gzip reader: %v", err)
			return "", err
		}
		gzrs = append(gzrs, gzr)
	}

	var tarReaders []*tar.Reader
	for _, gzr := range gzrs {
		tarReaders = append(tarReaders, tar.NewReader(gzr))
	}
	return e.readBackup(tarReaders)
}

func (e *Extractor) writeFile(target string, tarRdr *tar.Reader) error {
	file, err := e.fs.Create(target)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.Copy(file, tarRdr); err != nil {
		return err
	}
	return nil
}

// Reads from a slice of tar.Reader objects. Files in the later tar archives will overwrite files
// with the same path in the same archive. This allows later updates to "modify" files in the
// archive without having to recreate the entire archive.
func (e *Extractor) readBackup(tarRdrs []*tar.Reader) (string, error) {
	dir, err := e.fs.TempDir("", "")
	if err != nil {
		e.log.Infof("error creating temp dir: %v", err)
		return "", err
	}

	for _, tarRdr := range tarRdrs {
		for {
			header, err := tarRdr.Next()

			if err == io.EOF {
				break
			}
			if err != nil {
				e.log.Infof("error reading tar: %v", err)
				return "", err
			}

			target := filepath.Join(dir, header.Name) //nolint:gosec

			switch header.Typeflag {
			case tar.TypeDir:
				err := e.fs.MkdirAll(target, header.FileInfo().Mode())
				if err != nil {
					e.log.Infof("mkdirall error: %v", err)
					return "", err
				}

			case tar.TypeReg:
				// make sure we have the directory created
				err := e.fs.MkdirAll(filepath.Dir(target), header.FileInfo().Mode())
				if err != nil {
					e.log.Infof("mkdirall error: %v", err)
					return "", err
				}

				// create the file
				if err := e.writeFile(target, tarRdr); err != nil {
					e.log.Infof("error copying: %v", err)
					return "", err
				}
			}
		}
	}

	return dir, nil
}

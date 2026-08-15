package objectstore_test

import (
	"path/filepath"
	"testing"

	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/objectstore/objectstoretest"
)

func TestLocalStore(t *testing.T) {
	objectstoretest.Run(t, func(t *testing.T) (objectstore.Store, func(string) string) {
		dir := t.TempDir()
		return objectstore.NewLocalStore(), func(name string) string {
			return filepath.Join(dir, filepath.FromSlash(name))
		}
	})
}

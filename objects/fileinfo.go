package objects

import (
	"io/fs"
	"os"
	"time"

	"github.com/PlakarKorp/integration-sdk"
)

type (
	FileInfo  = integration.FileInfo
	Attribute = integration.Attribute
)

const (
	AttributeExtended = integration.AttributeExtended
	AttributeADS      = integration.AttributeADS
	AttributeCustom   = integration.AttributeCustom
)

// Deprecated: please construct the object yourself, it's cleaner than
// calling this function.
func NewFileInfo(name string, size int64, mode os.FileMode, modTime time.Time, dev uint64, ino uint64, uid uint64, gid uint64, nlink uint16) FileInfo {
	return integration.NewFileInfo(name, size, mode, modTime, dev, ino, uid, gid, nlink)
}

// Deprecated: don't use this, it really make sense only in
// integration/fs because it's implemented only for [syscall.Stat_t].
func FileInfoFromStat(stat fs.FileInfo) FileInfo {
	return integration.FileInfoFromStat(stat)
}

func ParseFileInfoSortKeys(sortKeysStr string) ([]string, error) {
	return integration.ParseFileInfoSortKeys(sortKeysStr)
}

func SortFileInfos(infos []FileInfo, sortKeys []string) error {
	return integration.SortFileInfos(infos, sortKeys)
}

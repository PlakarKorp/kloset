//go:build !windows
// +build !windows

package objects

import (
	"io/fs"
	"syscall"
)

func FileInfoFromStat(stat fs.FileInfo) FileInfo {
	Ldev := uint64(0)
	Lino := uint64(0)
	Luid := uint64(0)
	Lgid := uint64(0)
	Lnlink := uint16(0)

	if sys, ok := stat.Sys().(*syscall.Stat_t); ok {
		Ldev = uint64(sys.Dev)
		Lino = uint64(sys.Ino)
		Luid = uint64(sys.Uid)
		Lgid = uint64(sys.Gid)
		Lnlink = uint16(sys.Nlink)
	}

	return FileInfo{
		Lname:    stat.Name(),
		Lsize:    stat.Size(),
		Lmode:    stat.Mode(),
		LmodTime: stat.ModTime(),
		Ldev:     Ldev,
		Lino:     Lino,
		Luid:     Luid,
		Lgid:     Lgid,
		Lnlink:   Lnlink,
	}
}

//go:build unix

package gocached

import "syscall"

// fsTotalBytes returns the total size in bytes of the filesystem holding dir.
func fsTotalBytes(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Blocks) * int64(st.Bsize), nil
}

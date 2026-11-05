package limit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTree writes files below root. A value that starts with "->" makes a
// symbolic link to the rest of the value.
func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		if target, ok := cutLink(content); ok {
			require.NoError(t, os.Symlink(target, path))
			continue
		}
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
}

func cutLink(s string) (string, bool) {
	if len(s) > 2 && s[:2] == "->" {
		return s[2:], true
	}
	return "", false
}

func TestFindCgroup_V2Namespaced(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"proc/self/cgroup":                  "0::/\n",
		"sys/fs/cgroup/cgroup.controllers":  "cpuset cpu io memory pids\n",
		"sys/fs/cgroup/memory.current":      "1\n",
		"sys/fs/cgroup/cpu.stat":            "usage_usec 1\n",
		"sys/fs/cgroup/pids.current":        "1\n",
		"sys/fs/cgroup/unrelated/something": "",
	})

	cg := findCgroup(filepath.Join(root, "proc"), filepath.Join(root, "sys"))
	dir := filepath.Join(root, "sys/fs/cgroup")
	assert.Equal(t, cgroup{v2: true, memory: dir, cpu: dir, pids: dir}, cg)
}

func TestFindCgroup_V2Nested(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"proc/self/cgroup":                                             "0::/system.slice/nebula.service\n",
		"sys/fs/cgroup/cgroup.controllers":                             "cpu memory pids\n",
		"sys/fs/cgroup/system.slice/nebula.service/cgroup.controllers": "memory\n",
	})

	cg := findCgroup(filepath.Join(root, "proc"), filepath.Join(root, "sys"))
	dir := filepath.Join(root, "sys/fs/cgroup/system.slice/nebula.service")
	assert.Equal(t, cgroup{v2: true, memory: dir}, cg)
}

func TestFindCgroup_V1(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"proc/self/cgroup": "12:pids:/docker/abc\n4:memory:/docker/abc\n3:cpu,cpuacct:/docker/abc\n0::/\n",
		// Docker mounts the container's cgroup at the mount point
		"sys/fs/cgroup/memory/memory.limit_in_bytes": "1\n",
		"sys/fs/cgroup/cpu/cpu.cfs_quota_us":         "1\n",
		// a host without a cgroup namespace shows the path
		"sys/fs/cgroup/pids/docker/abc/pids.max": "1\n",
	})

	cg := findCgroup(filepath.Join(root, "proc"), filepath.Join(root, "sys"))
	assert.Equal(t, cgroup{
		memory: filepath.Join(root, "sys/fs/cgroup/memory"),
		cpu:    filepath.Join(root, "sys/fs/cgroup/cpu"),
		pids:   filepath.Join(root, "sys/fs/cgroup/pids/docker/abc"),
	}, cg)
}

func TestFindCgroup_None(t *testing.T) {
	root := t.TempDir()
	assert.Equal(t, cgroup{}, findCgroup(filepath.Join(root, "proc"), filepath.Join(root, "sys")))
}

package limit

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// cgroup holds the directories of the process's cgroup controllers. An empty
// directory means the controller is not available.
type cgroup struct {
	// v2 is true for the unified hierarchy (cgroup v2).
	v2 bool

	memory string
	cpu    string
	pids   string
}

// findCgroup locates the cgroup of the process from <proc>/self/cgroup and
// the cgroup file system under <sys>/fs/cgroup. In a container with its own
// cgroup namespace, the cgroup path is "/" and the mount point is the
// container's cgroup. Without a cgroup namespace (cgroup v1 in Docker), the
// runtime mounts the container's cgroup at the mount point too, and the
// path from /proc/self/cgroup does not exist below it.
func findCgroup(proc, sys string) cgroup {
	root := filepath.Join(sys, "fs", "cgroup")

	data, err := os.ReadFile(filepath.Join(proc, "self", "cgroup"))
	if err != nil {
		return cgroup{}
	}

	// hierarchy-ID:controller-list:cgroup-path
	paths := map[string]string{}
	for line := range strings.Lines(string(data)) {
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == "0" && parts[1] == "" {
			paths[""] = parts[2]
			continue
		}
		for _, controller := range strings.Split(parts[1], ",") {
			paths[controller] = parts[2]
		}
	}

	if exists(filepath.Join(root, "cgroup.controllers")) {
		path, ok := paths[""]
		if !ok {
			return cgroup{}
		}
		dir := filepath.Join(root, path)
		if !exists(dir) {
			dir = root
		}

		controllers, _ := os.ReadFile(filepath.Join(dir, "cgroup.controllers"))
		enabled := strings.Fields(string(controllers))
		cg := cgroup{v2: true}
		if slices.Contains(enabled, "memory") || exists(filepath.Join(dir, "memory.current")) {
			cg.memory = dir
		}
		if slices.Contains(enabled, "cpu") || exists(filepath.Join(dir, "cpu.stat")) {
			cg.cpu = dir
		}
		if slices.Contains(enabled, "pids") || exists(filepath.Join(dir, "pids.current")) {
			cg.pids = dir
		}
		return cg
	}

	v1 := func(controller string) string {
		path, ok := paths[controller]
		if !ok {
			return ""
		}
		mount := filepath.Join(root, controller)
		if !exists(mount) {
			return ""
		}
		if dir := filepath.Join(mount, path); exists(dir) {
			return dir
		}
		return mount
	}

	return cgroup{
		memory: v1("memory"),
		cpu:    v1("cpu"),
		pids:   v1("pids"),
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

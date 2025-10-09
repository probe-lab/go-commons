package db

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMapping(t *testing.T) {
	t.Run("invalid unequal projects", func(t *testing.T) {
		_, err := NewMapping([]string{"project"}, []string{}, []string{})
		assert.Error(t, err)
	})

	t.Run("invalid unequal networks", func(t *testing.T) {
		_, err := NewMapping([]string{}, []string{"network"}, []string{})
		assert.Error(t, err)
	})

	t.Run("invalid unequal items", func(t *testing.T) {
		_, err := NewMapping([]string{}, []string{}, []string{"item"})
		assert.Error(t, err)
	})

	t.Run("invalid unequal items filled", func(t *testing.T) {
		_, err := NewMapping([]string{"project"}, []string{"network"}, []string{"item0", "item1"})
		assert.Error(t, err)
	})

	t.Run("empty map valid", func(t *testing.T) {
		m, err := NewMapping([]string{}, []string{}, []string{})
		assert.NoError(t, err)
		assert.Len(t, m, 0)
	})

	t.Run("valid mapping single entry", func(t *testing.T) {
		m, err := NewMapping([]string{"project"}, []string{"network"}, []string{"item"})
		assert.NoError(t, err)
		assert.Len(t, m, 1)
		assert.Len(t, m["project"], 1)
		assert.Equal(t, m["project"]["network"], "item")
	})

	t.Run("valid mapping multi networks", func(t *testing.T) {
		m, err := NewMapping([]string{"project0", "project0"}, []string{"network0", "network1"}, []string{"item0", "item1"})
		assert.NoError(t, err)
		assert.Len(t, m, 1)
		assert.Len(t, m["project0"], 2)
		assert.Equal(t, m["project0"]["network0"], "item0")
		assert.Equal(t, m["project0"]["network1"], "item1")
	})

	t.Run("duplicate networks overwrite", func(t *testing.T) {
		m, err := NewMapping([]string{"project0", "project0"}, []string{"network0", "network0"}, []string{"item0", "item1"})
		assert.NoError(t, err)
		assert.Len(t, m, 1)
		assert.Len(t, m["project0"], 1)
		assert.Equal(t, m["project0"]["network0"], "item1") // last item wins
	})

	t.Run("get mappings", func(t *testing.T) {
		m, err := NewMapping([]string{"project0", "project0"}, []string{"network0", "network1"}, []string{"item0", "item1"})
		assert.NoError(t, err)

		val, found := m.Get("project0", "network0")
		assert.True(t, found)
		assert.Equal(t, val, "item0")

		val, found = m.Get("project0", "network1")
		assert.True(t, found)
		assert.Equal(t, val, "item1")

		_, found = m.Get("invalid", "")
		assert.False(t, found)

		_, found = m.Get("project0", "invalid")
		assert.False(t, found)
	})

	t.Run("for each", func(t *testing.T) {
		m, err := NewMapping([]string{"project0", "project0"}, []string{"network0", "network1"}, []string{"item0", "item1"})
		assert.NoError(t, err)

		gotProjects := []string{}
		gotNetworks := []string{}
		gotItems := []string{}
		m.ForEach(func(project, network string, item string) {
			gotProjects = append(gotProjects, project)
			gotNetworks = append(gotNetworks, network)
			gotItems = append(gotItems, item)
		})

		// map access is random internally
		sort.Strings(gotProjects)
		sort.Strings(gotNetworks)
		sort.Strings(gotItems)

		assert.Equal(t, []string{"project0", "project0"}, gotProjects)
		assert.Equal(t, []string{"network0", "network1"}, gotNetworks)
		assert.Equal(t, []string{"item0", "item1"}, gotItems)
	})
}

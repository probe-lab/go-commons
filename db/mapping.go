package db

import (
	"fmt"
	"log/slog"
	"strings"
)

type Mapping[T any] map[string]map[string]T

func NewMapping[T any](projects []string, networks []string, items []T) (Mapping[T], error) {
	if len(projects) != len(networks) || len(networks) != len(items) {
		return nil, fmt.Errorf("projects (%d), networks (%d) and %T (%d) must have the same length", len(projects), len(networks), items, len(items))
	}

	mapping := make(Mapping[T])
	for i, project := range projects {
		project = strings.ToLower(project)
		network := strings.ToLower(networks[i])
		item := items[i]

		if _, found := mapping[project]; !found {
			mapping[project] = make(map[string]T)
		}

		if _, found := mapping[project][network]; found {
			slog.Warn("Duplicate network for same project", "project", project, "network", network)
		}

		mapping[project][network] = item

	}

	return mapping, nil
}

func (d Mapping[T]) Get(project string, network string) (T, bool) {
	networks, found := d[project]
	if !found {
		return *new(T), false
	}

	item, found := networks[network]
	return item, found
}

func (d Mapping[T]) ForEach(fn func(project string, network string, item T)) {
	for project, networks := range d {
		for network, item := range networks {
			fn(project, network, item)
		}
	}
}

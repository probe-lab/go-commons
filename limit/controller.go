package limit

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// Config configures a [Controller].
type Config struct {
	// Initial is the limit at the start.
	Initial int

	// Min is the lowest limit, at least 1.
	Min int

	// Max is the highest limit. Zero means no maximum: the capacities in the
	// readings bound the limit.
	Max int

	// Settle is how long the controller waits after a change before it
	// raises the limit again. Set it to about the duration of one task: the
	// resources of a new task show only while the task runs.
	Settle time.Duration

	// Required names the capacities that a reading must contain for the
	// limit to grow above Initial. Without them, the controller can't tell
	// how much room is left.
	Required []string

	// Approach is the share of the gap between the limit and the ceiling
	// that one step closes, so that the limit approaches the ceiling from
	// below.
	Approach float64

	// Growth is the largest share by which one step raises the limit.
	Growth float64

	// Busy is the share of the limit that must be in use for the limit to
	// grow. More room does not help if the tasks don't use what they have.
	Busy float64

	// PressureCut is the share by which a hard pressure lowers the limit.
	PressureCut float64

	// DamageCut is the share by which a damage lowers the limit. It also
	// bounds how much one step lowers the limit when a capacity is above its
	// target.
	DamageCut float64

	// DamageCap is the share of the limit at the time of a damage above which
	// the limit never grows again.
	DamageCap float64
}

// DefaultConfig returns the default controller configuration.
func DefaultConfig() Config {
	return Config{
		Initial:     1000,
		Min:         10,
		Settle:      15 * time.Second,
		Approach:    0.5,
		Growth:      0.25,
		Busy:        0.9,
		PressureCut: 0.1,
		DamageCut:   0.3,
		DamageCap:   0.8,
	}
}

// Validate checks the configuration.
func (cfg Config) Validate() error {
	switch {
	case cfg.Min < 1:
		return fmt.Errorf("min must be at least 1, got %d", cfg.Min)
	case cfg.Initial < cfg.Min:
		return fmt.Errorf("initial (%d) must be at least min (%d)", cfg.Initial, cfg.Min)
	case cfg.Max != 0 && cfg.Max < cfg.Initial:
		return fmt.Errorf("max (%d) must be zero or at least initial (%d)", cfg.Max, cfg.Initial)
	case cfg.Settle < 0:
		return fmt.Errorf("settle must not be negative, got %s", cfg.Settle)
	}

	shares := []struct {
		name  string
		value float64
	}{
		{"approach", cfg.Approach},
		{"growth", cfg.Growth},
		{"busy", cfg.Busy},
		{"pressure cut", cfg.PressureCut},
		{"damage cut", cfg.DamageCut},
		{"damage cap", cfg.DamageCap},
	}
	for _, s := range shares {
		if s.value <= 0 || s.value > 1 {
			return fmt.Errorf("%s must be more than 0 and at most 1, got %v", s.name, s.value)
		}
	}

	return nil
}

// State is what the process does at the time of a reading.
type State struct {
	// Inflight is the number of tasks that run.
	Inflight int

	// Backlog is the number of tasks that wait for a free slot.
	Backlog int
}

// Decision is the outcome of [Controller.Update].
type Decision struct {
	// Limit is the new limit.
	Limit int

	// Previous is the limit before the update.
	Previous int

	// Ceiling is the highest limit at which every capacity stays within its
	// target, projected from the current use. It is zero if no capacity
	// bounds the limit.
	Ceiling int

	// Bound names the capacity that sets the ceiling.
	Bound string

	// Reason tells why the limit changed or stayed.
	Reason string
}

// Changed reports whether the limit changed.
func (d Decision) Changed() bool {
	return d.Limit != d.Previous
}

// A Controller computes the limit from readings. It is not safe for
// concurrent use.
type Controller struct {
	cfg Config

	limit int

	// capped is the limit that the controller never exceeds again after a
	// damage. Zero means none.
	capped int

	// changed is when the limit last changed.
	changed time.Time
}

// NewController returns a controller that starts at cfg.Initial.
func NewController(cfg Config) (*Controller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Controller{
		cfg:   cfg,
		limit: cfg.Initial,
	}, nil
}

// Limit returns the current limit.
func (c *Controller) Limit() int {
	return c.limit
}

// Capped returns the limit that the controller never exceeds again because
// of a damage, or zero.
func (c *Controller) Capped() int {
	return c.capped
}

// Update computes the next limit from the state of the process and a
// reading:
//
//  1. A damage cuts the limit by DamageCut and caps it at DamageCap of the
//     limit before the cut for the rest of the run.
//  2. A capacity above its target lowers the limit to the projected
//     ceiling, by at most DamageCut per update.
//  3. A pressure at its hard threshold cuts the limit by PressureCut.
//  4. A pressure at its soft threshold holds the limit.
//  5. Otherwise, if the tasks use the limit, tasks wait, and the last change
//     is Settle ago, the limit closes Approach of the gap to the ceiling,
//     by at most Growth.
//
// A cut takes effect when the tasks above the new limit have ended. Until
// then, or for Settle, damages and pressures don't cut the limit again.
func (c *Controller) Update(now time.Time, st State, r Reading) Decision {
	d := Decision{Previous: c.limit, Limit: c.limit}

	// The first update starts the clock: the tasks that run at the initial
	// limit need Settle to show their use.
	if c.changed.IsZero() {
		c.changed = now
	}

	ceiling, bound := c.ceiling(st, r)
	if ceiling < math.MaxInt {
		d.Ceiling, d.Bound = ceiling, bound
	}

	cutTookEffect := st.Inflight <= c.limit || now.Sub(c.changed) >= c.cfg.Settle

	// 1. Damage
	if damaged := damages(r); len(damaged) > 0 {
		if !cutTookEffect {
			d.Reason = "damage while the last cut takes effect: " + strings.Join(damaged, ", ")
			return d
		}

		capped := max(c.cfg.Min, scale(c.limit, c.cfg.DamageCap))
		if c.capped == 0 || capped < c.capped {
			c.capped = capped
		}
		c.set(now, &d, max(c.cfg.Min, scale(c.limit, 1-c.cfg.DamageCut)))
		d.Reason = "damage: " + strings.Join(damaged, ", ")
		return d
	}

	// 2. Capacity above its target. The projection is reliable only when
	// the tasks use the resource, so the limit only shrinks for a resource
	// that is above its target.
	if over := overTarget(r); len(over) > 0 && ceiling < st.Inflight && ceiling < c.limit {
		next := max(c.cfg.Min, ceiling, scale(c.limit, 1-c.cfg.DamageCut))
		if next < c.limit {
			c.set(now, &d, next)
			d.Reason = "above target: " + strings.Join(over, ", ")
			return d
		}
	}

	// 3. Hard pressure
	if hard := pressures(r, true); len(hard) > 0 {
		if !cutTookEffect {
			d.Reason = "pressure while the last cut takes effect: " + strings.Join(hard, ", ")
			return d
		}
		c.set(now, &d, max(c.cfg.Min, scale(c.limit, 1-c.cfg.PressureCut)))
		d.Reason = "pressure: " + strings.Join(hard, ", ")
		return d
	}

	// 4. Soft pressure
	if soft := pressures(r, false); len(soft) > 0 {
		d.Reason = "holding for pressure: " + strings.Join(soft, ", ")
		return d
	}

	// 5. Growth
	switch {
	case st.Backlog == 0:
		d.Reason = "no tasks wait"
		return d
	case float64(st.Inflight) < c.cfg.Busy*float64(c.limit):
		d.Reason = fmt.Sprintf("%d of %d slots in use", st.Inflight, c.limit)
		return d
	case now.Sub(c.changed) < c.cfg.Settle:
		d.Reason = "settling after the last change"
		return d
	}

	// the target is the lowest of the bounds, named for the reason
	target, bounded := ceiling, "the "+bound+" ceiling"
	if bound == "" {
		bounded = "no ceiling"
	}
	if waiting := st.Inflight + st.Backlog; waiting < target {
		target, bounded = waiting, "the waiting tasks"
	}
	if c.cfg.Max > 0 && c.cfg.Max < target {
		target, bounded = c.cfg.Max, "the maximum"
	}
	if c.capped > 0 && c.capped < target {
		target, bounded = c.capped, "the cap after a damage"
	}
	if missing := c.missing(r); len(missing) > 0 && max(c.limit, c.cfg.Initial) < target {
		target = max(c.limit, c.cfg.Initial)
		bounded = "the initial limit, can't read " + strings.Join(missing, ", ")
	}

	if target <= c.limit {
		d.Reason = "at " + bounded
		return d
	}

	step := int(math.Ceil(float64(target-c.limit) * c.cfg.Approach))
	step = min(step, max(1, scale(c.limit, c.cfg.Growth)))
	c.set(now, &d, c.limit+max(1, step))
	d.Reason = "growing toward " + bounded

	return d
}

// set changes the limit.
func (c *Controller) set(now time.Time, d *Decision, limit int) {
	c.limit = limit
	c.changed = now
	d.Limit = limit
}

// ceiling returns the highest limit at which every capacity stays within its
// target, and the name of the capacity that sets it. It assumes that the use
// of a capacity grows in proportion to the tasks. The part of the use that
// doesn't grow with the tasks counts as their use too, so the ceiling errs
// low. It is math.MaxInt if no capacity bounds the limit.
func (c *Controller) ceiling(st State, r Reading) (int, string) {
	ceiling, bound := math.MaxInt, ""
	if st.Inflight <= 0 {
		return ceiling, bound
	}

	for _, capacity := range r.Capacities {
		if capacity.Size <= 0 || capacity.Used <= 0 || capacity.Target <= 0 {
			continue
		}

		projected := float64(st.Inflight) * capacity.Target * capacity.Size / capacity.Used
		if projected >= math.MaxInt {
			continue
		}

		if p := int(projected); p < ceiling {
			ceiling, bound = p, capacity.Name
		}
	}

	return ceiling, bound
}

// missing returns the required capacities that the reading lacks.
func (c *Controller) missing(r Reading) []string {
	var missing []string
	for _, name := range c.cfg.Required {
		if _, ok := r.Capacity(name); !ok {
			missing = append(missing, name)
		}
	}
	return missing
}

// damages describes the damages of the reading.
func damages(r Reading) []string {
	var out []string
	for _, d := range r.Damages {
		if d.Count > 0 {
			out = append(out, fmt.Sprintf("%s +%d", d.Name, d.Count))
		}
	}
	slices.Sort(out)
	return out
}

// overTarget describes the capacities above their target.
func overTarget(r Reading) []string {
	var out []string
	for _, c := range r.Capacities {
		if c.Size > 0 && c.Target > 0 && c.Used > c.Target*c.Size {
			out = append(out, fmt.Sprintf("%s at %.0f%% (target %.0f%%)", c.Name, 100*c.Share(), 100*c.Target))
		}
	}
	slices.Sort(out)
	return out
}

// pressures describes the pressures at their hard or soft threshold.
func pressures(r Reading, hard bool) []string {
	var out []string
	for _, p := range r.Pressures {
		threshold := p.Soft
		if hard {
			threshold = p.Hard
		}
		if threshold > 0 && p.Value >= threshold {
			out = append(out, fmt.Sprintf("%s %.3g (threshold %.3g)", p.Name, p.Value, threshold))
		}
	}
	slices.Sort(out)
	return out
}

// scale returns n times share, rounded down.
func scale(n int, share float64) int {
	return int(float64(n) * share)
}

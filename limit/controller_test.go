package limit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

func newController(t *testing.T, mod func(*Config)) *Controller {
	t.Helper()
	cfg := DefaultConfig()
	if mod != nil {
		mod(&cfg)
	}
	c, err := NewController(cfg)
	require.NoError(t, err)
	return c
}

// files returns a reading with an open-file capacity.
func files(used, size float64) Reading {
	return Reading{Capacities: []Capacity{{Name: SignalFiles, Used: used, Size: size, Target: 0.5}}}
}

func busy(limit int) State {
	return State{Inflight: limit, Backlog: 10 * limit}
}

func TestConfig_Validate(t *testing.T) {
	assert.NoError(t, DefaultConfig().Validate())

	tests := map[string]func(*Config){
		"zero min":          func(c *Config) { c.Min = 0 },
		"initial below min": func(c *Config) { c.Initial = 5 },
		"max below initial": func(c *Config) { c.Max = 500 },
		"negative settle":   func(c *Config) { c.Settle = -time.Second },
		"zero approach":     func(c *Config) { c.Approach = 0 },
		"growth above one":  func(c *Config) { c.Growth = 1.5 },
		"zero damage cap":   func(c *Config) { c.DamageCap = 0 },
	}
	for name, mod := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			mod(&cfg)
			assert.Error(t, cfg.Validate())
		})
	}
}

func TestController_GrowsTowardCeilingFromBelow(t *testing.T) {
	c := newController(t, nil)

	// 1000 tasks use 10000 of 65536 files. Half of the files allow
	// 1000 * 32768 / 10000 = 3276 tasks.
	d := c.Update(t0, busy(1000), files(10000, 65536))
	assert.Equal(t, 1000, d.Limit, "the first update waits for the tasks to show their use")
	assert.Equal(t, 3276, d.Ceiling)
	assert.Equal(t, SignalFiles, d.Bound)
	assert.Contains(t, d.Reason, "settling")

	d = c.Update(t0.Add(5*time.Second), busy(1000), files(10000, 65536))
	assert.False(t, d.Changed(), "settle time not over")

	// one step closes half of the gap, at most 25%
	d = c.Update(t0.Add(15*time.Second), busy(1000), files(10000, 65536))
	assert.Equal(t, 1250, d.Limit)
	assert.Equal(t, "growing toward the files ceiling", d.Reason)

	// the use grows with the tasks, the ceiling stays
	now := t0.Add(15 * time.Second)
	limit := 1250
	for range 20 {
		now = now.Add(15 * time.Second)
		d = c.Update(now, busy(limit), files(float64(limit)*10, 65536))
		require.LessOrEqual(t, d.Limit, 3276, "never above the ceiling")
		limit = d.Limit
	}
	assert.InDelta(t, 3276, limit, 2)

	d = c.Update(now.Add(15*time.Second), busy(limit), files(float64(limit)*10, 65536))
	assert.False(t, d.Changed())
	assert.Equal(t, "at the files ceiling", d.Reason)
}

func TestController_GrowsOnlyWhenTasksWaitAndUseTheLimit(t *testing.T) {
	c := newController(t, nil)
	c.Update(t0, busy(1000), files(10000, 65536))
	now := t0.Add(time.Minute)

	d := c.Update(now, State{Inflight: 1000}, files(10000, 65536))
	assert.False(t, d.Changed())
	assert.Equal(t, "no tasks wait", d.Reason)

	d = c.Update(now, State{Inflight: 500, Backlog: 10}, files(5000, 65536))
	assert.False(t, d.Changed())
	assert.Equal(t, "500 of 1000 slots in use", d.Reason)

	// no more than the tasks that run and wait
	d = c.Update(now, State{Inflight: 1000, Backlog: 100}, files(10000, 65536))
	assert.Equal(t, 1050, d.Limit)
	assert.Equal(t, "growing toward the waiting tasks", d.Reason)
}

func TestController_SoftPressureHolds(t *testing.T) {
	c := newController(t, nil)
	c.Update(t0, busy(1000), files(1000, 65536))

	r := files(1000, 65536)
	r.Pressures = []Pressure{{Name: SignalSchedLatency, Value: 0.006, Soft: 0.005, Hard: 0.02}}
	d := c.Update(t0.Add(time.Minute), busy(1000), r)
	assert.False(t, d.Changed())
	assert.Contains(t, d.Reason, "holding for pressure: sched_latency")
}

func TestController_HardPressureCuts(t *testing.T) {
	c := newController(t, nil)
	c.Update(t0, busy(1000), files(1000, 65536))

	r := files(1000, 65536)
	r.Pressures = []Pressure{{Name: SignalUDPQueue, Value: 0.6, Soft: 0.25, Hard: 0.5}}
	d := c.Update(t0.Add(time.Second), busy(1000), r)
	assert.Equal(t, 900, d.Limit)
	assert.Contains(t, d.Reason, "pressure: udp_queue")

	// 1000 tasks still run: the cut has not taken effect
	d = c.Update(t0.Add(2*time.Second), busy(1000), r)
	assert.Equal(t, 900, d.Limit)
	assert.Contains(t, d.Reason, "while the last cut takes effect")

	// it has once the tasks above the limit ended
	d = c.Update(t0.Add(3*time.Second), busy(900), r)
	assert.Equal(t, 810, d.Limit)
}

func TestController_DamageCutsAndCaps(t *testing.T) {
	c := newController(t, func(cfg *Config) { cfg.Initial = 2000 })
	c.Update(t0, busy(2000), files(1000, 1e6))

	r := files(1000, 1e6)
	r.Damages = []Damage{{Name: SignalUDPDrops, Count: 12}, {Name: SignalTCPDrops}}
	d := c.Update(t0.Add(time.Second), busy(2000), r)
	assert.Equal(t, 1400, d.Limit)
	assert.Equal(t, "damage: udp_drops +12", d.Reason)
	assert.Equal(t, 1600, c.Capped())

	// the limit grows back, but stays below 80% of where the damage happened
	now := t0.Add(time.Second)
	limit := d.Limit
	for range 10 {
		now = now.Add(time.Minute)
		d = c.Update(now, busy(limit), files(float64(limit), 1e6))
		limit = d.Limit
	}
	assert.Equal(t, 1600, limit)
	assert.Equal(t, "at the cap after a damage", d.Reason)
}

func TestController_CapacityAboveTargetShrinks(t *testing.T) {
	c := newController(t, func(cfg *Config) { cfg.Initial = 2000 })
	c.Update(t0, busy(2000), files(1000, 65536))

	// 2000 tasks use 60% of the files; 50% allow 1666
	d := c.Update(t0.Add(time.Second), busy(2000), files(0.6*65536, 65536))
	assert.Equal(t, 1666, d.Limit)
	assert.Contains(t, d.Reason, "above target: files at 60% (target 50%)")

	// the tasks still run: the projection doesn't change
	d = c.Update(t0.Add(2*time.Second), busy(2000), files(0.6*65536, 65536))
	assert.False(t, d.Changed())

	// one step lowers the limit by at most 30%
	d = c.Update(t0.Add(3*time.Second), busy(1666), files(0.99*65536, 65536))
	assert.Equal(t, 1166, d.Limit)
}

func TestController_LowUseDoesNotShrink(t *testing.T) {
	c := newController(t, nil)
	c.Update(t0, busy(1000), files(1000, 65536))

	// at the end of a run, few tasks run, and the static use dominates.
	// The projection is not reliable then: don't shrink.
	d := c.Update(t0.Add(time.Second), State{Inflight: 3, Backlog: 0}, files(1000, 65536))
	assert.False(t, d.Changed())
	assert.Equal(t, 98, d.Ceiling)
}

func TestController_MissingRequiredCapacity(t *testing.T) {
	c := newController(t, func(cfg *Config) {
		cfg.Required = []string{SignalFiles, SignalMemory}
	})
	c.Update(t0, busy(1000), files(1000, 65536))

	d := c.Update(t0.Add(time.Minute), busy(1000), files(1000, 65536))
	assert.False(t, d.Changed())
	assert.Equal(t, "at the initial limit, can't read memory", d.Reason)

	// after a cut, the limit grows back to the initial limit
	r := files(1000, 65536)
	r.Damages = []Damage{{Name: SignalUDPDrops, Count: 1}}
	d = c.Update(t0.Add(2*time.Minute), busy(1000), r)
	assert.Equal(t, 700, d.Limit)

	now := t0.Add(2 * time.Minute)
	limit := d.Limit
	for range 10 {
		now = now.Add(time.Minute)
		limit = c.Update(now, busy(limit), files(float64(limit), 65536)).Limit
	}
	assert.Equal(t, 800, limit, "the damage cap is lower than the initial limit")
}

func TestController_MinAndMax(t *testing.T) {
	c := newController(t, func(cfg *Config) {
		cfg.Initial, cfg.Min, cfg.Max = 20, 10, 25
	})
	c.Update(t0, busy(20), Reading{})

	d := c.Update(t0.Add(time.Minute), busy(20), Reading{})
	assert.Equal(t, 23, d.Limit)
	assert.Equal(t, "growing toward the maximum", d.Reason)

	for i := 2; i < 6; i++ {
		d = c.Update(t0.Add(time.Duration(i)*time.Minute), busy(d.Limit), Reading{})
	}
	assert.Equal(t, 25, d.Limit)
	assert.Equal(t, "at the maximum", d.Reason)

	r := Reading{Damages: []Damage{{Name: SignalTCPDrops, Count: 1}}}
	limit := d.Limit
	now := t0.Add(6 * time.Minute)
	for range 10 {
		now = now.Add(time.Minute)
		limit = c.Update(now, busy(limit), r).Limit
	}
	assert.Equal(t, 10, limit)
}

func TestController_NoCapacity(t *testing.T) {
	c := newController(t, nil)
	c.Update(t0, busy(1000), Reading{})

	// without capacities, the waiting tasks bound the limit
	d := c.Update(t0.Add(time.Minute), busy(1000), Reading{})
	assert.Equal(t, 1250, d.Limit)
	assert.Equal(t, "growing toward the waiting tasks", d.Reason)
	assert.Zero(t, d.Ceiling)
	assert.Empty(t, d.Bound)
}

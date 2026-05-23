package pipeline

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Persister is an optional interface for saving/loading pipeline step state.
type Persister interface {
	SavePipelineStep(pipelineName, stepName string) error
	StartPipelineStep(pipelineName, stepName string) error
	FailPipelineStep(pipelineName, stepName string) error
	LoadPipelineSteps(pipelineName string) ([]string, error)
	DeletePipeline(pipelineName string) error
}

// StepFn is a function that executes one step of a pipeline.
type StepFn func(ctx context.Context) error

// Step represents a single step in a pipeline with its rollback handler.
type Step struct {
	Name    string
	Do      StepFn // forward execution
	Undo    StepFn // rollback (nil = not reversible)
	Retry   int    // max retry attempts on failure (0 = no retry)
	Timeout time.Duration
}

// Pipeline orchestrates a sequence of steps with automatic rollback on failure.
type Pipeline struct {
	Name     string
	Steps    []Step
	state    *State
	Persist  Persister // optional: persists step completion across restarts
}

// State tracks which steps have completed during pipeline execution.
type State struct {
	Completed map[string]bool
}

// New creates a new Pipeline. If persist is non-nil, previously completed
// steps are restored from storage (enabling crash recovery).
func New(name string, persist Persister) *Pipeline {
	p := &Pipeline{
		Name:    name,
		Persist: persist,
		state: &State{
			Completed: make(map[string]bool),
		},
	}
	if persist != nil {
		steps, err := persist.LoadPipelineSteps(name)
		if err == nil {
			for _, s := range steps {
				p.state.Completed[s] = true
			}
		}
	}
	return p
}

// Add appends a step to the pipeline.
func (p *Pipeline) Add(step Step) *Pipeline {
	if step.Retry < 0 {
		step.Retry = 0
	}
	p.Steps = append(p.Steps, step)
	return p
}

// Run executes all steps in sequence. On failure, completed steps are rolled
// back in reverse order. Returns the first error encountered.
func (p *Pipeline) Run(ctx context.Context) error {
	log.Printf("[pipeline:%s] starting %d steps", p.Name, len(p.Steps))

	for i, step := range p.Steps {
		if p.state.Completed[step.Name] {
			log.Printf("[pipeline:%s] skip %s (already completed)", p.Name, step.Name)
			continue
		}

		if p.Persist != nil {
			p.Persist.StartPipelineStep(p.Name, step.Name)
		}

		if err := p.executeStep(ctx, &step); err != nil {
			log.Printf("[pipeline:%s] FAIL at step %d/%d (%s): %v", p.Name, i+1, len(p.Steps), step.Name, err)
			if p.Persist != nil {
				p.Persist.FailPipelineStep(p.Name, step.Name)
			}
			p.rollback(ctx, i)
			return fmt.Errorf("pipeline %q step %q: %w", p.Name, step.Name, err)
		}

		p.state.Completed[step.Name] = true
		if p.Persist != nil {
			if err := p.Persist.SavePipelineStep(p.Name, step.Name); err != nil {
				log.Printf("[pipeline:%s] persist %s failed: %v", p.Name, step.Name, err)
			}
		}
		log.Printf("[pipeline:%s] step %d/%d (%s) OK", p.Name, i+1, len(p.Steps), step.Name)
	}

	log.Printf("[pipeline:%s] complete", p.Name)
	return nil
}

// RetryFrom re-runs the pipeline starting from the named step.
// Steps before the named step are marked as incomplete and will be re-executed.
func (p *Pipeline) RetryFrom(ctx context.Context, stepName string) error {
	found := false
	for _, s := range p.Steps {
		if s.Name == stepName {
			found = true
		}
		if found {
			delete(p.state.Completed, s.Name)
		}
	}
	if !found {
		return fmt.Errorf("step %q not found in pipeline %q", stepName, p.Name)
	}
	return p.Run(ctx)
}

// Completed returns the names of steps that have completed.
func (p *Pipeline) Completed() []string {
	var names []string
	for _, s := range p.Steps {
		if p.state.Completed[s.Name] {
			names = append(names, s.Name)
		}
	}
	return names
}

// Reset clears all completion state.
func (p *Pipeline) Reset() {
	p.state.Completed = make(map[string]bool)
}

func (p *Pipeline) executeStep(ctx context.Context, step *Step) error {
	if step.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, step.Timeout)
		defer cancel()
	}

	var lastErr error
	attempts := step.Retry + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			log.Printf("[pipeline:%s] retry %s (%d/%d)", p.Name, step.Name, attempt, attempts)
		}
		if err := step.Do(ctx); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("after %d attempts: %w", attempts, lastErr)
}

// rollback runs Undo for completed steps in reverse order.
func (p *Pipeline) rollback(ctx context.Context, failedAt int) {
	log.Printf("[pipeline:%s] rolling back %d completed steps", p.Name, failedAt)
	for i := failedAt - 1; i >= 0; i-- {
		step := &p.Steps[i]
		if step.Undo == nil {
			log.Printf("[pipeline:%s] rollback skip %s (no undo)", p.Name, step.Name)
			continue
		}
		log.Printf("[pipeline:%s] rollback %s", p.Name, step.Name)
		if err := step.Undo(ctx); err != nil {
			log.Printf("[pipeline:%s] rollback %s FAILED: %v", p.Name, step.Name, err)
			// Continue rolling back other steps even if one fails.
		}
		delete(p.state.Completed, step.Name)
		if p.Persist != nil {
			p.Persist.DeletePipeline(p.Name)
		}
	}
}

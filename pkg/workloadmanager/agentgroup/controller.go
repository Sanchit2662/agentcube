/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package agentgroup implements the AgentGroup orchestrator controller — the
// proof-of-concept for AgentCube's multi-agent capability (upstream issue
// volcano-sh/agentcube#301).
//
// The controller watches AgentGroup CRs and drives a fleet of agent sandboxes
// through the lifecycle Pending -> Initializing -> Running, creating one
// agent-sandbox Sandbox per agent replica and reporting fleet readiness on the
// AgentGroup status. Gang scheduling, the shared context store and the
// inter-agent message bus are deliberately out of scope for this PoC and are
// tracked in IMPLEMENTATION_PLAN.md sections 5-8.
package agentgroup

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

// conditionReady is the AgentGroup status condition type the controller maintains.
const conditionReady = "Ready"

// AgentGroupController reconciles AgentGroup objects, managing the lifecycle of
// a multi-agent sandbox fleet. It follows the same controller-runtime patterns
// as workloadmanager.CodeInterpreterReconciler: it embeds client.Client, filters
// events with GenerationChangedPredicate, and writes status idempotently to
// avoid self-triggered reconcile loops.
type AgentGroupController struct {
	client.Client
	Scheme *runtime.Scheme
}

// SetupWithManager registers the controller with the manager. It also watches
// the Sandbox CRs it owns, so fleet readiness changes re-trigger reconciliation.
func (c *AgentGroupController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&runtimev1alpha1.AgentGroup{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&sandboxv1alpha1.Sandbox{}).
		Complete(c)
}

// Reconcile drives one AgentGroup toward its desired state. Fleet teardown is
// handled by Kubernetes garbage collection via the owner references the
// controller stamps on every Sandbox, so no finalizer is needed.
func (c *AgentGroupController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	ag := &runtimev1alpha1.AgentGroup{}
	if err := c.Get(ctx, req.NamespacedName, ag); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch ag.Status.Phase {
	case "", runtimev1alpha1.AgentGroupPending:
		return c.reconcilePending(ctx, ag)
	case runtimev1alpha1.AgentGroupInitializing:
		return c.reconcileInitializing(ctx, ag)
	case runtimev1alpha1.AgentGroupRunning:
		return c.reconcileRunning(ctx, ag)
	case runtimev1alpha1.AgentGroupFailed:
		// Terminal: nothing more to do.
		return ctrl.Result{}, nil
	default:
		logger.Info("unknown AgentGroup phase, resetting to Pending", "phase", ag.Status.Phase)
		ag.Status.Phase = runtimev1alpha1.AgentGroupPending
		return ctrl.Result{Requeue: true}, c.updateStatus(ctx, ag)
	}
}

// reconcilePending validates the spec and admits the group to Initializing.
func (c *AgentGroupController) reconcilePending(ctx context.Context,
	ag *runtimev1alpha1.AgentGroup) (ctrl.Result, error) {
	if err := validateSpec(ag); err != nil {
		return ctrl.Result{}, c.fail(ctx, ag, "InvalidSpec", err.Error())
	}

	ag.Status.TotalAgents = totalAgentCount(ag)
	if ag.Status.StartTime == nil {
		now := metav1.Now()
		ag.Status.StartTime = &now
	}
	ag.Status.Phase = runtimev1alpha1.AgentGroupInitializing
	apimeta.SetStatusCondition(&ag.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             "Accepted",
		Message:            "spec validated, provisioning agent fleet",
		ObservedGeneration: ag.Generation,
	})
	return ctrl.Result{Requeue: true}, c.updateStatus(ctx, ag)
}

// reconcileInitializing ensures every agent sandbox exists and promotes the
// group to Running once the whole fleet reports Ready.
func (c *AgentGroupController) reconcileInitializing(ctx context.Context,
	ag *runtimev1alpha1.AgentGroup) (ctrl.Result, error) {
	if err := c.ensureFleet(ctx, ag); err != nil {
		if reason, msg, fatal := classifyFleetError(err); fatal {
			return ctrl.Result{}, c.fail(ctx, ag, reason, msg)
		}
		return ctrl.Result{}, err // transient: controller-runtime retries with backoff
	}

	agents, ready, err := c.observeFleet(ctx, ag)
	if err != nil {
		return ctrl.Result{}, err
	}
	ag.Status.Agents = agents
	ag.Status.ReadyAgents = ready

	if ready < ag.Status.TotalAgents {
		// Fleet still coming up. The Owns(&Sandbox{}) watch re-triggers us
		// when a sandbox becomes Ready; persist progress in the meantime.
		return ctrl.Result{}, c.updateStatus(ctx, ag)
	}

	ag.Status.Phase = runtimev1alpha1.AgentGroupRunning
	apimeta.SetStatusCondition(&ag.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "FleetReady",
		Message:            fmt.Sprintf("all %d agent sandboxes are ready", ready),
		ObservedGeneration: ag.Generation,
	})
	return ctrl.Result{}, c.updateStatus(ctx, ag)
}

// reconcileRunning monitors fleet health. If a sandbox is lost the group fails
// (PoC failure handling: every FailurePolicy is treated as FailFast).
func (c *AgentGroupController) reconcileRunning(ctx context.Context,
	ag *runtimev1alpha1.AgentGroup) (ctrl.Result, error) {
	agents, ready, err := c.observeFleet(ctx, ag)
	if err != nil {
		return ctrl.Result{}, err
	}
	ag.Status.Agents = agents
	ag.Status.ReadyAgents = ready

	if ready < ag.Status.TotalAgents {
		return ctrl.Result{}, c.fail(ctx, ag, "AgentLost",
			fmt.Sprintf("only %d of %d agent sandboxes are ready", ready, ag.Status.TotalAgents))
	}
	return ctrl.Result{}, c.updateStatus(ctx, ag)
}

// ensureFleet creates any missing agent sandboxes. It is idempotent: sandbox
// names are deterministic, so an already-created sandbox is skipped.
func (c *AgentGroupController) ensureFleet(ctx context.Context, ag *runtimev1alpha1.AgentGroup) error {
	logger := log.FromContext(ctx)

	for _, agent := range ag.Spec.Agents {
		replicas := agent.Replicas
		if replicas == 0 {
			replicas = 1
		}
		podSpec, err := resolveRuntimePodSpec(ctx, c.Client, ag.Namespace, agent.RuntimeRef)
		if err != nil {
			return fmt.Errorf("agent %q: %w", agent.Name, err)
		}
		for r := int32(0); r < replicas; r++ {
			sb := buildSandbox(ag, agent, r, podSpec)
			if err := controllerutil.SetControllerReference(ag, sb, c.Scheme); err != nil {
				return fmt.Errorf("agent %q: set owner reference: %w", agent.Name, err)
			}
			if err := c.Create(ctx, sb); err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue // already provisioned on an earlier reconcile
				}
				return fmt.Errorf("agent %q: create sandbox %s: %w", agent.Name, sb.Name, err)
			}
			logger.Info("created agent sandbox", "agentGroup", ag.Name,
				"agent", agent.Name, "sandbox", sb.Name)
		}
	}
	return nil
}

// observeFleet lists the sandboxes owned by the group and derives per-agent
// status plus the total ready count.
func (c *AgentGroupController) observeFleet(ctx context.Context,
	ag *runtimev1alpha1.AgentGroup) ([]runtimev1alpha1.AgentStatus, int32, error) {
	sandboxes := &sandboxv1alpha1.SandboxList{}
	if err := c.List(ctx, sandboxes,
		client.InNamespace(ag.Namespace),
		client.MatchingLabels{LabelAgentGroup: ag.Name}); err != nil {
		return nil, 0, fmt.Errorf("list agent sandboxes: %w", err)
	}

	// Index sandboxes by agent name.
	byAgent := make(map[string][]sandboxv1alpha1.Sandbox)
	for i := range sandboxes.Items {
		sb := sandboxes.Items[i]
		byAgent[sb.Labels[LabelAgentName]] = append(byAgent[sb.Labels[LabelAgentName]], sb)
	}

	statuses := make([]runtimev1alpha1.AgentStatus, 0, len(ag.Spec.Agents))
	var totalReady int32
	for _, agent := range ag.Spec.Agents {
		want := agent.Replicas
		if want == 0 {
			want = 1
		}
		var names []string
		var ready int32
		for _, sb := range byAgent[agent.Name] {
			names = append(names, sb.Name)
			if sandboxReady(&sb) {
				ready++
			}
		}
		totalReady += ready
		statuses = append(statuses, runtimev1alpha1.AgentStatus{
			Name:         agent.Name,
			Phase:        agentPhase(ready, want),
			SandboxNames: names,
			Message:      fmt.Sprintf("%d/%d sandboxes ready", ready, want),
		})
	}
	return statuses, totalReady, nil
}

// fail moves the group to the terminal Failed phase with a Ready=False condition.
func (c *AgentGroupController) fail(ctx context.Context, ag *runtimev1alpha1.AgentGroup,
	reason, message string) error {
	ag.Status.Phase = runtimev1alpha1.AgentGroupFailed
	apimeta.SetStatusCondition(&ag.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: ag.Generation,
	})
	return c.updateStatus(ctx, ag)
}

// updateStatus persists the AgentGroup status subresource.
func (c *AgentGroupController) updateStatus(ctx context.Context, ag *runtimev1alpha1.AgentGroup) error {
	ag.Status.ObservedGeneration = ag.Generation
	if err := c.Status().Update(ctx, ag); err != nil {
		return fmt.Errorf("update AgentGroup status: %w", err)
	}
	return nil
}

// sandboxReady reports whether a Sandbox has reached its Ready condition.
func sandboxReady(sb *sandboxv1alpha1.Sandbox) bool {
	for _, cond := range sb.Status.Conditions {
		if cond.Type == string(sandboxv1alpha1.SandboxConditionReady) {
			return cond.Status == metav1.ConditionTrue
		}
	}
	return false
}

// agentPhase derives a coarse per-agent phase from its ready/desired counts.
func agentPhase(ready, want int32) string {
	if ready >= want {
		return "Running"
	}
	return "Pending"
}

// totalAgentCount sums Replicas across all agent specs (treating 0 as 1).
func totalAgentCount(ag *runtimev1alpha1.AgentGroup) int32 {
	var total int32
	for _, agent := range ag.Spec.Agents {
		if agent.Replicas == 0 {
			total++
			continue
		}
		total += agent.Replicas
	}
	return total
}

// validateSpec rejects structurally invalid groups before any sandbox is created.
func validateSpec(ag *runtimev1alpha1.AgentGroup) error {
	if len(ag.Spec.Agents) == 0 {
		return fmt.Errorf("at least one agent is required")
	}
	seen := make(map[string]struct{}, len(ag.Spec.Agents))
	for _, agent := range ag.Spec.Agents {
		if agent.Name == "" {
			return fmt.Errorf("agent name must not be empty")
		}
		if _, dup := seen[agent.Name]; dup {
			return fmt.Errorf("duplicate agent name %q", agent.Name)
		}
		seen[agent.Name] = struct{}{}
		if agent.RuntimeRef.Name == "" {
			return fmt.Errorf("agent %q: runtimeRef.name must not be empty", agent.Name)
		}
	}
	return nil
}

// classifyFleetError decides whether an ensureFleet error is fatal (a bad spec
// reference that will never succeed) or transient (retry with backoff).
func classifyFleetError(err error) (reason, message string, fatal bool) {
	if apierrors.IsNotFound(err) {
		return "RuntimeNotFound", err.Error(), true
	}
	return "", err.Error(), false
}

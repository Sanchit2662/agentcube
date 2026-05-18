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

package agentgroup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

// newScheme builds a scheme with the AgentCube and agent-sandbox types the
// controller needs.
func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, runtimev1alpha1.AddToScheme(s))
	require.NoError(t, sandboxv1alpha1.AddToScheme(s))
	return s
}

// mkCodeInterpreter returns a minimal CodeInterpreter the agents can reference.
func mkCodeInterpreter(name string) *runtimev1alpha1.CodeInterpreter {
	return &runtimev1alpha1.CodeInterpreter{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: runtimev1alpha1.CodeInterpreterSpec{
			Template: &runtimev1alpha1.CodeInterpreterSandboxTemplate{
				Image: "ghcr.io/volcano-sh/picod:latest",
			},
		},
	}
}

// mkGroup builds an AgentGroup with one single-replica agent per name, each
// referencing the given CodeInterpreter.
func mkGroup(name, ciRef string, agents ...string) *runtimev1alpha1.AgentGroup {
	specs := make([]runtimev1alpha1.AgentSpec, len(agents))
	for i, a := range agents {
		specs[i] = runtimev1alpha1.AgentSpec{
			Name:     a,
			Replicas: 1,
			RuntimeRef: runtimev1alpha1.RuntimeReference{
				Kind: runtimeKindCodeInterpreter,
				Name: ciRef,
			},
		}
	}
	return &runtimev1alpha1.AgentGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: runtimev1alpha1.AgentGroupSpec{
			Topology:      runtimev1alpha1.TopologyHierarchical,
			FailurePolicy: runtimev1alpha1.FailFast,
			Agents:        specs,
		},
	}
}

// mkReadySandbox builds a Sandbox carrying the group/agent labels and a true
// Ready condition, as if the agent-sandbox controller had already started it.
func mkReadySandbox(ag *runtimev1alpha1.AgentGroup, agent string, replica int32) *sandboxv1alpha1.Sandbox {
	return &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName(ag, agent, replica),
			Namespace: ag.Namespace,
			Labels: map[string]string{
				LabelAgentGroup: ag.Name,
				LabelAgentName:  agent,
			},
		},
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:               string(sandboxv1alpha1.SandboxConditionReady),
				Status:             metav1.ConditionTrue,
				Reason:             "Ready",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
}

// drive runs Reconcile to a fixed point (bounded) and returns the final group.
func drive(t *testing.T, c *AgentGroupController, cl client.Client,
	ag *runtimev1alpha1.AgentGroup) *runtimev1alpha1.AgentGroup {
	t.Helper()
	key := client.ObjectKeyFromObject(ag)
	req := ctrl.Request{NamespacedName: key}
	for i := 0; i < 8; i++ {
		if _, err := c.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("reconcile iteration %d failed: %v", i, err)
		}
	}
	got := &runtimev1alpha1.AgentGroup{}
	require.NoError(t, cl.Get(context.Background(), key, got))
	return got
}

func TestAgentGroupReconcile(t *testing.T) {
	tests := []struct {
		name string
		// seedRuntime controls whether the referenced CodeInterpreter exists.
		seedRuntime bool
		// group is the AgentGroup under test.
		group *runtimev1alpha1.AgentGroup
		// readySandboxes lists (agent,replica) sandboxes pre-created as Ready.
		readySandboxes []struct {
			agent   string
			replica int32
		}
		wantPhase      runtimev1alpha1.AgentGroupPhase
		wantReady      int32
		wantTotal      int32
		wantConditionR metav1.ConditionStatus
	}{
		{
			name:           "new group provisions fleet and waits in Initializing",
			seedRuntime:    true,
			group:          mkGroup("g-init", "ci", "planner", "executor"),
			wantPhase:      runtimev1alpha1.AgentGroupInitializing,
			wantReady:      0,
			wantTotal:      2,
			wantConditionR: metav1.ConditionFalse,
		},
		{
			name:        "all sandboxes ready promotes group to Running",
			seedRuntime: true,
			group:       mkGroup("g-run", "ci", "planner", "executor"),
			readySandboxes: []struct {
				agent   string
				replica int32
			}{{"planner", 0}, {"executor", 0}},
			wantPhase:      runtimev1alpha1.AgentGroupRunning,
			wantReady:      2,
			wantTotal:      2,
			wantConditionR: metav1.ConditionTrue,
		},
		{
			name:        "partially ready fleet stays in Initializing",
			seedRuntime: true,
			group:       mkGroup("g-partial", "ci", "planner", "executor"),
			readySandboxes: []struct {
				agent   string
				replica int32
			}{{"planner", 0}},
			wantPhase:      runtimev1alpha1.AgentGroupInitializing,
			wantReady:      1,
			wantTotal:      2,
			wantConditionR: metav1.ConditionFalse,
		},
		{
			name:           "duplicate agent name is rejected as Failed",
			seedRuntime:    true,
			group:          mkGroup("g-dup", "ci", "planner", "planner"),
			wantPhase:      runtimev1alpha1.AgentGroupFailed,
			wantConditionR: metav1.ConditionFalse,
		},
		{
			name:           "missing runtime reference fails the group",
			seedRuntime:    false,
			group:          mkGroup("g-missing", "ci-absent", "planner"),
			wantPhase:      runtimev1alpha1.AgentGroupFailed,
			wantTotal:      1,
			wantConditionR: metav1.ConditionFalse,
		},
		{
			name:        "Peer topology is rejected as Failed",
			seedRuntime: true,
			group: func() *runtimev1alpha1.AgentGroup {
				g := mkGroup("g-peer", "ci", "planner", "executor")
				g.Spec.Topology = runtimev1alpha1.TopologyPeer
				return g
			}(),
			wantPhase:      runtimev1alpha1.AgentGroupFailed,
			wantConditionR: metav1.ConditionFalse,
		},
		{
			name:        "dependency on an unknown agent fails the group",
			seedRuntime: true,
			group: func() *runtimev1alpha1.AgentGroup {
				g := mkGroup("g-baddep", "ci", "planner", "executor")
				g.Spec.Dependencies = []runtimev1alpha1.AgentDependency{
					{From: "planner", To: "ghost"},
				}
				return g
			}(),
			wantPhase:      runtimev1alpha1.AgentGroupFailed,
			wantConditionR: metav1.ConditionFalse,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newScheme(t)
			objs := []client.Object{tc.group}
			if tc.seedRuntime {
				objs = append(objs, mkCodeInterpreter("ci"))
			}
			for _, rs := range tc.readySandboxes {
				objs = append(objs, mkReadySandbox(tc.group, rs.agent, rs.replica))
			}

			cl := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				WithStatusSubresource(&runtimev1alpha1.AgentGroup{}).
				Build()
			c := &AgentGroupController{Client: cl, Scheme: scheme}

			got := drive(t, c, cl, tc.group)

			assert.Equal(t, tc.wantPhase, got.Status.Phase, "phase")
			assert.Equal(t, tc.wantReady, got.Status.ReadyAgents, "readyAgents")
			if tc.wantTotal > 0 {
				assert.Equal(t, tc.wantTotal, got.Status.TotalAgents, "totalAgents")
			}
			cond := findCondition(got.Status.Conditions, conditionReady)
			require.NotNil(t, cond, "Ready condition must be set")
			assert.Equal(t, tc.wantConditionR, cond.Status, "Ready condition status")
		})
	}
}

// TestAgentGroupRunningDetectsAgentLoss verifies that, once Running, losing a
// sandbox transitions the group to Failed under the PoC's FailFast handling.
func TestAgentGroupRunningDetectsAgentLoss(t *testing.T) {
	scheme := newScheme(t)
	ag := mkGroup("g-loss", "ci", "planner", "executor")
	ag.Status.Phase = runtimev1alpha1.AgentGroupRunning
	ag.Status.TotalAgents = 2

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ag, mkCodeInterpreter("ci"), mkReadySandbox(ag, "planner", 0)).
		WithStatusSubresource(&runtimev1alpha1.AgentGroup{}).
		Build()
	c := &AgentGroupController{Client: cl, Scheme: scheme}

	_, err := c.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(ag),
	})
	require.NoError(t, err)

	got := &runtimev1alpha1.AgentGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(ag), got))
	assert.Equal(t, runtimev1alpha1.AgentGroupFailed, got.Status.Phase)
	cond := findCondition(got.Status.Conditions, conditionReady)
	require.NotNil(t, cond)
	assert.Equal(t, "AgentLost", cond.Reason)
}

// TestAgentGroupCreatesOwnedSandboxes verifies the controller provisions one
// Sandbox per agent replica, each owned by the AgentGroup so deletion cascades.
func TestAgentGroupCreatesOwnedSandboxes(t *testing.T) {
	scheme := newScheme(t)
	ag := mkGroup("g-own", "ci", "planner")
	ag.Spec.Agents[0].Replicas = 3

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ag, mkCodeInterpreter("ci")).
		WithStatusSubresource(&runtimev1alpha1.AgentGroup{}).
		Build()
	c := &AgentGroupController{Client: cl, Scheme: scheme}

	drive(t, c, cl, ag)

	sandboxes := &sandboxv1alpha1.SandboxList{}
	require.NoError(t, cl.List(context.Background(), sandboxes,
		client.MatchingLabels{LabelAgentGroup: "g-own"}))
	require.Len(t, sandboxes.Items, 3, "one sandbox per replica")
	for _, sb := range sandboxes.Items {
		require.Len(t, sb.OwnerReferences, 1, "sandbox must be owned by the AgentGroup")
		assert.Equal(t, "AgentGroup", sb.OwnerReferences[0].Kind)
		assert.Equal(t, "g-own", sb.OwnerReferences[0].Name)
		assert.Equal(t, "agent", sb.Spec.PodTemplate.Spec.Containers[0].Name)
	}
}

// TestAgentGroupRejectsPeerTopology verifies that a Peer-topology group fails
// with the UnsupportedTopology reason rather than being silently accepted.
func TestAgentGroupRejectsPeerTopology(t *testing.T) {
	scheme := newScheme(t)
	ag := mkGroup("g-peer-reason", "ci", "planner")
	ag.Spec.Topology = runtimev1alpha1.TopologyPeer

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ag, mkCodeInterpreter("ci")).
		WithStatusSubresource(&runtimev1alpha1.AgentGroup{}).
		Build()
	c := &AgentGroupController{Client: cl, Scheme: scheme}

	got := drive(t, c, cl, ag)
	assert.Equal(t, runtimev1alpha1.AgentGroupFailed, got.Status.Phase)
	cond := findCondition(got.Status.Conditions, conditionReady)
	require.NotNil(t, cond)
	assert.Equal(t, "UnsupportedTopology", cond.Reason)
}

func TestValidateSpec(t *testing.T) {
	tests := []struct {
		name    string
		group   *runtimev1alpha1.AgentGroup
		wantErr bool
	}{
		{"valid", mkGroup("g", "ci", "a", "b"), false},
		{"no agents", mkGroup("g", "ci"), true},
		{"duplicate names", mkGroup("g", "ci", "a", "a"), true},
		{
			name: "empty runtimeRef name",
			group: func() *runtimev1alpha1.AgentGroup {
				g := mkGroup("g", "ci", "a")
				g.Spec.Agents[0].RuntimeRef.Name = ""
				return g
			}(),
			wantErr: true,
		},
		{
			name: "valid dependency edge",
			group: func() *runtimev1alpha1.AgentGroup {
				g := mkGroup("g", "ci", "a", "b")
				g.Spec.Dependencies = []runtimev1alpha1.AgentDependency{{From: "a", To: "b"}}
				return g
			}(),
			wantErr: false,
		},
		{
			name: "dependency to unknown agent",
			group: func() *runtimev1alpha1.AgentGroup {
				g := mkGroup("g", "ci", "a", "b")
				g.Spec.Dependencies = []runtimev1alpha1.AgentDependency{{From: "a", To: "ghost"}}
				return g
			}(),
			wantErr: true,
		},
		{
			name: "dependency from an agent to itself",
			group: func() *runtimev1alpha1.AgentGroup {
				g := mkGroup("g", "ci", "a", "b")
				g.Spec.Dependencies = []runtimev1alpha1.AgentDependency{{From: "a", To: "a"}}
				return g
			}(),
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSpec(tc.group)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestTotalAgentCount(t *testing.T) {
	ag := mkGroup("g", "ci", "a", "b", "c")
	ag.Spec.Agents[0].Replicas = 2
	ag.Spec.Agents[1].Replicas = 0 // treated as 1
	ag.Spec.Agents[2].Replicas = 3
	assert.Equal(t, int32(6), totalAgentCount(ag))
}

func TestSandboxReady(t *testing.T) {
	ready := &sandboxv1alpha1.Sandbox{
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1alpha1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			}},
		},
	}
	notReady := &sandboxv1alpha1.Sandbox{
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1alpha1.SandboxConditionReady),
				Status: metav1.ConditionFalse,
			}},
		},
	}
	assert.True(t, sandboxReady(ready))
	assert.False(t, sandboxReady(notReady))
	assert.False(t, sandboxReady(&sandboxv1alpha1.Sandbox{}), "no conditions means not ready")
}

func TestPodSpecFromCodeInterpreter(t *testing.T) {
	empty := ""
	tmpl := &runtimev1alpha1.CodeInterpreterSandboxTemplate{
		Image:            "img:1",
		Command:          []string{"/bin/agent"},
		RuntimeClassName: &empty, // empty string must normalize to nil
		Environment:      []corev1.EnvVar{{Name: "K", Value: "V"}},
	}
	spec := podSpecFromCodeInterpreter(tmpl)
	require.Len(t, spec.Containers, 1)
	assert.Equal(t, "img:1", spec.Containers[0].Image)
	assert.Nil(t, spec.RuntimeClassName, "empty runtimeClassName should normalize to nil")
	assert.Equal(t, "V", spec.Containers[0].Env[0].Value)
}

// findCondition returns the condition of the given type, or nil.
func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

const (
	// LabelAgentGroup marks every Sandbox owned by an AgentGroup. It is the
	// selector the controller uses to discover the fleet it created.
	LabelAgentGroup = "runtime.agentcube.io/agent-group"
	// LabelAgentName identifies which agent within the group a Sandbox backs.
	LabelAgentName = "runtime.agentcube.io/agent-name"

	// runtimeKindCodeInterpreter / runtimeKindAgentRuntime are the accepted
	// RuntimeReference.Kind values.
	runtimeKindCodeInterpreter = "CodeInterpreter"
	runtimeKindAgentRuntime    = "AgentRuntime"
)

// sandboxName returns the deterministic Sandbox name for one agent replica.
// It is stable across reconciles so the controller can detect which sandboxes
// it has already created (idempotent fleet provisioning).
func sandboxName(ag *runtimev1alpha1.AgentGroup, agent string, replica int32) string {
	return fmt.Sprintf("%s-%s-%d", ag.Name, agent, replica)
}

// resolveRuntimePodSpec reads the runtime CR referenced by an agent and
// returns the corev1.PodSpec its sandboxes should run. It supports both
// CodeInterpreter and AgentRuntime so a group can mix runtime kinds.
func resolveRuntimePodSpec(ctx context.Context, c client.Client, namespace string,
	ref runtimev1alpha1.RuntimeReference) (corev1.PodSpec, error) {
	key := client.ObjectKey{Namespace: namespace, Name: ref.Name}

	switch ref.Kind {
	case runtimeKindCodeInterpreter:
		ci := &runtimev1alpha1.CodeInterpreter{}
		if err := c.Get(ctx, key, ci); err != nil {
			return corev1.PodSpec{}, fmt.Errorf("get CodeInterpreter %s: %w", ref.Name, err)
		}
		if ci.Spec.Template == nil {
			return corev1.PodSpec{}, fmt.Errorf("CodeInterpreter %s has no template", ref.Name)
		}
		return podSpecFromCodeInterpreter(ci.Spec.Template), nil

	case runtimeKindAgentRuntime:
		ar := &runtimev1alpha1.AgentRuntime{}
		if err := c.Get(ctx, key, ar); err != nil {
			return corev1.PodSpec{}, fmt.Errorf("get AgentRuntime %s: %w", ref.Name, err)
		}
		if ar.Spec.Template == nil {
			return corev1.PodSpec{}, fmt.Errorf("AgentRuntime %s has no template", ref.Name)
		}
		return *ar.Spec.Template.Spec.DeepCopy(), nil

	default:
		return corev1.PodSpec{}, fmt.Errorf("unsupported runtime kind %q", ref.Kind)
	}
}

// podSpecFromCodeInterpreter mirrors workloadmanager.buildSandboxByCodeInterpreter:
// it turns a CodeInterpreter template into a single-container PodSpec. The PoC
// uses the cold-path (no warm pool / SandboxClaim) so the logic stays small.
func podSpecFromCodeInterpreter(t *runtimev1alpha1.CodeInterpreterSandboxTemplate) corev1.PodSpec {
	runtimeClassName := t.RuntimeClassName
	if runtimeClassName != nil && *runtimeClassName == "" {
		runtimeClassName = nil
	}
	env := make([]corev1.EnvVar, len(t.Environment))
	copy(env, t.Environment)

	return corev1.PodSpec{
		ImagePullSecrets: t.ImagePullSecrets,
		RuntimeClassName: runtimeClassName,
		Containers: []corev1.Container{
			{
				Name:            "agent",
				Image:           t.Image,
				ImagePullPolicy: t.ImagePullPolicy,
				Command:         t.Command,
				Args:            t.Args,
				Env:             env,
				Resources:       t.Resources,
			},
		},
	}
}

// buildSandbox constructs the Sandbox CR for one agent replica. Every sandbox
// is labelled with the group and agent so the controller can list its fleet,
// and carries the AgentGroup owner reference so Kubernetes garbage-collects
// the whole fleet when the group is deleted (PoC teardown strategy).
func buildSandbox(ag *runtimev1alpha1.AgentGroup, agent runtimev1alpha1.AgentSpec,
	replica int32, podSpec corev1.PodSpec) *sandboxv1alpha1.Sandbox {
	labels := map[string]string{
		LabelAgentGroup: ag.Name,
		LabelAgentName:  agent.Name,
	}
	return &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName(ag, agent.Name, replica),
			Namespace: ag.Namespace,
			Labels:    labels,
		},
		Spec: sandboxv1alpha1.SandboxSpec{
			PodTemplate: sandboxv1alpha1.PodTemplate{
				Spec:       podSpec,
				ObjectMeta: sandboxv1alpha1.PodMetadata{Labels: labels},
			},
			Replicas: ptr.To[int32](1),
		},
	}
}

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TopologyType describes how agents in an AgentGroup relate to one another.
type TopologyType string

const (
	// TopologyHierarchical: one orchestrator agent plus worker agents that
	// collaborate along the dependency graph. This is the only topology
	// implemented in the proof-of-concept; Peer is reserved for a later phase.
	TopologyHierarchical TopologyType = "Hierarchical"
	// TopologyPeer: all agents are equal, with no dependency edges.
	TopologyPeer TopologyType = "Peer"
)

// FailurePolicyType controls what happens when one agent's sandbox fails.
type FailurePolicyType string

const (
	// FailFast tears down the whole group on the first agent failure.
	FailFast FailurePolicyType = "FailFast"
	// RetrySubAgent recreates the failed agent's sandbox (not in the PoC).
	RetrySubAgent FailurePolicyType = "RetrySubAgent"
	// ContinueDegraded keeps the group running with surviving agents (not in the PoC).
	ContinueDegraded FailurePolicyType = "ContinueDegraded"
)

// AgentGroupPhase is the high-level lifecycle phase of an AgentGroup.
type AgentGroupPhase string

const (
	// AgentGroupPending means the group has been accepted but not yet acted on.
	AgentGroupPending AgentGroupPhase = "Pending"
	// AgentGroupInitializing means the agent sandbox fleet is being created.
	AgentGroupInitializing AgentGroupPhase = "Initializing"
	// AgentGroupRunning means every agent sandbox is ready.
	AgentGroupRunning AgentGroupPhase = "Running"
	// AgentGroupFailed means the group could not be brought up or an agent failed.
	AgentGroupFailed AgentGroupPhase = "Failed"
)

// RuntimeReference references an existing AgentCube runtime CR that supplies
// an agent's sandbox template.
type RuntimeReference struct {
	// Kind is the referenced runtime kind.
	// +kubebuilder:validation:Enum=CodeInterpreter;AgentRuntime
	Kind string `json:"kind"`
	// Name of the referenced CR in the AgentGroup's namespace.
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// AgentDependency is a directed edge in the agent graph: the From agent must
// reach Running before the To agent starts. It is only meaningful for the
// Hierarchical topology. The proof-of-concept controller validates these edges
// but brings the whole fleet up together; ordered startup is later work.
type AgentDependency struct {
	// From is the AgentSpec.Name of the upstream agent.
	// +kubebuilder:validation:Required
	From string `json:"from"`
	// To is the AgentSpec.Name of the downstream agent.
	// +kubebuilder:validation:Required
	To string `json:"to"`
}

// AgentSpec declares one member of the agent fleet.
type AgentSpec struct {
	// Name uniquely identifies this agent within the AgentGroup.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Role is a human-readable role label (e.g. "planner", "executor").
	// +optional
	Role string `json:"role,omitempty"`

	// RuntimeRef points at the CodeInterpreter or AgentRuntime CR that
	// defines this agent's sandbox.
	// +kubebuilder:validation:Required
	RuntimeRef RuntimeReference `json:"runtimeRef"`

	// Replicas is the number of identical sandboxes for this agent.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`
}

// AgentGroupSpec is the desired state of an AgentGroup.
type AgentGroupSpec struct {
	// Topology selects how agents collaborate. The PoC implements Hierarchical.
	// +kubebuilder:default=Hierarchical
	// +kubebuilder:validation:Enum=Hierarchical;Peer
	Topology TopologyType `json:"topology"`

	// Agents is the fleet definition. At least one agent is required.
	// +kubebuilder:validation:MinItems=1
	Agents []AgentSpec `json:"agents"`

	// Dependencies are directed edges between agents for the Hierarchical
	// topology: the From agent must reach Running before the To agent starts.
	// The proof-of-concept controller validates these edges but brings the
	// whole fleet up together; ordered startup is later work.
	// +optional
	Dependencies []AgentDependency `json:"dependencies,omitempty"`

	// FailurePolicy controls per-agent failure handling. The PoC implements
	// FailFast; the other values are accepted but treated as FailFast.
	// +kubebuilder:default=FailFast
	// +kubebuilder:validation:Enum=FailFast;RetrySubAgent;ContinueDegraded
	FailurePolicy FailurePolicyType `json:"failurePolicy"`
}

// AgentStatus is the observed state of a single agent in the fleet.
type AgentStatus struct {
	// Name matches the corresponding AgentSpec.Name.
	Name string `json:"name"`
	// Phase is "Pending", "Running" or "Failed".
	Phase string `json:"phase"`
	// SandboxNames lists the Sandbox CRs backing this agent.
	// +optional
	SandboxNames []string `json:"sandboxNames,omitempty"`
	// Message carries the last status or failure detail for this agent.
	// +optional
	Message string `json:"message,omitempty"`
}

// AgentGroupStatus is the observed state of an AgentGroup.
type AgentGroupStatus struct {
	// Phase is the group-level lifecycle phase.
	// +optional
	Phase AgentGroupPhase `json:"phase,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Agents is the per-agent observed state.
	// +optional
	Agents []AgentStatus `json:"agents,omitempty"`

	// ReadyAgents counts agent sandboxes that have reached the Ready state.
	// +optional
	ReadyAgents int32 `json:"readyAgents,omitempty"`

	// TotalAgents is the expected sandbox count summed over all replicas.
	// +optional
	TotalAgents int32 `json:"totalAgents,omitempty"`

	// StartTime is when the group entered the Initializing phase.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Conditions follows the standard metav1.Condition convention.
	// The PoC sets the "Ready" condition type.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// AgentGroup declares a fleet of AgentCube sandboxes that collaborate on a
// single logical task under unified lifecycle management.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=ag
// +kubebuilder:printcolumn:name="Topology",type="string",JSONPath=".spec.topology"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.readyAgents"
// +kubebuilder:printcolumn:name="Total",type="string",JSONPath=".status.totalAgents"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AgentGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of the AgentGroup.
	Spec AgentGroupSpec `json:"spec"`
	// Status represents the observed state of the AgentGroup.
	Status AgentGroupStatus `json:"status,omitempty"`
}

// AgentGroupList contains a list of AgentGroup.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type AgentGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AgentGroup{}, &AgentGroupList{})
}

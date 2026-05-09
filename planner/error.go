package planner

import "fmt"

// PlannerErrorKind identifies the category of planner error.
type PlannerErrorKind int

const (
	// ErrInvalidInput — invalid input provided to the planner.
	ErrInvalidInput PlannerErrorKind = iota
	// ErrInternal — internal planner logic error, indicates a bug.
	ErrInternal
	// ErrNotFound — requested resource not found.
	ErrNotFound
	// ErrRegistry — error from the capability registry.
	ErrRegistry
	// ErrExecution — error during plan execution.
	ErrExecution
	// ErrInvalidPath — invalid capability chain path.
	ErrInvalidPath
)

// String returns the kind name for error messages.
func (k PlannerErrorKind) String() string {
	switch k {
	case ErrInvalidInput:
		return "InvalidInput"
	case ErrInternal:
		return "Internal"
	case ErrNotFound:
		return "NotFound"
	case ErrRegistry:
		return "Registry"
	case ErrExecution:
		return "Execution"
	case ErrInvalidPath:
		return "InvalidPath"
	default:
		return "Unknown"
	}
}

// PlannerError is the error type for all planner operations.
type PlannerError struct {
	Kind    PlannerErrorKind
	Message string
}

func (e *PlannerError) Error() string {
	return fmt.Sprintf("%s: %s", e.Kind.String(), e.Message)
}

// NewInvalidInputError creates an InvalidInput planner error.
func NewInvalidInputError(msg string) *PlannerError {
	return &PlannerError{Kind: ErrInvalidInput, Message: msg}
}

// NewInternalError creates an Internal planner error.
func NewInternalError(msg string) *PlannerError {
	return &PlannerError{Kind: ErrInternal, Message: msg}
}

// NewNotFoundError creates a NotFound planner error.
func NewNotFoundError(msg string) *PlannerError {
	return &PlannerError{Kind: ErrNotFound, Message: msg}
}

// NewFabricRegistryError creates a Registry planner error.
func NewFabricRegistryError(msg string) *PlannerError {
	return &PlannerError{Kind: ErrRegistry, Message: msg}
}

// NewExecutionError creates an Execution planner error.
func NewExecutionError(msg string) *PlannerError {
	return &PlannerError{Kind: ErrExecution, Message: msg}
}

// NewInvalidPathError creates an InvalidPath planner error.
func NewInvalidPathError(msg string) *PlannerError {
	return &PlannerError{Kind: ErrInvalidPath, Message: msg}
}

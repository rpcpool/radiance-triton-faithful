package indexcidtooffset

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
)

// ErrNotFound is used to signal when a Node could not be found. The specific
// meaning will depend on the DAGService implementation, which may be trying
// to read nodes locally but also, trying to find them remotely.
//
// The Cid field can be filled in to provide additional context.
type ErrNotFound struct {
	Cid cid.Cid
}

// Error implements the error interface and returns a human-readable
// message for this error.
func (e ErrNotFound) Error() string {
	return "ipld: could not find " + fmt.Sprint(e.Cid.String())
}

// Is allows to check whether any error is of this ErrNotFound type.
// Do not use this directly, but rather errors.Is(yourError, ErrNotFound).
func (e ErrNotFound) Is(err error) bool {
	switch err.(type) {
	case ErrNotFound:
		return true
	default:
		return false
	}
}

// NotFound returns true.
func (e ErrNotFound) NotFound() bool {
	return true
}

// IsNotFound returns if the given error is or wraps an ErrNotFound
// (equivalent to errors.Is(err, ErrNotFound{}))
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound{})
}

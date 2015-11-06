// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"errors"
	"fmt"

	cp "github.com/soundcloud/cotterpin"
)

// Errors.
var (
	ErrConflict        = errors.New("object already exists")
	ErrInsClaimed      = errors.New("instance is already claimed")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrInvalidFile     = errors.New("invalid file")
	ErrInvalidKey      = errors.New("invalid key")
	ErrInvalidPort     = errors.New("invalid port")
	ErrInvalidShare    = errors.New("invalid share")
	ErrInvalidState    = errors.New("invalid state")
	ErrBadProcName     = errors.New("invalid proc type name: only alphanumeric chars allowed")
	ErrUnauthorized    = errors.New("operation is not permitted")
	ErrNotFound        = errors.New("object not found")
	ErrTagShadowing    = errors.New("revision already exists with tag name")
)

// Error is the wrapper type to express custom errors.
type Error struct {
	Err     error
	Message string
}

// NewError wraps the given error with a custom message.
func NewError(err error, msg string) *Error {
	return &Error{err, msg}
}

func (e *Error) Error() string {
	return e.Message
}

func unwrapErr(err error) error {
	switch e := err.(type) {
	case *cp.Error:
		return e.Err
	case *Error:
		return e.Err
	}

	return err
}

// IsErrConflict is a helper to test for ErrConflict.
func IsErrConflict(err error) bool {
	return unwrapErr(err) == ErrConflict
}

// IsErrUnauthorized is a helper to test for ErrUnauthorized.
func IsErrUnauthorized(err error) bool {
	return unwrapErr(err) == ErrUnauthorized
}

// IsErrNotFound is a helper to test for ErrNotFound.
func IsErrNotFound(err error) bool {
	err = unwrapErr(err)

	return err == cp.ErrNoEnt || err == ErrNotFound
}

// IsErrInsClaimed is a helper to test for ErrInsClaimed.
func IsErrInsClaimed(err error) bool {
	return unwrapErr(err) == ErrInsClaimed
}

// IsErrInvalidArgument is a helper to test for ErrInvalidArgument.
func IsErrInvalidArgument(err error) bool {
	return unwrapErr(err) == ErrInvalidArgument
}

// IsErrInvalidFile is a helper to test for ErrInvalidFile.
func IsErrInvalidFile(err error) bool {
	return unwrapErr(err) == ErrInvalidFile
}

// IsErrInvalidKey is a helper to test for ErrInvalidKey.
func IsErrInvalidKey(err error) bool {
	return unwrapErr(err) == ErrInvalidKey
}

// IsErrInvalidPort is a helper to test for ErrInvalidPort.
func IsErrInvalidPort(err error) bool {
	return unwrapErr(err) == ErrInvalidPort
}

// IsErrInvalidShare is a helper to test for ErrInvalidShare.
func IsErrInvalidShare(err error) bool {
	return unwrapErr(err) == ErrInvalidShare
}

// IsErrInvalidState is a helper to test for ErrInvalidState.
func IsErrInvalidState(err error) bool {
	return unwrapErr(err) == ErrInvalidState
}

// IsErrTagShadowing is a helper to test for ErrTagShadowing.
func IsErrTagShadowing(err error) bool {
	return unwrapErr(err) == ErrTagShadowing
}

func errorf(err error, format string, args ...interface{}) *Error {
	return NewError(err, fmt.Sprintf(format, args...))
}

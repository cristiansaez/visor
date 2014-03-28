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

var (
	ErrConflict        = errors.New("object already exists")
	ErrInsClaimed      = errors.New("instance is already claimed")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrInvalidKey      = errors.New("invalid key")
	ErrInvalidState    = errors.New("invalid state")
	ErrInvalidFile     = errors.New("invalid file")
	ErrBadProcName     = errors.New("invalid proc type name: only alphanumeric chars allowed")
	ErrUnauthorized    = errors.New("operation is not permitted")
	ErrNotFound        = errors.New("object not found")
)

type Error struct {
	Err     error
	Message string
}

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

func IsErrConflict(err error) bool {
	return unwrapErr(err) == ErrConflict
}

func IsErrUnauthorized(err error) bool {
	return unwrapErr(err) == ErrUnauthorized
}

func IsErrNotFound(err error) bool {
	err = unwrapErr(err)

	return err == cp.ErrNoEnt || err == ErrNotFound
}

func IsErrInsClaimed(err error) bool {
	return unwrapErr(err) == ErrInsClaimed
}

func IsErrInvalidState(err error) bool {
	return err == ErrInvalidState
}

func IsErrInvalidFile(err error) bool {
	return unwrapErr(err) == ErrInvalidFile
}

func IsErrInvalidArgument(err error) bool {
	return unwrapErr(err) == ErrInvalidArgument
}

func IsErrInvalidKey(err error) bool {
	return unwrapErr(err) == ErrInvalidKey
}

func errorf(err error, format string, args ...interface{}) *Error {
	return NewError(err, fmt.Sprintf(format, args...))
}

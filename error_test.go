package visor

import (
	"errors"
	"testing"

	cp "github.com/soundcloud/cotterpin"
)

type errorCase struct {
	err      error
	expected bool
}

func testErrFn(t *testing.T, fn func(error) bool, cases []errorCase) {
	for i, tt := range cases {
		if got := fn(tt.err); got != tt.expected {
			t.Errorf("%d. expected %t, got %t", i, tt.expected, got)
		}
	}
}

func TestIsErrConflict(t *testing.T) {
	testErrFn(t, IsErrConflict, []errorCase{
		{nil, false},
		{errors.New("error"), false},
		{cp.NewError(cp.ErrBadPath, "bad path"), false},
		{NewError(ErrConflict, "conflict"), true},
	})
}

func TestIsErrUnauthorized(t *testing.T) {
	testErrFn(t, IsErrUnauthorized, []errorCase{
		{nil, false},
		{errors.New("error"), false},
		{cp.NewError(cp.ErrBadPath, "bad path"), false},
		{NewError(ErrUnauthorized, "unauthorized"), true},
	})
}

func TestIsErrNotFound(t *testing.T) {
	testErrFn(t, IsErrNotFound, []errorCase{
		{nil, false},
		{errors.New("error"), false},
		{cp.NewError(cp.ErrBadPath, "bad path"), false},
		{cp.NewError(cp.ErrNoEnt, "not found"), true},
		{NewError(ErrNotFound, "not found"), true},
	})
}

func TestIsErrInsClaimed(t *testing.T) {
	testErrFn(t, IsErrInsClaimed, []errorCase{
		{nil, false},
		{errors.New("error"), false},
		{cp.NewError(cp.ErrBadPath, "bad path"), false},
		{NewError(ErrInsClaimed, "claimed"), true},
	})
}

func TestIsErrInvalidState(t *testing.T) {
	testErrFn(t, IsErrInvalidState, []errorCase{
		{nil, false},
		{errors.New("error"), false},
		{cp.NewError(cp.ErrBadPath, "bad path"), false},
		{ErrInvalidState, true},
		{NewError(ErrInvalidState, "invalid state"), true},
	})
}

func TestIsErrInvalidFile(t *testing.T) {
	testErrFn(t, IsErrInvalidFile, []errorCase{
		{nil, false},
		{errors.New("error"), false},
		{cp.NewError(cp.ErrBadPath, "bad path"), false},
		{NewError(ErrInvalidFile, "invalid file"), true},
	})
}

func TestIsErrInvalidArgument(t *testing.T) {
	testErrFn(t, IsErrInvalidArgument, []errorCase{
		{nil, false},
		{errors.New("error"), false},
		{cp.NewError(cp.ErrBadPath, "bad path"), false},
		{NewError(ErrInvalidArgument, "invalid argument"), true},
	})
}

func TestIsErrInvalidKey(t *testing.T) {
	testErrFn(t, IsErrInvalidKey, []errorCase{
		{nil, false},
		{errors.New("error"), false},
		{cp.NewError(cp.ErrBadPath, "bad path"), false},
		{NewError(ErrInvalidKey, "invalid key"), true},
	})
}

package store

import (
	"errors"
)

const (
	DefaultMaxKeySize   = 1 << 10
	DefaultMaxValueSize = 1 << 20
)

var (
	ErrEmptyKey      = errors.New("store: invalid key")
	ErrKeyTooLarge   = errors.New("store: key length higher than the defined limit")
	ErrValueTooLarge = errors.New("store: value length higher than the defined limit")
)

type Store interface {
	Get(key string) (value []byte, found bool)
	Set(key string, value []byte) error
	Delete(key string) (deleted bool)
	Size() int
}

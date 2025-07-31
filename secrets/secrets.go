package secrets

import (
	"context"
	"errors"
	"fmt"
)

var ErrUnknownProvider = errors.New("unknown secrets provider")

type Provider interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
}

type ProviderFn func(ctx context.Context, config map[string]string) (Provider, error)

var backends = make(map[string]ProviderFn)

func Register(name string, fn ProviderFn) error {
	if name == "" {
		return errors.New("secrets provider name cannot be empty")
	}
	if fn == nil {
		return errors.New("secrets provider function cannot be nil")
	}
	if _, exists := backends[name]; exists {
		return fmt.Errorf("secrets provider '%s' already registered", name)
	} else {
		backends[name] = fn
	}
	return nil
}

func Unregister(name string) error {
	if name == "" {
		return errors.New("secrets provider name cannot be empty")
	}
	if _, exists := backends[name]; !exists {
		return fmt.Errorf("secrets provider '%s' not registered", name)
	}
	delete(backends, name)
	return nil
}

func Backends() []string {
	names := make([]string, 0, len(backends))
	for name := range backends {
		names = append(names, name)
	}
	return names
}

func NewProvider(ctx context.Context, config map[string]string) (Provider, error) {
	if providerFn, ok := backends[config["provider"]]; !ok {
		return nil, ErrUnknownProvider
	} else {
		return providerFn(ctx, config)
	}
}

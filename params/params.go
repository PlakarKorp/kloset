package params

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

type Requirement int

const (
	Optional Requirement = iota
	Required
)

var ErrUnknownParam = errors.New("unknown parameter")

type param struct {
	name string
	r    Requirement
	fn   func(string) error
	seen bool
}

type Params struct {
	fields map[string]*param

	Unknown func(string) error
}

func New() *Params {
	return &Params{
		fields: make(map[string]*param),
	}
}

func (p *Params) Func(name string, r Requirement, fn func(string) error) {
	if _, ok := p.fields[name]; ok {
		panic(fmt.Sprintf("double registration of %s", name))
	}

	p.fields[name] = &param{
		name: name,
		r:    r,
		fn:   fn,
	}
}

func (p *Params) Bool(name string, b *bool, r Requirement) {
	p.Func(name, r, func(str string) error {
		v, err := strconv.ParseBool(str)
		if err != nil {
			return err
		}
		*b = v
		return nil
	})
}

func (p *Params) Int(name string, i *int, r Requirement) {
	p.Func(name, r, func(str string) error {
		v, err := strconv.ParseInt(str, 10, 0)
		if err != nil {
			return err
		}
		*i = int(v)
		return nil
	})
}

func (p *Params) String(name string, s *string, r Requirement) {
	p.Func(name, r, func(str string) error {
		*s = str
		return nil
	})
}

func (p *Params) Url(name string, u **url.URL, r Requirement) {
	p.Func(name, r, func(str string) error {
		v, err := url.Parse(str)
		if err != nil {
			return err
		}
		*u = v
		return nil
	})
}

func (p *Params) Parse(params map[string]string) error {
	for key, val := range params {
		param, ok := p.fields[key]
		if !ok {
			if p.Unknown == nil {
				return fmt.Errorf("%w: %s", ErrUnknownParam, key)
			}
			if err := p.Unknown(key); err != nil {
				return err
			}
			continue
		}

		if param.seen {
			return fmt.Errorf("%s specified multiple times", key)
		}
		param.seen = true

		if err := param.fn(val); err != nil {
			return fmt.Errorf("failed to validate %q: %w", key, err)
		}
	}

	for name, param := range p.fields {
		if param.r == Required && !param.seen {
			return fmt.Errorf("missing required parameter %q", name)
		}
	}

	return nil
}

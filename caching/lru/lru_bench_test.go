package lru

import (
	"fmt"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	for _, target := range []int{16, 256, 4096} {
		b.Run(fmt.Sprintf("target=%d", target), func(b *testing.B) {
			cache := New[int, int](target, nil)
			defer cache.Close()

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := cache.Put(i, i); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPutUpdateExisting(b *testing.B) {
	cache := New[int, int](256, nil)
	defer cache.Close()

	for i := range 256 {
		if err := cache.Put(i, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if err := cache.Put(i%256, i); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetHit(b *testing.B) {
	cache := New[int, int](256, nil)
	defer cache.Close()

	for i := range 256 {
		if err := cache.Put(i, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		_, _ = cache.Get(i % 256)
	}
}

func BenchmarkGetMiss(b *testing.B) {
	cache := New[int, int](256, nil)
	defer cache.Close()

	for i := range 256 {
		if err := cache.Put(i, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		_, _ = cache.Get(i + 1_000_000)
	}
}

func BenchmarkGetParallel(b *testing.B) {
	cache := New[int, int](256, nil)
	defer cache.Close()

	for i := range 256 {
		if err := cache.Put(i, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = cache.Get(i % 256)
			i++
		}
	})
}

// BenchmarkPutEviction measures Put cost when every insert forces an
// eviction, i.e. the cache is kept permanently full.
func BenchmarkPutEviction(b *testing.B) {
	const target = 256
	cache := New[int, int](target, nil)
	defer cache.Close()

	for i := range target {
		if err := cache.Put(i, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if err := cache.Put(target+i, i); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutEvictionWithCallback(b *testing.B) {
	const target = 256
	cache := New(target, func(K int, V int) error { return nil })
	defer cache.Close()

	for i := range target {
		if err := cache.Put(i, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if err := cache.Put(target+i, i); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMixed simulates a workload with 90% reads and 10% writes,
// where writes cycle through a fixed key space and steadily trigger
// evictions once the cache is warm.
func BenchmarkMixed(b *testing.B) {
	const target = 512
	cache := New[int, int](target, nil)
	defer cache.Close()

	for i := range target {
		if err := cache.Put(i, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if i%10 == 0 {
			if err := cache.Put(target+i, i); err != nil {
				b.Fatal(err)
			}
		} else {
			_, _ = cache.Get(i % target)
		}
	}
}

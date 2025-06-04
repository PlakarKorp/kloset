package plugins

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"syscall"

	"github.com/PlakarKorp/kloset/snapshot/importer"
	grpc_importer "github.com/PlakarKorp/kloset/snapshot/importer/pkg"

	"github.com/PlakarKorp/kloset/snapshot/exporter"
	grpc_exporter "github.com/PlakarKorp/kloset/snapshot/exporter/pkg"

	"github.com/PlakarKorp/kloset/storage"
	grpc_storage "github.com/PlakarKorp/kloset/storage/pkg"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func wrap[T any, O any](fn any) func(context.Context, *O, string, map[string]string) (T, error) {
	raw := fn.(func(context.Context, *interface{}, string, map[string]string) (interface{}, error))
	return func(ctx context.Context, opts *O, proto string, config map[string]string) (T, error) {
		var optsIface interface{} = opts
		val, err := raw(ctx, &optsIface, proto, config)
		if err != nil {
			var zero T
			return zero, err
		}
		t, ok := val.(T)
		if !ok {
			var zero T
			fmt.Printf("wrap error: val is %v (type %T)\n", val, val)
			return zero, fmt.Errorf("invalid return type: expected %T but got %T", zero, val)
		}
		return t, nil
	}
}

type PluginType interface {
	Register(name string, fn any)
	GrpcClient(client grpc.ClientConnInterface) any
	Wrap(fn any) any
}

type pluginTypes[T any, O any] struct {
	registerFn func(name string, fn func(context.Context, *O, string, map[string]string) (T, error))
	grpcClient func(client grpc.ClientConnInterface) T
}

func (p *pluginTypes[T, O]) Register(name string, fn any) {
	typedFn, ok := fn.(func(context.Context, *O, string, map[string]string) (T, error))
	if !ok {
		panic("invalid function signature for plugin")
	}
	p.registerFn(name, typedFn)
}

func (p *pluginTypes[T, O]) GrpcClient(client grpc.ClientConnInterface) any {
	return p.grpcClient(client)
}

func (p *pluginTypes[T, O]) Wrap(fn any) any {
	return wrap[T, O](fn)
}

var pTypes = map[string]PluginType{
	"importer": &pluginTypes[importer.Importer, importer.ImporterOptions]{
		registerFn: func(name string, fn func(context.Context, *importer.ImporterOptions, string, map[string]string) (importer.Importer, error)) {
			importer.Register(name, fn)
		},
		grpcClient: func(client grpc.ClientConnInterface) importer.Importer {
			return &importer.GrpcImporter{
				GrpcClient: grpc_importer.NewImporterClient(client),
			}
		},
	},
	"exporter": &pluginTypes[exporter.Exporter, exporter.ExporterOptions]{
		registerFn: func(name string, fn func(context.Context, *exporter.ExporterOptions, string, map[string]string) (exporter.Exporter, error)) {
			exporter.Register(name, fn)
		},
		grpcClient: func(client grpc.ClientConnInterface) exporter.Exporter {
			return &exporter.GrpcExporter{
				GrpcClient: grpc_exporter.NewExporterClient(client),
			}
		},
	},
	"storage": &pluginTypes[storage.Store, storage.StoreOptions]{
		registerFn: func(name string, fn func(context.Context, *storage.StoreOptions, string, map[string]string) (storage.Store, error)) {
			storage.Register(fn, name)
		},
		grpcClient: func(client grpc.ClientConnInterface) storage.Store {
			return &storage.GrpcStorage{
				GrpcClient: grpc_storage.NewStoreClient(client),
			}
		},
	},
}


func LoadBackends(ctx context.Context, pluginPath string) error {
	dirEntries, err := os.ReadDir(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to read plugins directory: %w", err)
	}

	pluginAcceptTypes := map[string]bool{"importer": true, "exporter": true, "storage": true}
	re := regexp.MustCompile(`^([a-z0-9][a-zA-Z0-9\+.\-]*)-(v[0-9]+\.[0-9]+\.[0-9]+)\.ptar$`)

	for _, pluginEntry := range dirEntries {
		if !pluginEntry.IsDir() || !pluginAcceptTypes[pluginEntry.Name()] {
			continue
		}
		pluginFolderPath := filepath.Join(pluginPath, pluginEntry.Name())
		pluginFiles, err := os.ReadDir(pluginFolderPath)
		if err != nil {
			return fmt.Errorf("failed to read plugin folder %s: %w", pluginEntry.Name(), err)
		}

		for _, entry := range pluginFiles {
			matches := re.FindStringSubmatch(entry.Name())
			if matches == nil {
				continue
			}
			key := matches[1]
			pluginFileName := matches[0]

			pType, ok := pTypes[pluginEntry.Name()]
			if !ok {
				return fmt.Errorf("unknown plugin type: %s", pluginEntry.Name())
			}
			wrappedFunc := pType.Wrap(func(ctx context.Context, _ *any, name string, config map[string]string) (any, error) {
				_, connFd, err := forkChild(filepath.Join(pluginFolderPath, pluginFileName), config)
				if err != nil {
					return nil, fmt.Errorf("failed to fork child: %w", err)
				}
				connFp := os.NewFile(uintptr(connFd), "grpc-conn")
				conn, err := net.FileConn(connFp)
				if err != nil {
					return nil, fmt.Errorf("failed to create file conn: %w", err)
				}
				client, err := grpc.NewClient("127.0.0.1:0",
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
						return conn, nil
					}),
				)
				if err != nil {
					return nil, fmt.Errorf("failed to create client context: %w", err)
				}
				return pType.GrpcClient(client), nil
			})
			pType.Register(key, wrappedFunc)
		}
	}
	return nil
}


func forkChild(pluginPath string, config map[string]string) (int, int, error) {
	sp, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, syscall.AF_UNSPEC)
	if err != nil {
		return -1, -1, fmt.Errorf("failed to create socketpair: %w", err)
	}

	procAttr := syscall.ProcAttr{}
	procAttr.Files = []uintptr{
		os.Stdin.Fd(),
		os.Stdout.Fd(),
		os.Stderr.Fd(),
		uintptr(sp[0]),
	}

	var pid int

	argv := []string{pluginPath, fmt.Sprintf("%v", config)}
	pid, err = syscall.ForkExec(pluginPath, argv, &procAttr)
	if err != nil {
		return -1, -1, fmt.Errorf("failed to ForkExec: %w", err)
	}

	if syscall.Close(sp[0]) != nil {
		return -1, -1, fmt.Errorf("failed to close socket: %w", err)
	}

	return pid, sp[1], nil
}

package plugins

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"
	"path/filepath"
	"regexp"

	"github.com/PlakarKorp/kloset/snapshot/importer"
	grpc_importer "github.com/PlakarKorp/kloset/snapshot/importer/pkg"

	"github.com/PlakarKorp/kloset/snapshot/exporter"
	grpc_exporter "github.com/PlakarKorp/kloset/snapshot/exporter/pkg"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type genericFn = func(context.Context, string, map[string]string) (interface{}, error)

func wrap[T any](fn interface{}) func(context.Context, string, map[string]string) (T, error) {
	raw := fn.(genericFn)
	return func(ctx context.Context, proto string, config map[string]string) (T, error) {
		val, err := raw(ctx, proto, config)
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

type pluginTypes struct {
	registerFn    func(string, interface{})
	grpcClient    func(interface{}) interface{}
}

var pTypes = map[string]pluginTypes{
	"importer": {
		registerFn:    func (name string, fn interface{}) {
			importer.Register(name, wrap[importer.Importer](fn))
		},
		grpcClient:    func(client interface{}) interface{} {
			return &importer.GrpcImporter{
				GrpcClient: grpc_importer.NewImporterClient(client.(*grpc.ClientConn)),
			}
		},
	},
	"exporter": {
		registerFn:		func (name string, fn interface{}) {
			exporter.Register(name, wrap[exporter.Exporter](fn))
		},
		grpcClient:    func(client interface{}) interface{} {	
			return &exporter.GrpcExporter{
				GrpcClient: grpc_exporter.NewExporterClient(client.(*grpc.ClientConn)),
			}
		},
	},
}
	

func LoadBackends(ctx context.Context, pluginPath string) error {
	dirEntries, err := os.ReadDir(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to read plugins directory: %w", err)
	}

	pluginTypes := map[string]bool{"importer": true, "exporter": true, "storage": true}
	re := regexp.MustCompile(`^([a-z0-9][a-zA-Z0-9\+.\-]*)-(v[0-9]+\.[0-9]+\.[0-9]+)\.ptar$`)

	for _, pluginEntry := range dirEntries {
		if !pluginEntry.IsDir() || !pluginTypes[pluginEntry.Name()] {
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
			fmt.Printf("Registering %s plugin: %s\n", pluginEntry.Name(), key)
			pType.registerFn(key, func(ctx context.Context, name string, config map[string]string) (interface{}, error) {
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
				return pType.grpcClient(client), nil
			})
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

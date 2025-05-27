package importer

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"

	// "regexp"
	"io/fs"
	"path/filepath"

	"github.com/PlakarKorp/kloset/objects"
	grpc_importer "github.com/PlakarKorp/kloset/snapshot/importer/grpc/pkg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcImporter struct {
	grpcClient grpc_importer.ImporterClient
}

func (g *grpcImporter) Origin() string {
	info, err := g.grpcClient.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetOrigin()
}

func (g *grpcImporter) Type() string {
	info, err := g.grpcClient.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetType()
}

func (g *grpcImporter) Root() string {
	info, err := g.grpcClient.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetRoot()
}

func (g *grpcImporter) Scan() (<-chan *ScanResult, error) {
	stream, err := g.grpcClient.Scan(context.Background(), &grpc_importer.ScanRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to start scan: %w", err)
	}

	results := make(chan *ScanResult, 100)
	go func() {
		defer close(results)
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					fmt.Fprintf(os.Stderr, "Scan completed successfully.\n")
					break
				}
				results <- NewScanError("", fmt.Errorf("failed to receive scan response: %w", err))
				return
			}
			if response.GetError() != nil {
				results <- NewScanError(response.GetPathname(), fmt.Errorf("scan error: %s", response.GetError().GetMessage()))
				continue
			}
			if response.GetRecord() == nil {
				results <- NewScanError(response.GetPathname(), fmt.Errorf("scan error: no record"))
				continue
			}
			isXattr := false
			if response.GetRecord().GetXattr() != nil {
				isXattr = true
			}
			results <- &ScanResult{
				Record: &ScanRecord{
					Pathname: response.GetPathname(),
					FileInfo: objects.FileInfo{
						Lname:      response.GetRecord().GetFileinfo().GetName(),
						Lsize:      response.GetRecord().GetFileinfo().GetSize(),
						Lmode:      fs.FileMode(response.GetRecord().GetFileinfo().GetMode()),
						LmodTime:   response.GetRecord().GetFileinfo().GetModTime().AsTime(),
						Ldev:       response.GetRecord().GetFileinfo().GetDev(),
						Lino:       response.GetRecord().GetFileinfo().GetIno(),
						Luid:       response.GetRecord().GetFileinfo().GetUid(),
						Lgid:       response.GetRecord().GetFileinfo().GetGid(),
						Lnlink:     uint16(response.GetRecord().GetFileinfo().GetNlink()),
						Lusername:  response.GetRecord().GetFileinfo().GetUsername(),
						Lgroupname: response.GetRecord().GetFileinfo().GetGroupname(),
					},
					Target:         response.GetRecord().Target,
					FileAttributes: response.GetRecord().GetFileAttributes(),
					IsXattr:        isXattr,
					XattrName:      response.GetRecord().GetXattr().GetName(),
					XattrType:      objects.Attribute(response.GetRecord().GetXattr().GetType()),
				},
				Error: nil,
			}
		}
	}()
	return results, nil
}

func (g *grpcImporter) NewReader(path string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (g *grpcImporter) NewExtendedAttributeReader(path string, xattr string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (g *grpcImporter) Close() error {
	if g.grpcClient != nil {
		if conn, ok := g.grpcClient.(interface{ Close() error }); ok {
			return conn.Close()
		}
	}
	return nil
}

func LoadBackends(ctx context.Context, pluginPath string) error {
	dirEntries, err := os.ReadDir(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to read plugins directory: %w", err)
	}

	for _, entry := range dirEntries {

		// foo-v1.0.0.ptar
		// re := regexp.MustCompile(`^([a-z0-9]+[a-zA0-9\+.-]*)-(v[0-9]+[a-z0\.[0-9]+\.[0-9]+)\.ptar$`)
		// matches := re.FindStringSubmatch(entry.Name())
		// if matches == nil {
		// 	panic("invalid plugin name")
		// }
		// name := matches[1]
		name := entry.Name()

		Register(name, func(ctx context.Context, name string, config map[string]string) (Importer, error) {
			pluginFileName := entry.Name()

			_, connFd, err := forkChild(filepath.Join(pluginPath, name), pluginFileName)
			if err != nil {
				return nil, fmt.Errorf("failed to fork child: %w", err)
			}

			connFp := os.NewFile(uintptr(connFd), "grpc-conn")
			conn, err := net.FileConn(connFp)
			if err != nil {
				return nil, fmt.Errorf("failed to create file conn: %w", err)
			}

			client, err := grpc.NewClient("127.0.0.1:0",
				grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO: Use WithTransportCredentials because insecure is deprecated.
				grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
					return conn, nil
				}),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create client context: %w", err)
			}

			importerClient := grpc_importer.NewImporterClient(client)
			return &grpcImporter{
				grpcClient: importerClient,
			}, nil
		})
	}
	return nil
}

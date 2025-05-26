package importer

import (
	"context"
	"net"
	"os"
	"fmt"
	"io"
	// "regexp"
	"io/fs"
	"time"
	"path/filepath"

	"google.golang.org/grpc"
	grpc_importer "github.com/PlakarKorp/kloset/snapshot/importer/grpc/pkg"
	"github.com/PlakarKorp/kloset/objects"
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
						Lname:		response.GetRecord().GetFileinfo().GetName(),
						Lsize:		response.GetRecord().GetFileinfo().GetSize(),
						Lmode:		fs.FileMode(response.GetRecord().GetFileinfo().GetMode()),
						LmodTime:	response.GetRecord().GetFileinfo().GetModTime().AsTime(),
						Ldev:		response.GetRecord().GetFileinfo().GetDev(),
						Lino:		response.GetRecord().GetFileinfo().GetIno(),
						Luid:		response.GetRecord().GetFileinfo().GetUid(),
						Lgid:		response.GetRecord().GetFileinfo().GetGid(),
						Lnlink:		uint16(response.GetRecord().GetFileinfo().GetNlink()),
						Lusername:	response.GetRecord().GetFileinfo().GetUsername(),
						Lgroupname:	response.GetRecord().GetFileinfo().GetGroupname(),
					},
					Target:             response.GetRecord().Target,	
					FileAttributes:     response.GetRecord().GetFileAttributes(),
					IsXattr:            isXattr,
					XattrName:          response.GetRecord().GetXattr().GetName(),
					XattrType:          objects.Attribute(response.GetRecord().GetXattr().GetType()),
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
		fmt.Printf("Loading plugin: %s\n", name)

		Register(name, func(ctx context.Context, name string, config map[string]string) (Importer, error) {
			pluginFileName := entry.Name()

			_, connFd, err := forkChild(filepath.Join(pluginPath, name), pluginFileName)
			if err != nil {
				return nil, fmt.Errorf("failed to fork child: %w", err)
			}

			time.Sleep(100 * time.Millisecond)

			connFp := os.NewFile(uintptr(connFd), "grpc-conn")
			conn, err := net.FileConn(connFp)
			if err != nil {
				return nil, fmt.Errorf("failed to create file conn: %w", err)
			}

			dialer := func(ctx context.Context, addr string) (net.Conn, error) {
				return conn, nil
			}

			cc, err := grpc.DialContext(
				ctx,
				"unused-target",
				grpc.WithInsecure(),  // TODO: Use WithTransportCredentials because insecure is deprecated.
				grpc.WithContextDialer(dialer),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to dial context: %w", err)
			}

			client := grpc_importer.NewImporterClient(cc)
			return &grpcImporter{
				grpcClient: client,
			}, nil
		})
	}
	return nil
}

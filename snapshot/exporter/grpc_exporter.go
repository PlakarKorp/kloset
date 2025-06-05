package exporter

import (
	"context"
	"io"

	"github.com/PlakarKorp/kloset/objects"
	grpc_exporter "github.com/PlakarKorp/kloset/snapshot/exporter/pkg"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type GrpcExporter struct {
	GrpcClient grpc_exporter.ExporterClient
}

func (g *GrpcExporter) Close() error {
	_, err := g.GrpcClient.Close(context.Background(), &grpc_exporter.CloseRequest{})
	return err
}

func (g *GrpcExporter) CreateDirectory(pathname string) error {
	_, err := g.GrpcClient.CreateDirectory(context.Background(), &grpc_exporter.CreateDirectoryRequest{Pathname: pathname})
	if err != nil {
		return err
	}
	return nil
}

func (g *GrpcExporter) Root() string {
	info, err := g.GrpcClient.Root(context.Background(), &grpc_exporter.RootRequest{})
	if err != nil {
		return ""
	}
	return info.RootPath
}

func (g *GrpcExporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	_, err := g.GrpcClient.SetPermissions(context.Background(), &grpc_exporter.SetPermissionsRequest{
		Pathname: pathname,
		FileInfo: &grpc_exporter.FileInfo{
			Name: 		fileinfo.Lname,
			Mode:	 	uint32(fileinfo.Lmode),
			ModTime: 	timestamppb.New(fileinfo.LmodTime),
			Dev: 		fileinfo.Ldev,
			Ino: 		fileinfo.Lino,
			Uid: 		fileinfo.Luid,
			Gid: 		fileinfo.Lgid,
			Nlink: 		uint32(fileinfo.Lnlink),
			Username: 	fileinfo.Lusername,
			Groupname: 	fileinfo.Lgroupname,
			Flags: 		fileinfo.Flags,
		},
	})
	return err
}

func (g *GrpcExporter) StoreFile(pathname string, fp io.Reader, size int64) error {
	stream, err := g.GrpcClient.StoreFile(context.Background())
	if err != nil {
		return err
	}

	buf := make([]byte, 32*1024)
	for {
		n, err := fp.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		
		if err := stream.Send(&grpc_exporter.StoreFileRequest{
			Pathname: pathname,
			Fp: &grpc_exporter.IoReader{
				Chunk: buf[:n],
			},
			Size: uint64(size),
		}); err != nil {
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

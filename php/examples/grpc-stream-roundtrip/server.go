// A minimal live gRPC server for the xrr PHP streaming round-trip.
package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	pb "xrrserver/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedStreamServiceServer
}

// Download streams chunks for a name; "empty" streams nothing and "boom"
// fails mid-stream after two chunks.
func (s *server) Download(req *pb.Chunk, stream pb.StreamService_DownloadServer) error {
	switch req.GetValue() {
	case "empty":
		return nil
	case "boom":
		for _, c := range []string{"log-chunk-1", "log-chunk-2"} {
			if err := stream.Send(&pb.Chunk{Value: c}); err != nil {
				return err
			}
		}
		return status.Error(codes.Unavailable, "connection reset")
	}
	for i := 1; i <= 3; i++ {
		if err := stream.Send(&pb.Chunk{Value: fmt.Sprintf("chunk-%d", i)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) Upload(stream pb.StreamService_UploadServer) error {
	var n int32
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.Summary{Count: n})
		}
		if err != nil {
			return err
		}
		n++
	}
}

func (s *server) Converse(stream pb.StreamService_ConverseServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.Chunk{Value: "re:" + in.GetValue()}); err != nil {
			return err
		}
	}
}

func main() {
	addr := "127.0.0.1:0"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	// Announce the bound port so the harness can connect.
	fmt.Println(lis.Addr().(*net.TCPAddr).Port)
	os.Stdout.Sync()

	g := grpc.NewServer()
	pb.RegisterStreamServiceServer(g, &server{})
	if err := g.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

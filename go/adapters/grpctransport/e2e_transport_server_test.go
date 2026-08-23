// Package grpctransport_test — separate-process gRPC server for the
// transport-capture e2e.
//
// The server exercises all three stream shapes plus concurrency, runs REAL
// child commands (so recorded frames carry genuine chunk boundaries and
// timing), listens on a real localhost TCP port in its OWN OS process, and
// requires a per-run auth token in metadata. A recording made through it
// therefore demonstrably required live credentials and a live socket.
package grpctransport_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Re-exec contract between the test process and the server process.
const (
	tpServerEnv   = "XRR_TRANSPORT_E2E_SERVER"
	tpTokenEnv    = "XRR_TRANSPORT_TOKEN"
	tpTokenHeader = "authorization"
	tpAddrPrefix  = "XRR_TRANSPORT_ADDR="
	tpServiceName = "xrrtransport.Sandbox"
)

// TestMain re-execs as the e2e server when the env contract says so.
func TestMain(m *testing.M) {
	if os.Getenv(tpServerEnv) == "1" {
		runTransportServer()
		return
	}
	os.Exit(m.Run())
}

var tpServiceDesc = grpc.ServiceDesc{
	ServiceName: tpServiceName,
	Streams: []grpc.StreamDesc{
		{StreamName: "Exec", Handler: tpExecHandler, ServerStreams: true},
		{StreamName: "Upload", Handler: tpUploadHandler, ClientStreams: true},
		{StreamName: "Converse", Handler: tpConverseHandler, ClientStreams: true, ServerStreams: true},
	},
}

// tpAuthInterceptor rejects any stream whose metadata token does not match
// the one the server was started with. The header is `authorization`
// specifically, so the recording exercises the credential path that
// sanitization must cover.
func tpAuthInterceptor(srv any, ss grpc.ServerStream,
	_ *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	want := os.Getenv(tpTokenEnv)
	if want == "" {
		return status.Error(codes.Internal, "server started without a token")
	}
	md, _ := metadata.FromIncomingContext(ss.Context())
	if got := md.Get(tpTokenHeader); len(got) == 0 || got[0] != "Bearer "+want {
		return status.Error(codes.Unauthenticated, "missing or invalid token")
	}
	return handler(srv, ss)
}

// tpExecHandler: server-streaming exec of a real child, streaming stdout
// chunks as the pipe delivers them. Non-zero exit surfaces as Aborted after
// whatever output already streamed — the mid-stream-error shape.
func tpExecHandler(_ any, stream grpc.ServerStream) error {
	req := new(wrapperspb.StringValue)
	if err := stream.RecvMsg(req); err != nil {
		return err
	}
	cmd := exec.CommandContext(stream.Context(), "/bin/sh", "-c", req.GetValue())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		return status.Errorf(codes.Internal, "start: %v", err)
	}
	buf := make([]byte, 64*1024)
	for {
		n, readErr := stdout.Read(buf)
		if n > 0 {
			if sendErr := stream.SendMsg(wrapperspb.Bytes(bytes.Clone(buf[:n]))); sendErr != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				return sendErr
			}
		}
		if readErr != nil {
			break
		}
	}
	if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return status.Errorf(codes.Aborted, "exec failed: exit status %d", exitErr.ExitCode())
		}
		return status.Errorf(codes.Internal, "wait: %v", err)
	}
	return nil
}

// tpUploadHandler: client-streaming upload piped into a real child's stdin,
// answered once the client half-closes.
func tpUploadHandler(_ any, stream grpc.ServerStream) error {
	cmd := exec.CommandContext(stream.Context(), "/bin/sh", "-c", "cat | wc -c")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdin pipe: %v", err)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		return status.Errorf(codes.Internal, "start: %v", err)
	}
	fail := func(format string, args ...any) error {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return status.Errorf(codes.Internal, format, args...)
	}
	for {
		part := new(wrapperspb.BytesValue)
		if recvErr := stream.RecvMsg(part); recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				break
			}
			return fail("recv: %v", recvErr)
		}
		if _, wErr := stdin.Write(part.GetValue()); wErr != nil {
			return fail("stdin write: %v", wErr)
		}
	}
	if err := stdin.Close(); err != nil {
		return fail("stdin close: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, "wait: %v", err)
	}
	return stream.SendMsg(wrapperspb.String("received:" + strings.TrimSpace(out.String())))
}

// tpConverseHandler: bidi ping/pong. Each received message is answered
// immediately, so send and recv genuinely interleave on the wire.
func tpConverseHandler(_ any, stream grpc.ServerStream) error {
	for {
		in := new(wrapperspb.StringValue)
		if err := stream.RecvMsg(in); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := stream.SendMsg(wrapperspb.String("pong:" + in.GetValue())); err != nil {
			return err
		}
	}
}

// runTransportServer is the child-process entrypoint.
func runTransportServer() {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "transport server: listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s%s\n", tpAddrPrefix, lis.Addr().String())
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}()
	srv := grpc.NewServer(grpc.StreamInterceptor(tpAuthInterceptor))
	srv.RegisterService(&tpServiceDesc, nil)
	if err := srv.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "transport server: serve: %v\n", err)
		os.Exit(1)
	}
}

// startTransportServer re-execs the test binary as the server process and
// scrapes its ephemeral listen address.
func startTransportServer(t *testing.T, token string) *tpServer {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), tpServerEnv+"=1", tpTokenEnv+"="+token)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	vs := &tpServer{cmd: cmd, stdin: stdin}
	t.Cleanup(vs.kill)
	vs.scrapeAddr(t, stdout)
	return vs
}

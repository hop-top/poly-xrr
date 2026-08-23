// Package xrr_test — separate-process server for the streaming validation
// e2e. The service mirrors a sandbox provider's control-plane shape:
// server-streaming Exec runs a REAL child command and streams its stdout
// chunks as the pipe produces them (genuine chunking and timing
// nondeterminism), and client-streaming Upload pipes the received frames
// through a real child's stdin — file transfer implemented as exec+stdin.
//
// The server is re-exec'd from the test binary (TestMain dispatches on an
// env flag), listens on an ephemeral localhost TCP port, and requires a
// per-run auth token in metadata, so a recording made through it demonstrably
// needed live credentials and a live process on the other end of a socket.
package xrr_test

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
	valServerEnv   = "XRR_STREAM_VALIDATION_SERVER"
	valTokenEnv    = "XRR_VALIDATION_TOKEN"
	valTokenHeader = "x-validation-token"
	valAddrPrefix  = "XRR_VALIDATION_ADDR="
	valServiceName = "xrrvalidation.Sandbox"
)

// TestMain re-execs as the validation server when the env contract says so;
// otherwise it runs the package's tests normally.
func TestMain(m *testing.M) {
	if os.Getenv(valServerEnv) == "1" {
		runValidationServer()
		return
	}
	os.Exit(m.Run())
}

var valServiceDesc = grpc.ServiceDesc{
	ServiceName: valServiceName,
	Streams: []grpc.StreamDesc{
		{StreamName: "Exec", Handler: valExecHandler, ServerStreams: true},
		{StreamName: "Upload", Handler: valUploadHandler, ClientStreams: true},
	},
}

// valAuthInterceptor rejects any stream whose metadata token does not match
// the one the server process was started with. This is what makes the
// record phase honest: live calls require real credentials, and the replay
// phase proves the cassette does not.
func valAuthInterceptor(srv any, ss grpc.ServerStream,
	_ *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	want := os.Getenv(valTokenEnv)
	if want == "" {
		return status.Error(codes.Internal, "validation server started without a token")
	}
	md, _ := metadata.FromIncomingContext(ss.Context())
	if got := md.Get(valTokenHeader); len(got) == 0 || got[0] != want {
		return status.Error(codes.Unauthenticated, "missing or invalid validation token")
	}
	return handler(srv, ss)
}

// valExecHandler mirrors a sandbox provider's server-streaming exec: run
// the requested script under /bin/sh and stream stdout chunks exactly as
// the pipe delivers them. A non-zero exit surfaces as an Aborted status
// carrying the exit code, after whatever output was already streamed —
// the mid-stream-error shape.
func valExecHandler(_ any, stream grpc.ServerStream) error {
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
			break // io.EOF once the child exits and the pipe drains
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

// valUploadHandler mirrors upload-as-exec: the client streams file chunks,
// the server pipes them into a real child command's stdin (`cat | wc -c`),
// and once the client half-closes it answers with the child's report.
func valUploadHandler(_ any, stream grpc.ServerStream) error {
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
				break // client half-closed: the upload is complete
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

// runValidationServer is the child-process entrypoint: listen on an
// ephemeral localhost TCP port, print the address for the parent to scrape,
// and serve until killed. The stdin watchdog exits the process when the
// parent disappears, so a failed test never leaks a listener.
func runValidationServer() {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "validation server: listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s%s\n", valAddrPrefix, lis.Addr().String())
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}()
	srv := grpc.NewServer(grpc.StreamInterceptor(valAuthInterceptor))
	srv.RegisterService(&valServiceDesc, nil)
	if err := srv.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "validation server: serve: %v\n", err)
		os.Exit(1)
	}
}

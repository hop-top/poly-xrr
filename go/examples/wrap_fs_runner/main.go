// Package main demonstrates the canonical adoption pattern for the
// xrr fs adapter: wrap an existing filesystem interface so it
// transparently records and replays through an xrr session.
//
// Pattern in three parts (same shape as wrap_command_runner):
//
//  1. Real — the existing app interface (stable, in production).
//  2. Wrapper — satisfies Real but routes through an xrr session.
//  3. Caller — uses Real and never knows xrr exists.
//
// For tests that mutate disk and need determinism, install a
// PathNormalizer mapping the test tmpdir to a stable placeholder
// like "$TMP". The cassette will store the normalized path and
// replay cleanly across test runs.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	xrr "hop.top/xrr"
	xfs "hop.top/xrr/adapters/fs"
)

// FS is the existing app interface — stable, used everywhere in the
// consuming codebase. We do NOT change it.
type FS interface {
	WriteFile(ctx context.Context, path string, data []byte, mode os.FileMode) error
	Mkdir(ctx context.Context, path string, mode os.FileMode) error
	Remove(ctx context.Context, path string) error
	RemoveAll(ctx context.Context, path string) error
	Rename(ctx context.Context, oldpath, newpath string) error
	Chmod(ctx context.Context, path string, mode os.FileMode) error
	Chown(ctx context.Context, path string, uid, gid int) error
	Symlink(ctx context.Context, target, link string) error
	Link(ctx context.Context, oldpath, newpath string) error
	Truncate(ctx context.Context, path string, size int64) error
}

// RealFS shells out to the standard library. Production impl.
type RealFS struct{}

func (RealFS) WriteFile(_ context.Context, p string, d []byte, m os.FileMode) error {
	return os.WriteFile(p, d, m)
}

func (RealFS) Mkdir(_ context.Context, p string, m os.FileMode) error {
	return os.Mkdir(p, m)
}
func (RealFS) Remove(_ context.Context, p string) error    { return os.Remove(p) }
func (RealFS) RemoveAll(_ context.Context, p string) error { return os.RemoveAll(p) }
func (RealFS) Rename(_ context.Context, o, n string) error { return os.Rename(o, n) }
func (RealFS) Chmod(_ context.Context, p string, m os.FileMode) error {
	return os.Chmod(p, m)
}

func (RealFS) Chown(_ context.Context, p string, u, g int) error {
	return os.Chown(p, u, g)
}
func (RealFS) Symlink(_ context.Context, t, l string) error { return os.Symlink(t, l) }
func (RealFS) Link(_ context.Context, o, n string) error    { return os.Link(o, n) }
func (RealFS) Truncate(_ context.Context, p string, s int64) error {
	return os.Truncate(p, s)
}

// Wrapper satisfies FS but routes every call through an xrr session.
type Wrapper struct {
	inner   FS
	sess    *xrr.FileSession
	adapter *xfs.Adapter
}

// NewWrapper wires inner + session + adapter.
func NewWrapper(inner FS, sess *xrr.FileSession, adapter *xfs.Adapter) *Wrapper {
	return &Wrapper{inner: inner, sess: sess, adapter: adapter}
}

func (w *Wrapper) record(ctx context.Context, req *xfs.Request, do func() error) error {
	start := time.Now()
	_, err := w.sess.Record(ctx, w.adapter, req, func() (xrr.Response, error) {
		runErr := do()
		return &xfs.Response{DurationMs: time.Since(start).Milliseconds()}, runErr
	})
	return err
}

// np applies the adapter's PathNormalizer at request construction
// time so that Request.Path / Request.Dest stored on the cassette
// envelope match what Fingerprint hashes (per the spec's
// "cassettes store post-normalizer paths" contract).
func (w *Wrapper) np(p string) string { return w.adapter.Normalize(p) }

// WriteFile records or replays a file write.
//
// Data is converted from []byte → string here. The cassette format
// stores data as a UTF-8 string; callers writing binary content
// must base64-encode at a higher level (see spec/cassette-format-v1.md
// "Data Field Encoding").
func (w *Wrapper) WriteFile(ctx context.Context, path string, data []byte, mode os.FileMode) error {
	m := uint32(mode)
	req := &xfs.Request{Op: xfs.OpWrite, Path: w.np(path), Data: string(data), Mode: &m}
	return w.record(ctx, req, func() error {
		return w.inner.WriteFile(ctx, path, data, mode)
	})
}

func (w *Wrapper) Mkdir(ctx context.Context, path string, mode os.FileMode) error {
	m := uint32(mode)
	req := &xfs.Request{Op: xfs.OpMkdir, Path: w.np(path), Mode: &m}
	return w.record(ctx, req, func() error { return w.inner.Mkdir(ctx, path, mode) })
}

func (w *Wrapper) Remove(ctx context.Context, path string) error {
	req := &xfs.Request{Op: xfs.OpRemove, Path: w.np(path)}
	return w.record(ctx, req, func() error { return w.inner.Remove(ctx, path) })
}

func (w *Wrapper) RemoveAll(ctx context.Context, path string) error {
	req := &xfs.Request{Op: xfs.OpRemove, Path: w.np(path), Recursive: true}
	return w.record(ctx, req, func() error { return w.inner.RemoveAll(ctx, path) })
}

func (w *Wrapper) Rename(ctx context.Context, oldpath, newpath string) error {
	req := &xfs.Request{Op: xfs.OpRename, Path: w.np(oldpath), Dest: w.np(newpath)}
	return w.record(ctx, req, func() error { return w.inner.Rename(ctx, oldpath, newpath) })
}

func (w *Wrapper) Chmod(ctx context.Context, path string, mode os.FileMode) error {
	m := uint32(mode)
	req := &xfs.Request{Op: xfs.OpChmod, Path: w.np(path), Mode: &m}
	return w.record(ctx, req, func() error { return w.inner.Chmod(ctx, path, mode) })
}

func (w *Wrapper) Chown(ctx context.Context, path string, uid, gid int) error {
	req := &xfs.Request{Op: xfs.OpChown, Path: w.np(path), UID: &uid, GID: &gid}
	return w.record(ctx, req, func() error { return w.inner.Chown(ctx, path, uid, gid) })
}

func (w *Wrapper) Symlink(ctx context.Context, target, link string) error {
	req := &xfs.Request{Op: xfs.OpSymlink, Path: w.np(target), Dest: w.np(link)}
	return w.record(ctx, req, func() error { return w.inner.Symlink(ctx, target, link) })
}

func (w *Wrapper) Link(ctx context.Context, oldpath, newpath string) error {
	req := &xfs.Request{Op: xfs.OpHardlink, Path: w.np(oldpath), Dest: w.np(newpath)}
	return w.record(ctx, req, func() error { return w.inner.Link(ctx, oldpath, newpath) })
}

func (w *Wrapper) Truncate(ctx context.Context, path string, size int64) error {
	req := &xfs.Request{Op: xfs.OpTruncate, Path: w.np(path), Size: &size}
	return w.record(ctx, req, func() error { return w.inner.Truncate(ctx, path, size) })
}

// Compile-time check: Wrapper satisfies FS.
var _ FS = (*Wrapper)(nil)

// main demonstrates record-then-replay against a tmpdir, with the
// canonical PathNormalizer installed.
func main() {
	tmp, err := os.MkdirTemp("", "xrr-fs-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	cassetteDir := filepath.Join(tmp, "cassettes")
	if err := os.MkdirAll(cassetteDir, 0o755); err != nil {
		log.Fatal(err)
	}
	target := filepath.Join(tmp, "hello.txt")

	normalizer := func(p string) string { return strings.Replace(p, tmp, "$TMP", 1) }
	adapter := xfs.NewAdapter().WithNormalizer(normalizer)
	ctx := context.Background()

	// Record
	{
		sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassetteDir))
		w := NewWrapper(RealFS{}, sess, adapter)
		if err := w.WriteFile(ctx, target, []byte("hello\n"), 0o644); err != nil {
			log.Fatalf("record WriteFile: %v", err)
		}
		fmt.Println("recorded:", target)
	}

	// Verify cassette landed
	entries, _ := os.ReadDir(cassetteDir)
	for _, e := range entries {
		fmt.Println("cassette:", e.Name())
	}

	// Replay against a panic-on-call inner FS — proves no disk write
	// happens on replay.
	{
		sess := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(cassetteDir))
		w := NewWrapper(panickyFS{}, sess, adapter)
		if err := w.WriteFile(ctx, target, []byte("hello\n"), 0o644); err != nil {
			log.Fatalf("replay WriteFile: %v", err)
		}
		fmt.Println("replayed without touching disk")
	}
}

// panickyFS panics on any call — used in replay to prove the inner
// FS is never invoked.
type panickyFS struct{}

func (panickyFS) WriteFile(context.Context, string, []byte, os.FileMode) error {
	panic("inner FS called during replay")
}
func (panickyFS) Mkdir(context.Context, string, os.FileMode) error { panic("nope") }
func (panickyFS) Remove(context.Context, string) error             { panic("nope") }
func (panickyFS) RemoveAll(context.Context, string) error          { panic("nope") }
func (panickyFS) Rename(context.Context, string, string) error     { panic("nope") }
func (panickyFS) Chmod(context.Context, string, os.FileMode) error { panic("nope") }
func (panickyFS) Chown(context.Context, string, int, int) error    { panic("nope") }
func (panickyFS) Symlink(context.Context, string, string) error    { panic("nope") }
func (panickyFS) Link(context.Context, string, string) error       { panic("nope") }
func (panickyFS) Truncate(context.Context, string, int64) error    { panic("nope") }

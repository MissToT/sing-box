package route

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

func newConnectionTestContext(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

// duplexTestConn is a half-close capable connection assembled from two net.Pipe pairs, so
// one peer can stop sending while the other direction keeps working.
type duplexTestConn struct {
	reader net.Conn
	writer net.Conn
}

func newDuplexTestConnPair() (local *duplexTestConn, peer *duplexTestConn) {
	localRead, peerWrite := net.Pipe()
	peerRead, localWrite := net.Pipe()
	return &duplexTestConn{reader: localRead, writer: localWrite},
		&duplexTestConn{reader: peerRead, writer: peerWrite}
}

func (c *duplexTestConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *duplexTestConn) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

func (c *duplexTestConn) Close() error {
	err := c.reader.Close()
	writeErr := c.writer.Close()
	if err == nil {
		err = writeErr
	}
	return err
}

func (c *duplexTestConn) CloseWrite() error {
	return c.writer.Close()
}

func (c *duplexTestConn) LocalAddr() net.Addr {
	return duplexTestAddr{}
}

func (c *duplexTestConn) RemoteAddr() net.Addr {
	return duplexTestAddr{}
}

func (c *duplexTestConn) SetDeadline(deadline time.Time) error {
	_ = c.reader.SetDeadline(deadline)
	return c.writer.SetDeadline(deadline)
}

func (c *duplexTestConn) SetReadDeadline(deadline time.Time) error {
	return c.reader.SetReadDeadline(deadline)
}

func (c *duplexTestConn) SetWriteDeadline(deadline time.Time) error {
	return c.writer.SetWriteDeadline(deadline)
}

type duplexTestAddr struct{}

func (duplexTestAddr) Network() string {
	return "pipe"
}

func (duplexTestAddr) String() string {
	return "pipe"
}

// halfCloseTestConn exposes CloseWrite, which the TUN stack conn also does.
type halfCloseTestConn struct {
	net.Conn
}

func (c *halfCloseTestConn) CloseWrite() error {
	return nil
}

type halfCloseTestDialer struct {
	conn *duplexTestConn
}

func (d *halfCloseTestDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return d.conn, nil
}

func (d *halfCloseTestDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	panic("unexpected ListenPacket")
}

// newHalfClosedTestConnection starts a managed connection whose outbound side is a duplex
// test conn, and returns the client side plus the outbound peer.
func newHalfClosedTestConnection(t *testing.T, idleTimeout time.Duration) (net.Conn, *duplexTestConn) {
	t.Helper()
	clientConn, inboundConn := net.Pipe()
	remoteConn, remotePeer := newDuplexTestConnPair()
	connectionManager := NewConnectionManager(newConnectionTestContext(t), log.NewNOPFactory().NewLogger("connection"))
	connectionManager.halfCloseIdleTimeout = idleTimeout
	connectionManager.NewConnection(newConnectionTestContext(t), &halfCloseTestDialer{conn: remoteConn}, &halfCloseTestConn{Conn: inboundConn}, adapter.InboundContext{
		Destination: M.ParseSocksaddrHostPort("example.com", 443),
	}, nil)
	return clientConn, remotePeer
}

func TestConnectionManagerClosesIdleHalfClosedConnection(t *testing.T) {
	clientConn, remotePeer := newHalfClosedTestConnection(t, 100*time.Millisecond)
	defer clientConn.Close()
	defer remotePeer.Close()

	// The outbound side finishes first, so the client connection is half-closed. The
	// client never closes its own direction, which must not keep the connection.
	require.NoError(t, remotePeer.CloseWrite())

	_ = clientConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err := clientConn.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestConnectionManagerKeepsActiveHalfClosedConnection(t *testing.T) {
	clientConn, remotePeer := newHalfClosedTestConnection(t, 100*time.Millisecond)
	defer clientConn.Close()
	defer remotePeer.Close()

	require.NoError(t, remotePeer.CloseWrite())
	go func() {
		_, _ = io.Copy(io.Discard, remotePeer)
	}()

	// Traffic in the direction that is still open must keep the connection alive.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = clientConn.SetWriteDeadline(time.Now().Add(time.Second))
		_, err := clientConn.Write([]byte("ping"))
		require.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}
}

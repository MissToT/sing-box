package dialer

import (
	"context"
	"net"
	"net/netip"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func DialSerialNetwork(ctx context.Context, dialer N.Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			panic("invalid usage")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if parallelDialer, isParallel := dialer.(ParallelNetworkDialer); isParallel {
		return parallelDialer.DialParallelNetwork(ctx, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	if C.TCPConcurrent && len(destinationAddresses) > 1 {
		return dialConcurrentNetwork(ctx, dialer, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	var errors []error
	if parallelDialer, isParallel := dialer.(ParallelInterfaceDialer); isParallel {
		for _, address := range destinationAddresses {
			conn, err := parallelDialer.DialParallelInterface(ctx, network, M.SocksaddrFrom(address, destination.Port), strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
			if err == nil {
				return conn, nil
			}
			errors = append(errors, err)
		}
	} else {
		for _, address := range destinationAddresses {
			conn, err := dialer.DialContext(ctx, network, M.SocksaddrFrom(address, destination.Port))
			if err == nil {
				return conn, nil
			}
			errors = append(errors, err)
		}
	}
	return nil, E.Errors(errors...)
}

func dialConcurrentNetwork(ctx context.Context, dialer N.Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	type dialResult struct {
		net.Conn
		error
		address netip.Addr
	}
	returned := make(chan struct{})
	defer close(returned)

	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan dialResult) // 无缓冲,修正泄漏问题

	racer := func(address netip.Addr) {
		var conn net.Conn
		var err error
		if parallelDialer, isParallel := dialer.(ParallelInterfaceDialer); isParallel {
			conn, err = parallelDialer.DialParallelInterface(dialCtx, network, M.SocksaddrFrom(address, destination.Port), strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
		} else {
			conn, err = dialer.DialContext(dialCtx, network, M.SocksaddrFrom(address, destination.Port))
		}
		select {
		case results <- dialResult{Conn: conn, error: err, address: address}:
		case <-returned:
			if conn != nil {
				conn.Close()
			}
		}
	}

	for _, address := range destinationAddresses {
		go racer(address)
	}

	var errors []error
	for i := 0; i < len(destinationAddresses); i++ {
		res := <-results
		if res.error == nil {
			cancel()
			if factory := service.FromContext[log.Factory](ctx); factory != nil {
				factory.NewLogger("dialer").DebugContext(ctx, "winner ", res.address, " (", destination, ")")
			}
			return res.Conn, nil
		}
		errors = append(errors, res.error)
	}
	if factory := service.FromContext[log.Factory](ctx); factory != nil {
		factory.NewLogger("dialer").DebugContext(ctx, "all failed (", destination, ")")
	}
	return nil, E.Errors(errors...)
}

// dialConcurrentNetworkPreferred 在开启 tcp_concurrent 时保留 IPv4/IPv6 优先级:
// 若地址列表只包含单一地址族,直接退化为普通的 dialConcurrentNetwork;
// 若同时包含两族地址,则按 preferIPv6 分为 primaries/fallbacks,
// 先只对 primaries 发起并发竞速,超过 fallbackDelay 仍未成功时才追加对 fallbacks 的并发竞速。
func dialConcurrentNetworkPreferred(ctx context.Context, dialer N.Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, preferIPv6 bool, fallbackDelay time.Duration) (net.Conn, error) {
	addresses4 := common.Filter(destinationAddresses, func(address netip.Addr) bool {
		return address.Is4() || address.Is4In6()
	})
	addresses6 := common.Filter(destinationAddresses, func(address netip.Addr) bool {
		return address.Is6() && !address.Is4In6()
	})
	if len(addresses4) == 0 || len(addresses6) == 0 {
		return dialConcurrentNetwork(ctx, dialer, network, destination, destinationAddresses, nil, nil, nil, fallbackDelay)
	}
	if fallbackDelay == 0 {
		fallbackDelay = N.DefaultFallbackDelay
	}

	var primaries, fallbacks []netip.Addr
	if preferIPv6 {
		primaries, fallbacks = addresses6, addresses4
	} else {
		primaries, fallbacks = addresses4, addresses6
	}

	returned := make(chan struct{})
	defer close(returned)

	type dialResult struct {
		net.Conn
		error
		primary bool
		done    bool
	}
	results := make(chan dialResult) // unbuffered
	startRacer := func(ctx context.Context, primary bool) {
		ras := primaries
		if !primary {
			ras = fallbacks
		}
		c, err := dialConcurrentNetwork(ctx, dialer, network, destination, ras, nil, nil, nil, fallbackDelay)
		select {
		case results <- dialResult{Conn: c, error: err, primary: primary, done: true}:
		case <-returned:
			if c != nil {
				c.Close()
			}
		}
	}

	var primary, fallback dialResult
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()
	go startRacer(primaryCtx, true)
	fallbackTimer := time.NewTimer(fallbackDelay)
	defer fallbackTimer.Stop()
	for {
		select {
		case <-fallbackTimer.C:
			fallbackCtx, fallbackCancel := context.WithCancel(ctx)
			defer fallbackCancel()
			go startRacer(fallbackCtx, false)

		case res := <-results:
			if res.error == nil {
				return res.Conn, nil
			}
			if res.primary {
				primary = res
			} else {
				fallback = res
			}
			if primary.done && fallback.done {
				return nil, primary.error
			}
			if res.primary && fallbackTimer.Stop() {
				fallbackTimer.Reset(0)
			}
		}
	}
}

func DialParallelNetwork(ctx context.Context, dialer ParallelInterfaceDialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, preferIPv6 bool, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			panic("invalid usage")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}

	if fallbackDelay == 0 {
		fallbackDelay = N.DefaultFallbackDelay
	}

	returned := make(chan struct{})
	defer close(returned)

	addresses4 := common.Filter(destinationAddresses, func(address netip.Addr) bool {
		return address.Is4() || address.Is4In6()
	})
	addresses6 := common.Filter(destinationAddresses, func(address netip.Addr) bool {
		return address.Is6() && !address.Is4In6()
	})
	if len(addresses4) == 0 || len(addresses6) == 0 {
		return DialSerialNetwork(ctx, dialer, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	var primaries, fallbacks []netip.Addr
	if preferIPv6 {
		primaries = addresses6
		fallbacks = addresses4
	} else {
		primaries = addresses4
		fallbacks = addresses6
	}
	type dialResult struct {
		net.Conn
		error
		primary bool
		done    bool
	}
	results := make(chan dialResult) // unbuffered
	startRacer := func(ctx context.Context, primary bool) {
		ras := primaries
		if !primary {
			ras = fallbacks
		}
		c, err := DialSerialNetwork(ctx, dialer, network, destination, ras, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
		select {
		case results <- dialResult{Conn: c, error: err, primary: primary, done: true}:
		case <-returned:
			if c != nil {
				c.Close()
			}
		}
	}
	var primary, fallback dialResult
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()
	go startRacer(primaryCtx, true)
	fallbackTimer := time.NewTimer(fallbackDelay)
	defer fallbackTimer.Stop()
	for {
		select {
		case <-fallbackTimer.C:
			fallbackCtx, fallbackCancel := context.WithCancel(ctx)
			defer fallbackCancel()
			go startRacer(fallbackCtx, false)

		case res := <-results:
			if res.error == nil {
				return res.Conn, nil
			}
			if res.primary {
				primary = res
			} else {
				fallback = res
			}
			if primary.done && fallback.done {
				return nil, primary.error
			}
			if res.primary && fallbackTimer.Stop() {
				fallbackTimer.Reset(0)
			}
		}
	}
}

// listenConcurrentNetworkPacket 是 dialConcurrentNetwork 的 UDP/PacketConn 版本。
// 注意: UDP 的 ListenPacket/连接建立不会做真实的远端可达性握手验证,
// 多个地址可能几乎同时"成功",此处并发的收益主要是快速跳过本机路由不可达的地址,
// 并不等同于 TCP 并发竞速能挑出"真正最快可达"的远端服务器。
func listenConcurrentNetworkPacket(ctx context.Context, dialer N.Dialer, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.PacketConn, netip.Addr, error) {
	type packetResult struct {
		net.PacketConn
		error
		address netip.Addr
	}
	returned := make(chan struct{})
	defer close(returned)

	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan packetResult) // 无缓冲,修正泄漏问题

	racer := func(address netip.Addr) {
		var conn net.PacketConn
		var err error
		if parallelDialer, isParallel := dialer.(ParallelInterfaceDialer); isParallel {
			conn, err = parallelDialer.ListenSerialInterfacePacket(dialCtx, M.SocksaddrFrom(address, destination.Port), strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
		} else {
			conn, err = dialer.ListenPacket(dialCtx, M.SocksaddrFrom(address, destination.Port))
		}
		select {
		case results <- packetResult{PacketConn: conn, error: err, address: address}:
		case <-returned:
			if conn != nil {
				conn.Close()
			}
		}
	}

	for _, address := range destinationAddresses {
		go racer(address)
	}

	var errors []error
	for i := 0; i < len(destinationAddresses); i++ {
		res := <-results
		if res.error == nil {
			cancel()
			return res.PacketConn, res.address, nil
		}
		errors = append(errors, res.error)
	}
	return nil, netip.Addr{}, E.Errors(errors...)
}

// listenConcurrentNetworkPacketPreferred 是 dialConcurrentNetworkPreferred 的 UDP/PacketConn 版本。
func listenConcurrentNetworkPacketPreferred(ctx context.Context, dialer N.Dialer, destination M.Socksaddr, destinationAddresses []netip.Addr, preferIPv6 bool, fallbackDelay time.Duration) (net.PacketConn, netip.Addr, error) {
	addresses4 := common.Filter(destinationAddresses, func(address netip.Addr) bool {
		return address.Is4() || address.Is4In6()
	})
	addresses6 := common.Filter(destinationAddresses, func(address netip.Addr) bool {
		return address.Is6() && !address.Is4In6()
	})
	if len(addresses4) == 0 || len(addresses6) == 0 {
		return listenConcurrentNetworkPacket(ctx, dialer, destination, destinationAddresses, nil, nil, nil, fallbackDelay)
	}
	if fallbackDelay == 0 {
		fallbackDelay = N.DefaultFallbackDelay
	}

	var primaries, fallbacks []netip.Addr
	if preferIPv6 {
		primaries, fallbacks = addresses6, addresses4
	} else {
		primaries, fallbacks = addresses4, addresses6
	}

	returned := make(chan struct{})
	defer close(returned)

	type packetResult struct {
		net.PacketConn
		address netip.Addr
		error
		primary bool
		done    bool
	}
	results := make(chan packetResult) // unbuffered
	startRacer := func(ctx context.Context, primary bool) {
		ras := primaries
		if !primary {
			ras = fallbacks
		}
		conn, addr, err := listenConcurrentNetworkPacket(ctx, dialer, destination, ras, nil, nil, nil, fallbackDelay)
		select {
		case results <- packetResult{PacketConn: conn, address: addr, error: err, primary: primary, done: true}:
		case <-returned:
			if conn != nil {
				conn.Close()
			}
		}
	}

	var primary, fallback packetResult
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()
	go startRacer(primaryCtx, true)
	fallbackTimer := time.NewTimer(fallbackDelay)
	defer fallbackTimer.Stop()
	for {
		select {
		case <-fallbackTimer.C:
			fallbackCtx, fallbackCancel := context.WithCancel(ctx)
			defer fallbackCancel()
			go startRacer(fallbackCtx, false)

		case res := <-results:
			if res.error == nil {
				return res.PacketConn, res.address, nil
			}
			if res.primary {
				primary = res
			} else {
				fallback = res
			}
			if primary.done && fallback.done {
				return nil, netip.Addr{}, primary.error
			}
			if res.primary && fallbackTimer.Stop() {
				fallbackTimer.Reset(0)
			}
		}
	}
}

func ListenSerialNetworkPacket(ctx context.Context, dialer N.Dialer, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.PacketConn, netip.Addr, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			panic("invalid usage")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if parallelDialer, isParallel := dialer.(ParallelNetworkDialer); isParallel {
		return parallelDialer.ListenSerialNetworkPacket(ctx, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	if C.TCPConcurrent && len(destinationAddresses) > 1 {
		return listenConcurrentNetworkPacket(ctx, dialer, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	var errors []error
	if parallelDialer, isParallel := dialer.(ParallelInterfaceDialer); isParallel {
		for _, address := range destinationAddresses {
			conn, err := parallelDialer.ListenSerialInterfacePacket(ctx, M.SocksaddrFrom(address, destination.Port), strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
			if err == nil {
				return conn, address, nil
			}
			errors = append(errors, err)
		}
	} else {
		for _, address := range destinationAddresses {
			conn, err := dialer.ListenPacket(ctx, M.SocksaddrFrom(address, destination.Port))
			if err == nil {
				return conn, address, nil
			}
			errors = append(errors, err)
		}
	}
	return nil, netip.Addr{}, E.Errors(errors...)
}

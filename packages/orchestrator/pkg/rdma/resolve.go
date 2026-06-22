package rdma

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
)

const sysfsInfiniband = "/sys/class/infiniband"

// ResolveDeviceAndAddr picks the RDMA device + IPv4 for this node:
// preferDevice (if set) → preferSubnet match → first device with up IPv4.
func ResolveDeviceAndAddr(preferDevice, preferSubnet string) (device, addr string, err error) {
	if preferDevice != "" {
		a, e := resolveAddrForDevice(preferDevice)
		if e != nil {
			return "", "", fmt.Errorf("device %s: %w", preferDevice, e)
		}
		return preferDevice, a, nil
	}

	devices, err := listRDMADevices()
	if err != nil {
		return "", "", err
	}
	if len(devices) == 0 {
		return "", "", errors.New("no RDMA devices found under " + sysfsInfiniband)
	}

	var subnet *net.IPNet
	if preferSubnet != "" {
		_, subnet, err = net.ParseCIDR(preferSubnet)
		if err != nil {
			return "", "", fmt.Errorf("parse RDMA_SUBNET %q: %w", preferSubnet, err)
		}
	}

	var lastErr error
	for _, dev := range devices {
		a, e := resolveAddrForDevice(dev)
		if e != nil {
			lastErr = e
			continue
		}
		if subnet != nil && !subnet.Contains(net.ParseIP(a)) {
			lastErr = fmt.Errorf("device %s addr %s not in subnet %s", dev, a, preferSubnet)
			continue
		}
		return dev, a, nil
	}
	if subnet != nil {
		return "", "", fmt.Errorf("no RDMA device's IP falls in %s (last err: %v)", preferSubnet, lastErr)
	}
	return "", "", fmt.Errorf("no RDMA device with usable IPv4 (last err: %v)", lastErr)
}

func ResolveAdvertiseAddr(rdmaDevice string) (string, error) {
	return resolveAddrForDevice(rdmaDevice)
}

func listRDMADevices() ([]string, error) {
	entries, err := os.ReadDir(sysfsInfiniband)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", sysfsInfiniband, err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	// Bond devices first — fleet deployments typically prefer bonded NICs.
	sort.SliceStable(out, func(i, j int) bool {
		bi := isBondDevice(out[i])
		bj := isBondDevice(out[j])
		if bi != bj {
			return bi
		}
		return out[i] < out[j]
	})
	return out, nil
}

func isBondDevice(name string) bool {
	return len(name) >= len("_bond_") && containsBond(name)
}

func containsBond(s string) bool {
	for i := 0; i+5 <= len(s); i++ {
		if s[i:i+5] == "bond_" {
			return true
		}
	}
	return false
}

func resolveAddrForDevice(rdmaDevice string) (string, error) {
	if rdmaDevice == "" {
		return "", errors.New("rdma device name is empty")
	}

	netdevDir := filepath.Join(sysfsInfiniband, rdmaDevice, "device", "net")
	entries, err := os.ReadDir(netdevDir)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", netdevDir, err)
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("no netdev associated with %s", rdmaDevice)
	}

	var lastErr error
	for _, entry := range entries {
		netdev := entry.Name()
		iface, err := net.InterfaceByName(netdev)
		if err != nil {
			lastErr = fmt.Errorf("InterfaceByName %s: %w", netdev, err)
			continue
		}
		if iface.Flags&net.FlagUp == 0 {
			lastErr = fmt.Errorf("netdev %s is down", netdev)
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			lastErr = fmt.Errorf("Addrs on %s: %w", netdev, err)
			continue
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil {
				continue
			}
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
				continue
			}
			ip4 := ip.To4()
			if ip4 == nil {
				continue
			}
			return ip4.String(), nil
		}
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("no usable IPv4 on any netdev under %s", netdevDir)
}

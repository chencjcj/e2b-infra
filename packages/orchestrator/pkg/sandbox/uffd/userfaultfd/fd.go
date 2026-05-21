package userfaultfd

// https://docs.kernel.org/admin-guide/mm/userfaultfd.html
// https://man7.org/linux/man-pages/man2/userfaultfd.2.html
// https://github.com/torvalds/linux/blob/master/fs/userfaultfd.c
// https://github.com/loopholelabs/userfaultfd-go/blob/main/pkg/constants/cgo.go

/*
#include <sys/syscall.h>
#include <fcntl.h>
#include <linux/userfaultfd.h>
#include <sys/ioctl.h>

struct uffd_pagefault {
	__u64 flags;
	__u64 address;
	__u32 ptid;
};

#ifndef UFFD_FEATURE_WP_ASYNC
#define UFFD_FEATURE_WP_ASYNC (1 << 15)
#endif

struct uffd_remove {
	__u64 start;
	__u64 end;
};

struct e2b_uffdio_continue {
	__u64 start;
	__u64 len;
	__u64 mode;
	__s64 mapped;
};
*/
import "C"

import (
	"fmt"
	"syscall"
	"unsafe"
)

const (
	NR_userfaultfd = C.__NR_userfaultfd

	UFFD_API = C.UFFD_API

	UFFD_EVENT_PAGEFAULT = C.UFFD_EVENT_PAGEFAULT
	UFFD_EVENT_REMOVE    = C.UFFD_EVENT_REMOVE

	UFFDIO_REGISTER_MODE_MISSING = C.UFFDIO_REGISTER_MODE_MISSING
	UFFDIO_REGISTER_MODE_WP      = C.UFFDIO_REGISTER_MODE_WP

	UFFDIO_COPY_MODE_WP = C.UFFDIO_COPY_MODE_WP

	UFFDIO_WRITEPROTECT_MODE_WP = C.UFFDIO_WRITEPROTECT_MODE_WP

	UFFDIO_ZEROPAGE_MODE_DONTWAKE = C.UFFDIO_ZEROPAGE_MODE_DONTWAKE

	UFFDIO_API          = C.UFFDIO_API
	UFFDIO_REGISTER     = C.UFFDIO_REGISTER
	UFFDIO_UNREGISTER   = C.UFFDIO_UNREGISTER
	UFFDIO_COPY         = C.UFFDIO_COPY
	UFFDIO_ZEROPAGE     = C.UFFDIO_ZEROPAGE
	UFFDIO_WRITEPROTECT = C.UFFDIO_WRITEPROTECT
	UFFDIO_WAKE         = C.UFFDIO_WAKE

	UFFD_PAGEFAULT_FLAG_WRITE = C.UFFD_PAGEFAULT_FLAG_WRITE
	UFFD_PAGEFAULT_FLAG_MINOR = C.UFFD_PAGEFAULT_FLAG_MINOR
	UFFD_PAGEFAULT_FLAG_WP    = C.UFFD_PAGEFAULT_FLAG_WP

	UFFD_FEATURE_MISSING_HUGETLBFS = C.UFFD_FEATURE_MISSING_HUGETLBFS
	UFFD_FEATURE_EVENT_REMOVE      = C.UFFD_FEATURE_EVENT_REMOVE
	UFFD_FEATURE_WP_ASYNC          = C.UFFD_FEATURE_WP_ASYNC

	// Hardcoded because CGo cannot resolve the __u64 casts / _IOWR macro.
	// Values from linux/userfaultfd.h (kernel 6.8).
	UFFD_FEATURE_MINOR_HUGETLBFS  = 1 << 9
	UFFD_FEATURE_MINOR_SHMEM      = 1 << 10
	UFFDIO_REGISTER_MODE_MINOR    = 1 << 2
	UFFDIO_CONTINUE               = 0xC020AA07
	UFFDIO_CONTINUE_MODE_DONTWAKE = 1 << 0
	UFFDIO_CONTINUE_MODE_WP       = 1 << 1
)

type (
	CULong = C.ulonglong
	CUChar = C.uchar
	CLong  = C.longlong

	UffdMsg       = C.struct_uffd_msg
	UffdPagefault = C.struct_uffd_pagefault
	UffdRemove    = C.struct_uffd_remove

	UffdioAPI          = C.struct_uffdio_api
	UffdioRegister     = C.struct_uffdio_register
	UffdioRange        = C.struct_uffdio_range
	UffdioCopy         = C.struct_uffdio_copy
	UffdioZero         = C.struct_uffdio_zeropage
	UffdioWriteProtect = C.struct_uffdio_writeprotect
	UffdioContinue     = C.struct_e2b_uffdio_continue
)

func newUffdioAPI(api, features CULong) UffdioAPI {
	return UffdioAPI{
		api:      api,
		features: features,
	}
}

func newUffdioRange(start, length CULong) UffdioRange {
	return UffdioRange{
		start: start,
		len:   length,
	}
}

func newUffdioRegister(start, length, mode CULong) UffdioRegister {
	return UffdioRegister{
		_range: newUffdioRange(start, length),
		mode:   mode,
	}
}

func newUffdioCopy(b []byte, address CULong, pagesize CULong, mode CULong, bytesCopied CLong) UffdioCopy {
	return UffdioCopy{
		src:  CULong(uintptr(unsafe.Pointer(&b[0]))),
		dst:  address,
		len:  pagesize,
		mode: mode,
		copy: bytesCopied,
	}
}

func newUffdioZero(address, pagesize, mode CULong) UffdioZero {
	return UffdioZero{
		_range:   newUffdioRange(address, pagesize),
		mode:     mode,
		zeropage: 0,
	}
}

func newUffdioWriteProtect(address, pagesize, mode CULong) UffdioWriteProtect {
	return UffdioWriteProtect{
		_range: newUffdioRange(address, pagesize),
		mode:   mode,
	}
}

func newUffdioContinue(address, pagesize, mode CULong) UffdioContinue {
	return UffdioContinue{
		start:  address,
		len:    pagesize,
		mode:   mode,
		mapped: 0,
	}
}

func getMsgEvent(msg *UffdMsg) CUChar {
	return msg.event
}

func getMsgArg(msg *UffdMsg) [24]byte {
	return msg.arg
}

func getPagefaultAddress(pagefault *UffdPagefault) uintptr {
	return uintptr(pagefault.address)
}

// Fd is a helper type that wraps uffd fd.
type Fd uintptr

// mode: UFFDIO_COPY_MODE_WP
// When we use both missing and wp, we need to use UFFDIO_COPY_MODE_WP, otherwise copying would unprotect the page
func (f Fd) copy(addr, pagesize uintptr, data []byte, mode CULong) error {
	cpy := newUffdioCopy(data, CULong(addr)&^CULong(pagesize-1), CULong(pagesize), mode, 0)

	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(f), UFFDIO_COPY, uintptr(unsafe.Pointer(&cpy))); errno != 0 {
		return errno
	}

	// Check if the copied size matches the requested pagesize
	if cpy.copy != CLong(pagesize) {
		return fmt.Errorf("UFFDIO_COPY copied %d bytes, expected %d", cpy.copy, pagesize)
	}

	return nil
}

func (f Fd) zero(addr, pagesize uintptr, mode CULong) error {
	zero := newUffdioZero(CULong(addr)&^CULong(pagesize-1), CULong(pagesize), mode)

	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(f), UFFDIO_ZEROPAGE, uintptr(unsafe.Pointer(&zero))); errno != 0 {
		return errno
	}

	// Check if the bytes actually zeroed out by the kernel match the page size
	if zero.zeropage != CLong(pagesize) {
		return fmt.Errorf("UFFDIO_ZEROPAGE copied %d bytes, expected %d", zero.zeropage, pagesize)
	}

	return nil
}

func (f Fd) writeProtect(addr, pagesize uintptr, mode CULong) error {
	writeProtect := newUffdioWriteProtect(CULong(addr)&^CULong(pagesize-1), CULong(pagesize), mode)

	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(f), UFFDIO_WRITEPROTECT, uintptr(unsafe.Pointer(&writeProtect))); errno != 0 {
		return errno
	}

	return nil
}

func (f Fd) wake(addr, pagesize uintptr) error {
	uffdRange := newUffdioRange(CULong(addr)&^CULong(pagesize-1), CULong(pagesize))

	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(f), UFFDIO_WAKE, uintptr(unsafe.Pointer(&uffdRange))); errno != 0 {
		return errno
	}

	return nil
}

func (f Fd) continueMapping(addr, pagesize uintptr, mode CULong) error {
	cont := newUffdioContinue(CULong(addr)&^CULong(pagesize-1), CULong(pagesize), mode)

	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(f), UFFDIO_CONTINUE, uintptr(unsafe.Pointer(&cont))); errno != 0 {
		return errno
	}

	if cont.mapped != CLong(pagesize) {
		return fmt.Errorf("UFFDIO_CONTINUE mapped %d bytes, expected %d", cont.mapped, pagesize)
	}

	return nil
}

func (f Fd) close() error {
	return syscall.Close(int(f))
}

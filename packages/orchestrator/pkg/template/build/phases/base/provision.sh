#!/bin/sh
set -eu

BUSYBOX="{{ .BusyBox }}"
RESULT_PATH="{{ .ResultPath }}"

echo "Starting provisioning script"

{{ if eq .Provider "gcp" }}
# GCP Specific logic
{{ end }}

# Configure DNS before any network use (align with e2b_val: static resolv.conf so apt/system work).
# Remove symlink if present (e.g. systemd-resolved); write static nameservers.
if [ -L /etc/resolv.conf ]; then
    rm -f /etc/resolv.conf
fi
cat > /etc/resolv.conf <<EOF
nameserver 8.8.8.8
nameserver 114.114.114.114
EOF
# Prevent systemd-resolved from taking over resolv.conf
if [ -f /etc/systemd/resolved.conf ]; then
    if ! grep -q "^DNSStubListener=" /etc/systemd/resolved.conf 2>/dev/null; then
        if grep -q "^\[Resolve\]" /etc/systemd/resolved.conf; then
            sed -i '/^\[Resolve\]/a DNSStubListener=no' /etc/systemd/resolved.conf 2>/dev/null || true
        else
            echo -e "\n[Resolve]\nDNSStubListener=no" >> /etc/systemd/resolved.conf
        fi
    else
        sed -i 's/^DNSStubListener=.*/DNSStubListener=no/' /etc/systemd/resolved.conf 2>/dev/null || true
    fi
fi
mkdir -p /etc/systemd/resolved.conf.d/
cat > /etc/systemd/resolved.conf.d/dns.conf <<EOF
[Resolve]
DNS=8.8.8.8 114.114.114.114
FallbackDNS=
Domains=
DNSSEC=no
EOF

echo "Making configuration immutable"
$BUSYBOX chattr +i /etc/resolv.conf

# Helper function to check if a package is installed
is_package_installed() {
    dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q "install ok installed"
}

# Install required packages if not already installed
PACKAGES="systemd systemd-sysv openssh-server sudo chrony socat curl ca-certificates fuse3 iptables git nfs-common"
echo "Checking presence of the following packages: $PACKAGES"

MISSING=""
for pkg in $PACKAGES; do
    if ! is_package_installed "$pkg"; then
        echo "Package $pkg is missing, will install it."
        MISSING="$MISSING $pkg"
    fi
done

if [ -n "$MISSING" ]; then
    echo "Missing packages detected, installing:$MISSING"

    # Use Aliyun mirror when archive.ubuntu.com is unreachable (e.g. China, restricted network).
    # Detects Ubuntu/Debian and replaces sources.list so apt can fetch packages.
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        DISTRO_ID="$ID"
        CODENAME=$(lsb_release -cs 2>/dev/null || echo "$VERSION_CODENAME")
        if [ -n "$CODENAME" ]; then
            case "$DISTRO_ID" in
                ubuntu)
                    echo "Detected Ubuntu, using Aliyun mirror for $CODENAME"
                    cat > /etc/apt/sources.list <<EOF
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME-updates main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME-backports main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME-security main restricted universe multiverse
EOF
                    ;;
                debian)
                    echo "Detected Debian, using Aliyun mirror for $CODENAME"
                    cat > /etc/apt/sources.list <<EOF
deb http://mirrors.aliyun.com/debian/ $CODENAME main contrib non-free non-free-firmware
deb http://mirrors.aliyun.com/debian/ $CODENAME-updates main contrib non-free non-free-firmware
deb http://mirrors.aliyun.com/debian/ $CODENAME-backports main contrib non-free non-free-firmware
deb http://mirrors.aliyun.com/debian-security/ $CODENAME-security main contrib non-free non-free-firmware
EOF
                    ;;
                *)
                    echo "Keeping default apt sources for $DISTRO_ID"
                    ;;
            esac
        fi
    fi

    apt-get -q update || {
        echo "E: apt-get update failed (no outbound internet from build VM). On the HOST: enable ip_forward, NAT/MASQUERADE for 169.254.0.0/30, or configure HTTP_PROXY for the build."
        exit 1
    }
    DEBIAN_FRONTEND=noninteractive DEBCONF_NOWARNINGS=yes apt-get -qq -o=Dpkg::Use-Pty=0 install -y --no-install-recommends $MISSING
    # After installing systemd, resolv.conf may have become a symlink again; restore static DNS.
    if [ -L /etc/resolv.conf ]; then
        $BUSYBOX chattr -i /etc/resolv.conf 2>/dev/null || true
        rm -f /etc/resolv.conf
        cat > /etc/resolv.conf <<EOF
nameserver 8.8.8.8
nameserver 114.114.114.114
EOF
    fi
else
    echo "All required packages are already installed."
fi

# Set /dev/fuse permissions to 666 for non-root access
# Use systemd-tmpfiles to set permissions at boot
mkdir -p /etc/tmpfiles.d
echo 'z /dev/fuse 0666 root root -' > /etc/tmpfiles.d/fuse.conf

echo "Setting up shell"
echo "export SHELL='/bin/bash'" >/etc/profile.d/shell.sh
echo "export PS1='\w \$ '" >/etc/profile.d/prompt.sh
echo "export PS1='\w \$ '" >>"/etc/profile"
echo "export PS1='\w \$ '" >>"/root/.bashrc"

echo "Use .bashrc and .profile"
echo "if [ -f ~/.bashrc ]; then source ~/.bashrc; fi; if [ -f ~/.profile ]; then source ~/.profile; fi" >>/etc/profile

echo "Remove root password"
passwd -d root

echo "Setting up chrony"
mkdir -p /etc/chrony
cat <<EOF >/etc/chrony/chrony.conf
refclock PHC /dev/ptp0 poll 2 dpoll 2
EOF

# Add a proxy config, as some environments expects it there (e.g. timemaster in Node Dockerimage)
echo "include /etc/chrony/chrony.conf" >/etc/chrony.conf

echo "Setting up SSH"
mkdir -p /etc/ssh
cat <<EOF >>/etc/ssh/sshd_config
PermitRootLogin yes
PermitEmptyPasswords yes
PasswordAuthentication yes
EOF

echo "Increasing inotify watch limit"
echo 'fs.inotify.max_user_watches=65536' | tee -a /etc/sysctl.conf

echo "Don't wait for ttyS0 (serial console kernel logs)"
# This is required when the Firecracker kernel args has specified console=ttyS0
systemctl mask serial-getty@ttyS0.service

echo "Disable network online wait"
systemctl mask systemd-networkd-wait-online.service

echo "Disable system first boot wizard"
# This was problem with Ubuntu 24.04, that differently calculate wizard should be called
# and Linux boot was stuck in wizard until envd wait timeout
systemctl mask systemd-firstboot.service

# Clean machine-id from Docker
rm -rf /etc/machine-id

echo "Linking systemd to init"
ln -sf /lib/systemd/systemd /usr/sbin/init

echo "Unlocking immutable configuration"
$BUSYBOX chattr -i /etc/resolv.conf

echo "Finished provisioning script"

# Delete itself
rm -rf /etc/init.d/rcS
rm -rf /usr/local/bin/provision.sh

# Report successful provisioning
printf "0" > "$RESULT_PATH"

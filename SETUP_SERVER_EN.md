# TGLog Test Environment Setup Guide

This document describes how to quickly set up a TGLog test environment.

## Environment Requirements

- **Number of Servers**: 2 servers
- **Disk Space**: At least 100GB available space per server
- **Operating System**: Linux
- **Network**: Client can access server port 20001 (TCP)

## Server Deployment

Execute the following operations on **both servers** separately.

### 0. Modify Kernel Parameters
For optimal performance, execute the following commands as root:
```bash
echo "net.core.rmem_max=256000000" >> /etc/sysctl.conf
echo "net.core.wmem_max=256000000" >> /etc/sysctl.conf
echo "net.core.rmem_default=104857600" >> /etc/sysctl.conf
echo "net.core.wmem_default=104857600" >> /etc/sysctl.conf
echo "net.ipv4.udp_rmem_min=104857600" >> /etc/sysctl.conf
echo "net.ipv4.udp_wmem_min=104857600" >> /etc/sysctl.conf
echo "net.ipv4.tcp_wmem=262144 114688000 131071000" >> /etc/sysctl.conf
echo "net.ipv4.tcp_rmem=262144 114688000 131071000" >> /etc/sysctl.conf
echo "net.core.netdev_max_backlog=10000" >> /etc/sysctl.conf
echo "net.ipv4.tcp_max_syn_backlog=10000" >> /etc/sysctl.conf
echo "net.core.somaxconn=4096" >> /etc/sysctl.conf
sysctl -p
```

### 1. Create Deployment Directory

```bash
sudo mkdir -p /data/tglog
sudo chown $USER:$USER /data/tglog
```

### 2. Download and Extract Server Package

```bash
cd /data/tglog
wget https://tglog-admin-1258344700.cos.ap-shanghai.myqcloud.com/downloads/tglog-v1.6.1.linux-amd64.tgz
tar -xzf tglog-v1.6.1.linux-amd64.tgz
```

### 3. Modify Configuration File

Edit the configuration file to change the network protocol from UDP to TCP:

```bash
cd /data/tglog/server
vi cfg/simplest.yaml
```

Find the `dataService.sources.source-udp.socket.network` configuration item and change its value from `udp` to `tcp`:

```yaml
dataService:
  sources:
    source-udp:
      type: socket
      socket:
        network: tcp  # Change from udp to tcp
        addr: ":20001"
```

### 4. Set Execute Permissions

```bash
cd /data/tglog/server
chmod +x bin/*.sh bin/tglog
```

### 5. Start Service

```bash
cd /data/tglog/server/bin
./start.sh
```

After successful startup, you will see output similar to:

```
{"level":"warn","ts":"2025-11-28T14:35:55.094+0800","msg":"using local config","path":"../cfg/simplest.yaml"}

pid is 1207
```

## Client Configuration

### 1. Get Server IP Addresses

Execute on server:

```bash
ip addr show | grep "inet " | grep -v 127.0.0.1
```

Assume the obtained IP addresses are:
- Server 1: `192.168.1.10`
- Server 2: `192.168.1.11`

### 2. Configure hosts File

Edit `/etc/hosts` file on **client machine**:

```bash
sudo vi /etc/hosts
```

Add the following two lines (replace IP addresses with actual server IPs):

```
192.168.1.10 dev.tglog.com
192.168.1.11 dev.tglog.com
```

After saving the file, the domain name `dev.tglog.com` will point to these two servers.

### 3. Verify Connection

```bash
# Test domain name resolution
ping dev.tglog.com

# Test port connectivity
telnet dev.tglog.com 20001
```

### 4. Modify Kernel Parameters
For optimal performance, execute the following commands as root:
```bash
echo "net.core.rmem_max=256000000" >> /etc/sysctl.conf
echo "net.core.wmem_max=256000000" >> /etc/sysctl.conf
echo "net.core.rmem_default=104857600" >> /etc/sysctl.conf
echo "net.core.wmem_default=104857600" >> /etc/sysctl.conf
echo "net.ipv4.udp_rmem_min=104857600" >> /etc/sysctl.conf
echo "net.ipv4.udp_wmem_min=104857600" >> /etc/sysctl.conf
echo "net.ipv4.tcp_wmem=262144 114688000 131071000" >> /etc/sysctl.conf
echo "net.ipv4.tcp_rmem=262144 114688000 131071000" >> /etc/sysctl.conf
echo "net.core.netdev_max_backlog=10000" >> /etc/sysctl.conf
echo "net.ipv4.tcp_max_syn_backlog=10000" >> /etc/sysctl.conf
echo "net.core.somaxconn=4096" >> /etc/sysctl.conf
sysctl -p
```

## Test Verification

### Send Test Log

```bash
# Using telnet
telnet dev.tglog.com 20001
# Input: testlog|field1|field2|field3

# Or using nc
echo "testlog|field1|field2|field3" | nc dev.tglog.com 20001

# Or
echo "testlog|field1|field2|field3" > /dev/tcp/dev.tglog.com/20001
```

### View Server Logs

View received logs on server:

```bash
cd /data/tglog/server/data
tail -f all-*.log
```

---

**Notes**:
- Service listening port: 20001 (TCP)
- Log storage directory: `/data/tglog/server/data/`
- Start/stop scripts: `/data/tglog/server/bin/start.sh` and `stop.sh`

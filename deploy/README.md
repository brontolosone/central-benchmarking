# Benchbonanza deployment notes

# Prerequisites
- A Linux system (kernel 6.18+) with hardware virtualization enabled and the KVM hypervisor available
- Some PostgreSQL server (v18+) somewhere reachable
- Python 3.14+
- uv 0.10.12+
- SystemD v259+
- QEMU 10.2.0+ & KVM
- virtiofsd 1.13.2+
- bindfs 1.17.7+
- A `btrfs` filesystem is currently assumed (but the code could be adapted to make execution of btrfs-specific parts conditional).
- Kernel capabilities:
  On both host kernel and guest kernel, you need:
    - CONFIG_VSOCKETS=y
  On the host kernel, you need:
    - CONFIG_VHOST_VSOCK=y
  On the guest kernel, you need:
  - CONFIG_VIRTIO_VSOCKETS=y
  - CONFIG_VIRTIO_BLK=y
  - CONFIG_VIRTIO_CONSOLE=y
  - CONFIG_VIRTIO_FS=y
  - CONFIG_VIRTIO_INPUT=y
  - CONFIG_VIRTIO_PCI=y
  - CONFIG_VIRTIO_RTC=y

# Not discussed here (yet):
- Creating a rootfs for virtiofsd to export
- Using a socket-activated virtiofsd instance

## Multitenancy
Multitenancy is supported through systemd instance names in template units.
This allows for loading different environment files, and the rest follows from there.
You can serve multiple tenants with the same `benchbonanza` installation, but each tenant
will need their own database.

## Decide on a tenant name
A non-multitenancy setup is just a single-tenant setup. The tenant will need a name, which will be used
in systemd unit instance specifiers, so it's best to keep to [a-zA-Z0-9_-].

For the following examples let's pretend we chose `odk` as a tenant name.

## Creating a user and group
```
useradd --groups kvm --no-create-home --system --user-group benchbonanza-odk
```
In a multitenant setup, it may be a good idea to create a separate user and group
for each tenant. If you add a regular user to the group created here, they will be able to access 
running VM guests on their serial virtual console and via SSH.

## Installing the software
1. Place the files of this repo some place accessible (read-only) for the abovecreated user(s). `/usr/local/benchbonanza` may not be a bad place.
2. From within the install root, (the directory that holds `pyproject.toml`), install the dependencies in a virtual environment.
   Use some variant of `uv sync --link-mode=clone --no-python-downloads --no-managed-python`  to do so.
   Again, make sure the abovecreated user(s) can read (and preferably, not write) the resulting `.venv` directory.

## Creating and initializing a database
As usual. Load the schema of [deploy/db_schema.sql](deploy/db_schema.sql). One database per tenant.

## Creating the configuration
1. Create a directory `/etc/benchbinge/odk`. Make it `r-x` for the `benchbonanza-odk` group created earlier.
2. Copy `deploy/vm.example.toml` into `/etc/benchbinge/odk/vm.toml` and adjust.
2. Copy `deploy/env.example.env` into `/etc/benchbinge/odk/env.env` and mind the permissions; the file may contain DB connection passwords. Strictly speaking only SystemD needs to be able to read this particular file, so it's OK to make it only readable for root.
3. Adjust the config:
   1. Use [libpq environment variables](https://www.postgresql.org/docs/18/libpq-envars.html) to configure access to the database created earlier.
   2. Make sure the `PATH` environment starts with the binpath of the virtual environment created earlier. If you installed `benchbonanza` into `/usr/local/benchbonanza` and followed the above instructions, then that path will be `/usr/local/benchbonanza/.venv/bin`.
   3. Adjust the other environment variables appropriately.

## Installing the services
1. Symlink the systemd `{bb-run-bench, bb-sync}.{service, timer}` files into your `/etc/systemd/system`.
2. If the .service / .timer files need adjusting, you can do that on two levels:
   1. Example: `systemctl edit bb-run-bench@.service` allows you to define overrides of the whole unit, all incantations.
   2. Example: `systemctl edit bb-sync@odk.timer` allows you to define overrides (eg, the sync frequency) for the particular "odk" tenant parameter incantation.
   Refer to a systemd manual for more information.
3. Start watching the logs (`journalctl -n0 -ef -u bb-run-bench.timer -u bb-sync.timer -u bb-run-bench.service -u bb-sync.service`).
4. Once satisfied with the configurations, use `systemctl enable --now bb-sync@odk.timer bb-sync@odk.service bb-run-bench@odk.timer bb-run-bench@odk.service bb-sync@odk.timer` to enable and activate, and check for trouble in the logs.

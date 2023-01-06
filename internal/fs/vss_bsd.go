//go:build freebsd || netbsd
// +build freebsd netbsd

package fs

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/mistifyio/go-zfs/v3"
	"github.com/restic/restic/internal/debug"
)

const VSSSupported = true

// MountPoint is a dummy for non-windows platforms to let client code compile.
type MountPoint struct {
	dataset *zfs.Dataset
	snap    *zfs.Dataset
}

// IsSnapshotted is true if this mount point was snapshotted successfully.
func (p *MountPoint) IsSnapshotted() bool {
	if p.snap == nil {
		return false
	}
	value, err := p.snap.GetProperty("type")
	if err != nil {
		return false
	}
	return value == zfs.DatasetSnapshot
}

// GetSnapshotDeviceObject returns root path to access the snapshot files and folders.
func (p *MountPoint) GetSnapshotDeviceObject() string {
	// zroot/ROOT/current                         5.78G  11.8G     3.28G  /
	// /.zfs/snapshot/restic-vss-1672430597
	_, snapshotName, found := strings.Cut(p.snap.Name, "@")
	if !found {
		panic(fmt.Sprintf("snapshot name is invalid: %s", p.snap.Name))
	}
	return filepath.Join(p.dataset.Mountpoint, ".zfs", "snapshot", snapshotName)
}

// VssSnapshot is a dummy for non-windows platforms to let client code compile.
type VssSnapshot struct {
	zpool          *zfs.Zpool
	name           string
	mountPointInfo map[string]MountPoint
}

// HasSufficientPrivilegesForVSS returns true if the user is allowed to use VSS.
func HasSufficientPrivilegesForVSS() error {
	_, err := zfs.ListZpools()
	if err != nil {
		return fmt.Errorf("unable to list zpools, zfs may not be available: %w", err)
	}
	return nil
}

// VolumeName returns the name of the volume given a corresponding path
func VolumeName(path string) string {
	debug.Log("VolumeName: %s", path)
	dataset := path
	for {
		debug.Log("Read dataset: %s", dataset)
		d, err := zfs.GetDataset(dataset)
		if err != nil {
			debug.Log("unable to open dataset %s: %v", dataset, err)
			dataset, _ = filepath.Split(dataset)
			if dataset == "" {
				return ""
			}
		}
		dataset = d.Name
		break
	}
	return dataset
}

// NewVssSnapshot creates a new vss snapshot. If creating the snapshots doesn't
// finish within the timeout an error is returned.
func NewVssSnapshot(volume string, timeoutInSeconds uint, uierr ErrorHandler) (VssSnapshot, error) {
	// TODO: We need to know which volume type this is, right now I'm hardcoding ZFS.
	// ZFS datasets don't have a leading slash

	// We cannot accurately determine what is being backed up, so we just snapshow the whole pool.
	// A path may or not be the root of the dataset.
	var d *zfs.Dataset
	var err error

	d, err = zfs.GetDataset(volume)
	if err != nil {
		uierr(volume, err)
		return VssSnapshot{}, err
	}
	debug.Log("Dataset: %s", spew.Sdump(d))

	zpoolName, _, found := strings.Cut(d.Name, "/")
	if !found {
		return VssSnapshot{}, fmt.Errorf("dataset has invalid name %s", zpoolName)
	}
	zpool, err := zfs.GetZpool(zpoolName)
	if err != nil {
		return VssSnapshot{}, fmt.Errorf("unable to get zpool %s: %w", zpoolName, err)
	}
	debug.Log("Zpool: %s", spew.Sdump(zpool))
	datasets, err := zpool.Datasets()
	if err != nil {
		return VssSnapshot{}, fmt.Errorf("unable to read datasets on zpool %s: %w", zpoolName, err)
	}
	mountPointInfo := make(map[string]MountPoint)
	//debug.Log("datasets: %s", spew.Sdump(datasets))
	sname := snapshotName()
	for _, d := range datasets {
		if d.Type != zfs.DatasetFilesystem {
			continue
		}
		snap, err := d.Snapshot(sname, false)
		if err != nil {
			uierr(d.Name, err)
			continue
		}
		mountPointInfo[d.Mountpoint] = MountPoint{
			dataset: d,
			snap:    snap,
		}
	}
	return VssSnapshot{
		zpool:          zpool,
		name:           sname,
		mountPointInfo: mountPointInfo,
	}, nil
}

func snapshotName() string {
	return fmt.Sprintf("restic-vss-%d", time.Now().Unix())
}

// Delete deletes the created snapshot.
func (p *VssSnapshot) Delete() error {
	// Delete our entries.
	for _, mp := range p.mountPointInfo {
		if mp.snap.Type != zfs.DatasetSnapshot {
			panic(fmt.Sprintf("Refusing to delete non-snapshot dataset %s, PLEASE REPORT THIS ISSUE", mp.snap.Name))
		}
		err := mp.snap.Destroy(zfs.DestroyDefault)
		if err != nil {
			debug.Log("Failed to delete snapshot %s", mp.snap.Name)
			return err
		}
		// TODO: Should we delete the mp.snap value now that the dataset is dead?
		mp.snap = nil
	}

	// Check for old entries.
	snaps, err := p.zpool.Snapshots()
	if err != nil {
		return fmt.Errorf("Unable to enumerate snapshots on pool %s: %w", p.zpool.Name, err)
	}

	for _, snap := range snaps {
		if snap.Type != zfs.DatasetSnapshot {
			continue
		}
		// A snapshot name is <volume>@<name>
		if strings.HasSuffix(snap.Name, "@restic-vss-") {
			debug.Log("Found dangling snapshots: %s", snap.Name)
		}
	}

	return nil
}

// GetSnapshotDeviceObject returns root path to access the snapshot files
// and folders.
func (p *VssSnapshot) GetSnapshotDeviceObject() string {
	return ""
}

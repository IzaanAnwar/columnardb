package segment

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const manifestVersion = 1

type Manifest struct {
	Version  int            `json:"version"`
	Segments []ManifestItem `json:"segments"`
}

type ManifestItem struct {
	ID          int    `json:"id"`
	Path        string `json:"path"`
	RecordCount int    `json:"record_count"`
}

func loadManifest(path string) (Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Manifest{Version: manifestVersion, Segments: []ManifestItem{}}, nil
		}
		return Manifest{}, fmt.Errorf("open manifest: %w", err)
	}
	defer file.Close()

	var m Manifest
	if err := json.NewDecoder(file).Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	if m.Version == 0 {
		m.Version = manifestVersion
	}
	return m, nil
}

func writeManifest(path string, m Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create manifest dir: %w", err)
	}

	temp, err := os.CreateTemp(filepath.Dir(path), "manifest-*.json")
	if err != nil {
		return fmt.Errorf("create manifest temp: %w", err)
	}
	tempName := temp.Name()

	enc := json.NewEncoder(temp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempName)
		return fmt.Errorf("encode manifest: %w", err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempName)
		return fmt.Errorf("close manifest temp: %w", err)
	}

	if err := os.Rename(tempName, path); err != nil {
		_ = os.Remove(tempName)
		return fmt.Errorf("replace manifest: %w", err)
	}
	return nil
}

func appendManifestItem(path string, item ManifestItem) error {
	m, err := loadManifest(path)
	if err != nil {
		return err
	}

	for _, existing := range m.Segments {
		if existing.ID == item.ID {
			return fmt.Errorf("manifest already contains segment id %d", item.ID)
		}
		if existing.Path == item.Path {
			return fmt.Errorf("manifest already contains segment path %q", item.Path)
		}
	}

	m.Segments = append(m.Segments, item)
	return writeManifest(path, m)
}

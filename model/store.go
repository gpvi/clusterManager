package model

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store provides persistent state management via SQLite.
type Store struct {
	db *sql.DB
}

// OpenStore opens (or creates) the SQLite database at the given path.
func OpenStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath+"?_journal=WAL&_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

func (s *Store) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS clusters (
		id            INTEGER PRIMARY KEY AUTOINCREMENT,
		name          TEXT NOT NULL UNIQUE,
		backend       TEXT NOT NULL DEFAULT 'podman',
		shards        INTEGER NOT NULL DEFAULT 0,
		nodes_per_shard INTEGER NOT NULL DEFAULT 2,
		redis_port    INTEGER NOT NULL DEFAULT 6379,
		image         TEXT NOT NULL,
		status        TEXT NOT NULL DEFAULT 'initial',
		created_at    TEXT NOT NULL,
		updated_at    TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS containers (
		id             INTEGER PRIMARY KEY AUTOINCREMENT,
		cluster_name   TEXT NOT NULL,
		name           TEXT NOT NULL,
		container_id   TEXT NOT NULL,
		host_ip        TEXT NOT NULL DEFAULT '127.0.0.1',
		host_port      INTEGER NOT NULL,
		container_ip   TEXT NOT NULL,
		container_port INTEGER NOT NULL,
		node_index     INTEGER NOT NULL,
		role           TEXT NOT NULL DEFAULT '',
		status         TEXT NOT NULL DEFAULT 'running',
		hostname       TEXT NOT NULL DEFAULT '',
		created_at     TEXT NOT NULL,
		UNIQUE(cluster_name, name)
	);

	CREATE TABLE IF NOT EXISTS operations (
		id            INTEGER PRIMARY KEY AUTOINCREMENT,
		cluster_name  TEXT NOT NULL,
		operation     TEXT NOT NULL,
		detail        TEXT NOT NULL DEFAULT '',
		success       INTEGER NOT NULL DEFAULT 1,
		created_at    TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_containers_cluster ON containers(cluster_name);
	CREATE INDEX IF NOT EXISTS idx_operations_cluster ON operations(cluster_name);
	`
	_, err := s.db.Exec(schema)
	if err != nil {
		return err
	}

	// Migrate existing databases: add hostname column if missing.
	var hasHostname int
	err = s.db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('containers') WHERE name='hostname'").Scan(&hasHostname)
	if err == nil && hasHostname == 0 {
		if _, err := s.db.Exec("ALTER TABLE containers ADD COLUMN hostname TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("add hostname column: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Cluster CRUD
// ---------------------------------------------------------------------------

type ClusterRecord struct {
	Name          string
	Backend       string
	Shards        int
	NodesPerShard int
	RedisPort     int
	Image         string
	Status        string
	CreatedAt     string
	UpdatedAt     string
}

func (s *Store) UpsertCluster(c ClusterRecord) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if c.CreatedAt == "" {
		c.CreatedAt = now
	}
	_, err := s.db.Exec(`
		INSERT INTO clusters (name, backend, shards, nodes_per_shard, redis_port, image, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			shards=excluded.shards, nodes_per_shard=excluded.nodes_per_shard,
			redis_port=excluded.redis_port, image=excluded.image,
			status=excluded.status, updated_at=excluded.updated_at
	`, c.Name, c.Backend, c.Shards, c.NodesPerShard, c.RedisPort, c.Image, c.Status, c.CreatedAt, now)
	return err
}

func (s *Store) UpdateClusterStatus(name, status string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec("UPDATE clusters SET status=?, updated_at=? WHERE name=?", status, now, name)
	return err
}

func (s *Store) GetCluster(name string) (*ClusterRecord, error) {
	r := &ClusterRecord{}
	err := s.db.QueryRow(
		"SELECT name, backend, shards, nodes_per_shard, redis_port, image, status, created_at, updated_at FROM clusters WHERE name=?",
		name,
	).Scan(&r.Name, &r.Backend, &r.Shards, &r.NodesPerShard, &r.RedisPort, &r.Image, &r.Status, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return r, err
}

func (s *Store) ListClusters() ([]ClusterRecord, error) {
	rows, err := s.db.Query("SELECT name, backend, shards, nodes_per_shard, redis_port, image, status, created_at, updated_at FROM clusters ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ClusterRecord
	for rows.Next() {
		var r ClusterRecord
		if err := rows.Scan(&r.Name, &r.Backend, &r.Shards, &r.NodesPerShard, &r.RedisPort, &r.Image, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (s *Store) DeleteCluster(name string) error {
	_, err := s.db.Exec("DELETE FROM containers WHERE cluster_name=?", name)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("DELETE FROM clusters WHERE name=?", name)
	return err
}

// ---------------------------------------------------------------------------
// Container CRUD
// ---------------------------------------------------------------------------

type ContainerRecord struct {
	ClusterName   string
	Name          string
	ContainerID   string
	HostIP        string
	HostPort      int
	ContainerIP   string
	ContainerPort int
	NodeIndex     int
	Role          string
	Status        string
	Hostname      string
}

func (s *Store) UpsertContainer(c ContainerRecord) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		INSERT INTO containers (cluster_name, name, container_id, host_ip, host_port, container_ip, container_port, node_index, role, status, hostname, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(cluster_name, name) DO UPDATE SET
			container_id=excluded.container_id, host_ip=excluded.host_ip,
			host_port=excluded.host_port, container_ip=excluded.container_ip,
			container_port=excluded.container_port, node_index=excluded.node_index,
			role=excluded.role, status=excluded.status,
			hostname=excluded.hostname
	`, c.ClusterName, c.Name, c.ContainerID, c.HostIP, c.HostPort, c.ContainerIP, c.ContainerPort, c.NodeIndex, c.Role, c.Status, c.Hostname, now)
	return err
}

func (s *Store) ListContainersByCluster(clusterName string) ([]ContainerRecord, error) {
	rows, err := s.db.Query(
		"SELECT cluster_name, name, container_id, host_ip, host_port, container_ip, container_port, node_index, role, status, hostname FROM containers WHERE cluster_name=? ORDER BY node_index",
		clusterName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ContainerRecord
	for rows.Next() {
		var c ContainerRecord
		if err := rows.Scan(&c.ClusterName, &c.Name, &c.ContainerID, &c.HostIP, &c.HostPort, &c.ContainerIP, &c.ContainerPort, &c.NodeIndex, &c.Role, &c.Status, &c.Hostname); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Operations log
// ---------------------------------------------------------------------------

func (s *Store) LogOperation(clusterName, operation, detail string, success bool) error {
	now := time.Now().UTC().Format(time.RFC3339)
	successInt := 0
	if success {
		successInt = 1
	}
	_, err := s.db.Exec(
		"INSERT INTO operations (cluster_name, operation, detail, success, created_at) VALUES (?, ?, ?, ?, ?)",
		clusterName, operation, detail, successInt, now,
	)
	return err
}

func (s *Store) ListOperations(clusterName string) ([]map[string]string, error) {
	rows, err := s.db.Query(
		"SELECT operation, detail, success, created_at FROM operations WHERE cluster_name=? ORDER BY id DESC LIMIT 50",
		clusterName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var op, detail, ts string
		var success int
		if err := rows.Scan(&op, &detail, &success, &ts); err != nil {
			return nil, err
		}
		res = append(res, map[string]string{"operation": op, "detail": detail, "success": fmt.Sprintf("%v", success == 1), "created_at": ts})
	}
	return res, nil
}

package data

// ClusterRecord represents a persisted cluster configuration row.
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

// ContainerRecord represents a persisted container row.
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

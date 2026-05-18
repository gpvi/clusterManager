package agent

import (
	"encoding/json"
	"log"
	"net/http"

	"redisClusterManager/model"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Server is an HTTP wrapper that exposes tool functions as JSON endpoints.
type Server struct {
	clientset  kubernetes.Interface
	restConfig *rest.Config
	cfg        *model.RuntimeConfig
	mux        *http.ServeMux
}

// NewServer creates a new Server with all routes registered.
func NewServer(clientset kubernetes.Interface, restConfig *rest.Config, cfg *model.RuntimeConfig) *Server {
	s := &Server{
		clientset:  clientset,
		restConfig: restConfig,
		cfg:        cfg,
		mux:        http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

// Start begins listening on addr and serving HTTP requests.
func (s *Server) Start(addr string) error {
	log.Printf("Agent tool server starting on %s", addr)
	return http.ListenAndServe(addr, s.mux)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/tools/create_cluster", s.handleCreateCluster)
	s.mux.HandleFunc("/tools/get_status", s.handleGetStatus)
	s.mux.HandleFunc("/tools/scale_cluster", s.handleScaleCluster)
	s.mux.HandleFunc("/tools/list_clusters", s.handleListClusters)
	s.mux.HandleFunc("/tools/diagnose", s.handleDiagnose)
	s.mux.HandleFunc("/tools/events", s.handleGetEvents)
}

// --- Handlers ---

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req CreateClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	resp, err := CreateClusterTool(r.Context(), s.clientset, s.restConfig, req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req GetStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	resp, err := GetStatusTool(r.Context(), s.clientset, s.restConfig, req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleScaleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req ScaleClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if err := ScaleClusterTool(r.Context(), s.clientset, s.restConfig, req.Name, req.Shards); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":    req.Name,
		"message": "scaled successfully",
		"success": true,
	})
}

func (s *Server) handleListClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	names, err := ListClustersTool(r.Context(), s.clientset, s.restConfig)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"clusters": names,
		"count":    len(names),
	})
}

func (s *Server) handleDiagnose(w http.ResponseWriter, r *http.Request) {
	// DiagnoseTool uses its own k8s clientset created from cfg,
	// so we call it with the context and cfg directly.
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req DiagnoseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	resp, err := DiagnoseTool(r.Context(), s.cfg, req.ClusterName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	clusterName := r.URL.Query().Get("name")
	if clusterName == "" {
		writeError(w, http.StatusBadRequest, "query parameter 'name' is required")
		return
	}

	namespace := s.cfg.KubeNamespace
	events, err := GetEventsTool(r.Context(), s.clientset, namespace, clusterName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if events == nil {
		events = []EventInfo{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"cluster_name": clusterName,
		"events":       events,
		"count":        len(events),
	})
}

// --- HTTP helpers ---

func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, statusCode int, msg string) {
	writeJSON(w, statusCode, map[string]string{"error": msg})
}

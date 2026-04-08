package redshift

import (
	"strings"
	"testing"
)

func TestBuildConnStrFromDataApiClusterConfig(t *testing.T) {
	got := buildConnStrFromDataApiClusterConfig("my-cluster", "myuser", "mydb", "us-east-1")
	want := "myuser@cluster(my-cluster)/mydb?region=us-east-1&transactionMode=non-transactional&requestMode=blocking"
	if got != want {
		t.Errorf("buildConnStrFromDataApiClusterConfig() = %q, want %q", got, want)
	}
}

func TestNewDataApiClusterConfig_MissingUsername(t *testing.T) {
	_, err := NewDataApiClusterConfig("my-cluster", "", "mydb", "us-east-1", 1)
	if err == nil {
		t.Fatal("expected error when username is empty, got nil")
	}
	if !strings.Contains(err.Error(), "username") {
		t.Errorf("expected error to mention 'username', got: %v", err)
	}
}

func TestNewDataApiClusterConfig_HappyPath(t *testing.T) {
	cfg, err := NewDataApiClusterConfig("my-cluster", "myuser", "mydb", "us-east-1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "myuser@cluster(my-cluster)/mydb?region=us-east-1&transactionMode=non-transactional&requestMode=blocking"
	if cfg.ConnStr != want {
		t.Errorf("cfg.ConnStr = %q, want %q", cfg.ConnStr, want)
	}
}

func TestBuildConnStrFromDataApiWorkgroupConfig_Unchanged(t *testing.T) {
	got := buildConnStrFromDataApiConfig("my-workgroup", "mydb", "ap-southeast-2")
	want := "workgroup(my-workgroup)/mydb?region=ap-southeast-2&transactionMode=non-transactional&requestMode=blocking"
	if got != want {
		t.Errorf("buildConnStrFromDataApiConfig() = %q, want %q", got, want)
	}
}

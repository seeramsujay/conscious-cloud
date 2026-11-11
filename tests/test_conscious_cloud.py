import json
import yaml
from pathlib import Path
from workers.echo_server_test import test_worker_helpers

def test_docker_compose_validity():
    compose_path = Path("docker-compose.yaml")
    assert compose_path.exists()
    content = yaml.safe_load(compose_path.read_text())
    assert "services" in content
    assert "envoy" in content["services"]
    assert "worker-a" in content["services"]
    assert "worker-b" in content["services"]

def test_control_plane_go_mod():
    mod_path = Path("control-plane/go.mod")
    assert mod_path.exists()
    content = mod_path.read_text()
    assert "module" in content
    assert "go " in content

def test_terraform_configs():
    tf_path = Path("infrastructure/main.tf")
    var_path = Path("infrastructure/variables.tf")
    assert tf_path.exists() and var_path.exists()
    assert "resource" in tf_path.read_text() or "variable" in var_path.read_text()

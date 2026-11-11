import importlib.util
from pathlib import Path

def test_worker_helpers():
    echo_path = Path(__file__).parent / "echo-server.py"
    assert echo_path.exists()
    spec = importlib.util.spec_from_file_location("echo_server", str(echo_path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert hasattr(mod, "EchoHandler")
    assert hasattr(mod, "ThreadingHTTPServer")

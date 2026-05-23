import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class EchoHandler(BaseHTTPRequestHandler):
    delay = 0

    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "healthy"}).encode())
            return

        time.sleep(EchoHandler.delay)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps(
                {
                    "method": "GET",
                    "path": self.path,
                    "host": self.server.server_address[0],
                    "port": self.server.server_address[1],
                }
            ).encode()
        )

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else b""

        time.sleep(EchoHandler.delay)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps(
                {
                    "method": "POST",
                    "path": self.path,
                    "body": body.decode(),
                    "host": self.server.server_address[0],
                    "port": self.server.server_address[1],
                }
            ).encode()
        )


def main():
    parser = argparse.ArgumentParser(description="Dummy stateless worker")
    parser.add_argument("--port", type=int, default=8080, help="Listen port")
    parser.add_argument(
        "--delay", type=float, default=0.0, help="Simulated processing delay (seconds)"
    )
    args = parser.parse_args()

    EchoHandler.delay = args.delay

    server = HTTPServer(("0.0.0.0", args.port), EchoHandler)
    print(f"Echo worker listening on 0.0.0.0:{args.port}, delay={args.delay}s")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Worker stopped.")
        server.server_close()


if __name__ == "__main__":
    main()

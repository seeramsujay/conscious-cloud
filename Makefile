.PHONY: control-plane envoy worker orchestrator load-test

control-plane:
	go run ./control-plane/main.go

envoy:
	docker run --rm -v $(PWD)/data-plane/envoy.yaml:/etc/envoy/envoy.yaml:ro \
		-p 10000:10000 -p 9901:9901 \
		--add-host host.docker.internal:host-gateway \
		envoyproxy/envoy:v1.30-latest -c /etc/envoy/envoy.yaml

worker:
	python3 workers/echo-server.py --port 8080

worker2:
	python3 workers/echo-server.py --port 8081

orchestrator:
	python3 orchestrator/main.py --daemon

load-test:
	# Requires hey: go install github.com/rakyll/hey@latest
	hey -n 10000 -c 100 http://localhost:10000/

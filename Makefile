ENGINE=main.go
SERVICE=4200

dependency:
	go mod tidy
	go mod verify
.PHONY: dependency

debug:
	go run cmd/server/${ENGINE} service --srvport ${SERVICE}
.PHONY: debug
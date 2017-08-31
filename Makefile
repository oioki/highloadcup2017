all: highload

highload: *.go
	go build -o highload *.go
	strip highload

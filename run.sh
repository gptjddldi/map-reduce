go build -buildmode=plugin mrapps/wc.go
go run main.go wc.so

PKGS                      := $(shell go list ./... | grep -v /tests | grep -v /xcpb | grep -v /gpb | grep -v /generated)
GO                        := go

all:
	go build -o main ./dbconnect/main.go ./dbconnect/auth.go  ./dbconnect/errors.go
	GOOS=linux GOARCH=amd64 go build -o main_x86_64 ./dbconnect/main.go


test:
	@$(foreach pkg, $(PKGS),\
		$(GO) test -v -run '(Test|Example)' $(pkg) || exit 1)

clean:
	rm -f main
	rm -f main_x86_64
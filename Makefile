PKGS                      := $(shell go list ./... | grep -v /tests | grep -v /xcpb | grep -v /gpb | grep -v /generated)
GO                        := go
SRCS                      := $(shell find . -name "*.go" | grep -v /tests | grep -v /xcpb | grep -v /gpb | grep -v /generated)

all: $(SRCS)
	go build -o main $(SRCS)
	GOOS=linux GOARCH=amd64 go build -o main_x86_64 $(SRCS)


test:
	@$(foreach pkg, $(PKGS),\
		$(GO) test -v -run '(Test|Example)' $(pkg) || exit 1)

clean:
	rm -f main
	rm -f main_x86_64
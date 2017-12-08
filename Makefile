# fe makefile
#
# $ GOOS=windows GOARCH=amd64 GOPATH=${PWD}/build go build -o fe cmd/fe-main.go
# $ GOOS=freebsd GOARCH=amd64 GOPATH=${PWD}/build go build -o fe cmd/fe-main.go

all: build build/bin/fe

.PHONY: build/bin/fe clean

build/bin/fe:
	GOPATH=${PWD}/build go build -o $@ cmd/fe-main.go 

build: 
	@if [ ! -d build ]; then				\
		mkdir -p build/src build/src/cmd build/bin;	\
		ln -sf ${PWD} build/src/fe;			\
		ln -sf ${PWD}/fe-main.go build/src/cmd/;	\
	fi

clean:
	/bin/rm -rf build

%.go:
	@true

# EOF

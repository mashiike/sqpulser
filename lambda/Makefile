.PHONY: test clean

bootstrap: ../*.go ../go.* ../cmd/sqpulser/*.go
	GOARCH=amd64 GOOS=linux go build -o bootstrap ../cmd/sqpulser

deploy: bootstrap
	lambroll deploy

clean:
	rm -f bootstrap

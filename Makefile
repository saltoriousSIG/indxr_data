#!make
include .env
export $(shell sed 's/=.*//' .env)

# Run the hypertable migration
migrate-hypertable:
	@echo "Running hypertable migration"
	migrate -path ./migrations -database $(TIMESCALE_CONNECTION_URL) goto 2


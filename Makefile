.PHONY: help up down restart logs test init clean

help:
	@echo "Available commands:"
	@echo "  make up       - Start Docker services"
	@echo "  make down     - Stop Docker services"
	@echo "  make restart  - Restart Docker services"
	@echo "  make logs     - Show service logs"
	@echo "  make init     - Initialize Iceberg tables"
	@echo "  make test     - Run integration tests"
	@echo "  make clean    - Stop services and remove volumes"

up:
	docker-compose up -d
	@echo "Services starting... Wait ~30 seconds, then run 'make init'"

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

init:
	@echo "Initializing Iceberg tables..."
	@./scripts/init_iceberg.sh

test:
	@echo "Running integration tests..."
	python tests/test_integration.py

clean:
	docker-compose down -v
	@echo "All services and data removed"


.PHONY: help up down restart logs test init clean

help:
	@echo "Available commands:"
	@echo "  make up             - Start Docker services"
	@echo "  make down           - Stop Docker services"
	@echo "  make restart        - Restart Docker services"
	@echo "  make logs           - Show service logs"
	@echo "  make init           - Initialize Iceberg tables"
	@echo "  make test           - Run unit tests"
	@echo "  make integration_test - Run integration tests"
	@echo "  make clean          - Stop services and remove volumes"

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
	@echo "Running unit tests..."
	uv run --group test pytest --disable-socket --allow-unix-socket tests/unit_tests/ -v

integration_test:
	@echo "Running integration tests..."
	uv run --group test --group test_integration pytest -n auto tests/integration_tests/ -v

clean:
	docker-compose down -v
	@echo "All services and data removed"


# Docker commands for job_insight_104 project

.PHONY: build up down logs clean restart shell

# Build Docker image
build:
	docker-compose build

# Start all services
up:
	docker-compose up -d

# Stop all services
down:
	docker-compose down

# View logs
logs:
	docker-compose logs -f

# View specific service logs
logs-analysis:
	docker-compose logs -f analysis

logs-crawler:
	docker-compose logs -f crawler

logs-scheduler:
	docker-compose logs -f scheduler

# Clean up containers and images
clean:
	docker-compose down -v
	docker system prune -f

# Restart all services
restart: down up

# Get shell access to analysis container
shell:
	docker-compose exec analysis bash

# Run individual services
run-analysis:
	docker-compose up analysis

run-crawler:
	docker-compose run --rm crawler

run-scheduler:
	docker-compose run --rm scheduler

# Development mode (with code changes reflected)
dev:
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml up

stage:
   echo 1

# 代码格式化检查
format-check:
	black --check .

# 自动格式化代码
format:
	black .


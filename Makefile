
extract:
	cd extractload && uv run python main.py	

transform:
	cd transform/dbt && uv run dbt run

analyze:
	docker run --name metaduck -d -p 80:3000 -m 2GB -e MB_PLUGINS_DIR=/home/plugins metaduck

run : extract transform analyze

build:
	cd analyze/metabase && docker build . --tag metaduck:latest



.PHONY: extract transform analyze












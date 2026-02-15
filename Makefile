help: # Show help for all commands
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | sort | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

init: # Install all python dependencies on virtual env
	uv pip install -r requirements.txt | make db-recreate

run: # Get data from API Euro Cotation. Example: make run last_days=3
	python3 main.py $(last_days)

db-teste: # test - run a select on database
	python3 db-teste.py

db-recreate: # Recreate database
	python3 db-recreate.py
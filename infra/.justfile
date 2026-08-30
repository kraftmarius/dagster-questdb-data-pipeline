mod databases

# List recipes
list:
    @just --list --unsorted

up *args:
    docker compose up {{args}}

down *args:
    docker compose down {{args}}

restart:
    docker compose restart

update:
    docker compose pull
    
    @just restart
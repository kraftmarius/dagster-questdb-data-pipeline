# List recipes
list:
    @just --list --unsorted

up *args:
    @docker compose up {{args}}

down *args:
    @docker compose down {{args}}

restart:
    @docker compose restart

logs:
    @docker compose logs -f

update:
    @docker compose pull
    @just restart
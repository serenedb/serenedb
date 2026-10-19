#!/bin/bash

owner="${1:-}"
if [[ -z "$owner" || "$owner" == none ]]; then
	echo "Usage: $0 <owner> (an empty owner and 'none' are refused)" >&2
	exit 0
fi

filter="label=serenedb.owner=$owner"

mapfile -t containers < <(docker ps -a --filter "$filter" --format '{{.Names}}')
if [[ ${#containers[@]} -ne 0 ]]; then
	echo "Removing the containers of $owner:"
	docker rm -fv "${containers[@]}"
fi

mapfile -t networks < <(docker network ls --filter "$filter" --format '{{.Name}}')
if [[ ${#networks[@]} -ne 0 ]]; then
	echo "Removing the networks of $owner:"
	docker network rm "${networks[@]}"
fi

mapfile -t volumes < <(docker volume ls --filter "$filter" --format '{{.Name}}')
if [[ ${#volumes[@]} -ne 0 ]]; then
	echo "Removing the volumes of $owner:"
	docker volume rm "${volumes[@]}"
fi

exit 0

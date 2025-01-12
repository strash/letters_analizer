#!/bin/bash

while IFS= read -r run; do
	$run
done < ./sitemap.txt

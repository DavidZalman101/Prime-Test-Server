#!/usr/bin/bash

total=0
count=0

for i in {1..100}; do
	output=$(./benchmark_client -n 1000 -t 4 -r)
	avg=$(echo "$output" | grep "Average:" | grep -oP '\d+\.\d+(?= μs)')

	if [[ -n "$avg" ]]; then
		total=$(awk -v t="$total" -v a="$avg" 'BEGIN { printf "%.10f\n", t + a }')
	((count++))
	fi
done

if [[ $count -gt 0 ]]; then
	average_of_averages=$(awk -v t="$total" -v c="$count" 'BEGIN { printf "%.10f\n", t / c }')
	echo "Latency: Average of Averages: $average_of_averages μs"
else
	echo "No averages were found."
fi


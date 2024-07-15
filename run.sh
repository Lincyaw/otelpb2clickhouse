#!/bin/bash

# Loop from 2 to 9
for i in {2..9}
do
  # Construct the file name based on the current iteration
  file_name="dejavus/dejavu_metric_d.pb_${i}.pb"

  # Execute the go run command with the constructed file name
  go run main.go --file=${file_name} --host=0.0.0.0 --port=4317 --keyValue=DataSource=dejavu_d -t 10
done

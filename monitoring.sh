#!/usr/bin/env bash
while [ true ]
do
  date
  sleep 60
  # Print RAM
  free -m
  # Print
  df -h
done

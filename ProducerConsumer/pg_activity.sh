#!/bin/bash

tempfile="ProducerConsumer/Pg_activity_Data/activities.csv"
sudo -u postgres pg_activity --output $tempfile -U postgres

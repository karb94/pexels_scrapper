#! /bin/sh

gcloud compute ssh "$1" -- "bash -c \"tic - << EOF
$(infocmp)
EOF\""


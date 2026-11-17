# Big-ML-MapReduce-Project

## Resources
- Original MapReduce Project:
https://github.com/10605/mapreduce-miniproject-s26
- Drive: https://drive.google.com/drive/u/2/folders/1HyjV_5ZbLzVtjlemQS31Of1P2weRxq67
- F26 Open-Ended Miniproject Guidelines: https://docs.google.com/document/d/1GCtal_u2QhtjLLqlS0Bt3q8u8nwLS1CeqT0hKRoKfQs/edit?usp=sharing

## Set-up Instructions
If setting things up for the first time, follow these steps:
- Install AWS CLI: Run `curl -fsSL https://awscli.amazonaws.com/v2/install.sh | bash` and amend PATH if it tells you to
- SSH Key: Run `mkdir -p ~/.ssh` and `ssh-keygen -t ed25519 -N '' -f "$HOME/.ssh/10605-wordcount"`
- Login: Run `export WC_SSH_KEY="$HOME/.ssh/10605-wordcount"` and `aws login --region us-east-1` every time you open a new terminal to work in
- Create S3 bucket: Create bucket with default settings `bigmlproject` in AWS
- Download data once: Run `python3.13 project.py fetch` and `python3.13 project.py publish --input data/prepared --s3-uri s3://bigmlproject/wordcount-v1/ --set-default`
- Delete the data: Run `rm -rf -- data/cache data/prepared` to free up space
- Baseline: Run `python project.py check` and `python3.13 project.py run --backend baseline --workers 4 --repetitions 1` and `python project.py report`

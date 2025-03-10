# py-ecom-search-module
Module for the Delivx Ecommerce App
## Installation
1. need to set env file on following path: "/usr/etc/"
2. Need to add virtual environment in project
    * run following command in terminal: virtualenv -p python3 enve
    * activate virtual env using following command: ". enve/bin/activate".
3. Need to install dependencies using following command
    ```bash
    pip install req.txt
    ```
4. need to install pm2 for start the django server: 
    * check npm is install or not and if not installed use following commands:
        1. sudo apt-get update
        2. sudo apt-get upgrade
        3. curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
        4. sudo apt-get install -y nodejs
    * install pm2 using following command:
        1. npm install pm2 -g
    * install pm2 logrotate for the rotate the log and log files:
        1. pm2 install pm2-logrotate
        2. pm2 set pm2-logrotate:max_size 100MB
        3. pm2 set pm2-logrotate:retain 7
5. need to set enve path in "ecosystem.config.js" file in "interpreter" variable
6. start pm2 server: pm2 start ecosystem.config.js      
7. need to start all kafka consumers which are in kafka_consumers folder
    1. need to set enve path in all "config.js" file which are in kafka_consumers folder in same variable "interpreter"
    2. start pm2 server: pm2 start categoryview.config.js, pm2 start favourite.config.js, pm2 start recentview.config.js


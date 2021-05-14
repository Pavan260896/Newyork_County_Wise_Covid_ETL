# Take_Home_Egen
Instructions for Execution

Option1: Multithreaded Python application for ETL Process
Steps to execute this application
1.	Pip install -r requirements.txt in “Option1” directory (to install required dependencies for this application)
2.	If we want to directly execute this application run starter.py file to start application in “Option1” folder
3.	If we want to schedule this application, please refer to instructions in “Scheduling Application” file in “Option1” folder
4.	Once the application is successfully executed, it will create a database file called “covid_data.db” in “Option1” directory. Open it with App DB Browser(SQLlite) to view tables inserted into it. If application does not exists install it
5.	Execute test_extract_transform.py for unit testing

Option2: Python application using Airflow platform
Steps to execute this application
1.	Install Docker and Docker Compose, if not available
2.	Navigate to directory Option2/airflow in terminal/cmd and run ‘docker-compose up -d’
3.	This command creates containers for Airflow and Postgres
4.	Open http://localhost:8080/ in web browser for Airflow webserver
5.	We would be to see ‘covid_data_loading’ DAG in DAGs section. By default it is scheduled to run at 9 AM everyday
6.	To manually start the execution, use the ‘Trigger DAG’ option

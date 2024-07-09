# Vk) RabbitMQ and PostgreSQL Integration
This project demonstrates the integration of RabbitMQ for message queuing and PostgreSQL for database storage using Python.
Prerequisites
Python 3.x installed
RabbitMQ installed and running locally
PostgreSQL installed and running locally
Python packages: pika, psycopg2 
Installation
Clone the repository:

git clone https://github.com/Rahimdzx/Vk.git
cd Vk



Configuration

RabbitMQ Configuration:

Ensure RabbitMQ is running locally.
Default connection parameters are used (localhost, guest/guest credentials).
PostgreSQL Configuration:

Ensure PostgreSQL is running locally.
Update connection details in main.py if necessary (dbname, user, password, host, port).
Usage :
python main.py

Sending Test Messages:

Modify and run send_message.py to send test messages to RabbitMQ:
python send_message.py



Contributing :
Contributions are welcome! Please fork the repository and submit pull requests.



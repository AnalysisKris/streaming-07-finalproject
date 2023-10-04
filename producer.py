"""
    This program sends a message to a queue on the RabbitMQ server.

    Author: Kristen Finley
    Date: September 20, 2023
"""

import pika
import sys
import webbrowser
import csv
import time
import os



# Declare constants
HOST = "localhost"
PORT = 9999
ADDRESS_TUPLE = (HOST, PORT)
SHOW_OFFER = True
INPUT_CSV = '/Users/kristenfinley/Documents/streaming-07-finalproject/breweries.csv'



# Configure logging
from util_logger import setup_logger

# If this is the main program being executed (and you're not importing it for its functions)
logger, logname = setup_logger(__file__)


# call a function to ask the user if they want to see the RabbitMQ admin webpage
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()


# create a blocking connection to the RabbitMQ server
conn = pika.BlockingConnection(pika.ConnectionParameters(HOST))
# use the connection to create a communication channel
ch = conn.channel()


# call a function to begin the main work of the program
def main_work():
    try:
        # delete the 3 existing queues (since queues will run multiple times)
        ch.queue_delete(queue="01-type")
        ch.queue_delete(queue="01-location")
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue="01-type", durable=True)
        ch.queue_declare(queue="02-location", durable=True)

        # Open the csv file for reading (with appropriate line endings
        # in case of Windows) and create a csv reader
        with open(INPUT_CSV, "r") as input_file:
            reader = csv.reader(input_file, delimiter=",")
            # skip the header row
            header = next(reader)
            # for each row in the file
            for row in reader:
                # get row variables
                id,name,brewery_type,address_1,address_2,address_3,city,state_province,postal_code,country,phone,website_url,longitude,latitude = row

                # create a tuples of messages to send to the queue
                message1 = f"Name: {name}, Type: {brewery_type}, Website: {website_url}"
                message2 = f"Name: {name}, City: {city}, State: {state_province}, Country: {country}"

                # Create a binary message from our tuples
                # Ensure indintation is within look, else only last row will send
                send_message("01-type", message1)
                send_message("02-location", message2)

    # except, in the event of an error OR user stops the process, do this
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("User interrupted the listening process.")
        sys.exit(0)
    # close the connection to the server
    finally:
        logger.info("Closing connection. Goodbye.")
        conn.close()


def send_message(queue_name, message):
    """
    Defines function to send messages to the queue each execution.
    This process runs and finishes.

    Parameters:
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    # use the channel to publish a message to the queue
    # every message passes through an exchange
    ch.basic_publish(exchange="", routing_key=queue_name, body=f"{message}")
    # log sent message
    logger.info(f"Sent to Queue: {queue_name}; {message}")
    # print a message to the console for the user
    print(" [*] Sending messages to queues. To exit press CTRL+C")

    # wait 1/2 seconds before sending the next message to the queue
    time.sleep(0.5)


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call your offer admin function()
    if SHOW_OFFER == True:
        # ask the user if they'd like to open the RabbitMQ Admin site
        offer_rabbitmq_admin_site()

    # call your main() function and catch KeyboardInterrupt during program shutdown.
    main_work()

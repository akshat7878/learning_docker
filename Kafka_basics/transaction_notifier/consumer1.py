from kafka import KafkaConsumer
import json

def user_login_and_listen():
    print("=== Transcation System ===")
        
    print(f"Listening for real-time alerts...")
               
    # Initialize Kafka Consumer listening to the notification topic
    consumer = KafkaConsumer(
        'audit-log',
        bootstrap_servers= ['kafka:9092'], # Adjust to 'kafka:9092' if running inside docker
        auto_offset_reset= 'latest',
        value_deserializer= lambda x: json.loads(x.decode('utf-8'))
    )
         
    for message in consumer:
        alert_data = message.value
            
        # Only show alerts meant for the logged-in user
        print(f"\nUser Name: {alert_data.get('name')}")
        print(f"Suspicious Transaction ID: {alert_data.get('tx_id')}")
        print(f"Amount: ${alert_data.get('amount'):.2f}\n")
    
if __name__ == "__main__":
    user_login_and_listen()
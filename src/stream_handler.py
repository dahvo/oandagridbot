
from oandapyV20.endpoints import pricing, transactions
import time, logging, threading



class StreamHandler:
    def __init__(self, stream_type, main_bot, instruments):
        self.api = main_bot.api
        self.stream_type = stream_type
        self.account_id = main_bot.account_id
        self.instruments = instruments
        self.running = True
        self.stream = self.get_stream()

    def get_stream(self):
        if self.stream_type == 'pricing':
            return pricing.PricingStream(accountID=self.account_id, params={"instruments": ",".join(self.instruments)})
        elif self.stream_type == 'transactions':
            return transactions.TransactionsStream(accountID=self.account_id)

    def run_stream(self):
        self.stream_thread = threading.Thread(target=self.start_stream)
        self.stream_thread.start()
        logging.info("Stream started.")

    def start_stream(self):
        while self.running:
            try:
                for msg in self.api.request(self.stream):
                    self.handle_message(msg)
            except Exception as e:
                logging.error(f"Error occurred: {e}")
                time.sleep(10)  # Simple reconnection delay

    def stop_stream(self):
        self.running = False
        self.stream_thread.join()  # Wait for the streaming thread to finish
        logging.info("Stream stopped.")

    def handle_message(self, msg):
        # Process the incoming message here
        logging.info(msg)
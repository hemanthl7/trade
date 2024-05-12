from SmartApi import SmartConnect
import pyotp
import pandas as pd
import concurrent.futures
import SmartApi.smartExceptions as se
from logzero import logger
import logging


log = logging.getLogger(__name__)

class OrderProcessor:

    def __init__(self, users_path):
        df = pd.read_csv(users_path)
        self.users = {}
        self.user_sessions = {}
        for index, row in df.iterrows():
            user_data = row.to_dict()
            user_connect = SmartConnect(api_key=user_data['apiKey'])
            user_session = user_connect.generateSession(user_data['userid'], user_data['pin'],
                                                        pyotp.TOTP(user_data['totpKey']).now())
            self.user_sessions[user_data['userid']] = user_session
            self.users[user_data['userid']] = user_connect

    def process(self, csv):
        df = pd.read_csv(csv)
        orders = list()
        for index, row in df.iterrows():
            order = row.to_dict()
            orders.append(order)
        self.run_orders(orders)

    def run_orders(self, orders):
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for order in orders:
                futures.append(executor.submit(self.execute_orders, order_params=order))
            for future in concurrent.futures.as_completed(futures):
                logger.info(future.result())

    def execute_orders(self, order_params):
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_id, user_connect, in self.users.items():
                futures.append(executor.submit(self.execute_user_order, user_id=user_id, user_connect=user_connect,
                                               order_params=order_params))
            for future in concurrent.futures.as_completed(futures):
                logger.info(future.result())

    def execute_user_order(self, user_id, user_connect, order_params):
        try:
            orderId = user_connect.placeOrder(order_params)
            logger.info("The order id is: {}".format(orderId))
        except se.TokenException as e:
            tokenSet = user_connect.renewAccessToken()
            user_connect.setRefreshToken(tokenSet['refreshToken'])
            user_connect.setAccessToken(tokenSet['jwtToken'])
            logger.info("{} User Session issue: {}".format(user_id, e.message))
            self.execute_user_order(user_id, user_connect, order_params)

        except Exception as e:
            logger.info("Order placement failed: {}".format(e.message))

    def stop_sessions(self):
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_id, user_connect, in self.users.items():
                futures.append(executor.submit(user_connect.terminateSession, clientCode=user_id))
            for future in concurrent.futures.as_completed(futures):
                logger.info(future.result())

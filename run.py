import watchdog.events
import watchdog.observers
import time
from orderProcesser import OrderProcessor
import shutil

src_path = r"./orders/toplace"
des_path = r"./orders/done"
users_path = r"./uselogin/users.csv"


class Handler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self, order_processor):
        # Set the patterns for PatternMatchingEventHandler
        watchdog.events.PatternMatchingEventHandler.__init__(self, patterns=['*.csv'],
                                                             ignore_directories=True, case_sensitive=False)
        self.order_processor = order_processor

    def on_created(self, event):
        print("Watchdog received created event - % s." % event.src_path)
        # Event is created, you can process it now
        self.order_processor.process(event.src_path)
        shutil.move(event.src_path, des_path)


if __name__ == "__main__":
    order_processor = OrderProcessor(users_path)
    event_handler = Handler(order_processor)
    observer = watchdog.observers.Observer()
    observer.schedule(event_handler, path=src_path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except BaseException as e:
        print(e)
        order_processor.stop_sessions()
        observer.stop()
    observer.join()

import os
import glob
import datetime


class MyLogger:
    def __init__(self, log_file=None, log_level="INFO", backup_count=48*12):
        self.log_level = log_level.upper()
        self.log_file = log_file
        self.backup_count = backup_count
        self.log_format = "{asctime} [{levelname}] - {message}"
        self.date_format = "%Y-%m-%d %H:%M:%S"
        self.handlers = []
        self.log_levels = ["INFO", "WARNING", "ERROR", "CRITICAL", "DEBUG"]

        if log_file:
            self.add_file_handler(log_file)
        self.add_console_handler()

        self.delete_old_files()

    def add_console_handler(self):
        console_handler = {
            'handler': 'console',
            'level': self.log_level,
            'formatter': self.log_format
        }
        self.handlers.append(console_handler)

    def add_file_handler(self, log_file):
        file_handler = {
            'handler': 'file',
            'level': self.log_level,
            'formatter': self.log_format,
            'filename': log_file
        }
        self.handlers.append(file_handler)

    def format_message(self, level, message):
        now = datetime.datetime.now()
        log_record = {
            'asctime': now.strftime(self.date_format),
            'levelname': level,
            'message': message
        }
        return log_record

    def delete_old_files(self):
        folder_path = os.path.dirname(self.log_file)
        files = sorted(glob.glob(os.path.join(folder_path, '*')), key=os.path.getmtime)
        if len(files) >= self.backup_count:
            for file_path in files[self.backup_count-1:]:
                try:
                    os.remove(file_path)
                except OSError as e:
                    print(f"Error deleting {file_path}: {e}")

    def log(self, level, message):
        for handler_info in self.handlers:
            handler_type = handler_info['handler']
            handler_level = handler_info['level']
            handler_formatter = handler_info['formatter']

            if level.upper() not in self.log_levels or handler_level not in self.log_levels:
                raise ValueError("Invalid log level")
            elif self.log_levels.index(level.upper()) >= self.log_levels.index(handler_level):
                s = handler_formatter.format(**self.format_message(level, message))
                if handler_type == 'console':
                    print(s)
                elif handler_type == 'file':
                    with open(handler_info['filename'], 'a', encoding='utf-8') as file:
                        file.write(s + '\n')

    def info(self, message):
        self.log("INFO", message)

    def warning(self, message):
        self.log("WARNING", message)

    def error(self, message):
        self.log("ERROR", message)

    def critical(self, message):
        self.log("CRITICAL", message)

    def debug(self, message):
        self.log("DEBUG", message)


if __name__ == "__main__":
    log_file_path = os.path.join(os.getcwd(), "example.log")
    my_logger = MyLogger(log_file=log_file_path, log_level="DEBUG")

    my_logger.info("This is an info message.")
    my_logger.warning("This is a warning message.")
    my_logger.error("This is an error message.")
    my_logger.critical("This is a critical message.")
    my_logger.debug("This is a debug message.")

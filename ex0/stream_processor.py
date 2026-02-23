# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  stream_processor.py                               :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: stmaire <stmaire@student.42.fr>           +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/02/23 08:49:19 by stmaire         #+#    #+#               #
#  Updated: 2026/02/23 14:57:20 by stmaire         ###   ########.fr        #
#                                                                           #
# ************************************************************************* #

from abc import ABC, abstractmethod
from typing import Any
import sys


class DataProcessor(ABC):
    """Create an abstract class to process data"""
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output : Processed {result} "


class NumericProcessor(DataProcessor):
    """Check and process numeric data"""
    def validate(self, data: Any) -> bool:
        try:
            if len(data) == 0:
                return False
            for d in data:
                float(d)
            return True
        except (TypeError, ValueError):
            print("Invalid numeric data", file=sys.stderr)
            return False

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "invalid numeric data"
        total = sum(data)
        length = len(data)
        avg = total / length if length > 0 else 0.0
        result = (f"{length} numeric values, sum={total}, avg={avg:.1f}")
        return self.format_output(result)


class TextProcessor(DataProcessor):
    """Check and process text data"""
    def validate(self, data: Any) -> bool:
        try:
            data.strip()
            return True
        except (AttributeError, TypeError):
            print("Invalid text data", file=sys.stderr)
            return False

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "Invalid string data"
        words = data.split()
        result = f"text: {len(data)} characters, {len(words)} words"
        return self.format_output(result)


class LogProcessor(DataProcessor):
    """Check and process log data"""
    def validate(self, data: Any) -> bool:
        try:
            data_up = data.upper()
            return "ERROR" in data_up or "INFO" in data_up
        except (AttributeError, TypeError):
            print("Invalid log data", file=sys.stderr)
            return False

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "Invalid log data"
        parts = data.split(":")
        level = parts[0].strip().upper() if len(parts) > 1 else "LOG"
        message = parts[1]. strip() if len(parts) > 1 else data
        result = f"{level} level detected: {message}"
        return self.format_output(result)

    def format_output(self, result: str) -> str:
        if "ERROR" in result:
            return f"[ALERT] {result}"
        else:
            return f"[INFO] {result}"


def run_data_processor_demo() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    num_processor = NumericProcessor()
    numeric_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {numeric_data}")
    if num_processor.validate(numeric_data):
        print("Validation: Numeric data verified")
    print(num_processor.process(numeric_data) + "\n")

    print("Initializing Text Processor...")
    text_processor = TextProcessor()
    text_data = "Hello Nexus World"
    print(f'Processing data: "{text_data}"')
    if text_processor.validate(text_data):
        print("Validation: Text data verified")
    print(text_processor.process(text_data) + "\n")

    print("Initializing Log Processor...")
    log_processor = LogProcessor()
    log_data = "ERROR: Connection timeout"
    print(f'Processing data: "{log_data}"')
    if log_processor.validate(log_data):
        print("Validation: Log entry verified")
    print(log_processor.process(log_data))

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    processors = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]
    data = [
        [1, 2, 3],
        "Welcome home",
        "INFO: System ready"
    ]
    for i in range(len(processors)):
        if processors[i].validate(data[i]):
            print(processors[i].process(data[i]))

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    run_data_processor_demo()

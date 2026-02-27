# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  data_stream.py                                    :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: stmaire <stmaire@student.42.fr>           +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/02/23 15:12:33 by stmaire         #+#    #+#               #
#  Updated: 2026/02/27 08:35:03 by stmaire         ###   ########.fr        #
#                                                                           #
# ************************************************************************* #

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if not criteria:
            return data_batch
        return [d for d in data_batch if str(criteria) in str(d)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
                "stream_id": self.stream_id,
                "elements_processed": self.count
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """
        Filter valid sensor data from data batch.

        Handle high-priority criteria
        """
        try:
            checked_list = []
            valid_keys = ["temp", "pressure", "humidity"]

            if criteria == "High-priority":
                for d in data_batch:
                    if isinstance(d, dict) and "temp" in d:
                        try:
                            if float(d["temp"]) > 35:
                                priority_dict = {
                                    key: value
                                    for key, value
                                    in d.items()
                                    if key in valid_keys
                                    }
                                checked_list.append(priority_dict)
                        except (ValueError, TypeError):
                            continue

            else:
                for d in data_batch:
                    if isinstance(d, dict):
                        try:
                            checked_dict = {
                                key: float(value)
                                for key, value in d.items()
                                if key in valid_keys
                                }
                            if checked_dict:
                                checked_list.append(checked_dict)
                        except (ValueError, TypeError):
                            continue

            return checked_list

        except (ValueError, TypeError):
            print(f"Error: Criteria '{criteria}' "
                  f"for data format is invalid for SensorStream.")
            return []

    def process_batch(self, data_batch: List[Any]) -> str:
        """
        Process a batch of sensor data.

        Calculates the average temperature from valid readings, updates the
        internal counter, and returns a formatted string for display.
        """

        checked_list = self.filter_data(data_batch)
        data_count = len(checked_list)
        self.count += data_count

        if data_count == 0:
            return "No valid data in batch."

        temps = [d["temp"] for d in checked_list if "temp" in d]
        temp_count = len(temps)
        avg_temp = sum(temps) / temp_count if temp_count != 0 else 0.0

        formatted_list = []
        for d in checked_list:
            for key, value in d.items():
                # :g remove unnecessary .0
                value_str = f"{value:g}"
                formatted_list.append(f"{key}:{value_str}")

        header = f"Processing sensor batch: [{', '.join(formatted_list)}]"
        analysis = (
            f"Sensor analysis: {data_count}"
            f"readings processed, avg temp: {avg_temp:.1f}°C"
        )

        result = header + "\n" + analysis
        return (result)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "Environmental Data"
        return (stats)


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """
        Filter valid transactions data from data batch.

        Handle high-priority criteria
        """
        try:
            checked_list = []
            valid_keys = ["buy", "sell"]

            if criteria == "High-priority":
                for d in data_batch:
                    if isinstance(d, dict):
                        try:
                            buy_value = d.get("buy", 0)
                            sell_value = d.get("sell", 0)
                            if int(buy_value) > 100 or int(sell_value) > 100:
                                priority_dict = {
                                    key: value
                                    for key, value in d.items()
                                    if key in valid_keys
                                    }
                                checked_list.append(priority_dict)
                        except (ValueError, TypeError):
                            continue

            else:
                for d in data_batch:
                    if isinstance(d, dict):
                        try:
                            checked_dict = {
                                key: int(value)
                                for key, value in d.items()
                                if key in valid_keys
                                }
                            if checked_dict:
                                checked_list.append(checked_dict)
                        except (ValueError, TypeError):
                            continue

            return checked_list

        except (ValueError, TypeError):
            print(f"Error: Criteria '{criteria}' "
                  f"for data format is invalid for SensorStream.")
            return []

    def process_batch(self, data_batch: List[Any]) -> str:
        """
        Process a batch of transactions data.

        Calculates the transactions net flow from valid readings, updates the
        internal counter, and returns a formatted string for display.
        """
        checked_list = self.filter_data(data_batch)
        data_count = len(checked_list)
        self.count += data_count

        if data_count == 0:
            return "No valid data in batch."

        sells = [d["sell"] for d in checked_list if "sell" in d]
        buys = [d["buy"] for d in checked_list if "buy" in d]
        net_flow = sum(buys) - sum(sells)

        formatted_list = []
        for d in checked_list:
            for key, value in d.items():
                formatted_list.append(f"{key}:{value}")
        if net_flow > 0:
            net_flow = f"+{int(net_flow)}"

        header = (
            f"Processing transaction batch: "
            f"[{', '.join(formatted_list)}]"
        )
        analysis = (
            f"Transaction analysis: "
            f"{data_count} operations, "
            f"net flow: {net_flow} units"
        )

        result = header + "\n" + analysis
        return result

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "Financial Data"
        return (stats)


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter valid system events data from data batch."""
        checked_list = []
        valid_words = ["login", "error", "logout"]

        for e in data_batch:
            try:
                if isinstance(e, str):
                    word = e.strip()
                    if word in valid_words:
                        checked_list.append(word)
            except (ValueError, TypeError):
                continue

        return checked_list

    def process_batch(self, data_batch: List[Any]) -> str:
        """
        Process a batch of events data.

        Counts events and errors, updates the
        internal counter, and returns a formatted string for display.
        """

        checked_list = self.filter_data(data_batch)
        data_count = len(checked_list)
        self.count += data_count

        if data_count == 0:
            return "No valid data in batch."

        error_count = len([e for e in checked_list if e.lower() == "error"])
        header = f"Processing event batch: [{', '.join(checked_list)}]"

        formatted_events = "events" if data_count > 1 else "event"
        analysis = (
            f"Event analysis: "
            f"{data_count} {formatted_events}, "
            f"{error_count} error detected"
        )
        result = header + "\n" + analysis

        return result

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "System Events"
        return (stats)


class StreamProcessor:
    def __init__(self, stream_tools: List[DataStream]) -> None:
        self.stream_tools = stream_tools

    def process_all_types(self, data_stream: Dict[str, Any]) -> None:
        """Process any type of data"""

        for tool in self.stream_tools:

            batch = data_stream.get(tool.stream_id, [])
            tool.process_batch(batch)
            stats = tool.get_stats()

            # garanty for mypy : value is an str
            stats_type = str(stats.get("type", ""))
            label = "readings" if "Environmental" in stats_type else\
                "operations" if "Financial" in stats_type else\
                "events"
            stream_name = "Sensor" if "Environmental" in stats_type else\
                "Transaction" if "Financial" in stats_type else\
                "Event"

            print(f"- {stream_name} data: "
                  f"{stats['elements_processed']} {label} processed")

    def high_security_process(self, data_stream: Dict[str, Any]) -> None:
        """Handle high-security filter from all types of data"""

        final_message = []

        for tool in self.stream_tools:

            batch = data_stream.get(tool.stream_id, [])

            high_priority_data = tool.filter_data(
                batch,
                criteria="High-priority"
                )
            count = len(high_priority_data)

            stats = tool.get_stats()
            stats_type = str(stats.get("type", ""))
            if "Environmental" in stats_type:
                message = (
                    "critical sensor alert" if count <= 1
                    else "critical sensor alerts"
                )
                final_message.append(f"{count} {message}")
            elif "Financial" in stats_type:
                message = (
                    "large transaction" if count <= 1
                    else "large transactions"
                )
                final_message.append(f"{count} {message}")

        print(f"Filtered results: {', '.join(final_message)}")


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    events_tool = EventStream("EVENT_001")

    data_stream = {
        "SENSOR_001": [
            {"temp": 40.0},
            {"temp": 38.5}
        ],
        "TRANS_001": [
            {"buy": 50.0},
            {"sell": 1200.0},
            {"buy": 15.0},
            {"sell": 30.0}
        ],
        "EVENT_001": ["login", "error", "logout"]
    }

    print("\nInitializing Sensor Stream...")
    sensor_tool = SensorStream("SENSOR_001")
    stats_sensor = sensor_tool.get_stats()
    print(f"Stream ID: {sensor_tool.stream_id}, Type: {stats_sensor['type']}")
    data_batch1 = [{"temp": 22.5}, {"humidity": 65}, {"pressure": 1013}]
    print(sensor_tool.process_batch(data_batch1))

    print("\nInitializing Transaction Stream...")
    transactions_tool = TransactionStream("TRANS_001")
    stats_trans = transactions_tool.get_stats()
    print(f"Stream ID: {transactions_tool.stream_id}, Type: "
          f"{stats_trans['type']}")
    data_batch2 = [{"buy": 100}, {"sell": 150}, {"buy": 75}]
    print(transactions_tool.process_batch(data_batch2))

    print("\nInitializing Event Stream...")
    event_tool = EventStream("EVENT_001")
    stats_event = event_tool.get_stats()
    print(f"Stream ID: {event_tool.stream_id}, Type: {stats_event['type']}")
    data_batch3 = ["login", "error", "logout"]
    print(event_tool.process_batch(data_batch3))

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print("\nBatch 1 Results:")

    sensor_tool.count = 0
    transactions_tool.count = 0
    event_tool.count = 0

    processor = StreamProcessor([sensor_tool, transactions_tool, events_tool])
    processor.process_all_types(data_stream)

    print("\nStream filtering active: High-priority data only")
    processor.high_security_process(data_stream)

    print("\nAll streams processed successfully. Nexus throughput optimal.")

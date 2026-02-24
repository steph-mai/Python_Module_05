# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  data_stream.py                                    :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: stmaire <stmaire@student.42.fr>           +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/02/23 15:12:33 by stmaire         #+#    #+#               #
#  Updated: 2026/02/24 17:21:12 by stmaire         ###   ########.fr        #
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
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
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

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        """Filter valid data from data batch. Handle high-priority criteria"""
        try:
            checked_list = []
            valid_keys = ["temp", "pressure", "humidity"]
            
            if criteria == "High-priority":      
                for d in data_batch:
                    if isinstance(d, dict) and "temp" in d:
                        try:
                            if float(d["temp"]) > 35:
                                priority_dict = {key: value for key, value in d.items() if key in valid_keys}
                                checked_list.append(priority_dict)
                        except (ValueError, TypeError):
                            continue
                
            else:
                for d in data_batch:
                    if isinstance(d, dict):
                        try:
                            checked_dict = {key: float(value) for key, value in d.items() if key in valid_keys}
                            if checked_dict:
                                checked_list.append(checked_dict)
                        except (ValueError, TypeError):
                                        continue
            
            return checked_list 
                       
        except (ValueError, TypeError): 
            print(f"Error: Criteria '{criteria}' or data format is invalid for SensorStream.")
            return[]

    def process_batch(self, data_batch: List[Any]) -> str:
        checked_list = self.filter_data(data_batch)
        data_count = len(checked_list)
        self.count += data_count
        
        if data_count == 0:
            return "No valid data in batch."
        
        temps = [d["temp"] for d in checked_list if "temp" in d]
        avg_temp = sum(temps) / data_count if data_count != 0 else 0.0

        formatted_list = []
        for d in checked_list:
            for key, value in d.items():
                formatted_list.append(f"{key}:{value}")
            
        header = f"Processing sensor batch: [{', '.join(formatted_list)}]"
        analysis = f"Sensor analysis: {data_count} readings processed, avg temp: {avg_temp:.1f}°C"
        
        result = header + "\n" + analysis
        return (result)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "Environmental Data"
        return (stats)

  
class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        try:
            checked_list = []
            valid_keys = ["buy", "sell"]
            
            if criteria == "High-priority":      
                for d in data_batch:
                    if isinstance(d, dict):
                        try:
                            buy_value = d.get("buy", 0)
                            sell_value = d.get("sell", 0)
                            if float(buy_value) > 100 or float(sell_value) > 100:
                                    priority_dict = {key: value for key, value in d.items() if key in valid_keys}
                                    checked_list.append(priority_dict)
                        except (ValueError, TypeError):
                            continue
                        
            else:
                for d in data_batch:
                    if isinstance(d, dict):
                        try:
                            checked_dict = {key: float(value) for key, value in d.items() if key in valid_keys}
                            if checked_dict:
                                checked_list.append(checked_dict)
                        except (ValueError, TypeError):
                                        continue
            
            return checked_list 
                       
        except (ValueError, TypeError): 
            print(f"Error: Criteria '{criteria}' or data format is invalid for SensorStream.")
            return[]

    def process_batch(self, data_batch: List[Any]) -> str:
        checked_list = self.filter_data(data_batch)
        data_count = len(checked_list)
        self.count += data_count
        
        if data_count == 0:
            return "No valid data in batch."
        
        sells = [d["sell"] for d in checked_list if "sell" in d]
        buys = [d["buy"] for d in checked_list if "buy" in d]
        net_flow = sum(sells) - sum(buys)
                
        formatted_list = []
        for d in checked_list:
            for key, value in d.items():
                 formatted_list.append(f"{key}:{value}")
        if net_flow > 0:
            net_flow = f"+{int(net_flow)}"
                 
        header = f"Processing transaction batch: [{(", ").join(formatted_list)}]"
        analysis = f"Transaction analysis: {data_count} operations, net flow: {net_flow} units"

        result = header + "\n" + analysis
        return result
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "Financial Data"
        return (stats)


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
          super().__init__(stream_id)
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
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
        checked_list = self.filter_data(data_batch)
        data_count = len(checked_list)
        self.count += data_count
        
        if data_count == 0:
            return "No valid data in batch."                   

        error_count = 0
        for e in checked_list:
             if e.lower() == "error":
                  error_count += 1
        
        header = f"Processing event batch: [{", ".join(checked_list)}]"          
        formatted_events = "events" if data_count > 1 else "event"
        analysis = f"Event analysis: {data_count} {formatted_events}, {error_count} error detected"
        result = header + "\n"+ analysis

        return result
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "System Events"
        return (stats)

class StreamProcessor:
    def __init__(self, streams: List[DataStream]) -> None:
          self.streams = streams

    def process_all_types(self, data_stream: Dict[str, Any]):
        for stream in self.streams:
            batch = data_stream.get(stream.stream_id, [])
            stream.process_batch(batch)
            stats = stream.get_stats()
        
        label = "readings" if "Environmental" in stats else\
                "operations" if "Financial" in stats else "events" if "Events" in stats
        
        #TODO affichage
            
    
     
        
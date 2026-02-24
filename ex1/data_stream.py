# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  data_stream.py                                    :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: stmaire <stmaire@student.42.fr>           +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/02/23 15:12:33 by stmaire         #+#    #+#               #
#  Updated: 2026/02/24 14:49:36 by stmaire         ###   ########.fr        #
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
        data_number = len(checked_list)
        self.count += data_number
        
        if data_number == 0:
            return "No valid data in batch."
        
        temps = [d["temp"] for d in checked_list if "temp" in d]
        avg_temp = sum(temps) / data_number if data_number != 0 else 0.0

        formated_items = ""
        for c in checked_list:
            for item in c.items():
            
        header = f"Processing sensor batch: {checked_list}"
        analysis = f"Sensor analysis: {data_number} readings processed, avg temp: {avg_temp:.1f}"
        
        result = header + "\n" + analysis
        return (result)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "Environmental Data"
        return (stats)
    
class TransactionStream(DataStream):
    def __init__(self, stream_id) -> None:
        super().__init__ (stream_id: str)

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        try:
            checked_data = []
            limit = float(criteria) if criteria is not None else None
            for d in data_batch:
                valid = float(d) 
                if limit == None or valid > limit:
                    checked_data.append(valid)
    
        
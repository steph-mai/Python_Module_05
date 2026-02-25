from abc import ABC, abstractmethod
from typing import List, Dict, Any, Protocol, Union
from collections import Counter

class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id):
        self.pipeline_id = pipeline_id
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage):
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        parsed_data = self.stages[0].process(data)
        print(f"Input: {parsed_data}")

        return parsed_data


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        pass


class StreamAdaper(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        pass


class InputStage:
    def process(self, data: Any) -> Any:
        if data is None:
            raise ValueError("Invalid data format: Data is None")

        if isinstance(data, dict):
            if "sensor" not in data or "value" not in data:
                raise ValueError("Invalid data format: Missing mandatory keys")
            
            if not isinstance(data["value"], (int, float)):
                raise ValueError("Invalid data format: Value must be a number")
            
            return data

        if isinstance(data, str):
            data = data.strip()
            if len(data) == 0:
                raise ValueError("Invalid data format: Empty string")
            return data
            
        return data

class TransformStage:
    def process(self, data: Any) -> Any:
        pass


class Outputstage:
    def process(self, data: Any) -> Any:
        pass

class NexusManager:
    def __init__(self):
        self.pipelines: List[ProcessingPipeline] = []
        self.stats = Counter()

    def add_pipeline(self, pipeline: ProcessingPipeline):
        self.pipelines.append(pipeline)
    
    def process(self, data: Any) -> Any:
        pass

if __name__ == "__main__":
    input_tool = InputStage()
    transform_tool = TransformStage()
    output_tool = Outputstage()

    pipeline = JSONAdapter("PIPELINE_JSON_1")

    pipeline.add_stage(input_tool)
    pipeline.add_stage(transform_tool)
    pipeline.add_stage(output_tool)

    pipeline.process('{"sensor": "temp", "value": 23.5, "unit": "°C"}')
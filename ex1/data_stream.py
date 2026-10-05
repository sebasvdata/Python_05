import typing
import abc


class NoDataError(Exception):
    pass


class DataProcessor(abc.ABC):

    def __init__(self) -> None:
        self._data: list[str] = []
        self._count = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        ...

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if len(self._data) == 0:
            raise NoDataError("No data available to output")
        info = self._data.pop(0)
        out = (self._count, info)
        self._count += 1
        return out


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True

        elif isinstance(data, list):
            if len(data) == 0:
                return False

            for element in data:
                if isinstance(
                    element, (int, float)
                    ) and not isinstance(
                        element, bool
                        ):
                    continue
                else:
                    return False
            return True

        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, (int, float)):
            self._data.append(str(data))
        else:
            for element in data:
                self._data.append(str(element))


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str) and not isinstance(data, bool):
            if len(data) == 0:
                return False
            else:
                return True

        elif isinstance(data, list):
            if len(data) == 0:
                return False

            for element in data:
                if isinstance(
                    element, str
                    ) and not isinstance(
                        element, bool
                        ):
                    continue
                else:
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, str):
            self._data.append(data)
        elif isinstance(data, list):
            for element in data:
                self._data.append(element)


class LogProcessor(DataProcessor):
    def _validate_one(self, data: typing.Any) -> bool:
        if not isinstance(data, dict):
            return False

        if len(data) != 2:
            return False

        if "log_level" not in data or "log_message" not in data:
            return False

        for key, value in data.items():
            if not isinstance(key, str):
                return False
            if not isinstance(value, str):
                return False
            if len(value) == 0:
                return False

        return True

    def validate(self, data: typing.Any) -> bool:
        if self._validate_one(data):
            return True

        if isinstance(data, list):
            if len(data) == 0:
                return False

            for one in data:
                if not self._validate_one(one):
                    return False
            return True

        return False

    def ingest(
        self, data: dict[str, str] | list[dict[str, str]]
    ) -> None:
        if self.validate(data):
            if isinstance(data, dict):
                self._data.append(
                     data["log_level"] + ': ' + data["log_message"]
                     )
            else:
                for one in data:
                    self._data.append(
                         one["log_level"] + ': ' + one["log_message"]
                         )
        else:
            raise ValueError("data is not dict,list of dict")


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for data in stream:
            for proc in self._processors:
                if proc.validate(data):
                    proc.ingest(data)
                    break
            else:
                print("DataStream error - "
                      f"Can't process element in stream: {data}")

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
        else:
            for proc in self._processors:
                proc_name = proc.__class__.__name__.replace(
                    "Processor", " Processor")
                proc_total = proc._count + len(proc._data)
                print(f"{proc_name}: "
                      f"total {proc_total} "
                      f"items processed, remaining {len(proc._data)} "
                      "on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")

    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)

    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {
                'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'
            },
            {
                'log_level': 'INFO',
                'log_message': 'User wil is connected'
            }
        ],
        42,
        ['Hi', 'five']
    ]

    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)

    stream.print_processors_stats()

    print("\nRegistering other data processors", end="")
    text_proc = TextProcessor()
    log_proc = LogProcessor()

    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    print("\nSend the same batch again", end="")
    stream.process_stream(batch)

    stream.print_processors_stats()

    print("\nConsume some elements from "
          "the data processors: Numeric 3, Text 2, Log 1", end="")
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    for _ in range(1):
        log_proc.output()

    stream.print_processors_stats()

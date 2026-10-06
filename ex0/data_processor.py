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


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    number = NumericProcessor()

    print(f"Trying to validate input '42': {number.validate(42)}")
    print(f"Trying to validate input 'Hello': {number.validate('hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        number.ingest('foo')
    except ValueError as error:
        print(f"Got exception: {error}")

    print("Processing data: [1, 2, 3, 4, 5]")
    number.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for out in range(3):
        result = number.output()
        print(f"Numeric value {result[0]}: {result[1]}")

    print("\nTesting Text Processor...")
    text = TextProcessor()

    print(f"Trying to validate input '42': {text.validate(42)}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    text.ingest(['Hello', 'Nexus', 'World'])

    print("Extracting 1 value...")
    result = text.output()
    print(f"Text value {result[0]}: {result[1]}")

    print("\nTesting Log Processor...")
    log = LogProcessor()

    print(f"Trying to validate input 'Hello': {log.validate('hello')}")
    log_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f"Processing data: {log_data}")
    log.ingest(log_data)

    print('Extracting 2 values...')
    result = log.output()
    print(f"Log entry {result[0]}: {result[1]}")
    result = log.output()
    print(f"Log entry {result[0]}: {result[1]}")

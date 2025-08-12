import typing
import random
import math


class DebugPopulateSeeder:
    """Wrapper around DEBUG POPULATE with fuzzy key sizes and a balanced type mix"""

    DEFAULT_TYPES = ["STRING", "LIST", "SET", "HASH", "ZSET"]

    def __init__(
        self,
        key_target=10_000,
        data_size=100,
        variance=5,
        samples=10,
        collection_size=None,
        types: typing.Optional[typing.List[str]] = None,
        seed=None,
    ):
        self.types = DebugPopulateSeeder.DEFAULT_TYPES if types is None else types
        self.uid = 1
        self.key_target = key_target
        self.data_size = data_size
        self.variance = variance
        self.samples = samples

        if collection_size is None:
            self.collection_size = data_size ** (1 / 3)
        else:
            self.collection_size = collection_size

    def run(self, client):
        """Run with specified options until key_target is met"""
        samples = [
            (dtype, f"k-s{self.uid}u{i}-") for i, dtype in enumerate(self.types * self.samples)
        ]

        # Handle samples in chuncks of 24 to not overload client pool and instance
        for dtype, prefix in samples:
            self._run_unit(client, dtype, prefix)

    def _run_unit(self, client, dtype: str, prefix: str):
        key_target = self.key_target // (self.samples * len(self.types))
        if dtype == "STRING":
            dsize = random.uniform(self.data_size / self.variance, self.data_size * self.variance)
            csize = 1
        else:
            csize = self.collection_size
            csize = math.ceil(random.uniform(csize / self.variance, csize * self.variance))
            dsize = self.data_size // csize

        args = ["DEBUG", "POPULATE", key_target, prefix, math.ceil(dsize)]
        args += ["RAND", "TYPE", dtype, "ELEMENTS", csize]
        return client.execute_command(*args)

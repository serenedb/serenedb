#!/usr/bin/python3

import argparse
import os
import json
import random
import time
import string

from dataclasses import dataclass
from typing import TextIO

class Config:
    DocCount = 20000
    DataSize = 200
    BigDataDocCount = 10
    BigDataSize = 20000
    ResourceRootPath = "resources/tests/iresearch"
    GoldenRootPath = f"{ResourceRootPath}/phrase_filter_bench_test_golden"
    BenchResourceFilePrefix = "interval_bench_"
    BigDataBenchResourceFilePrefix = f"{BenchResourceFilePrefix}big_data_"

    resource_types = {
        "RepeatPhrase": "repeat.json",
        "FreqsEqual": "freqs_equal.json",
        "FreqsDiscrete": "freqs_discrete.json"
    }

    for resource_type, file in resource_types.items():
        locals()[f"{resource_type}BenchResource"] = f"{ResourceRootPath}/{BenchResourceFilePrefix}{file}"
        locals()[f"Golden{resource_type}BenchResource"] = f"{GoldenRootPath}/{BenchResourceFilePrefix}{file}.golden"
        locals()[f"BigData{resource_type}BenchResource"] = f"{ResourceRootPath}/{BigDataBenchResourceFilePrefix}{file}"
        locals()[f"GoldenBigData{resource_type}BenchResource"] = f"{GoldenRootPath}/{BigDataBenchResourceFilePrefix}{file}.golden"


def parse_file(path: str):
    with open(path) as f:
        data = json.loads(f.read())
    return list(map(
        lambda x : x["phrase"],
        data
))

class DataGenerator:
    def __init__(self, file: TextIO, name: str, data_size: int):
        self.file = file
        self.name = name
        self.data = bytearray(data_size)

    def get_next_token(self) -> bytearray:
        raise NotImplementedError()

    def generate(self) -> None:
        pos = 0
        while pos != len(self.data):
            token = self.get_next_token()
            if pos + len(token) > len(self.data):
                self.data[pos:] = token[0:(len(self.data) - pos)]
                break
            self.data[pos:(pos + len(token))] = token
            pos += len(token)
            if pos < len(self.data):
                self.data[pos:(pos + 1)] = bytearray(' ', encoding="utf8")
                pos += 1

        print(f'\t{{"name":"{self.name}","phrase":"{self.data.decode("utf8")}"}}', file=self.file, end='')

    def refresh(self, name: str) -> None:
        self.data: bytearray = bytearray(len(self.data))
        self.name = name


class RepeatDataGenerator(DataGenerator):
    def __init__(self, file: str, name: str, data_size: int, repeat_pattern: list[str]):
        super().__init__(file, name, data_size)
        self.pattern = list(map(lambda x: bytearray(x, encoding="utf8"), repeat_pattern))
        self.pos = 0

    def get_next_token(self):
        token = self.pattern[self.pos]
        self.pos = (self.pos + 1) % len(self.pattern)
        return token


class FreqsDataGenerator(DataGenerator):
    def __init__(self, file: TextIO, name: str, data_size: int, tokens_with_freqs: dict[str, float]):
        super().__init__(file, name, data_size)
        self.pattern : list[bytearray] = []
        self.freqs : list[float] = []
        for token, freq in tokens_with_freqs.items():
            self.pattern.append(bytearray(token, encoding="utf8"))
            self.freqs.append(freq)
        total = sum(self.freqs)
        if abs(total - 1.0) > 0.0001:
            raise ValueError(f"Сумма частот должна быть равна 1.0 (текущая: {total})")
        self.rng = random.Random()
        self.rng.seed(int(time.time() * 1_000_000))

    def get_next_token(self):
        return self.pattern[self.rng.choices(
            population=range(len(self.freqs)),
            weights=self.freqs,
            k=1
        )[0]]


def generate_random_string(length=10, seed=None):
    if seed is not None:
        random.seed(seed)

    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


@dataclass
class Term:
    term: str
    min_offset: int
    max_offset: int


@dataclass
class Terms:
    terms: list[Term]


def count(terms: Terms, inp: str):
    inp = list(inp.split())

    def rec_count(cur_pos: int, cur_term: int) -> int:
        if cur_term >= len(terms.terms):
            return 1
        ans = 0
        for i in range(terms.terms[cur_term].min_offset, terms.terms[cur_term].max_offset + 1):
            if cur_pos + i >= len(inp):
                break
            if inp[cur_pos + i] != terms.terms[cur_term].term:
                continue
            ans += rec_count(cur_pos + i, cur_term + 1)
        return ans

    res = 0
    for i in range(len(inp)):
        if inp[i] == terms.terms[0].term:
            res += rec_count(i, 1)
    return res



def generation_loop(generator: type[DataGenerator], file: str, golden_file: str, doc_count: int, *args, **kwargs):
    name = 1
    names = set()

    def gen_next_name(seed: int):
        cand = generate_random_string(10, seed)
        while cand in names:
            seed += 1
            cand = generate_random_string(10, seed)
        names.add(cand)
        return cand

    os.makedirs(os.path.dirname(file), exist_ok=True)

    with open(file, "w") as f:
        print("[", file=f)
        gen = generator(f, gen_next_name(name), *args, **kwargs)
        for iter in range(doc_count - 1):
            gen.generate()
            print(",", file=f)
            name += 1
            gen.refresh(gen_next_name(name))
        gen.generate()
        print("\n]", file=f)

    terms = Terms([
        Term("fox", 0, 0),
        Term("quick", 1, 3),
        Term("brown", 1, 3),
        Term("jumps", 1, 3)
    ])

    terms2 = Terms([
        Term("fox", 0, 0),
        Term("quick", 1, 10),
        Term("brown", 1, 10),
        Term("jumps", 1, 10)
    ])

    data = parse_file(file)
    res1 = list(map(lambda x: count(terms, x), data))
    res2 = list(map(lambda x: count(terms2, x), data))

    os.makedirs(os.path.dirname(golden_file), exist_ok=True)

    with open(golden_file, "w") as f:
        print("Results of test 'single_search1'", file=f)
        for i in range(len(res1)):
            if res1[i]:
                print(i + 1, file=f)
        print("Results of test 'single_search2'", file=f)
        for i in range(len(res2)):
            if res2[i]:
                print(i + 1, file=f)
        print("Results of test 'freqs_search1'", file=f)
        for i in range(len(res1)):
            if res1[i]:
                print(res1[i], file=f)
        print("Results of test 'freqs_search2'", file=f)
        for i in range(len(res2)):
            if res2[i]:
                print(res2[i], file=f)


def main():
    parser = argparse.ArgumentParser(
        description="Generate benchmark test data for interval search"
    )
    parser.add_argument(
        "-r", "--root",
        type=str,
        required=True,
        help="Path to the project root directory"
    )
    args = parser.parse_args()
    root_path = args.root

    data = ["fox", "quick", "brown", "jumps", "second", "dog"]
    freq_eq = [0.1, 0.1, 0.1, 0.1, 0.3, 0.3]
    custom_freqs = [0.2, 0.1, 0.05, 0.1, 0.25, 0.3]

    def combine(tokens: list[str], freqs: list[float]):
        return dict(zip(tokens, freqs))

    inputs = [
        [
            RepeatDataGenerator,
            Config.RepeatPhraseBenchResource,
            Config.GoldenRepeatPhraseBenchResource,
            Config.DocCount,
            Config.DataSize,
            data,
        ],
        [
            FreqsDataGenerator,
            Config.FreqsEqualBenchResource,
            Config.GoldenFreqsEqualBenchResource,
            Config.DocCount,
            Config.DataSize,
            combine(data, freq_eq),
        ],
        [
            FreqsDataGenerator,
            Config.FreqsDiscreteBenchResource,
            Config.GoldenFreqsDiscreteBenchResource,
            Config.DocCount,
            Config.DataSize,
            combine(data, custom_freqs),
        ],
        [
            RepeatDataGenerator,
            Config.BigDataRepeatPhraseBenchResource,
            Config.GoldenBigDataRepeatPhraseBenchResource,
            Config.BigDataDocCount,
            Config.BigDataSize,
            data,
        ],
        [
            FreqsDataGenerator,
            Config.BigDataFreqsEqualBenchResource,
            Config.GoldenBigDataFreqsEqualBenchResource,
            Config.BigDataDocCount,
            Config.BigDataSize,
            combine(data, freq_eq),
        ],
        [
            FreqsDataGenerator,
            Config.BigDataFreqsDiscreteBenchResource,
            Config.GoldenBigDataFreqsDiscreteBenchResource,
            Config.BigDataDocCount,
            Config.BigDataSize,
            combine(data, custom_freqs),
        ],
    ]

    for i in range(len(inputs)):
        inputs[i][1] = f"{root_path}/{inputs[i][1]}"
        inputs[i][2] = f"{root_path}/{inputs[i][2]}"

        generation_loop(*inputs[i])


if __name__ == "__main__":
    main()

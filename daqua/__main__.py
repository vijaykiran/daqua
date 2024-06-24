import sys
from typing import List


def main(args: List[str]) -> None:
    if args is None:
        args = sys.argv[1:]


if __name__ == "__main__":
    main([])

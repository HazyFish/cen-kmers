from heapq import heapify, heappop, heappush
from typing import Callable, Iterable, NamedTuple


class BalancedPartition[T](NamedTuple):
    total_weight: int | float
    items: list[T]


def partition_with_balance[T](
    items: Iterable[T],
    n_groups: int,
    weight_of: Callable[[T], int | float],
) -> list[BalancedPartition[T]]:
    """
    Partition items into n_groups with balanced weights.

    :param items: List of items to partition.
    :param n_groups: Number of groups to create.
    :param weight_of: Function to compute the weight of each item.
    :return: List of tuples containing the total weight and the items in each group.
    """

    if n_groups <= 0:
        raise ValueError("Number of groups must be greater than zero.")

    groups = [BalancedPartition(total_weight=0, items=[]) for _ in range(n_groups)]

    heapify(groups)

    for item in sorted(items, key=weight_of, reverse=True):
        group = heappop(groups)
        group.items.append(item)
        heappush(
            groups,
            BalancedPartition(
                total_weight=group.total_weight + weight_of(item),
                items=group.items,
            ),
        )

    return groups

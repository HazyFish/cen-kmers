from scripts.lib.balanced_partitioning import partition_with_balance


def test_partition_with_balance():
    items = [1, 2, 3, 4, 5]
    n_groups = 2
    weight_of = lambda x: x

    partitions = partition_with_balance(items, n_groups, weight_of)

    assert len(partitions) == n_groups
    assert sum(partition.total_weight for partition in partitions) == sum(items)
    assert all(
        abs(partition.total_weight - sum(items) / n_groups) <= 1
        for partition in partitions
    )

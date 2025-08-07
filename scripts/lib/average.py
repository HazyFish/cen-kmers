def average_of(numbers: list[float]) -> float:
    if not numbers:
        raise ValueError("List is empty")

    return sum(numbers) / len(numbers)
